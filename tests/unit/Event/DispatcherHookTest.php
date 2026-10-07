<?php

namespace Tests\Unit\Event;

use Error;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Collection\Created as CollectionCreated;
use Utopia\Database\Event\Collection\Deleted as CollectionDeleted;
use Utopia\Database\Event\DispatcherHook;
use Utopia\Database\Event\Document\BatchCreated;
use Utopia\Database\Event\Document\BatchDeleted;
use Utopia\Database\Event\Document\BatchUpdated;
use Utopia\Database\Event\Document\Created as DocumentCreated;
use Utopia\Database\Event\Document\Deleted as DocumentDeleted;
use Utopia\Database\Event\Document\Updated as DocumentUpdated;
use Utopia\Database\Query;

class DispatcherHookTest extends TestCase
{
    private const array DOMAIN_EVENTS = [
        CollectionCreated::class,
        CollectionDeleted::class,
        DocumentCreated::class,
        DocumentUpdated::class,
        DocumentDeleted::class,
        BatchCreated::class,
        BatchUpdated::class,
        BatchDeleted::class,
    ];

    private DispatcherHook $hook;

    protected function setUp(): void
    {
        $this->hook = new DispatcherHook();
    }

    public function testTheHookHandlesOnlyEventsItHasAListenerOrADispatcherFor(): void
    {
        $this->assertFalse($this->hook->handles(Event::DocumentUpdate));

        $this->hook->on(DocumentUpdated::class, static function (): void {
        });

        $this->assertTrue($this->hook->handles(Event::DocumentUpdate));
        $this->assertFalse($this->hook->handles(Event::DocumentDelete));
        $this->assertFalse($this->hook->handles(Event::All), 'All selects timeouts and is never an event');

        $dispatching = new DispatcherHook(new class () {
            public function dispatch(object $event): object
            {
                return $event;
            }
        });
        $this->assertTrue($dispatching->handles(Event::DocumentDelete));
        $this->assertTrue($dispatching->handles(Event::DocumentRead));
        $this->assertFalse($dispatching->handles(Event::All));
    }

    public function testTheListenersOfTheEventClassReceiveTheEvent(): void
    {
        $received = [];
        $this->hook->on(DocumentCreated::class, static function (DocumentCreated $event) use (&$received): void {
            $received[] = $event;
        });
        $this->hook->on(DocumentUpdated::class, static function () {
            throw new RuntimeException('Not the event class');
        });

        $event = new DocumentCreated('users', new Document(['$id' => 'doc-1']));
        $this->hook->handle($event);

        $this->assertSame([$event], $received);
    }

    public function testHandleRunsEveryListenerThenRethrowsTheFirstException(): void
    {
        $first = new RuntimeException('first');
        $calls = [];

        $this->hook->on(DocumentCreated::class, function () use ($first, &$calls): void {
            $calls[] = 'first';

            throw $first;
        });
        $this->hook->on(DocumentCreated::class, function () use (&$calls): void {
            $calls[] = 'second';

            throw new RuntimeException('second');
        });
        $this->hook->on(DocumentCreated::class, function () use (&$calls): void {
            $calls[] = 'third';
        });

        $caught = null;
        try {
            $this->hook->handle(new DocumentCreated('test', new Document(['$id' => 'doc-5'])));
        } catch (RuntimeException $exception) {
            $caught = $exception;
        }

        $this->assertSame($first, $caught, 'The first listener exception must reach the database');
        $this->assertSame(['first', 'second', 'third'], $calls);
    }

    public function testDeleteCollectionDeliversCollectionDeletedOnce(): void
    {
        $database = HookFixture::memory();
        $database->addHook($this->hook);
        $received = $this->record();

        $database->deleteCollection(HookFixture::COLLECTION);

        $this->assertSame([CollectionDeleted::class], $received->classes());
        $this->assertInstanceOf(CollectionDeleted::class, $received->events[0]);
        $this->assertSame(HookFixture::COLLECTION, $received->events[0]->collection);
        $this->assertSame(HookFixture::COLLECTION, $received->events[0]->definition->getId());
    }

    public function testBulkWritesDeliverBulkEventsWithTheirCounts(): void
    {
        $database = HookFixture::memory();
        $database->addHook($this->hook);
        $received = $this->record();

        $database->createDocuments(HookFixture::COLLECTION, [
            new Document([Document::ID => 'first', 'title' => 'first', 'views' => 1]),
            new Document([Document::ID => 'second', 'title' => 'second', 'views' => 2]),
            new Document([Document::ID => 'third', 'title' => 'third', 'views' => 3]),
        ]);
        $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]), [
            Query::notEqual('title', 'third'),
        ]);
        $database->deleteDocuments(HookFixture::COLLECTION);

        $this->assertSame([BatchCreated::class, BatchUpdated::class, BatchDeleted::class], $received->classes());
        [$created, $updated, $deleted] = $received->events;
        $this->assertInstanceOf(BatchCreated::class, $created);
        $this->assertInstanceOf(BatchUpdated::class, $updated);
        $this->assertInstanceOf(BatchDeleted::class, $deleted);
        $this->assertSame([HookFixture::COLLECTION, 3], [$created->collection, $created->count]);
        $this->assertSame([HookFixture::COLLECTION, 2], [$updated->collection, $updated->count]);
        $this->assertSame([HookFixture::COLLECTION, 3], [$deleted->collection, $deleted->count]);
    }

    public function testAListenerErrorReachesTheCaller(): void
    {
        $database = HookFixture::memory();
        $database->addHook($this->hook);
        $this->hook->on(CollectionCreated::class, static function (): void {
            throw new Error('listener bug');
        });

        $this->expectException(Error::class);
        $this->expectExceptionMessage('listener bug');

        $database->createCollection(Collection::create(id: 'comments'));
    }

    public function testAListenerExceptionAtAnIsolatedEventIsSwallowedAndTheOtherListenersRun(): void
    {
        $database = HookFixture::memory();
        $database->addHook($this->hook);
        $ran = false;
        $this->hook->on(CollectionCreated::class, static function (): void {
            throw new RuntimeException('listener failure');
        });
        $this->hook->on(CollectionCreated::class, static function () use (&$ran): void {
            $ran = true;
        });

        $database->createCollection(Collection::create(id: 'comments'));

        $this->assertTrue($ran);
        $this->assertNotNull($database->findCollection('comments'));
    }

    public function testAListenerExceptionAtADocumentEventReachesTheCaller(): void
    {
        $database = HookFixture::memory();
        $database->addHook($this->hook);
        $ran = false;
        $this->hook->on(DocumentCreated::class, static function (): void {
            throw new RuntimeException('listener failure');
        });
        $this->hook->on(DocumentCreated::class, static function () use (&$ran): void {
            $ran = true;
        });

        $caught = null;
        try {
            HookFixture::seed($database, ['first']);
        } catch (RuntimeException $exception) {
            $caught = $exception;
        }

        $this->assertSame('listener failure', $caught?->getMessage(), 'A listener exception at document_create must reach the caller');
        $this->assertTrue($ran);
    }

    public function testADispatcherErrorReachesTheCaller(): void
    {
        $database = HookFixture::memory();
        $database->addHook(new DispatcherHook(new class () {
            public function dispatch(object $event): object
            {
                throw new Error('dispatcher bug');
            }
        }));

        $this->expectException(Error::class);
        $this->expectExceptionMessage('dispatcher bug');

        $database->deleteCollection(HookFixture::COLLECTION);
    }

    public function testADispatcherExceptionFollowsTheHookFailurePolicy(): void
    {
        $database = HookFixture::memory();
        $dispatcher = new class () {
            /** @var list<object> */
            public array $events = [];

            public function dispatch(object $event): object
            {
                $this->events[] = $event;

                throw new RuntimeException('dispatcher failure');
            }
        };
        $database->addHook(new DispatcherHook($dispatcher));

        $database->createCollection(Collection::create(id: 'comments'));
        $this->assertInstanceOf(CollectionCreated::class, $dispatcher->events[0]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('dispatcher failure');

        HookFixture::seed($database, ['first']);
    }

    private function record(): DispatcherHookRecording
    {
        $recording = new DispatcherHookRecording();
        foreach (self::DOMAIN_EVENTS as $class) {
            $this->hook->on($class, $recording->add(...));
        }

        return $recording;
    }
}
