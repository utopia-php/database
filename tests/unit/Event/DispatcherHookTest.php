<?php

namespace Tests\Unit\Event;

use Error;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Collection\Created as CollectionCreated;
use Utopia\Database\Event\Collection\Deleted as CollectionDeleted;
use Utopia\Database\Event\DispatcherHook;
use Utopia\Database\Event\Document\Created as DocumentCreated;
use Utopia\Database\Event\Document\Deleted as DocumentDeleted;
use Utopia\Database\Event\Document\Updated as DocumentUpdated;
use Utopia\Database\Event\Documents\Created as DocumentsCreated;
use Utopia\Database\Event\Documents\Deleted as DocumentsDeleted;
use Utopia\Database\Event\Documents\Updated as DocumentsUpdated;
use Utopia\Database\Query;

class DispatcherHookTest extends TestCase
{
    private const array DOMAIN_EVENTS = [
        CollectionCreated::class,
        CollectionDeleted::class,
        DocumentCreated::class,
        DocumentUpdated::class,
        DocumentDeleted::class,
        DocumentsCreated::class,
        DocumentsUpdated::class,
        DocumentsDeleted::class,
    ];

    private DispatcherHook $hook;

    protected function setUp(): void
    {
        $this->hook = new DispatcherHook();
    }

    public function testDocumentCreatedEvent(): void
    {
        $received = null;
        $this->hook->on(DocumentCreated::class, function (DocumentCreated $event) use (&$received) {
            $received = $event;
        });

        $doc = new Document([
            '$id' => 'doc-1',
            '$collection' => 'users',
        ]);

        $this->hook->handle(Event::DocumentCreate, $doc);

        $this->assertInstanceOf(DocumentCreated::class, $received);
        $this->assertEquals('users', $received->collection);
        $this->assertSame($doc, $received->document);
    }

    public function testDocumentUpdatedEvent(): void
    {
        $received = null;
        $this->hook->on(DocumentUpdated::class, function (DocumentUpdated $event) use (&$received) {
            $received = $event;
        });

        $doc = new Document([
            '$id' => 'doc-2',
            '$collection' => 'posts',
        ]);

        $this->hook->handle(Event::DocumentUpdate, $doc);

        $this->assertInstanceOf(DocumentUpdated::class, $received);
        $this->assertEquals('posts', $received->collection);
    }

    public function testDocumentDeletedEvent(): void
    {
        $received = null;
        $this->hook->on(DocumentDeleted::class, function (DocumentDeleted $event) use (&$received) {
            $received = $event;
        });

        $doc = new Document([
            '$id' => 'doc-3',
            '$collection' => 'users',
        ]);

        $this->hook->handle(Event::DocumentDelete, $doc);

        $this->assertInstanceOf(DocumentDeleted::class, $received);
        $this->assertEquals('doc-3', $received->documentId);
    }

    public function testDocumentDeletedEventFromAnObjectPayload(): void
    {
        $received = $this->record();

        $payload = new stdClass();
        $payload->collection = 'users';
        $payload->id = 'doc-6';
        $this->hook->handle(Event::DocumentDelete, $payload);

        $incomplete = new stdClass();
        $incomplete->collection = 'users';
        $this->hook->handle(Event::DocumentDelete, $incomplete);

        $this->assertCount(1, $received->events);
        $this->assertInstanceOf(DocumentDeleted::class, $received->events[0]);
        $this->assertSame('users', $received->events[0]->collection);
        $this->assertSame('doc-6', $received->events[0]->documentId);
    }

    public function testCollectionEventsFromTheirPayloads(): void
    {
        $received = $this->record();
        $collection = new Document(['$id' => 'posts']);

        $this->hook->handle(Event::CollectionCreate, $collection);
        $this->hook->handle(Event::CollectionDelete, $collection);
        $this->hook->handle(Event::CollectionDelete, 'comments');
        $this->hook->handle(Event::CollectionCreate, 'ignored');

        $this->assertSame(
            [
                [CollectionCreated::class, 'posts'],
                [CollectionDeleted::class, 'posts'],
                [CollectionDeleted::class, 'comments'],
            ],
            $received->summary(),
        );
        $this->assertInstanceOf(CollectionCreated::class, $received->events[0]);
        $this->assertSame($collection, $received->events[0]->document);
    }

    public function testUnhandledEventDoesNothing(): void
    {
        $called = false;
        $this->hook->on(DocumentCreated::class, function () use (&$called) {
            $called = true;
        });

        $this->hook->handle(Event::DatabaseCreate, 'test');

        $this->assertFalse($called);
    }

    public function testMultipleListeners(): void
    {
        $count = 0;
        $this->hook->on(DocumentCreated::class, function () use (&$count) {
            $count++;
        });
        $this->hook->on(DocumentCreated::class, function () use (&$count) {
            $count++;
        });

        $doc = new Document([
            '$id' => 'doc-4',
            '$collection' => 'test',
        ]);

        $this->hook->handle(Event::DocumentCreate, $doc);

        $this->assertEquals(2, $count);
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
            $this->hook->handle(Event::DocumentCreate, new Document(['$id' => 'doc-5', '$collection' => 'test']));
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

        $this->assertSame([[CollectionDeleted::class, HookFixture::COLLECTION]], $received->summary());
    }

    public function testBulkWritesDeliverNoSingleDocumentEventWithAnEmptyId(): void
    {
        $database = HookFixture::memory();
        $database->addHook($this->hook);
        $received = $this->record();

        $this->bulkWrites($database);

        foreach ($received->events as $event) {
            $this->assertNotInstanceOf(DocumentCreated::class, $event);
            $this->assertNotInstanceOf(DocumentUpdated::class, $event);
            $this->assertNotInstanceOf(DocumentDeleted::class, $event);
        }
    }

    public function testBulkWritesDeliverBulkEventsWithTheirCounts(): void
    {
        $database = HookFixture::memory();
        $database->addHook($this->hook);
        $received = $this->record();

        $this->bulkWrites($database);

        $this->assertSame(
            [
                [DocumentsCreated::class, HookFixture::COLLECTION],
                [DocumentsUpdated::class, HookFixture::COLLECTION],
                [DocumentsDeleted::class, HookFixture::COLLECTION],
            ],
            $received->summary(),
        );
        $this->assertInstanceOf(DocumentsCreated::class, $received->events[0]);
        $this->assertSame(3, $received->events[0]->count);
        $this->assertSame(Event::DocumentsCreate, $received->events[0]->event);
        $this->assertInstanceOf(DocumentsUpdated::class, $received->events[1]);
        $this->assertSame(2, $received->events[1]->count);
        $this->assertSame(Event::DocumentsUpdate, $received->events[1]->event);
        $this->assertInstanceOf(DocumentsDeleted::class, $received->events[2]);
        $this->assertSame(3, $received->events[2]->count);
        $this->assertSame(Event::DocumentsDelete, $received->events[2]->event);
    }

    public function testABulkPayloadWithoutACountDeliversNothing(): void
    {
        $received = $this->record();

        $this->hook->handle(Event::DocumentsCreate, new Document(['$collection' => 'posts']));
        $this->hook->handle(Event::DocumentsDelete, 'posts');

        $this->assertSame([], $received->events);
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

        $database->createCollection(new Collection(id: 'comments'));
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

        $database->createCollection(new Collection(id: 'comments'));

        $this->assertTrue($ran);
        $this->assertFalse($database->getCollection('comments')->isEmpty());
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

        $database->createCollection(new Collection(id: 'comments'));
        $this->assertInstanceOf(CollectionCreated::class, $dispatcher->events[0]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('dispatcher failure');

        HookFixture::seed($database, ['first']);
    }

    private function bulkWrites(Database $database): void
    {
        $database->createDocuments(HookFixture::COLLECTION, [
            new Document([Document::ID => 'first', 'title' => 'first', 'views' => 1]),
            new Document([Document::ID => 'second', 'title' => 'second', 'views' => 2]),
            new Document([Document::ID => 'third', 'title' => 'third', 'views' => 3]),
        ]);
        $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]), [
            Query::notEqual('title', 'third'),
        ]);
        $database->deleteDocuments(HookFixture::COLLECTION);
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
