<?php

namespace Tests\Unit\Hook;

use Closure;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Tests\Unit\Event\FailingLifecycle;
use Tests\Unit\Event\HookFixture;
use Tests\Unit\Event\NamedRecordingLifecycle;
use Tests\Unit\Event\RecordingLifecycle;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Decorator;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Named;
use Utopia\Database\Mirror;

use function Swoole\Coroutine\run;

final class NamedTest extends TestCase
{
    public function testAddHookReplacesTheHookRegisteredUnderTheSameName(): void
    {
        $database = HookFixture::memory();
        $replaced = new NamedRecordingLifecycle('audits');
        $replacement = new NamedRecordingLifecycle('audits');

        $database->addHook($replaced)->addHook($replacement);
        $database->getCollection(HookFixture::COLLECTION);

        $this->assertSame([], $replaced->getEvents());
        $this->assertSame([Event::CollectionRead], $replacement->getEvents());
    }

    public function testReplacementKeepsTheRegistrationPosition(): void
    {
        $database = HookFixture::memory();
        $journal = [];
        $record = static function (string $label) use (&$journal): void {
            $journal[] = $label;
        };

        $database
            ->addHook($this->journalingHook('audits', 'replaced', $record))
            ->addHook($this->journalingHook(null, 'unnamed', $record))
            ->addHook($this->journalingHook('audits', 'replacement', $record));
        $database->getCollection(HookFixture::COLLECTION);

        $this->assertSame(['replacement', 'unnamed'], $journal);
    }

    public function testUnnamedHooksKeepAppending(): void
    {
        $database = HookFixture::memory();
        $first = new RecordingLifecycle();
        $second = new RecordingLifecycle();

        $database->addHook($first)->addHook($second)->addHook($first);
        $database->getCollection(HookFixture::COLLECTION);

        $this->assertSame([Event::CollectionRead, Event::CollectionRead], $first->getEvents());
        $this->assertSame([Event::CollectionRead], $second->getEvents());
    }

    public function testSilentWithoutListenersSilencesEveryHook(): void
    {
        $database = HookFixture::memory();
        $named = new NamedRecordingLifecycle('audits');
        $unnamed = new RecordingLifecycle();
        $database->addHook($named)->addHook($unnamed);

        $database->silent(fn () => $database->getCollection(HookFixture::COLLECTION));

        $this->assertSame([], $named->getEvents());
        $this->assertSame([], $unnamed->getEvents());
    }

    public function testSilentWithListenersSilencesOnlyTheNamedHooks(): void
    {
        $database = HookFixture::memory();
        $audits = new NamedRecordingLifecycle('audits');
        $usage = new NamedRecordingLifecycle('usage');
        $unnamed = new RecordingLifecycle();
        $database->addHook($audits)->addHook($usage)->addHook($unnamed);

        $database->silent(fn () => $database->getCollection(HookFixture::COLLECTION), ['audits']);

        $this->assertSame([], $audits->getEvents());
        $this->assertSame([Event::CollectionRead], $usage->getEvents());
        $this->assertSame([Event::CollectionRead], $unnamed->getEvents());
    }

    public function testSilentWithListenersKeepsUnnamedHooksFiring(): void
    {
        $database = HookFixture::memory();
        $unnamed = new RecordingLifecycle();
        $database->addHook($unnamed);

        $database->silent(fn () => $database->getCollection(HookFixture::COLLECTION), ['audits']);

        $this->assertSame([Event::CollectionRead], $unnamed->getEvents());
    }

    public function testNestedSilenceNeverNarrowsAnOuterSilence(): void
    {
        $database = HookFixture::memory();
        $audits = new NamedRecordingLifecycle('audits');
        $usage = new NamedRecordingLifecycle('usage');
        $unnamed = new RecordingLifecycle();
        $database->addHook($audits)->addHook($usage)->addHook($unnamed);

        $database->silent(fn () => $database->silent(fn () => $database->getCollection(HookFixture::COLLECTION), ['audits']));
        $this->assertSame([], $unnamed->getEvents());

        $database->silent(function () use ($database): void {
            $database->silent(fn () => $database->listCollections(), ['usage']);
            $database->getCollection(HookFixture::COLLECTION);
        }, ['audits']);

        $this->assertSame([], $audits->getEvents());
        $this->assertSame([Event::CollectionRead], $usage->getEvents());
        $this->assertSame([Event::CollectionList, Event::CollectionRead], $unnamed->getEvents());
    }

    public function testSilenceEndsWhenTheCallbackThrows(): void
    {
        $database = HookFixture::memory();
        $audits = new NamedRecordingLifecycle('audits');
        $unnamed = new RecordingLifecycle();
        $database->addHook($audits)->addHook($unnamed);

        foreach ([null, ['audits']] as $listeners) {
            try {
                $database->silent(static fn () => throw new RuntimeException('callback failed'), $listeners);
            } catch (RuntimeException) {
            }
        }
        $database->getCollection(HookFixture::COLLECTION);

        $this->assertSame([Event::CollectionRead], $audits->getEvents());
        $this->assertSame([Event::CollectionRead], $unnamed->getEvents());
    }

    public function testNamedSilenceAlsoSilencesDocumentPurge(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first']);
        $database->addHook($this->failingNamedHook('regions', Event::DocumentPurge, new RuntimeException('region broadcast failed')));

        $database->silent(fn () => $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed'])), ['regions']);
        $database->silent(fn () => $database->purgeCachedDocument(HookFixture::COLLECTION, 'first'), ['regions']);

        $this->assertSame('renamed', $database->getDocument(HookFixture::COLLECTION, 'first')->getAttribute('title'));
    }

    public function testDecoratorsKeepRunningDuringANamedSilence(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first']);
        $database->addHook(new class () implements Decorator {
            public function decorate(Event $event, Document $collection, Document $document): Document
            {
                return $document->setAttribute('decorated', true);
            }
        });

        $named = $database->silent(fn () => $database->getDocument(HookFixture::COLLECTION, 'first'), ['audits']);
        $silenced = $database->silent(fn () => $database->getDocument(HookFixture::COLLECTION, 'first'));

        $this->assertTrue($named->getAttribute('decorated'));
        $this->assertNull($silenced->getAttribute('decorated'));
    }

    public function testNamedSilenceIsScopedToTheCoroutine(): void
    {
        $database = HookFixture::memory();
        $audits = new NamedRecordingLifecycle('audits');
        $database->addHook($audits);

        $hookFlags = Runtime::getHookFlags();

        try {
            run(static function () use ($database): void {
                $entered = new Channel(1);
                $released = new Channel(1);

                Coroutine::create(static function () use ($database, $entered, $released): void {
                    $database->silent(static function () use ($database, $entered, $released): void {
                        $database->getCollection(HookFixture::COLLECTION);
                        $entered->push(true);
                        $released->pop();
                    }, ['audits']);
                });

                Coroutine::create(static function () use ($database, $entered, $released): void {
                    $entered->pop();
                    $database->listCollections();
                    $released->push(true);
                });
            });
        } finally {
            Runtime::setHookFlags($hookFlags);
        }

        $this->assertSame([Event::CollectionList], $audits->getEvents());
    }

    public function testMirrorReplacesNamedHooksOnItsSource(): void
    {
        $source = HookFixture::memory();
        $mirror = new Mirror($source);
        $replaced = new NamedRecordingLifecycle('audits');
        $replacement = new NamedRecordingLifecycle('audits');

        $mirror->addHook($replaced)->addHook($replacement);
        $mirror->getCollection(HookFixture::COLLECTION);

        $this->assertSame([], $replaced->getEvents());
        $this->assertSame([Event::CollectionRead], $replacement->getEvents());
    }

    public function testMirrorForwardsNamedSilenceToItsSource(): void
    {
        $source = HookFixture::memory();
        $mirror = new Mirror($source);
        $audits = new NamedRecordingLifecycle('audits');
        $unnamed = new RecordingLifecycle();
        $mirror->addHook($audits)->addHook($unnamed);

        $mirror->silent(fn () => $mirror->getCollection(HookFixture::COLLECTION), ['audits']);

        $this->assertSame([], $audits->getEvents());
        $this->assertSame([Event::CollectionRead], $unnamed->getEvents());
    }

    public function testMirrorSilenceAlsoSilencesItsOwnDecorators(): void
    {
        $source = HookFixture::memory();
        HookFixture::seed($source, ['first']);
        $mirror = new Mirror($source);
        $mirror->addHook(new class () implements Decorator {
            public function decorate(Event $event, Document $collection, Document $document): Document
            {
                return $document->setAttribute('decorated', true);
            }
        });

        $silenced = $mirror->silent(fn () => $mirror->getDocument(HookFixture::COLLECTION, 'first'));

        $this->assertNull($silenced->getAttribute('decorated'));
        $this->assertTrue($mirror->getDocument(HookFixture::COLLECTION, 'first')->getAttribute('decorated'));
    }

    /**
     * @param  Closure(string): void  $record
     */
    private function journalingHook(?string $name, string $label, Closure $record): Lifecycle
    {
        $journaling = new class ($label, $record) implements Lifecycle {
            /**
             * @param  Closure(string): void  $record
             */
            public function __construct(
                private readonly string $label,
                private readonly Closure $record,
            ) {
            }

            public function handle(Event $event, mixed $data): void
            {
                ($this->record)($this->label);
            }
        };

        if ($name === null) {
            return $journaling;
        }

        return new class ($name, $journaling) implements Lifecycle, Named {
            public function __construct(
                private readonly string $name,
                private readonly Lifecycle $hook,
            ) {
            }

            public function getName(): string
            {
                return $this->name;
            }

            public function handle(Event $event, mixed $data): void
            {
                $this->hook->handle($event, $data);
            }
        };
    }

    private function failingNamedHook(string $name, Event $event, RuntimeException $failure): Lifecycle&Named
    {
        return new class ($name, new FailingLifecycle($event, $failure)) implements Lifecycle, Named {
            public function __construct(
                private readonly string $name,
                private readonly FailingLifecycle $failing,
            ) {
            }

            public function getName(): string
            {
                return $this->name;
            }

            public function handle(Event $event, mixed $data): void
            {
                $this->failing->handle($event, $data);
            }
        };
    }
}
