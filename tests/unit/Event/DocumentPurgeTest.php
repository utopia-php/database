<?php

namespace Tests\Unit\Event;

use ArrayObject;
use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use TypeError;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Unconfirmed as UnconfirmedException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Mirror;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class DocumentPurgeTest extends TestCase
{
    /**
     * @return iterable<string, array{Closure(): Database}>
     */
    public static function databases(): iterable
    {
        yield 'memory' => [HookFixture::memory(...)];
        yield 'sqlite' => [HookFixture::sqlite(...)];
    }

    /**
     * @return iterable<string, array{Closure(Database): mixed}>
     */
    public static function purgingCalls(): iterable
    {
        yield 'updateDocument' => [static fn (Database $database): mixed => $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']))];
        yield 'updateDocuments' => [static fn (Database $database): mixed => $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]))];
        yield 'upsertDocuments' => [static fn (Database $database): mixed => $database->upsertDocuments(HookFixture::COLLECTION, [new Document([Document::ID => 'first', 'title' => 'upserted', 'views' => 5])])];
        yield 'increaseDocumentAttribute' => [static fn (Database $database): mixed => $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views')];
        yield 'decreaseDocumentAttribute' => [static fn (Database $database): mixed => $database->decreaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views')];
        yield 'deleteDocument' => [static fn (Database $database): mixed => $database->deleteDocument(HookFixture::COLLECTION, 'first')];
        yield 'deleteDocuments' => [static fn (Database $database): mixed => $database->deleteDocuments(HookFixture::COLLECTION)];
        yield 'purgeCachedDocument' => [static fn (Database $database): mixed => $database->purgeCachedDocument(HookFixture::COLLECTION, 'first')];
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testUpdateDocumentPurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);

        $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));

        $this->assertSame([Event::DocumentPurge, Event::DocumentUpdate], $recorder->getEvents());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testUpdateDocumentPurgesBothIdentifiersOfARenamedDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first']);

        $database->updateDocument(HookFixture::COLLECTION, 'first', new Document([Document::ID => 'renamed']));

        $this->assertSame([Event::DocumentPurge, Event::DocumentPurge, Event::DocumentUpdate], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/renamed'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testUpdateDocumentsPurgesEveryUpdatedDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second', 'third']);

        $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]), [Query::notEqual('title', 'third')]);

        $this->assertSame([Event::DocumentPurge, Event::DocumentPurge, Event::DocumentsUpdate], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/second'], $this->purged($recorder));
    }

    public function testUpsertDocumentsPurgesEveryUpsertedDocument(): void
    {
        [$database, $recorder] = $this->seeded(HookFixture::sqlite(), ['first', 'second']);

        $database->upsertDocuments(HookFixture::COLLECTION, [
            new Document([Document::ID => 'first', 'title' => 'upserted', 'views' => 5]),
            new Document([Document::ID => 'fourth', 'title' => 'fourth', 'views' => 4]),
        ]);

        $this->assertSame([Event::DocumentPurge, Event::DocumentPurge, Event::DocumentsUpsert], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/fourth'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testIncreaseDocumentAttributePurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);

        $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views', 2);

        $this->assertSame([Event::DocumentPurge, Event::DocumentIncrease], $recorder->getEvents());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testDecreaseDocumentAttributePurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);

        $database->decreaseDocumentAttribute(HookFixture::COLLECTION, 'second', 'views');

        $this->assertSame([Event::DocumentPurge, Event::DocumentDecrease], $recorder->getEvents());
        $this->assertSame(['posts/second'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testDeleteDocumentPurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);

        $database->deleteDocument(HookFixture::COLLECTION, 'first');

        $this->assertSame([Event::DocumentPurge, Event::DocumentDelete], $recorder->getEvents());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testDeleteDocumentsPurgesEveryDeletedDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second', 'third']);

        $database->deleteDocuments(HookFixture::COLLECTION, [Query::notEqual('title', 'second')]);

        $this->assertSame([Event::DocumentPurge, Event::DocumentPurge, Event::DocumentsDelete], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/third'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testBatchWritesPurgeEveryBatch(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second', 'third']);

        $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]), batchSize: 2);
        $database->deleteDocuments(HookFixture::COLLECTION, batchSize: 2);

        $this->assertSame([
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentsUpdate,
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentsDelete,
        ], $recorder->getEvents());
        $this->assertSame([
            'posts/first',
            'posts/second',
            'posts/third',
            'posts/first',
            'posts/second',
            'posts/third',
        ], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testWritesThatChangeNothingPurgeNothing(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first']);

        $database->updateDocument(HookFixture::COLLECTION, 'missing', new Document(['title' => 'renamed']));
        $database->deleteDocument(HookFixture::COLLECTION, 'missing');
        $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]), [Query::equal('title', ['missing'])]);
        $database->deleteDocuments(HookFixture::COLLECTION, [Query::equal('title', ['missing'])]);

        $this->assertSame([Event::DocumentsUpdate, Event::DocumentsDelete], $recorder->getEvents());
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testPurgeCachedDocumentPurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first']);

        $database->purgeCachedDocument(HookFixture::COLLECTION, 'first');

        $this->assertSame([Event::DocumentPurge], $recorder->getEvents());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    public function testDocumentPurgeFiresOnceWhenTheTransactionIsRetried(): void
    {
        $adapter = new class () extends Memory {
            public int $commitFailures = 0;

            public function commitTransaction(): bool
            {
                if ($this->commitFailures > 0) {
                    $this->commitFailures--;

                    throw new TransactionException('Failed to commit transaction: commit lost');
                }

                return parent::commitTransaction();
            }
        };
        [$database, $recorder] = $this->seeded(HookFixture::database($adapter), ['first', 'second']);

        $adapter->commitFailures = 1;
        $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));
        $adapter->commitFailures = 1;
        $database->deleteDocuments(HookFixture::COLLECTION);

        $this->assertSame([
            Event::DocumentPurge,
            Event::DocumentUpdate,
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentsDelete,
        ], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/first', 'posts/second'], $this->purged($recorder));
    }

    /**
     * @param  Closure(Database): mixed  $call
     */
    #[DataProvider('purgingCalls')]
    public function testDocumentPurgeHookFailureReachesTheCaller(Closure $call): void
    {
        foreach ([new RuntimeException('region broadcast failed'), new TypeError('broken hook')] as $failure) {
            $database = HookFixture::sqlite();
            HookFixture::seed($database, ['first']);
            $later = new RecordingLifecycle();
            $database
                ->addHook(new FailingLifecycle(Event::DocumentPurge, $failure))
                ->addHook($later);

            $this->assertSame($failure, $this->failureOf(static fn () => $call($database)));
            $this->assertNotContains(Event::DocumentPurge, $later->getEvents());
        }
    }

    public function testDocumentPurgeThroughMirrorReachesTheCaller(): void
    {
        $source = HookFixture::sqlite();
        HookFixture::seed($source, ['first']);
        $mirror = new Mirror($source);
        $failure = new RuntimeException('region broadcast failed');
        $mirror->addHook(new FailingLifecycle(Event::DocumentPurge, $failure));

        $this->assertSame($failure, $this->failureOf(static fn () => $mirror->purgeCachedDocument(HookFixture::COLLECTION, 'first')));
        $this->assertSame($failure, $this->failureOf(static fn () => $mirror->updateDocument(
            HookFixture::COLLECTION,
            'first',
            new Document(['title' => 'renamed']),
        )));
    }

    /**
     * @param  list<string>  $ids
     * @return array{Database, RecordingLifecycle}
     */
    private function seeded(Database $database, array $ids): array
    {
        HookFixture::seed($database, $ids);

        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        return [$database, $recorder];
    }

    /**
     * @return list<string>
     */
    private function purged(RecordingLifecycle $recorder): array
    {
        $purged = [];
        foreach ($recorder->received(Event::DocumentPurge) as $event) {
            $this->assertInstanceOf(Event\Document\Purged::class, $event);
            $purged[] = $event->collection.'/'.$event->id;
        }

        return $purged;
    }

    /**
     * @param  callable(): mixed  $call
     */
    private function failureOf(callable $call): ?Throwable
    {
        try {
            $call();
        } catch (Throwable $failure) {
            return $failure;
        }

        return null;
    }

    /**
     * @return iterable<string, array{Closure(Database): mixed}>
     */
    public static function writes(): iterable
    {
        foreach (self::purgingCalls() as $name => $call) {
            if ($name !== 'purgeCachedDocument') {
                yield $name => $call;
            }
        }
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('writes')]
    public function testPurgeEventsInsideACallerTransactionFireAfterTheCommit(Closure $write): void
    {
        [$database, $recorder] = $this->seeded(HookFixture::sqlite(), ['first']);
        $inTransaction = $this->observePurges($database, static fn (): bool => $database->getAdapter()->inTransaction());

        $database->withTransaction(static fn (): mixed => $write($database));

        $this->assertSame([false], $inTransaction->getArrayCopy());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('writes')]
    public function testPurgeEventsAreDroppedWhenTheCallerTransactionRollsBack(Closure $write): void
    {
        [$database, $recorder] = $this->seeded(HookFixture::sqlite(), ['first']);
        $abandoned = new RuntimeException('abandoned');

        $this->assertSame($abandoned, $this->failureOf(static fn (): mixed => $database->withTransaction(
            static function () use ($database, $write, $abandoned): never {
                $write($database);

                throw $abandoned;
            },
        )));

        $this->assertSame([], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testPurgeEventsOfARolledBackNestedTransactionAreDropped(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);
        $abandoned = new RuntimeException('abandoned');

        $database->withTransaction(function () use ($database, $abandoned): void {
            $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));

            $this->assertSame($abandoned, $this->failureOf(static fn (): mixed => $database->withTransaction(
                static function () use ($database, $abandoned): never {
                    $database->updateDocument(HookFixture::COLLECTION, 'second', new Document(['title' => 'renamed']));

                    throw $abandoned;
                },
            )));
        });

        $this->assertSame(['posts/first'], $this->purged($recorder));
        $this->assertSame('second', $database->getDocument(HookFixture::COLLECTION, 'second')->getAttribute('title'));
    }

    /**
     * Without savepoints a nested call runs inside the caller's transaction and nothing
     * rolls its writes back when it fails: once the caller catches the failure, those
     * writes commit with it, so their purge events must fire after that commit.
     */
    public function testPurgeEventsOfAFailedNestedCallWithoutSavepointsFireAfterTheOuterCommit(): void
    {
        $adapter = new class () extends Memory {
            #[\Override]
            public function capabilities(): array
            {
                return \array_values(\array_filter(
                    parent::capabilities(),
                    static fn (Capability $capability): bool => $capability !== Capability::TransactionNested,
                ));
            }

            #[\Override]
            public function withTransaction(callable $callback): mixed
            {
                if ($this->inTransaction()) {
                    return $callback();
                }

                return parent::withTransaction($callback);
            }
        };
        [$database, $recorder] = $this->seeded(HookFixture::database($adapter), ['first', 'second']);
        $abandoned = new RuntimeException('abandoned');

        $database->withTransaction(function () use ($database, $abandoned): void {
            $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));

            $this->assertSame($abandoned, $this->failureOf(static fn (): mixed => $database->withTransaction(
                static function () use ($database, $abandoned): never {
                    $database->updateDocument(HookFixture::COLLECTION, 'second', new Document(['title' => 'renamed']));

                    throw $abandoned;
                },
            )));
        });

        $this->assertSame('renamed', $database->getDocument(HookFixture::COLLECTION, 'second')->getAttribute('title'), 'Nothing rolls the nested write back');
        $this->assertSame(['posts/first', 'posts/second'], $this->purged($recorder));
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('writes')]
    public function testPurgeEventsOfARetriedCallerTransactionFireOnce(Closure $write): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public int $commitFailures = 0;

            #[\Override]
            public function commitTransaction(): bool
            {
                if ($this->inTransaction === 1 && $this->commitFailures > 0) {
                    $this->commitFailures--;

                    throw new TransactionException('Failed to commit transaction: commit lost');
                }

                return parent::commitTransaction();
            }
        };
        [$database, $recorder] = $this->seeded(HookFixture::database($adapter), ['first']);

        $adapter->commitFailures = 1;
        $database->withTransaction(static fn (): mixed => $write($database));

        $this->assertSame(0, $adapter->commitFailures);
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    public function testPurgeEventsOfWritesWithoutAnAdapterTransactionFireAtOnce(): void
    {
        $adapter = new class () extends Memory {
            #[\Override]
            public function withTransaction(callable $callback): mixed
            {
                return $callback();
            }
        };
        [$database, $recorder] = $this->seeded(HookFixture::database($adapter), ['first']);
        $abandoned = new RuntimeException('abandoned');

        $this->assertSame($abandoned, $this->failureOf(static fn (): mixed => $database->withTransaction(
            static function () use ($database, $abandoned): never {
                $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));

                throw $abandoned;
            },
        )));

        $this->assertSame('renamed', $database->getDocument(HookFixture::COLLECTION, 'first')->getAttribute('title'));
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('writes')]
    public function testPurgeEventFiresWhenThePostCommitInvalidationFails(Closure $write): void
    {
        $failure = new RuntimeException('cache unavailable');
        $cache = new class ($failure) extends MemoryCache {
            public bool $failing = false;

            public function __construct(private readonly RuntimeException $failure)
            {
            }

            #[\Override]
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if ($this->failing) {
                    throw $this->failure;
                }

                return parent::save($key, $data, $hash);
            }

            #[\Override]
            public function purge(string $key, string $hash = ''): bool
            {
                if ($this->failing) {
                    throw $this->failure;
                }

                return parent::purge($key, $hash);
            }
        };
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public ?Closure $afterCommit = null;

            #[\Override]
            public function commitTransaction(): bool
            {
                $committed = parent::commitTransaction();
                if (! $this->inTransaction()) {
                    $this->afterCommit?->__invoke();
                }

                return $committed;
            }
        };
        $database = HookFixture::database($adapter)->setCache(new Cache($cache));
        [$database, $recorder] = $this->seeded($database, ['first']);

        $adapter->afterCommit = static function () use ($cache): void {
            $cache->failing = true;
        };

        $this->assertSame($failure, $this->failureOf(static fn (): mixed => $write($database)));
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    public function testQueuedPurgeEventsKeepTheirTenant(): void
    {
        $database = (new Database(new Memory(), new Cache(new None())))
            ->setAuthorization(new Authorization())
            ->setDatabase('hooks')
            ->setNamespace('hooks_'.\uniqid())
            ->setSharedTables(true)
            ->setTenant(null)
            ->setTenantPerDocument(true);
        $database->create();
        $database->createCollection(Collection::create(
            id: HookFixture::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ));
        foreach ([7, 8] as $tenant) {
            $database->createDocument(HookFixture::COLLECTION, new Document([
                Document::ID => 'first',
                Document::TENANT => $tenant,
                'title' => 'first',
            ]));
        }
        $tenants = $this->observePurges($database, static fn (): int|string|null => $database->getTenant());

        $database->withTransaction(static function () use ($database): void {
            $database->withTenant(7, static fn (): Document => $database->updateDocument(
                HookFixture::COLLECTION,
                'first',
                new Document(['title' => 'renamed']),
            ));
            $database->withTenant(8, static fn (): int => $database->updateDocuments(
                HookFixture::COLLECTION,
                new Document(['title' => 'renamed']),
                [Query::equal('$id', ['first'])],
            ));
        });

        $this->assertSame([7, 8], $tenants->getArrayCopy());
        $this->assertNull($database->getTenant());
    }

    public function testQueuedPurgeEventsStaySilenced(): void
    {
        [$database, $recorder] = $this->seeded(HookFixture::sqlite(), ['first', 'second']);
        $named = new NamedRecordingLifecycle('audit');
        $database->addHook($named);

        $database->withTransaction(static function () use ($database): void {
            $database->silent(static fn (): Document => $database->updateDocument(
                HookFixture::COLLECTION,
                'first',
                new Document(['title' => 'renamed']),
            ));
            $database->silent(static fn (): Document => $database->updateDocument(
                HookFixture::COLLECTION,
                'second',
                new Document(['title' => 'renamed']),
            ), ['audit']);
        });

        $this->assertSame(['posts/second'], $this->purged($recorder));
        $this->assertSame([], $named->received(Event::DocumentPurge));
    }

    public function testEveryQueuedPurgeEventIsDeliveredWhenAListenerFails(): void
    {
        [$database, $recorder] = $this->seeded(HookFixture::sqlite(), ['first', 'second']);
        $failure = new RuntimeException('region broadcast failed');
        $database->addHook(new FailingLifecycle(Event::DocumentPurge, $failure));

        $this->assertSame($failure, $this->failureOf(static function () use ($database): void {
            $database->withTransaction(static function () use ($database): void {
                $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));
                $database->updateDocument(HookFixture::COLLECTION, 'second', new Document(['title' => 'renamed']));
            });
        }));

        $this->assertSame(['posts/first', 'posts/second'], $this->purged($recorder));
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function savepoints(): iterable
    {
        yield 'with savepoints' => [true];
        yield 'without savepoints' => [false];
    }

    /**
     * The writes on an adapter with and without savepoints. The adapter without them (Memory) has no upserts.
     *
     * @return iterable<string, array{Closure(Database): mixed, bool}>
     */
    public static function unconfirmedWrites(): iterable
    {
        foreach (self::writes() as $name => [$write]) {
            yield $name.' with savepoints' => [$write, true];

            if ($name !== 'upsertDocuments') {
                yield $name.' without savepoints' => [$write, false];
            }
        }
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('unconfirmedWrites')]
    public function testPurgeEventsOfAnUnconfirmedCommitFire(Closure $write, bool $savepoints): void
    {
        foreach ([
            'own transaction' => static fn (Database $database): mixed => $write($database),
            'caller transaction' => static fn (Database $database): mixed => $database->withTransaction(static fn (): mixed => $write($database)),
        ] as $scope => $call) {
            [$database, $recorder] = $this->unconfirmed($savepoints, ['first']);

            $thrown = $this->failureOf(static fn (): mixed => $call($database));

            $this->assertInstanceOf(UnconfirmedException::class, $thrown, $scope);
            $this->assertSame(['posts/first'], $this->purged($recorder), $scope.': the write may be stored, so its purge must be announced');
        }
    }

    #[DataProvider('savepoints')]
    public function testOnlyTheUnconfirmedAttemptOfARetriedTransactionAnnouncesItsPurgeEvents(bool $savepoints): void
    {
        [$database, $recorder] = $this->unconfirmed($savepoints, ['first', 'second'], commitFailures: 1);
        $attempts = 0;
        $attempt = static function () use ($database, &$attempts): Document {
            $attempts++;

            return $database->updateDocument(HookFixture::COLLECTION, $attempts === 1 ? 'first' : 'second', new Document(['title' => 'renamed']));
        };

        $thrown = $this->failureOf(static fn (): mixed => $database->withTransaction($attempt));

        $this->assertSame(2, $attempts);
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame(['posts/second'], $this->purged($recorder));
    }

    #[DataProvider('savepoints')]
    public function testPurgeEventsAreDroppedWhenATransactionThatWouldBeUnconfirmedRollsBack(bool $savepoints): void
    {
        [$database, $recorder] = $this->unconfirmed($savepoints, ['first']);
        $abandoned = new RuntimeException('abandoned');

        $this->assertSame($abandoned, $this->failureOf(static fn (): mixed => $database->withTransaction(
            static function () use ($database, $abandoned): never {
                $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));

                throw $abandoned;
            },
        )));

        $this->assertSame([], $this->purged($recorder));
        $this->assertSame('first', $database->getDocument(HookFixture::COLLECTION, 'first')->getAttribute('title'));
    }

    #[DataProvider('savepoints')]
    public function testAnUnconfirmedCommitStaysTheFailureWhenAPurgeListenerFails(bool $savepoints): void
    {
        [$database, $recorder] = $this->unconfirmed($savepoints, ['first', 'second']);
        $database->addHook(new FailingLifecycle(Event::DocumentPurge, new RuntimeException('region broadcast failed')));

        $thrown = $this->failureOf(static function () use ($database): void {
            $database->withTransaction(static function () use ($database): void {
                $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));
                $database->updateDocument(HookFixture::COLLECTION, 'second', new Document(['title' => 'renamed']));
            });
        });

        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame(['posts/first', 'posts/second'], $this->purged($recorder));
    }

    /**
     * An Exception\Unconfirmed the callback throws, as one from another database's commit does, says nothing about
     * this transaction: it rolls back, so its purge events are dropped.
     */
    #[DataProvider('savepoints')]
    public function testPurgeEventsAreDroppedWhenTheCallbackThrowsAnotherCommitsUnconfirmed(bool $savepoints): void
    {
        [$database, $recorder] = $this->unconfirmed($savepoints, ['first'], confirmed: true);
        $foreign = new UnconfirmedException('Failed to commit transaction: the commit could not be confirmed');

        $this->assertSame($foreign, $this->failureOf(static fn (): mixed => $database->withTransaction(
            static function () use ($database, $foreign): never {
                $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));

                throw $foreign;
            },
        )));

        $this->assertSame([], $this->purged($recorder));
        $this->assertSame('first', $database->getDocument(HookFixture::COLLECTION, 'first')->getAttribute('title'));
    }

    #[DataProvider('savepoints')]
    public function testPurgeEventsAreDroppedWhenARetriedCallbackThrowsAnotherCommitsUnconfirmed(bool $savepoints): void
    {
        [$database, $recorder] = $this->unconfirmed($savepoints, ['first', 'second'], commitFailures: 1, confirmed: true);
        $foreign = new UnconfirmedException('Failed to commit transaction: the commit could not be confirmed');
        $attempts = 0;
        $attempt = static function () use ($database, $foreign, &$attempts): Document {
            $attempts++;
            $updated = $database->updateDocument(HookFixture::COLLECTION, $attempts === 1 ? 'first' : 'second', new Document(['title' => 'renamed']));

            if ($attempts > 1) {
                throw $foreign;
            }

            return $updated;
        };

        $this->assertSame($foreign, $this->failureOf(static fn (): mixed => $database->withTransaction($attempt)));

        $this->assertSame(2, $attempts);
        $this->assertSame([], $this->purged($recorder));
    }

    #[DataProvider('savepoints')]
    public function testAnUnconfirmedCommitRethrownByALaterTransactionDropsThatTransactionsPurgeEvents(bool $savepoints): void
    {
        [$database, $recorder] = $this->unconfirmed($savepoints, ['first', 'second']);
        $unconfirmed = $this->failureOf(static fn (): mixed => $database->updateDocument(
            HookFixture::COLLECTION,
            'first',
            new Document(['title' => 'renamed']),
        ));
        $this->assertInstanceOf(UnconfirmedException::class, $unconfirmed);

        $this->assertSame($unconfirmed, $this->failureOf(static fn (): mixed => $database->withTransaction(
            static function () use ($database, $unconfirmed): never {
                $database->updateDocument(HookFixture::COLLECTION, 'second', new Document(['title' => 'renamed']));

                throw $unconfirmed;
            },
        )));

        $this->assertSame(['posts/first'], $this->purged($recorder));
        $this->assertSame('second', $database->getDocument(HookFixture::COLLECTION, 'second')->getAttribute('title'));
    }

    public function testPurgeEventsOfASavepointRolledBackOnAnotherCommitsUnconfirmedAreDropped(): void
    {
        [$database, $recorder] = $this->seeded(HookFixture::sqlite(), ['first', 'second']);
        $foreign = new UnconfirmedException('Failed to commit transaction: the commit could not be confirmed');

        $database->withTransaction(function () use ($database, $foreign): void {
            $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));

            $this->assertSame($foreign, $this->failureOf(static fn (): mixed => $database->withTransaction(
                static function () use ($database, $foreign): never {
                    $database->updateDocument(HookFixture::COLLECTION, 'second', new Document(['title' => 'renamed']));

                    throw $foreign;
                },
            )));
        });

        $this->assertSame('second', $database->getDocument(HookFixture::COLLECTION, 'second')->getAttribute('title'));
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * A seeded database whose outermost transactions from now on commit and then throw Exception\Unconfirmed, as a
     * MongoDB commit whose result could not be confirmed does, unless $confirmed. The next $commitFailures outermost
     * commits fail with Exception\Transaction instead, so the transaction runs again. Without $savepoints its adapter
     * runs a nested call in the open transaction, as the MongoDB adapter does.
     *
     * @param  list<string>  $ids
     * @return array{Database, RecordingLifecycle}
     */
    private function unconfirmed(bool $savepoints, array $ids, int $commitFailures = 0, bool $confirmed = false): array
    {
        if ($savepoints) {
            $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
                public bool $unconfirmed = false;

                public int $commitFailures = 0;

                #[\Override]
                public function withTransaction(callable $callback): mixed
                {
                    if ($this->inTransaction()) {
                        return parent::withTransaction($callback);
                    }

                    $result = parent::withTransaction($callback);

                    if ($this->unconfirmed) {
                        throw new UnconfirmedException('Failed to commit transaction: the commit could not be confirmed');
                    }

                    return $result;
                }

                #[\Override]
                public function commitTransaction(): bool
                {
                    if ($this->inTransaction === 1 && $this->commitFailures > 0) {
                        $this->commitFailures--;

                        throw new TransactionException('Failed to commit transaction: commit lost');
                    }

                    return parent::commitTransaction();
                }
            };
        } else {
            $adapter = new class () extends Memory {
                public bool $unconfirmed = false;

                public int $commitFailures = 0;

                #[\Override]
                public function capabilities(): array
                {
                    return \array_values(\array_filter(
                        parent::capabilities(),
                        static fn (Capability $capability): bool => $capability !== Capability::TransactionNested,
                    ));
                }

                #[\Override]
                public function withTransaction(callable $callback): mixed
                {
                    if ($this->inTransaction()) {
                        return $callback();
                    }

                    $result = parent::withTransaction($callback);

                    if ($this->unconfirmed) {
                        throw new UnconfirmedException('Failed to commit transaction: the commit could not be confirmed');
                    }

                    return $result;
                }

                #[\Override]
                public function commitTransaction(): bool
                {
                    if ($this->inTransaction === 1 && $this->commitFailures > 0) {
                        $this->commitFailures--;

                        throw new TransactionException('Failed to commit transaction: commit lost');
                    }

                    return parent::commitTransaction();
                }
            };
        }

        $seeded = $this->seeded(HookFixture::database($adapter), $ids);

        $adapter->unconfirmed = ! $confirmed;
        $adapter->commitFailures = $commitFailures;

        return $seeded;
    }

    /**
     * @param  Closure(): mixed  $observe
     * @return ArrayObject<int, mixed>
     */
    private function observePurges(Database $database, Closure $observe): ArrayObject
    {
        /** @var ArrayObject<int, mixed> $observed */
        $observed = new ArrayObject();
        $database->addHook(new class ($observe, $observed) implements Lifecycle {
            /**
             * @param  Closure(): mixed  $observe
             * @param  ArrayObject<int, mixed>  $observed
             */
            public function __construct(
                private readonly Closure $observe,
                private readonly ArrayObject $observed,
            ) {
            }

            public function handle(Domain $event): void
            {
                if ($event->event === Event::DocumentPurge) {
                    $this->observed->append(($this->observe)());
                }
            }
        });

        return $observed;
    }
}
