<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Event\FailingLifecycle;
use Tests\Unit\Support\CountingMemory;
use Tests\Unit\Support\InterleavingMemory;
use TypeError;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Pools\Pool as UtopiaPool;

final class TransactionDefinitionReadTest extends TestCase
{
    private const string COLLECTION = 'accounts';

    public function testATransactionReadsAnUnchangedDefinitionOnce(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        $reads = $database->withTransaction(function () use ($database, $adapter): int {
            $adapter->reset();
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));
            $database->getDocument(self::COLLECTION, 'ada');
            $database->getCollection(self::COLLECTION);

            return $adapter->metadataReads;
        });

        $this->assertSame(1, $reads);
        $this->assertSame(2, $database->getDocument(self::COLLECTION, 'ada')->getAttribute('balance'));
    }

    public function testADefinitionATransactionReadIsCachedForTheNextTransaction(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $database->withTransaction(fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2])));

        $reads = $database->withTransaction(function () use ($database, $adapter): int {
            $adapter->reset();
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 3]));
            $database->getDocument(self::COLLECTION, 'ada');

            return $adapter->metadataReads;
        });

        $this->assertSame(0, $reads);
        $this->assertSame(3, $database->getDocument(self::COLLECTION, 'ada')->getAttribute('balance'));
    }

    public function testADefinitionChangedAfterATransactionReadItIsReadAfresh(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $database->withTransaction(fn (): Collection => $database->getCollection(self::COLLECTION));
        $this->assertSame(0, $this->transactionReads($database, $adapter, fn (): Collection => $database->getCollection(self::COLLECTION)), 'the transaction did not cache its definition');

        $database->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::any()), Permission::update(Role::any())], documentSecurity: true));

        $collection = $database->withTransaction(fn (): Collection => $database->getCollection(self::COLLECTION));

        $this->assertTrue($collection->getAttribute('documentSecurity'));
    }

    public function testATransactionWithoutAUsableCacheReadsItsDefinitionOnce(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new Cache(new NoCache()));

        $this->assertSame(1, $this->transactionReads($database, $adapter, fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]))));
        $this->assertSame(1, $this->transactionReads($database, $adapter, fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 3]))));

        $adapter->reset();
        $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 4]));
        $database->getCollection(self::COLLECTION);

        $this->assertSame(2, $adapter->metadataReads, 'Outside a transaction every call reads the definition again');
        $this->assertSame(4, $database->getDocument(self::COLLECTION, 'ada')->getAttribute('balance'));
    }

    public function testACacheThatFailsDefinitionReadsAddsNoDefinitionRead(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new Cache(new class () extends MemoryCache {
            #[\Override]
            public function load(string $key, int $ttl, string $hash = ''): mixed
            {
                if (\str_contains($key, ':'.Database::METADATA.':')) {
                    throw new RuntimeException('cache unreachable');
                }

                return parent::load($key, $ttl, $hash);
            }

            /**
             * @param  array<int|string, mixed>|string  $data
             * @return bool|string|array<int|string, mixed>
             */
            #[\Override]
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if (\str_contains($key, ':'.Database::METADATA.':')) {
                    throw new RuntimeException('cache unreachable');
                }

                return parent::save($key, $data, $hash);
            }
        }));

        $reads = $this->transactionReads($database, $adapter, fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2])));

        $this->assertSame(1, $reads);
        $this->assertSame(2, $database->getDocument(self::COLLECTION, 'ada')->getAttribute('balance'));
    }

    public function testACacheThatRefusedADefinitionIsFilledByTransactionsOnceItAcceptsAgain(): void
    {
        $adapter = new CountingMemory();
        $cache = new class () extends MemoryCache {
            public bool $refusing = false;

            /**
             * @param  array<int|string, mixed>|string  $data
             * @return bool|string|array<int|string, mixed>
             */
            #[\Override]
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if ($this->refusing && \str_contains($key, ':'.Database::METADATA.':')) {
                    return false;
                }

                return parent::save($key, $data, $hash);
            }
        };
        $database = $this->database($adapter, new Cache($cache));
        $update = fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));

        $cache->refusing = true;
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $this->assertSame(2, $this->transactionReads($database, $adapter, $update));
        $this->assertSame(1, $this->transactionReads($database, $adapter, $update));
        $this->assertSame(1, $this->transactionReads($database, $adapter, $update));

        $cache->refusing = false;
        $database->getCollection(self::COLLECTION);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $this->assertSame(2, $this->transactionReads($database, $adapter, $update));
        $this->assertSame(0, $this->transactionReads($database, $adapter, $update), 'the transaction did not cache its definition');
    }

    public function testATransactionOnAReplicaReadPoolReadsItsDefinitionOnce(): void
    {
        $primary = new CountingMemory();
        $replica = new CountingMemory();
        foreach ([$primary, $replica] as $adapter) {
            $this->database($adapter, new Cache(new NoCache()), 'replicated');
        }
        $pool = new ReadWritePool($this->connections($primary), $this->connections($replica));
        $pool->setSticky(false);
        $database = new Database($pool, new Cache(new RedisLeasableCache()));
        $database->setDatabase('transactions')->setNamespace('replicated');
        $update = fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));

        $database->withTransaction($update);
        $primary->reset();
        $replica->reset();
        $database->withTransaction($update);

        $this->assertSame(1, $primary->metadataReads + $replica->metadataReads);
    }

    public function testATransactionUnderAnotherTenantCachesThatTenantsDefinition(): void
    {
        $adapter = new CountingMemory();
        $database = new Database($adapter, new Cache(new RedisLeasableCache()));
        $database->setDatabase('transactions')->setNamespace('transactions_'.\uniqid());
        $database->setSharedTables(true)->setTenant(1);
        $database->create();
        foreach ([1, 2] as $tenant) {
            $database->withTenant($tenant, function () use ($database): void {
                $this->createAccounts($database);
                $database->getCollection(self::COLLECTION);
            });
        }
        $database->withTenant(2, function () use ($database): void {
            $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        });
        $update = fn (): Document => $database->withTenant(2, fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2])));

        $this->assertSame(2, $this->readsOf($adapter, fn (): Document => $database->withTransaction($update)));
        $this->assertSame(0, $this->readsOf($adapter, fn (): Document => $database->withTransaction($update)), 'the transaction did not cache its tenant\'s definition');
    }

    public function testATransactionReadingARawDefinitionCachesTheRawDefinition(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $read = fn (): Document => $database->skipFilters(fn (): Document => $database->getDocument(Database::METADATA, self::COLLECTION));

        $this->assertSame(2, $this->readsOf($adapter, fn (): Document => $database->withTransaction($read)));
        $this->assertSame(0, $this->transactionReads($database, $adapter, $read), 'the transaction did not cache the raw definition');
        $this->assertIsString($database->withTransaction($read)->getAttribute('attributes'));
        $this->assertIsArray($database->getDocument(Database::METADATA, self::COLLECTION)->getAttribute('attributes'));
    }

    public function testATransactionWhoseInvalidationFailedLeavesItsDefinitionReadUncached(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $failure = new TypeError('purge listener failed');
        $database->addHook(new FailingLifecycle(Event::DocumentPurge, $failure));

        try {
            $database->withTransaction(fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2])));
            $this->fail('the purge listener did not fail the transaction');
        } catch (TypeError $error) {
            $this->assertSame($failure, $error);
        }

        $adapter->reset();
        $database->getCollection(self::COLLECTION);

        $this->assertSame(1, $adapter->metadataReads);
    }

    public function testANestedTransactionCachesItsDefinitionOnlyAfterTheOutermostCommit(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $adapter->reset();

        $readsBeforeOuterCommit = $database->withTransaction(function () use ($database, $adapter): int {
            $database->withTransaction(fn (): Document => $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2])));

            return $adapter->metadataReads;
        });

        $this->assertSame(1, $readsBeforeOuterCommit);
        $this->assertSame(2, $adapter->metadataReads);
        $this->assertSame(0, $this->transactionReads($database, $adapter, fn (): Collection => $database->getCollection(self::COLLECTION)));
    }

    public function testADefinitionChangedWhileItIsCachedAfterCommitIsNotCachedStale(): void
    {
        $adapter = new InterleavingMemory();
        $cache = new Cache(new RedisLeasableCache());
        $database = $this->database($adapter, $cache);
        $writer = new Database($adapter, $cache);
        $writer->setDatabase($database->getDatabase())->setNamespace($database->getNamespace());
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        $database->withTransaction(function () use ($database, $writer, $adapter): void {
            $database->getCollection(self::COLLECTION);
            $adapter->afterNextDefinitionRead(static function () use ($writer): void {
                $writer->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::any()), Permission::update(Role::any())], documentSecurity: true));
            });
        });

        $read = fn (): Collection => $database->getCollection(self::COLLECTION);
        $this->assertSame(2, $this->transactionReads($database, $adapter, $read), 'the stale definition was cached, or the lost lease stopped the refills');
        $this->assertSame(0, $this->transactionReads($database, $adapter, $read), 'the transaction did not cache its definition');
        $this->assertTrue($database->getCollection(self::COLLECTION)->getAttribute('documentSecurity'));
    }

    public function testARolledBackTransactionLeavesItsDefinitionReadUncached(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        try {
            $database->withTransaction(function () use ($database): void {
                $database->getCollection(self::COLLECTION);

                throw new \DomainException('rolled back');
            });
        } catch (\DomainException) {
        }

        $adapter->reset();
        $database->getCollection(self::COLLECTION);

        $this->assertSame(1, $adapter->metadataReads);
    }

    public function testASchemaChangeInsideTheTransactionIsRead(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);

        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        [$before, $after] = $database->withTransaction(function () use ($database): array {
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));
            $before = $database->getCollection(self::COLLECTION);
            $database->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::users())], documentSecurity: true));

            return [$before, $database->getCollection(self::COLLECTION)];
        });

        $this->assertFalse($before->getAttribute('documentSecurity'));
        $this->assertTrue($after->getAttribute('documentSecurity'));
        $this->assertSame(['read("users")'], $after->getPermissions());
    }

    public function testADefinitionReadInOneTransactionIsNotReusedByTheNext(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $database->withTransaction(function () use ($database): void {
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));
            $database->getCollection(self::COLLECTION);
        });

        $database->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::any()), Permission::update(Role::any())], documentSecurity: true));
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        $collection = $database->withTransaction(function () use ($database): Collection {
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 3]));

            return $database->getCollection(self::COLLECTION);
        });

        $this->assertTrue($collection->getAttribute('documentSecurity'));
    }

    public function testChangingADefinitionReadInATransactionDoesNotChangeTheNextRead(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);

        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        $second = $database->withTransaction(function () use ($database): Collection {
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));
            $first = $database->getCollection(self::COLLECTION);
            $first->setAttribute('name', 'changed');
            /** @var list<Document> $attributes */
            $attributes = $first->getAttribute('attributes');
            $attributes[0]->setAttribute('size', 1);

            return $database->getCollection(self::COLLECTION);
        });

        $this->assertSame(self::COLLECTION, $second->getAttribute('name'));
        $this->assertSame(0, $second->attributes()[0]->toDocument()->getAttribute('size'));
    }

    /**
     * @return array<string, array{Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [new CountingMemory()],
            'sqlite' => [new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    #[DataProvider('adapters')]
    public function testARawDefinitionReadInATransactionLeavesLaterWritesWorking(Adapter $adapter): void
    {
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        [$raw, $created] = $database->withTransaction(fn (): array => [
            $database->skipFilters(fn (): Document => $database->getDocument(Database::METADATA, self::COLLECTION)),
            $database->createDocument(self::COLLECTION, new Document([Document::ID => 'grace', 'balance' => 5])),
        ]);

        $this->assertIsString($raw->getAttribute('attributes'));
        $this->assertSame(5, $created->getAttribute('balance'));
        $this->assertSame(5, $database->getDocument(self::COLLECTION, 'grace')->getAttribute('balance'));
    }

    #[DataProvider('adapters')]
    public function testAFilteredDefinitionReadInATransactionLeavesLaterRawReadsRaw(Adapter $adapter): void
    {
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        [$collection, $raw] = $database->withTransaction(fn (): array => [
            $database->getCollection(self::COLLECTION),
            $database->skipFilters(fn (): Document => $database->getDocument(Database::METADATA, self::COLLECTION)),
        ]);

        $this->assertSame('balance', $collection->attributes()[0]->key);
        $encoded = $raw->getAttribute('attributes');
        $this->assertIsString($encoded);
        $attributes = \json_decode($encoded, true);
        $this->assertIsArray($attributes);
        $this->assertIsArray($attributes[0]);
        $this->assertSame('balance', $attributes[0]['key']);
    }

    /**
     * @param  callable(): mixed  $callback
     */
    private function transactionReads(Database $database, CountingMemory $adapter, callable $callback): int
    {
        return $this->readsOf($adapter, fn (): mixed => $database->withTransaction($callback));
    }

    /**
     * @param  callable(): mixed  $callback
     */
    private function readsOf(CountingMemory $adapter, callable $callback): int
    {
        $adapter->reset();
        $callback();

        return $adapter->metadataReads;
    }

    /**
     * @return UtopiaPool<Adapter>
     */
    private function connections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        return $connections;
    }

    private function database(Adapter $adapter, ?Cache $cache = null, ?string $namespace = null): Database
    {
        $database = new Database($adapter, $cache ?? new Cache(new RedisLeasableCache()));
        $database->setDatabase('transactions')->setNamespace($namespace ?? 'transactions_'.\uniqid());
        $database->create();
        $this->createAccounts($database);
        $database->getDocument(self::COLLECTION, 'ada');

        return $database;
    }

    private function createAccounts(Database $database): void
    {
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::integer(key: 'balance')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ada', 'balance' => 1]));
    }
}
