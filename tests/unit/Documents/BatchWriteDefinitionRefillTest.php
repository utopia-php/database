<?php

namespace Tests\Unit\Documents;

use ArrayObject;
use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;

final class BatchWriteDefinitionRefillTest extends TestCase
{
    private const string COLLECTION = 'events';

    /**
     * @return array<string, array{Closure(Database, int): int}>
     */
    public static function batchWrites(): array
    {
        return [
            'createDocuments' => [static fn (Database $database, int $round): int => $database->createDocuments(self::COLLECTION, [
                self::event('created'.$round, $round),
            ])],
            'updateDocuments' => [static fn (Database $database, int $round): int => $database->updateDocuments(self::COLLECTION, new Document(['count' => $round]))],
            'deleteDocuments' => [static fn (Database $database, int $round): int => $database->deleteDocuments(self::COLLECTION, [
                Query::equal('$id', ['deleted'.$round]),
            ])],
        ];
    }

    /**
     * @param  Closure(Database, int): int  $write
     */
    #[DataProvider('batchWrites')]
    public function testABatchWriteLeavesItsCollectionDefinitionCached(Closure $write): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new RedisLeasableCache());
        $write($database, 1);

        $adapter->reset();
        $write($database, 2);
        $definition = $database->getCollection(self::COLLECTION);

        $this->assertSame(0, $adapter->metadataReads, 'A batch write must leave the definition it read cached');
        $this->assertSame('count', $definition->attributes()[0]->key);
    }

    public function testADocumentCachedBeforeABatchWriteIsReadAfreshAndCachedAgain(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new RedisLeasableCache());
        $this->assertSame(1, $database->getDocument(self::COLLECTION, 'first')->getAttribute('count'));

        $database->updateDocuments(self::COLLECTION, new Document(['count' => 5]));

        $this->assertSame(5, $database->getDocument(self::COLLECTION, 'first')->getAttribute('count'), 'The batch write must retire the copy cached before it');
        $adapter->reset();
        $this->assertSame(5, $database->getDocument(self::COLLECTION, 'first')->getAttribute('count'));
        $this->assertSame(0, $adapter->documentReads + $adapter->metadataReads, 'The document must be cached again under the epoch the write published');
    }

    public function testASchemaChangeDuringABatchWriteIsRead(): void
    {
        $adapter = new CountingMemory();
        /** @var ArrayObject<int, Closure(): mixed> $schemaChanges */
        $schemaChanges = new ArrayObject();
        $cache = $this->cacheRunningOnce(':_metadata:'.self::COLLECTION, static function () use ($schemaChanges): bool {
            foreach ($schemaChanges as $schemaChange) {
                $schemaChange();

                return true;
            }

            return false;
        });
        $database = $this->database($adapter, $cache);
        $other = new Database($adapter, new Cache($cache));
        $other->setDatabase($database->getDatabase())->setNamespace($database->getNamespace());
        $other->getAuthorization()->addRole(Role::any()->toString());
        $schemaChanges->append(static fn (): Attribute => $other->createAttribute(self::COLLECTION, Attribute::string(key: 'label', size: 32)));

        $database->updateDocuments(self::COLLECTION, new Document(['count' => 3]));

        $this->assertSame(['count', 'label'], \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection(self::COLLECTION)->attributes(),
        ), 'A definition changed while the batch write ran must not be cached again as it was');
    }

    public function testABatchWriteInATransactionCachesItsDefinitionAfterTheCommit(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new RedisLeasableCache());

        $database->withTransaction(function () use ($database): void {
            $database->createDocuments(self::COLLECTION, [self::event('inside', 2)]);
            $database->updateDocuments(self::COLLECTION, new Document(['count' => 7]));
        });

        $adapter->reset();
        $definition = $database->getCollection(self::COLLECTION);

        $this->assertSame(0, $adapter->metadataReads);
        $this->assertSame(self::COLLECTION, $definition->getId());
        $this->assertSame(7, $database->getDocument(self::COLLECTION, 'inside')->getAttribute('count'));
    }

    public function testARolledBackBatchWriteKeepsItsDefinitionAndDocumentsCorrect(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new RedisLeasableCache());
        $this->assertSame(1, $database->getDocument(self::COLLECTION, 'first')->getAttribute('count'));

        try {
            $database->withTransaction(function () use ($database): void {
                $database->updateDocuments(self::COLLECTION, new Document(['count' => 9]));

                throw new RuntimeException('rollback');
            });
        } catch (RuntimeException) {
        }

        $adapter->reset();
        $this->assertSame(self::COLLECTION, $database->getCollection(self::COLLECTION)->getId());
        $this->assertSame(1, $database->getDocument(self::COLLECTION, 'first')->getAttribute('count'));
        $this->assertSame(0, $adapter->metadataReads);
    }

    public function testEachTenantKeepsItsOwnDefinitionAcrossABatchWrite(): void
    {
        $adapter = new CountingMemory();
        $cache = new RedisLeasableCache();
        $database = new Database($adapter, new Cache($cache));
        $database->setDatabase('refills')->setNamespace('refills_'.\uniqid())->setSharedTables(true)->setTenant(1);
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        foreach ([1 => 'count', 2 => 'total'] as $tenant => $key) {
            $database->setTenant($tenant);
            $database->createCollection(Collection::create(id: self::COLLECTION, attributes: [Attribute::integer(key: $key)], permissions: [
                Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()),
            ], documentSecurity: false));
            $database->getCollection(self::COLLECTION);
        }

        $database->setTenant(1);
        $database->createDocuments(self::COLLECTION, [new Document(['$id' => 'one', 'count' => 1, '$permissions' => [Permission::read(Role::any())]])]);
        $database->createDocuments(self::COLLECTION, [new Document(['$id' => 'two', 'count' => 2, '$permissions' => [Permission::read(Role::any())]])]);

        $adapter->reset();
        $first = $database->getCollection(self::COLLECTION)->attributes()[0]->key;
        $database->setTenant(2);
        $second = $database->getCollection(self::COLLECTION)->attributes()[0]->key;

        $this->assertSame(['count', 'total'], [$first, $second]);
        $this->assertSame(0, $adapter->metadataReads);
    }

    private static function event(string $id, int $count): Document
    {
        return new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
            'count' => $count,
        ]);
    }

    private function database(CountingMemory $adapter, CacheAdapter $cache): Database
    {
        $database = new Database($adapter, new Cache($cache));
        $database->setDatabase('refills')->setNamespace('refills_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(id: self::COLLECTION, attributes: [Attribute::integer(key: 'count')], permissions: [
            Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any()),
        ], documentSecurity: false));
        $database->createDocument(self::COLLECTION, self::event('first', 1));
        foreach ([1, 2] as $round) {
            $database->createDocument(self::COLLECTION, self::event('deleted'.$round, 1));
        }
        $database->getCollection(self::COLLECTION);

        return $database;
    }

    /**
     * A cache that runs the callback after purging a key ending in the suffix, until the callback reports that it
     * acted; it acts once.
     *
     * @param  Closure(): bool  $callback
     */
    private function cacheRunningOnce(string $suffix, Closure $callback): CacheAdapter&Leasable
    {
        return new class ($suffix, $callback) implements CacheAdapter, Leasable {
            private RedisLeasableCache $cache;

            private bool $ran = false;

            /**
             * @param  Closure(): bool  $callback
             */
            public function __construct(private readonly string $suffix, private readonly Closure $callback)
            {
                $this->cache = new RedisLeasableCache();
            }

            public function load(string $key, int $ttl, string $hash = ''): mixed
            {
                return $this->cache->load($key, $ttl, $hash);
            }

            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                return $this->cache->save($key, $data, $hash);
            }

            public function touch(string $key, string $hash = ''): bool
            {
                return $this->cache->touch($key, $hash);
            }

            /** @return array<string> */
            public function list(string $key): array
            {
                return $this->cache->list($key);
            }

            public function purge(string $key, string $hash = ''): bool
            {
                $purged = $this->cache->purge($key, $hash);
                if (! $this->ran && \str_ends_with($key, $this->suffix)) {
                    $this->ran = true;
                    $this->ran = ($this->callback)();
                }

                return $purged;
            }

            public function flush(): bool
            {
                return $this->cache->flush();
            }

            public function ping(): bool
            {
                return true;
            }

            public function getSize(): int
            {
                return $this->cache->getSize();
            }

            public function getName(?string $key = null): string
            {
                return 'running-once';
            }

            public function getGeneration(string $key): string
            {
                return $this->cache->getGeneration($key);
            }

            public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
            {
                return $this->cache->saveWithLease($key, $data, $hash, $generation);
            }
        };
    }
}
