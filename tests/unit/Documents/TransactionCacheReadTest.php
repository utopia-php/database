<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter as DatabaseAdapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;

final class TransactionCacheReadTest extends TestCase
{
    /**
     * @return array<string, array{Closure(Database): mixed}>
     */
    public static function singleDocumentWrites(): array
    {
        return [
            'updateDocument' => [
                static fn (Database $database): Document => $database->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed'])),
            ],
            'increaseDocumentAttribute' => [
                static fn (Database $database): Document => $database->increaseDocumentAttribute('webhooks', 'hook', 'count'),
            ],
            'decreaseDocumentAttribute' => [
                static fn (Database $database): Document => $database->decreaseDocumentAttribute('webhooks', 'hook', 'count'),
            ],
            'deleteDocument' => [
                static fn (Database $database): bool => $database->deleteDocument('webhooks', 'hook'),
            ],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('singleDocumentWrites')]
    public function testWritesReadTheirCollectionDefinitionFromTheCache(Closure $write): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->getDocument('webhooks', 'hook');

        $adapter->reset();
        $write($database);

        $this->assertSame(0, $adapter->metadataReads, 'A write must read its collection definition from the cache inside its own transaction (7.3.12: 0 reads)');
    }

    public function testReadsInsideATransactionAreServedFromTheCache(): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());
        $database->createDocument('webhooks', $this->hook('written'));
        $database->createDocument('webhooks', $this->hook('sibling'));
        $database->getDocument('webhooks', 'written');
        $sibling = $database->getDocument('webhooks', 'sibling');

        $adapter->reset();
        $read = $database->withTransaction(function () use ($database): Document {
            $database->updateDocument('webhooks', 'written', new Document(['name' => 'renamed']));

            return $database->getDocument('webhooks', 'sibling');
        });

        $this->assertSame($sibling->getArrayCopy(), $read->getArrayCopy());
        $this->assertSame(0, $adapter->metadataReads, 'withTransaction(update + get) must read no collection definition on a warm cache (7.3.12: 0 reads)');
        $this->assertSame(1, $adapter->documentReads, 'Only the locking read of the written document may reach the adapter (7.3.12: 1 read)');
    }

    public function testATransactionReadsItsOwnWrites(): void
    {
        [$writer, $reader, $path] = $this->createSharedSQLiteDatabases();

        try {
            $this->assertSame('original', $writer->getDocument('users', 'user')->getAttribute('name'));

            $observed = $writer->withTransaction(function () use ($writer, $reader): array {
                $writer->updateDocument('users', 'user', new Document(['name' => 'renamed']));
                $committed = $reader->getDocument('users', 'user')->getAttribute('name');
                $own = $writer->getDocument('users', 'user');

                return [$committed, $own->getAttribute('name')];
            });

            $this->assertSame('original', $observed[0], 'A reader outside the transaction reads the committed row and caches it');
            $this->assertSame('renamed', $observed[1], 'A transaction must read its own write, not a copy another reader cached after it');
            $this->assertSame('renamed', $reader->getDocument('users', 'user')->getAttribute('name'));
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testATransactionReadsItsOwnWritesUnderAnotherCasing(): void
    {
        [$writer, $reader] = $this->createSharedMemoryDatabases();
        $this->assertSame('hook', $writer->getDocument('webhooks', 'HOOK')->getAttribute('name'));

        $read = $writer->withTransaction(function () use ($writer, $reader): Document {
            $writer->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed']));
            $this->assertSame('hook', $reader->getDocument('webhooks', 'HOOK')->getAttribute('name'));

            return $writer->getDocument('webhooks', 'HOOK');
        });

        $this->assertSame('renamed', $read->getAttribute('name'), 'A transaction must read its own write under any casing the adapter matches to the written id');
    }

    public function testATransactionStartedOnTheAdapterReadsItsOwnWrites(): void
    {
        [$writer, $reader] = $this->createSharedMemoryDatabases();

        $read = $writer->getAdapter()->withTransaction(function () use ($writer, $reader): Document {
            $writer->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed']));
            $this->assertSame('hook', $reader->getDocument('webhooks', 'hook')->getAttribute('name'));

            return $writer->withTransaction(fn (): Document => $writer->getDocument('webhooks', 'hook'));
        });

        $this->assertSame('renamed', $read->getAttribute('name'), 'A transaction the database did not start must not serve cached copies of what it wrote');
    }

    public function testATransactionReadsTheDocumentsItsBatchWriteChanged(): void
    {
        [$writer, $reader] = $this->createSharedMemoryDatabases();

        $read = $writer->withTransaction(function () use ($writer, $reader): Document {
            $writer->updateDocuments('webhooks', new Document(['name' => 'renamed']));
            $this->assertSame('hook', $reader->getDocument('webhooks', 'hook')->getAttribute('name'));

            return $writer->getDocument('webhooks', 'hook');
        });

        $this->assertSame('renamed', $read->getAttribute('name'), 'A transaction must read the documents its batch write changed');
    }

    public function testATransactionReadsTheCollectionDefinitionItChanged(): void
    {
        [$writer, $reader] = $this->createSharedMemoryDatabases();

        $read = $writer->withTransaction(function () use ($writer, $reader): Document {
            $writer->updateCollection('webhooks', new CollectionUpdate(permissions: [Permission::read(Role::any()), Permission::update(Role::any())], documentSecurity: false));
            $this->assertTrue($reader->getCollection('webhooks')->getAttribute('documentSecurity'));

            return $writer->getCollection('webhooks');
        });

        $this->assertFalse($read->getAttribute('documentSecurity'), 'A transaction must read the collection definition its schema change wrote');
    }

    public function testReadsInsideATransactionFillNothing(): void
    {
        $fills = 0;
        $database = $this->createDatabase(new CountingMemory(), $this->fillCountingCache(function () use (&$fills): void {
            $fills++;
        }));
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->createDocument('webhooks', $this->hook('cold'));
        $database->getDocument('webhooks', 'hook');

        $before = $fills;
        $database->withTransaction(function () use ($database): void {
            $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
            $this->assertSame('hook', $database->getDocument('webhooks', 'cold')->getAttribute('name'));
            $this->assertTrue($database->getDocument('webhooks', 'absent')->isEmpty());
            $this->assertNull($database->findCollection('absent'));
        });

        $this->assertSame($before, $fills, 'A read inside a transaction must not save to the cache');
    }

    public function testANestedRelationshipCreateReadsNoCollectionDefinition(): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());
        $database->addHook(new Relationships($database));
        $database->createCollection(Collection::create(id: 'libraries', attributes: [
            Attribute::string(key: 'name'),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));
        $database->createCollection(Collection::create(id: 'books', attributes: [
            Attribute::string(key: 'title'),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));
        $database->createRelationship('libraries', Relationship::oneToMany(
            relatedCollection: 'books',
            twoWay: true,
            key: 'books',
            twoWayKey: 'library',
        ));
        $database->createDocument('libraries', $this->library('warm', 'warm'));

        $adapter->reset();
        $database->createDocument('libraries', $this->library('library', 'book'));

        $this->assertSame(0, $adapter->metadataReads, 'A create with 3 nested documents must read no collection definition on a warm cache (7.3.12: 0 reads)');
        $books = $database->getDocument('libraries', 'library')->getAttribute('books');
        $this->assertIsArray($books);
        $this->assertCount(3, $books);
    }

    public function testAMissingCollectionCostsOneMetadataRead(): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());

        $adapter->reset();
        $this->assertNull($database->findCollection('missing'));
        $this->assertSame(1, $adapter->metadataReads, 'A missing collection must cost one read of its definition (7.3.12: 1 read)');

        $this->assertNull($database->findCollection('missing'));
        $this->assertSame(1, $adapter->metadataReads, 'A missing collection must be served from the cache once read');
    }

    public function testCreateCollectionChecksItsIdWithOneMetadataRead(): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());

        $adapter->reset();
        $database->createCollection(Collection::create(id: 'logs', attributes: [
            Attribute::string(key: 'message'),
        ], permissions: [Permission::read(Role::any())]));

        $this->assertSame(1, $adapter->metadataReads, 'createCollection() must check that its id is free with one read of the definition (7.3.12: 1 read)');
    }

    public function testCreateCollectionAfterAProbeReadsNoDefinition(): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());
        $this->assertNull($database->findCollection('logs'));
        $database->createCollection(Collection::create(id: 'audits', permissions: [Permission::read(Role::any())]));

        $adapter->reset();
        $database->createCollection(Collection::create(id: 'logs', permissions: [Permission::read(Role::any())]));

        $this->assertSame(0, $adapter->metadataReads, 'A cached miss for the new id must survive other definitions being written (7.3.12: 0 reads)');
    }

    private function hook(string $id): Document
    {
        return new Document([
            '$id' => $id,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'hook',
        ]);
    }

    private function library(string $id, string $prefix): Document
    {
        $books = [];
        for ($index = 0; $index < 3; $index++) {
            $books[] = [
                '$id' => $prefix.$index,
                '$permissions' => [Permission::read(Role::any())],
                'title' => $prefix.$index,
            ];
        }

        return new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            'name' => $id,
            'books' => $books,
        ]);
    }

    /**
     * @param  Closure(): void  $onFill  Called on every save to the cache
     */
    private function fillCountingCache(Closure $onFill): CacheAdapter&Leasable
    {
        return new class ($onFill) implements CacheAdapter, Leasable {
            private RedisLeasableCache $cache;

            /**
             * @param  Closure(): void  $onFill
             */
            public function __construct(private readonly Closure $onFill)
            {
                $this->cache = new RedisLeasableCache();
            }

            public function load(string $key, int $ttl, string $hash = ''): mixed
            {
                return $this->cache->load($key, $ttl, $hash);
            }

            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                ($this->onFill)();

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
                return $this->cache->purge($key, $hash);
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
                return 'fill-counting';
            }

            public function getGeneration(string $key): string
            {
                return $this->cache->getGeneration($key);
            }

            public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
            {
                ($this->onFill)();

                return $this->cache->saveWithLease($key, $data, $hash, $generation);
            }
        };
    }

    /**
     * @return array{Database, Database}
     */
    private function createSharedMemoryDatabases(): array
    {
        $cache = new RedisLeasableCache();
        $namespace = 'transaction_cache_'.\uniqid();
        $writer = $this->createDatabase(new CountingMemory(), $cache, $namespace);
        $reader = $this->createDatabase(new CountingMemory(), $cache, $namespace);
        foreach ([$writer, $reader] as $database) {
            $database->createDocument('webhooks', $this->hook('hook'));
        }

        return [$writer, $reader];
    }

    /**
     * @return array{Database, Database, string}
     */
    private function createSharedSQLiteDatabases(): array
    {
        $path = \tempnam(\sys_get_temp_dir(), 'transaction-cache-');
        if ($path === false) {
            throw new \RuntimeException('Failed to create SQLite test database');
        }

        $attributes = SQLite::getPDOAttributes();
        $attributes[\PDO::ATTR_PERSISTENT] = false;
        $writerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
        $readerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
        $writerConnection->exec('PRAGMA journal_mode = WAL');
        $writerConnection->exec('PRAGMA busy_timeout = 1000');
        $readerConnection->exec('PRAGMA busy_timeout = 1000');

        $cache = new Cache(new RedisLeasableCache());
        $writer = new Database(new SQLite($writerConnection), $cache);
        $reader = new Database(new SQLite($readerConnection), $cache);
        $namespace = 'transaction_cache_'.\uniqid();
        foreach ([$writer, $reader] as $database) {
            $this->configure($database, $namespace);
        }

        $writer->create();
        $writer->createCollection(Collection::create(id: 'users', attributes: [
            Attribute::string(key: 'name', required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));
        $writer->createDocument('users', new Document([
            '$id' => 'user',
            'name' => 'original',
        ]));

        return [$writer, $reader, $path];
    }

    private function removeSQLiteFiles(string $path): void
    {
        foreach ([$path, $path.'-wal', $path.'-shm'] as $file) {
            if (\is_file($file)) {
                \unlink($file);
            }
        }
    }

    private function createDatabase(DatabaseAdapter $adapter, CacheAdapter $cache, ?string $namespace = null): Database
    {
        $database = $this->configure(new Database($adapter, new Cache($cache)), $namespace ?? 'transaction_cache_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
            Attribute::integer(key: 'count', default: 10),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        return $database;
    }

    private function configure(Database $database, string $namespace): Database
    {
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace($namespace);
        $database->getAuthorization()->addRole(Role::any()->toString());

        return $database;
    }
}
