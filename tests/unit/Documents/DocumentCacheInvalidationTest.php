<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Cache\PausedSQLite;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Throwable;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter as DatabaseAdapter;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;

final class DocumentCacheInvalidationTest extends TestCase
{
    public const string PURGE_FAILURE = 'The cache refused the purge';

    private const int DOCUMENTS = 5;

    public function testRepeatedWritesAndReadsKeepTheKeyCountBounded(): void
    {
        $cache = new RedisLeasableCache();
        $database = $this->createDatabase(new CountingMemory(), $cache);
        for ($index = 0; $index < self::DOCUMENTS; $index++) {
            $database->createDocument('webhooks', $this->hook('hook'.$index));
        }

        $keysAfterFirstRound = 0;
        for ($round = 1; $round <= 20; $round++) {
            for ($index = 0; $index < self::DOCUMENTS; $index++) {
                $database->updateDocument('webhooks', 'hook'.$index, new Document(['name' => 'round '.$round]));
                $this->assertSame('round '.$round, $database->getDocument('webhooks', 'hook'.$index)->getAttribute('name'));
            }

            if ($round === 1) {
                $keysAfterFirstRound = \count($cache->keys());
            }
        }

        $this->assertSame(
            $keysAfterFirstRound,
            \count($cache->keys()),
            'A purged key stays behind in Redis with no expiry, so writes and reads of the same documents must not add keys',
        );
    }

    public function testACacheWithoutFieldsServesEachSelectionItsOwnCopy(): void
    {
        $database = $this->createDatabase(new CountingMemory(), new MemoryCache());
        $database->createDocument('webhooks', $this->hook('hook'));

        for ($round = 0; $round < 2; $round++) {
            $plain = $database->getDocument('webhooks', 'hook');
            $this->assertSame('description', $plain->getAttribute('description'), 'A read without a selection must get every attribute');

            $projected = $database->getDocument('webhooks', 'hook', [Query::select(['name'])]);
            $this->assertSame('hook', $projected->getAttribute('name'));
            $this->assertFalse($projected->offsetExists('description'), 'A projected read must get only what it selected');
        }
    }

    public function testACachedMissUnderOneCasingDoesNotHideAnotherCasing(): void
    {
        $database = $this->createDatabase($this->caseSensitiveAdapter(), new MemoryCache());
        $database->createDocument('webhooks', $this->hook('Hook'));

        $this->assertTrue($database->getDocument('webhooks', 'hook')->isEmpty(), 'The adapter stores ids case-sensitively');
        $this->assertSame('Hook', $database->getDocument('webhooks', 'Hook')->getId(), 'A cached miss for one casing must not answer another');
        $this->assertTrue($database->getDocument('webhooks', 'hook')->isEmpty(), 'A cached document must not answer another casing of its id');
        $this->assertSame('Hook', $database->getDocument('webhooks', 'Hook')->getId());
    }

    public function testAWriteKeepsItsSiblingsCached(): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());
        $database->createDocument('webhooks', $this->hook('written'));
        $database->createDocument('webhooks', $this->hook('sibling'));
        $sibling = $database->getDocument('webhooks', 'sibling');

        $database->updateDocument('webhooks', 'written', new Document(['name' => 'renamed']));
        $adapter->reset();

        $this->assertSame($sibling->getArrayCopy(), $database->getDocument('webhooks', 'sibling')->getArrayCopy());
        $this->assertSame(0, $adapter->documentReads, 'A write to one document must leave its siblings cached (7.3.12: 0 reads)');
        $this->assertSame('renamed', $database->getDocument('webhooks', 'written')->getAttribute('name'));
    }

    /**
     * @return array<string, array{Closure(Database): mixed}>
     */
    public static function singleDocumentWrites(): array
    {
        return [
            'createDocument' => [
                static fn (Database $database): Document => $database->createDocument('webhooks', new Document([
                    '$id' => 'created',
                    '$permissions' => [Permission::read(Role::any())],
                    'name' => 'created',
                ])),
            ],
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
    public function testSingleDocumentWritesDoNotBlockTheCollection(Closure $write): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->createDocument('webhooks', $this->hook('sibling'));
        $database->getDocument('webhooks', 'hook');
        $database->getDocument('webhooks', 'sibling');

        $write($database);
        $adapter->reset();

        $this->assertSame('hook', $database->getDocument('webhooks', 'sibling')->getAttribute('name'));
        $this->assertSame(0, $adapter->documentReads, 'A single-document write must leave the rest of its collection cached');
    }

    public function testACollectionDefinitionWriteKeepsTheOtherDefinitionsCached(): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());
        $database->createCollection(Collection::create(id: 'logs', permissions: [Permission::read(Role::any())]));
        $database->getCollection('webhooks');
        $database->getCollection('logs');

        $database->updateCollection('logs', new CollectionUpdate(permissions: [Permission::read(Role::any()), Permission::create(Role::any())], documentSecurity: true));
        $adapter->reset();

        $this->assertNotNull($database->findCollection('webhooks'));
        $this->assertSame(0, $adapter->metadataReads, 'A write to one collection definition must leave the other definitions cached');
        $this->assertTrue($database->getCollection('logs')->getAttribute('documentSecurity'), 'The written definition must be read again');
    }

    public function testAFailedPurgeInsideTheTransactionRollsTheWriteBack(): void
    {
        /** @var bool $refusing */
        $refusing = false;
        $database = $this->createDatabase(new CountingMemory(), $this->purgeRefusingCache(
            static function () use (&$refusing): bool {
                $refused = $refusing;
                $refusing = false;

                return $refused;
            }
        ));
        $database->createDocument('webhooks', $this->hook('hook'));
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $refusing = true;
        $failure = null;
        try {
            $database->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed']));
        } catch (Throwable $error) {
            $failure = $error->getMessage();
        }

        $this->assertSame(self::PURGE_FAILURE, $failure, 'A failed purge inside the transaction must reach the caller');

        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'), 'A write whose document could not be purged must roll back');
    }

    public function testAFailedPostCommitPurgeNeverServesAPreCommitFill(): void
    {
        $path = \tempnam(\sys_get_temp_dir(), 'document-cache-invalidation-');
        if ($path === false) {
            throw new RuntimeException('Failed to create SQLite test database');
        }

        try {
            $attributes = [
                \PDO::ATTR_PERSISTENT => false,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_EMULATE_PREPARES => true,
                \PDO::ATTR_STRINGIFY_FETCHES => true,
            ];
            $writerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
            $readerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
            $writerConnection->exec('PRAGMA journal_mode = WAL');
            $writerConnection->exec('PRAGMA busy_timeout = 1000');
            $readerConnection->exec('PRAGMA busy_timeout = 1000');

            /** @var bool $refusing */
            $refusing = false;
            $cache = $this->purgeRefusingCache(static function () use (&$refusing): bool {
                $refused = $refusing;
                $refusing = false;

                return $refused;
            });
            $adapter = new PausedSQLite($writerConnection);
            $writer = $this->createDatabase($adapter, $cache, 'shared_invalidation_'.\uniqid());
            $reader = $this->configure(new Database(new SQLite($readerConnection), new Cache($cache)), $writer->getNamespace());
            $writer->createDocument('webhooks', $this->hook('hook'));
            $this->assertSame('hook', $reader->getDocument('webhooks', 'hook')->getAttribute('name'));

            $duringCommit = null;
            $adapter->pauseNextCommit(function () use ($reader, &$duringCommit, &$refusing): void {
                $duringCommit = $reader->getDocument('webhooks', 'hook')->getAttribute('name');
                $refusing = true;
            });

            $failure = null;
            try {
                $writer->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed']));
            } catch (Throwable $error) {
                $failure = $error->getMessage();
            }

            $this->assertSame(self::PURGE_FAILURE, $failure, 'A failed purge after the commit must reach the caller');

            $this->assertSame('hook', $duringCommit, 'A reader outside the transaction reads the committed row and caches it');
            $this->assertSame('renamed', $reader->getDocument('webhooks', 'hook')->getAttribute('name'), 'A fill made before the commit must never be served after it, even when the purge after the commit fails');
        } finally {
            foreach ([$path, $path.'-wal', $path.'-shm'] as $file) {
                if (\is_file($file)) {
                    \unlink($file);
                }
            }
        }
    }

    public function testARollbackKeepsServingTheCommittedDocument(): void
    {
        $database = $this->createDatabase(new CountingMemory(), new RedisLeasableCache());
        $database->createDocument('webhooks', $this->hook('hook'));
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        try {
            $database->withTransaction(function () use ($database): void {
                $database->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed']));

                throw new RuntimeException('rollback');
            });
        } catch (RuntimeException) {
        }

        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
    }

    public function testCreateAndDeleteChurnLeavesOnlyAGenerationPerDocument(): void
    {
        $cache = new RedisLeasableCache();
        $database = $this->createDatabase(new CountingMemory(), $cache);
        $database->createDocument('webhooks', $this->hook('warm'));
        $database->getDocument('webhooks', 'warm');
        $keys = \count($cache->keys());
        $values = $cache->countValues();

        $churned = 10;
        for ($index = 0; $index < $churned; $index++) {
            $id = 'churn'.$index;
            $database->createDocument('webhooks', $this->hook($id));
            $this->assertFalse($database->getDocument('webhooks', $id)->isEmpty());
            $this->assertTrue($database->deleteDocument('webhooks', $id));
            $this->assertTrue($database->getDocument('webhooks', $id)->isEmpty());
            $database->purgeCachedDocument('webhooks', $id);
        }

        $this->assertLessThanOrEqual($keys + $churned, \count($cache->keys()), 'A purge keeps at most one generation-only key per document id ever written');
        $this->assertLessThanOrEqual($values, $cache->countValues(), 'A churned document must leave no cached value behind');
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
            'description' => 'description',
        ]);
    }

    private function caseSensitiveAdapter(): DatabaseMemory
    {
        return new class () extends DatabaseMemory {
            #[\Override]
            protected function documentKey(string $id, int|string|null $tenant = null): string
            {
                return $this->sharedTables ? ($tenant ?? $this->getTenant()).'|'.$id : $id;
            }
        };
    }

    /**
     * @param  Closure(): bool  $refuses  Whether the purge throws, asked once per purge
     */
    private function purgeRefusingCache(Closure $refuses): CacheAdapter&Leasable
    {
        return new class ($refuses) implements CacheAdapter, Leasable {
            private RedisLeasableCache $cache;

            /**
             * @param  Closure(): bool  $refuses
             */
            public function __construct(private readonly Closure $refuses)
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
                if (($this->refuses)()) {
                    throw new RuntimeException(DocumentCacheInvalidationTest::PURGE_FAILURE);
                }

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
                return 'purge-failing';
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

    private function createDatabase(DatabaseAdapter $adapter, CacheAdapter $cache, ?string $namespace = null): Database
    {
        $database = $this->configure(new Database($adapter, new Cache($cache)), $namespace ?? 'document_cache_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
            Attribute::string(key: 'description'),
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
