<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Cache\CountingCache;
use Tests\Unit\Cache\PausedSQLite;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter as DatabaseAdapter;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

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

    public function testAnUpdateInvalidatesTheCacheOnceLikeADelete(): void
    {
        $cache = new CountingCache(new RedisLeasableCache());
        $database = $this->createDatabase(new CountingMemory(), $cache);
        $database->createDocument('webhooks', $this->hook('updated'));
        $database->createDocument('webhooks', $this->hook('deleted'));
        $database->getDocument('webhooks', 'updated');
        $database->getDocument('webhooks', 'deleted');

        $cache->resetOperations();
        $database->updateDocument('webhooks', 'updated', new Document(['name' => 'renamed']));
        $update = $cache->getOperations();

        $cache->resetOperations();
        $database->deleteDocument('webhooks', 'deleted');
        $delete = $cache->getOperations();

        $this->assertSame($delete, $update, 'updateDocument() must invalidate its document once, as deleteDocument() does');
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
     * @return array<string, array{Closure(Database): mixed, int}>
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
                3,
            ],
            'updateDocument' => [
                static fn (Database $database): Document => $database->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed'])),
                6,
            ],
            'increaseDocumentAttribute' => [
                static fn (Database $database): Document => $database->increaseDocumentAttribute('webhooks', 'hook', 'count'),
                4,
            ],
            'decreaseDocumentAttribute' => [
                static fn (Database $database): Document => $database->decreaseDocumentAttribute('webhooks', 'hook', 'count'),
                4,
            ],
            'deleteDocument' => [
                static fn (Database $database): bool => $database->deleteDocument('webhooks', 'hook'),
                6,
            ],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('singleDocumentWrites')]
    public function testSingleDocumentWritesDoNotBlockTheCollection(Closure $write, int $baseline): void
    {
        $cache = new CountingCache(new RedisLeasableCache());
        $database = $this->createDatabase(new CountingMemory(), $cache);
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->getDocument('webhooks', 'hook');

        $cache->resetOperations();
        $write($database);

        $this->assertSame(
            8,
            $cache->getOperations(),
            "Cache round trips of the write on a warm cache: the collection lookup (6 until one round trip per lookup) and one purge of the document inside the transaction and one after it (7.3.12: {$baseline})",
        );
    }

    public function testACollectionDefinitionWriteKeepsTheOtherDefinitionsCached(): void
    {
        $adapter = new CountingMemory();
        $database = $this->createDatabase($adapter, new RedisLeasableCache());
        $database->createCollection(new Collection(id: 'logs', permissions: [Permission::read(Role::any())]));
        $database->getCollection('webhooks');
        $database->getCollection('logs');

        $database->updateCollection('logs', [Permission::read(Role::any()), Permission::create(Role::any())], true);
        $adapter->reset();

        $this->assertFalse($database->getCollection('webhooks')->isEmpty());
        $this->assertSame(0, $adapter->metadataReads, 'A write to one collection definition must leave the other definitions cached');
        $this->assertTrue($database->getCollection('logs')->getAttribute('documentSecurity'), 'The written definition must be read again');
        $this->assertSame(1, $adapter->metadataReads);
    }

    public function testAFailedPurgeInsideTheTransactionRollsTheWriteBack(): void
    {
        $refusing = false;
        $database = $this->createDatabase(new CountingMemory(), $this->purgeRefusingCache(
            static function (string $key) use (&$refusing): bool {
                return $refusing && \str_ends_with($key, ':hook');
            }
        ));
        $database->createDocument('webhooks', $this->hook('hook'));
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $refusing = true;
        $failure = null;
        try {
            $database->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed']));
        } catch (RuntimeException $error) {
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
            $attributes = SQLite::getPDOAttributes();
            $attributes[\PDO::ATTR_PERSISTENT] = false;
            $writerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
            $readerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
            $writerConnection->exec('PRAGMA journal_mode = WAL');
            $writerConnection->exec('PRAGMA busy_timeout = 1000');
            $readerConnection->exec('PRAGMA busy_timeout = 1000');

            $refusing = false;
            $cache = $this->purgeRefusingCache(static function (string $key) use (&$refusing): bool {
                return $refusing && \str_ends_with($key, ':hook');
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
            } catch (RuntimeException $error) {
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

        $churned = 10;
        for ($index = 0; $index < $churned; $index++) {
            $id = 'churn'.$index;
            $database->createDocument('webhooks', $this->hook($id));
            $this->assertFalse($database->getDocument('webhooks', $id)->isEmpty());
            $this->assertTrue($database->deleteDocument('webhooks', $id));
            $this->assertTrue($database->getDocument('webhooks', $id)->isEmpty());
            $this->assertTrue($database->purgeCachedDocument('webhooks', $id));
        }

        $this->assertSame($keys + $churned, \count($cache->keys()), 'A purge keeps one generation-only key per document id ever written');
        for ($index = 0; $index < $churned; $index++) {
            $documentKey = \strtolower($database->getCacheBaseKeys('webhooks', 'churn'.$index)[1]);
            $this->assertSame([], $cache->list($documentKey), 'A churned document must leave no cached value behind');
        }
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
     * @param  Closure(string): bool  $refuses  Whether a purge of the key throws
     */
    private function purgeRefusingCache(Closure $refuses): CacheAdapter&Leasable
    {
        return new class ($refuses) implements CacheAdapter, Leasable {
            private RedisLeasableCache $cache;

            /**
             * @param  Closure(string): bool  $refuses
             */
            public function __construct(private readonly Closure $refuses)
            {
                $this->cache = new RedisLeasableCache();
            }

            public function load(string $key, int $ttl, string $hash = ''): mixed
            {
                return $this->cache->load($key, $ttl, $hash);
            }

            public function save(string $key, array|string $data, string $hash = ''): bool|string|array
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
                if (($this->refuses)($key)) {
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
        $database->createCollection(new Collection(id: 'webhooks', attributes: [
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
