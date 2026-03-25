<?php

namespace Tests\E2E\Adapter;

use Redis;
use RuntimeException;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO;

class MariaDBTest extends Base
{
    protected static ?Database $database = null;
    protected static ?PDO $pdo = null;
    protected static string $namespace;

    /**
     * @return Database
     */
    public function getDatabase(bool $fresh = false): Database
    {
        if (!is_null(self::$database) && !$fresh) {
            return self::$database;
        }

        $dbHost = 'mariadb';
        $dbPort = '3306';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new MariaDB($pdo), $cache);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;
        return self::$database = $database;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = "`" . $this->getDatabase()->getDatabase() . "`.`" . $this->getDatabase()->getNamespace() . "_" . $collection . "`";
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        self::$pdo->exec($sql);

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $sqlTable = "`" . $this->getDatabase()->getDatabase() . "`.`" . $this->getDatabase()->getNamespace() . "_" . $collection . "`";
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        self::$pdo->exec($sql);

        return true;
    }

    /**
     * Build a Cache mock where every method throws, simulating a lost connection.
     */
    private function buildBrokenCache(): Cache
    {
        $broken = $this->createMock(Cache::class);
        $broken->method('load')->willThrowException(new RuntimeException('cache unavailable'));
        $broken->method('save')->willThrowException(new RuntimeException('cache unavailable'));
        $broken->method('purge')->willThrowException(new RuntimeException('cache unavailable'));
        $broken->method('list')->willThrowException(new RuntimeException('cache unavailable'));
        $broken->method('flush')->willThrowException(new RuntimeException('cache unavailable'));
        return $broken;
    }

    /**
     * Scaffold a collection used by all cache fail-open tests.
     * Returns the Database with a working cache so data is seeded properly.
     */
    private function seedCacheFailOpenCollection(string $collection): Database
    {
        $database = $this->getDatabase();
        $database->getAuthorization()->addRole(Role::any()->toString());

        $database->createCollection($collection, attributes: [
            new Document([
                '$id' => ID::custom('title'),
                'type' => Database::VAR_STRING,
                'size' => 255,
                'required' => true,
            ]),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createDocument($collection, new Document([
            '$id' => ID::custom('seed'),
            'title' => 'original',
        ]));

        // Prime the read cache so the next read would normally come from cache.
        $database->getDocument($collection, 'seed');

        return $database;
    }

    public function testCacheFailOpenOnRead(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        try {
            $database->setCache($this->buildBrokenCache());

            // getDocument must fall back to the database and return the document.
            $doc = $database->getDocument($collection, 'seed');
            $this->assertFalse($doc->isEmpty());
            $this->assertEquals('original', $doc->getAttribute('title'));
        } finally {
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testCacheFailOpenOnCreate(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        try {
            $database->setCache($this->buildBrokenCache());

            // createDocument must persist to the database even if the cache save fails.
            $doc = $database->createDocument($collection, new Document([
                '$id' => ID::custom('new'),
                'title' => 'created',
            ]));
            $this->assertFalse($doc->isEmpty());
            $this->assertEquals('created', $doc->getAttribute('title'));
        } finally {
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testCacheFailOpenOnUpdate(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        try {
            $database->setCache($this->buildBrokenCache());

            // updateDocument must persist to the database even if the cache purge fails.
            $doc = $database->updateDocument($collection, 'seed', new Document([
                '$id' => 'seed',
                'title' => 'updated',
            ]));
            $this->assertEquals('updated', $doc->getAttribute('title'));
        } finally {
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testCacheFailOpenOnDelete(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        try {
            $database->setCache($this->buildBrokenCache());

            // deleteDocument must remove the row from the database even if the cache purge fails.
            $result = $database->deleteDocument($collection, 'seed');
            $this->assertTrue($result);

            // Restore working cache, evict the stale entry that couldn't be purged
            // while the cache was broken, then confirm the row is gone in the DB.
            $database->setCache($originalCache);
            $database->purgeCachedDocument($collection, 'seed');
            $this->assertTrue($database->getDocument($collection, 'seed')->isEmpty());
        } finally {
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testCacheFailOpenPurgeCachedDocument(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        try {
            $database->setCache($this->buildBrokenCache());

            // purgeCachedDocument must return false and must not throw.
            $result = $database->purgeCachedDocument($collection, 'seed');
            $this->assertFalse($result);
        } finally {
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testCacheFailOpenPurgeCachedCollection(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        try {
            $database->setCache($this->buildBrokenCache());

            // purgeCachedCollection must return false and must not throw.
            $result = $database->purgeCachedCollection($collection);
            $this->assertFalse($result);
        } finally {
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testCacheFailureEmitsPurgeFailureEvent(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        $failures = 0;
        $database->on(Database::EVENT_CACHE_PURGE_FAILURE, 'test-listener', function () use (&$failures) {
            $failures++;
        });

        try {
            $database->setCache($this->buildBrokenCache());

            // These operations touch the write-path cache and should fire EVENT_CACHE_PURGE_FAILURE.
            $database->updateDocument($collection, 'seed', new Document(['$id' => 'seed', 'title' => 'x'])); // purge fails
            $database->purgeCachedDocument($collection, 'seed'); // purge fails

            $this->assertGreaterThan(0, $failures, 'EVENT_CACHE_PURGE_FAILURE was never emitted');
        } finally {
            $database->on(Database::EVENT_CACHE_PURGE_FAILURE, 'test-listener', null);
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testCacheReadFailureEmitsReadFailureEvent(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        $readFailures = 0;
        $purgeFailures = 0;

        $database->on(Database::EVENT_CACHE_READ_FAILURE, 'test-read-listener', function () use (&$readFailures) {
            $readFailures++;
        });
        $database->on(Database::EVENT_CACHE_PURGE_FAILURE, 'test-purge-listener', function () use (&$purgeFailures) {
            $purgeFailures++;
        });

        try {
            $database->setCache($this->buildBrokenCache());

            // getDocument with a broken cache must emit EVENT_CACHE_READ_FAILURE, not EVENT_CACHE_PURGE_FAILURE.
            $doc = $database->getDocument($collection, 'seed');
            $this->assertFalse($doc->isEmpty(), 'getDocument must fall back to DB when cache is broken');
            $this->assertGreaterThan(0, $readFailures, 'EVENT_CACHE_READ_FAILURE was never emitted');
            $this->assertEquals(0, $purgeFailures, 'getDocument must not emit EVENT_CACHE_PURGE_FAILURE on a read miss');
        } finally {
            $database->on(Database::EVENT_CACHE_READ_FAILURE, 'test-read-listener', null);
            $database->on(Database::EVENT_CACHE_PURGE_FAILURE, 'test-purge-listener', null);
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testThrowingCachePurgeFailureListenerDoesNotPropagate(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        $database->on(Database::EVENT_CACHE_PURGE_FAILURE, 'throwing-listener', function () {
            throw new RuntimeException('Listener exploded');
        });

        try {
            $database->setCache($this->buildBrokenCache());

            // The listener throws, but the operation must still complete without propagating the exception.
            $doc = $database->updateDocument($collection, 'seed', new Document(['$id' => 'seed', 'title' => 'updated']));
            $this->assertEquals('updated', $doc->getAttribute('title'));

            $result = $database->purgeCachedDocument($collection, 'seed');
            $this->assertFalse($result); // returns false when cache is broken, but does not throw

            $deleted = $database->deleteDocument($collection, 'seed');
            $this->assertTrue($deleted);
        } finally {
            $database->on(Database::EVENT_CACHE_PURGE_FAILURE, 'throwing-listener', null);
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testThrowingCacheReadFailureListenerDoesNotPropagate(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        $database->on(Database::EVENT_CACHE_READ_FAILURE, 'throwing-listener', function () {
            throw new RuntimeException('Read listener exploded');
        });

        try {
            $database->setCache($this->buildBrokenCache());

            // The listener throws, but getDocument must still fall back to DB without propagating.
            $doc = $database->getDocument($collection, 'seed');
            $this->assertFalse($doc->isEmpty());
            $this->assertEquals('original', $doc->getAttribute('title'));
        } finally {
            $database->on(Database::EVENT_CACHE_READ_FAILURE, 'throwing-listener', null);
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testThrowingDocumentPurgeListenerDoesNotPropagate(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);

        $database->on(Database::EVENT_DOCUMENT_PURGE, 'throwing-listener', function () {
            throw new RuntimeException('Purge listener exploded');
        });

        try {
            // purgeCachedDocument must succeed even when the EVENT_DOCUMENT_PURGE listener throws.
            $result = $database->purgeCachedDocument($collection, 'seed');
            $this->assertTrue($result);
        } finally {
            $database->on(Database::EVENT_DOCUMENT_PURGE, 'throwing-listener', null);
            $database->deleteCollection($collection);
        }
    }

    public function testUpdateDocumentPersistsDespiteBrokenCachePurge(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        try {
            $database->setCache($this->buildBrokenCache());

            // The cache purge now happens outside the DB transaction.
            // A broken cache must never roll back the write.
            $doc = $database->updateDocument($collection, 'seed', new Document([
                '$id' => 'seed',
                'title' => 'persisted',
            ]));
            $this->assertEquals('persisted', $doc->getAttribute('title'));

            // Restore cache and confirm the DB row was actually written.
            $database->setCache($originalCache);
            $database->purgeCachedDocument($collection, 'seed');
            $fresh = $database->getDocument($collection, 'seed');
            $this->assertEquals('persisted', $fresh->getAttribute('title'));
        } finally {
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }

    public function testDeleteDocumentPersistsDespiteBrokenCachePurge(): void
    {
        $collection = __FUNCTION__;
        $database = $this->seedCacheFailOpenCollection($collection);
        $originalCache = $database->getCache();

        try {
            $database->setCache($this->buildBrokenCache());

            // The cache purge now happens outside the DB transaction.
            // A broken cache must never prevent the delete from committing.
            $deleted = $database->deleteDocument($collection, 'seed');
            $this->assertTrue($deleted);

            // Restore cache, evict any stale entry, then confirm the row is gone.
            $database->setCache($originalCache);
            $database->purgeCachedDocument($collection, 'seed');
            $this->assertTrue($database->getDocument($collection, 'seed')->isEmpty());
        } finally {
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }
}
