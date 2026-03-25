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

            // Each operation that touches the cache should fire the event at least once.
            $database->getDocument($collection, 'seed');       // load fails
            $database->updateDocument($collection, 'seed', new Document(['$id' => 'seed', 'title' => 'x'])); // purge fails
            $database->purgeCachedDocument($collection, 'seed'); // purge fails

            $this->assertGreaterThan(0, $failures, 'EVENT_CACHE_PURGE_FAILURE was never emitted');
        } finally {
            $database->on(Database::EVENT_CACHE_PURGE_FAILURE, 'test-listener', null);
            $database->setCache($originalCache);
            $database->deleteCollection($collection);
        }
    }
}
