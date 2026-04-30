<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisCacheAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Database;

/**
 * Shared base for the Redis adapter test suites. Creates ONE Database
 * instance per test class via a lazy `getDatabase()` so the inherited
 * Base scope tests (which chain state across methods) see a stable
 * collection set, mirroring the `MemoryTest` pattern. The two concrete
 * subclasses (`RedisTest`, `SharedTables\RedisTest`) share the same
 * pattern and only differ in shared-tables configuration.
 */
abstract class RedisBase extends Base
{
    public static ?Database $database = null;
    public static ?Redis $redisClient = null;
    public static string $redisNamespace = '';

    public static function getAdapterName(): string
    {
        return 'redis';
    }

    public function getDatabase(): Database
    {
        if (self::$database !== null) {
            return self::$database;
        }

        if (self::$authorization === null) {
            self::$authorization = new \Utopia\Database\Validator\Authorization();
        }

        $host = \getenv('REDIS_HOST') ?: 'redis-mirror';
        $port = (int) (\getenv('REDIS_PORT') ?: 6379);

        $client = new Redis();
        $client->connect($host, $port);
        self::$redisClient = $client;

        $cacheRedis = new Redis();
        $cacheRedis->connect('redis', 6379);
        $cacheRedis->flushAll();
        $cache = new Cache(new RedisCacheAdapter($cacheRedis));

        $adapter = new RedisAdapter($client);

        self::$redisNamespace = 'utopia_test_' . \uniqid();
        $database = new Database($adapter, $cache);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace(self::$redisNamespace);

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        // Redis keeps no out-of-band schema; raw column drops do not apply.
        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        return true;
    }

    public static function tearDownAfterClass(): void
    {
        try {
            if (self::$redisNamespace !== '' && self::$redisClient instanceof Redis) {
                $client = self::$redisClient;
                $iterator = null;
                // Adapter-produced keys live under `KEY_PREFIX:{ns}:...`. The
                // SCAN pattern must include the prefix or test cleanup leaks
                // every key written during the run.
                $pattern = RedisAdapter::KEY_PREFIX . ':' . self::$redisNamespace . ':*';
                while (($keys = $client->scan($iterator, $pattern, 500)) !== false) {
                    if (\is_array($keys) && \count($keys) > 0) {
                        $client->del($keys);
                    }
                    if ($iterator === 0) {
                        break;
                    }
                }
            }
        } finally {
            self::$database = null;
            self::$redisClient = null;
            self::$redisNamespace = '';
            parent::tearDownAfterClass();
        }
    }
}
