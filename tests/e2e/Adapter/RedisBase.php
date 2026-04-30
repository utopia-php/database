<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisCacheAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Database;

/**
 * Shared base for the Redis adapter test suites. Provisions a fresh,
 * isolated namespace per test method and tears it down via SCAN/DEL so
 * concurrent tests sharing the same Redis instance never clobber each
 * other.
 *
 * The two concrete subclasses (`RedisTest`, `SharedTables\RedisTest`)
 * inherit the full Base scope coverage and only differ in shared-tables
 * configuration applied in their own `setUp()`.
 */
abstract class RedisBase extends Base
{
    protected ?Database $database = null;
    protected ?Redis $redisClient = null;
    protected string $redisNamespace = '';

    public static function getAdapterName(): string
    {
        return 'redis';
    }

    protected function getRedisClient(): Redis
    {
        if ($this->redisClient instanceof Redis) {
            return $this->redisClient;
        }

        $host = \getenv('REDIS_HOST') ?: 'redis-mirror';
        $port = (int) (\getenv('REDIS_PORT') ?: 6379);

        $client = new Redis();
        $client->connect($host, $port);

        return $this->redisClient = $client;
    }

    protected function makeNamespace(): string
    {
        return 'utopia_test_' . \uniqid();
    }

    public function setUp(): void
    {
        parent::setUp();

        $this->redisNamespace = $this->makeNamespace();

        $cacheRedis = new Redis();
        $cacheRedis->connect('redis', 6379);
        $cache = new Cache(new RedisCacheAdapter($cacheRedis));

        $adapter = new RedisAdapter($this->getRedisClient());

        $database = new Database($adapter, $cache);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace($this->redisNamespace);

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        $this->database = $database;
    }

    public function tearDown(): void
    {
        try {
            if ($this->redisNamespace !== '' && $this->redisClient instanceof Redis) {
                $client = $this->redisClient;
                $iterator = null;
                $pattern = $this->redisNamespace . ':*';
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
            $this->database = null;
            $this->redisClient = null;
            $this->redisNamespace = '';
            parent::tearDown();
        }
    }

    public function getDatabase(): Database
    {
        if ($this->database === null) {
            throw new \RuntimeException('Database not initialised — setUp() must run first.');
        }
        return $this->database;
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
}
