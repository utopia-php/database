<?php

namespace Tests\E2E\Adapter\Schemaless;

use Exception;
use Redis;
use Tests\E2E\Adapter\Base;
use Tests\E2E\Adapter\Scopes\MongoReadFilterTests;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Mongo\Client;

class MongoDBTest extends Base
{
    use MongoReadFilterTests;

    public static ?Database $database = null;

    #[\Override]
    protected static string $namespace;

    /**
     * Return name of adapter
     */
    public static function getAdapterName(): string
    {
        return 'mongodb';
    }

    /**
     * @throws Exception
     */
    #[\Override]
    public function getDatabase(): Database
    {
        if (! is_null(self::$database)) {
            return self::$database;
        }

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->select(12);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $schema = $this->testDatabase;
        $client = new Client(
            $schema,
            'mongo',
            27017,
            'root',
            'password',
            false
        );

        $database = new Database(new Mongo($client), $cache);
        $database->setSchemaless(true);
        assert(self::$authorization !== null);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($schema)
            ->setNamespace(static::$namespace = 'myapp_'.uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }

    /**
     * @throws Exception
     */
    #[\Override]
    public function testCreateExistsDelete(): void
    {
        $database = $this->getDatabase();

        $this->assertTrue($database->create());
        $this->assertTrue($database->exists($this->testDatabase));
        $this->assertFalse($database->exists($this->testDatabase.'Absent'));
        $this->assertTrue($database->delete($this->testDatabase));
        $this->assertFalse($database->exists($this->testDatabase));
        $this->assertTrue($database->create());
        $this->assertTrue($database->exists($this->testDatabase));
        $this->assertSame($database, $database->setDatabase($this->testDatabase));
    }

    #[\Override]
    protected function deleteColumn(string $collection, string $column): bool
    {
        return true;
    }

    #[\Override]
    protected function deleteIndex(string $collection, string $index): bool
    {
        return true;
    }
}
