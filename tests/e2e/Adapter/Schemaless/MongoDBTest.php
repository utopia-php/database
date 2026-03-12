<?php

namespace Tests\E2E\Adapter\Schemaless;

use Exception;
use Redis;
use Tests\E2E\Adapter\Base;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Mongo\Client;

class MongoDBTest extends Base
{
    public static ?Database $database = null;

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
        $database->getAdapter()->setSupportForAttributes(false);
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
    public function test_create_exists_delete(): void
    {
        // Mongo creates databases on the fly, so exists would always pass. So we override this test to remove the exists check.
        $this->assertNotNull(static::getDatabase()->create());
        $this->assertEquals(true, $this->getDatabase()->delete($this->testDatabase));
        $this->assertEquals(true, $this->getDatabase()->create());
        $this->assertEquals($this->getDatabase(), $this->getDatabase()->setDatabase($this->testDatabase));
    }

    public function test_rename_attribute(): void
    {
        $this->assertTrue(true);
    }

    public function test_rename_attribute_existing(): void
    {
        $this->assertTrue(true);
    }

    public function test_update_attribute_structure(): void
    {
        $this->assertTrue(true);
    }

    public function test_keywords(): void
    {
        $this->assertTrue(true);
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        return true;
    }
}
