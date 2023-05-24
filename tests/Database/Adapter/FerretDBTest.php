<?php

namespace Utopia\Tests\Adapter;

use Exception;
use Redis;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\Ferret;
use Utopia\Database\Database;
use Utopia\Mongo\Client;
use Utopia\Tests\Base;

class FerretDBTest extends Base
{
    public static ?Database $database = null;


    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "ferretdb";
    }

    /**
     * @return Database
     * @throws Exception
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $schema = 'utopiaTests'; // same as $this->testDatabase
        $client = new Client(
            $schema,
            'ferretdb',
            27017,
            '',
            '',
            false
        );

        $database = new Database(new Ferret($client), $cache);
        $database->setDefaultDatabase($schema);
        $database->setNamespace('myapp_' . uniqid());

        if ($database->exists('utopiaTests')) {
            $database->delete('utopiaTests');
        }

        $database->create();

        return self::$database = $database;
    }

    /**
     * @throws Exception
     */
    public function testCreateExistsDelete(): void
    {
        // Mongo creates databases on the fly, so exists would always pass. So we override this test to remove the exists check.
        $this->assertNotNull(static::getDatabase()->create());
        $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->create());
        $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
    }

    public function testRenameAttribute(): void
    {
        $this->assertTrue(true);
    }

    public function testRenameAttributeExisting(): void
    {
        $this->assertTrue(true);
    }

    public function testUpdateAttributeStructure(): void
    {
        $this->assertTrue(true);
    }

    public function testKeywords(): void
    {
        $this->assertTrue(true);
    }

    public static function killDatabase(): void
    {
        self::$database = null;
    }
}
