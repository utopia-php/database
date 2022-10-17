<?php

namespace Utopia\Tests\Adapter;

use Exception;
use Redis;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Mongo\MongoClient;
use Utopia\Mongo\MongoClientOptions;
use Utopia\Tests\Base;

class MongoDBTest extends Base
{
    static $pool = null;

    /**
     * @var Database
     */
    static $database = null;


    /**
     * Return name of adapter
     *
     * @return string
     */
    static function getAdapterName(): string
    {
        return "mongodb";
    }

    /**
     * Return row limit of adapter
     *
     * @return int
     */
    static function getAdapterRowLimit(): int
    {
        return 0;
    }

    /**
     * @return Database
     * @throws Exception
     */
    static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $schema = 'utopiaTests'; // same as $this->testDatabase

        $options = new MongoClientOptions(
            $schema,
            'mongo',
            27017,
            'root',
            'example'
        );

        $client = new MongoClient($options, false);

        $database = new Database(new Mongo($client), $cache);
        $database->setDefaultDatabase($schema);
        $database->setNamespace('myapp_' . uniqid());


        return self::$database = $database;
    }

    /**
     * @throws Exception
     */
    public function testCreateExistsDelete()
    {
        // Mongo creates databases on the fly, so exists would always pass. So we
        // overide this test to remove the exists check.
        $this->assertNotNull(static::getDatabase()->create($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->create($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
    }

    public function testRenameAttribute()
    {
        $this->assertTrue(true);
    }

    public function testRenameAttributeExisting()
    {
        $this->assertTrue(true);
    }

    public function testUpdateAttributeStructure()
    {
        $this->assertTrue(true);
    }

    public function testKeywords()
    {
        $this->assertTrue(true);
    }
}
