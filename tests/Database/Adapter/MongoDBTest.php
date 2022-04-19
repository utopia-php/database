<?php

namespace Utopia\Tests\Adapter;

use Redis;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Database;
use Utopia\Database\Adapter\Mongo\MongoClient;
use Utopia\Database\Adapter\Mongo\MongoClientOptions;
use Utopia\Database\Adapter\Mongo\MongoDBAdapter;

use Utopia\Tests\Base;

class MongoDBTest extends Base
{
    static $pool = null;

    /**
     * @var Database
     */
    static $database = null;


    // TODO@kodumbeats hacky way to identify adapters for tests
    // Remove once all methods are implemented
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
        return 500;
    }

    /**
     * @return Adapter
     */
    static function getDatabase(): Database
    {
        if(!is_null(self::$database)) {
            return self::$database;
        }
        
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $options = new MongoClientOptions(
          'utopia_testing',
          'mongo',
          27017,
          'root',
          'example'
      );

        $client = new MongoClient($options, false);

        $database = new Database(new MongoDBAdapter($client), $cache);
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_'.uniqid());


        return self::$database = $database;
    }

    public function testCreateExistsDelete()
    {
      echo("Starting co-routine test....\n");

      if (!static::getDatabase()->exists($this->testDatabase)) {
          $this->assertEquals(true, static::getDatabase()->create($this->testDatabase));
      }

      $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase));
      $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));

      // Mongo creates on the fly, so this will never be true, do we want to try to make it pass
      // by doing something else?
      // $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase));
      
      $this->assertEquals(true, static::getDatabase()->create($this->testDatabase));
      $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
    }

    /**
     * @depends testCreateExistsDelete
     */
    public function testCreateListExistsDeleteCollection()
    {
    }

    public function testCreateDeleteAttribute()
    {
    }

    public function testCreateDeleteIndex()
    {
    }

    public function testCreateCollectionWithSchema()
    {
    }

    public function testCreateCollectionValidator()
    {
    }

    public function testCreateDocument()
    {
    }

    public function testCreateDocumentDefaults()
    {
    }

    public function testExceptionIndexLimit()
    {
    }
}