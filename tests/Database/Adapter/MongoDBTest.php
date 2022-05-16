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

/*
class MongoDBTest extends Base
{
    static $pool = null;

    /**
     * @var Database
     * /
    static $database = null;

    public function setUp(): void {
      // super::setUp(); 

      self::getPool();
      self::setupDB();
    }

    static function getPool() {
      if(self::$pool) {
        return self::$pool;
      }

      MongoDBTest::$pool = new \Swoole\ConnectionPool(function() {
        $options = new MongoClientOptions(
          'utopia_testing',
          'mongo',
          27017,
          'root',
          'example'
      );

        $client = new MongoClient($options);

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

  
        return $client;
      });

      
      return self::$pool;
    }

    static function setupDB() {
      $pool = self::getPool();
      
      $db = null;

      $res = \Swoole\Coroutine\run(function() use ($pool) {
        
        $client = $pool->get();

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $client = self::getPool()->get();
        $database = new Database(new MongoDBAdapter($client), $cache);
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_'.uniqid());

        
        $pool->put($client);

        $db = $database;

        \Swoole\Coroutine\Client\close();
      });

      var_dump($db);

      while($db == null){}

      //self::$database = $channel->pop();
    }

    // TODO@kodumbeats hacky way to identify adapters for tests
    // Remove once all methods are implemented
    /**
     * Return name of adapter
     *
     * @return string
     * /
    static function getAdapterName(): string
    {
        return "mongodb";
    }

    /**
     * Return row limit of adapter
     *
     * @return int
     * /
    static function getAdapterRowLimit(): int
    {
        return MongoDB::getRowLimit();
    }

    /**
     * @return Adapter
     * /
    static function getDatabase(): Database
    {
        // if(!is_null(self::$database)) {
        //     return self::$database;
        // }
        
        // $redis = new Redis();
        // $redis->connect('redis', 6379);
        // $redis->flushAll();
        // $cache = new Cache(new RedisAdapter($redis));

        // $client = self::getPool()->get();
        // $database = new Database(new MongoDBAdapter($client), $cache);
        // $database->setDefaultDatabase('utopiaTests');
        // $database->setNamespace('myapp_'.uniqid());


        // self::getPool()->put($client);

        // return self::$database = $database;
        return self::$database;
    }
}
*/
