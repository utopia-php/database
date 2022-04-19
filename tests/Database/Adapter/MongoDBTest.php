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
        return 500;
    }

    /**
     * @return Adapter
     * /
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
}
*/
