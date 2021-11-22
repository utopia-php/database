<?php

namespace Utopia\Tests\Adapter;

use Redis;
use MongoDB\Client;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Tests\Base;

class MongoDBTest extends Base
{
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
        return MongoDB::getRowLimit();
    }

    /**
     * @return Adapter
     */
    static function getDatabase(): Database
    {
        if(!is_null(self::$database)) {
            return self::$database;
        }

        $options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
        $client = new Client('mongodb://'.(getenv('MONGO_HOST') ?: 'mongo').'/',
            [
                'username' => getenv('MONGO_USER') ?: 'root',
                'password' => getenv('MONGO_PASS') ?: 'example',
            ],
            $options
        );

        $redisHost = getenv('REDIS_HOST') ?: 'redis';

        $redis = new Redis();
        $redis->connect($redisHost, 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new MongoDB($client), $cache);
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }
}