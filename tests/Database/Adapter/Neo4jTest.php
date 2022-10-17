<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Cache;
use Utopia\Database\Database;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\Neo4j;
use Utopia\Tests\Base;

class Neo4jTest extends Base
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
        return "neo4j";
    }

    /**
     * Return row limit of adapter
     *
     * @return int
     */
    static function getAdapterRowLimit(): int
    {
        return Neo4j::getRowLimit();
    }

    /**
     *
     * @return int
     */
    static function getUsedIndexes(): int
    {
        return Neo4j::getCountOfDefaultIndexes();
    }

    /**
     * @return Database
     */
    static function getDatabase(): Database
    {
        if(!is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'localhost';
        $dbPort = '7474';
        $dbUsername = 'neo4j';
        $dbPassword = 'password';

        $client = new Neo4jClient($dbHost, $dbPort, $dbUsername, $dbPassword);

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();

        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new Neo4j($client), $cache);
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }
}