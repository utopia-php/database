<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Cache;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MySQL;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Tests\Base;

class SQLiteTest extends Base
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
        return "sqlite";
    }

    /**
     * Return row limit of adapter
     *
     * @return int
     */
    static function getAdapterRowLimit(): int
    {
        return SQLite::getRowLimit();
    }

    /**
     *
     * @return int
     */
    static function getUsedIndexes(): int
    {
        return SQLite::getNumberOfDefaultIndexes();
    }

    /**
     * @return Database
     */
    static function getDatabase(): Database
    {
        if(!is_null(self::$database)) {
            return self::$database;
        }

        $pdo = new PDO("sqlite:".__DIR__."/database.sql");

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();

        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new MySQL($pdo), $cache);
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }

    /**
     * Return keywords reserved by database backend
     * Refference: https://mariadb.com/kb/en/reserved-words/
     *
     * @return string[]
     */
    static function getReservedKeywords(): array
    {
        // Same as MariaDB
        return MariaDBTest::getReservedKeywords();
    }
}