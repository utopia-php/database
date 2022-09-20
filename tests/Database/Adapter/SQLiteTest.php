<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Cache;
use Utopia\Database\Database;
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
        return SQLite::getCountOfDefaultIndexes();
    }

    /**
     * @return Database
     */
    static function getDatabase(): Database
    {
        if(!is_null(self::$database)) {
            return self::$database;
        }

        $sqliteDir = __DIR__."/database.sql";

        if(file_exists($sqliteDir)) {
            unlink($sqliteDir);
        }

        $pdo = new PDO("sqlite:".$sqliteDir, null, null, SQLite::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();

        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new SQLite($pdo), $cache);
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }
}