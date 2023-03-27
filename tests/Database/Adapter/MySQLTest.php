<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Cache;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MySQL;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Tests\Base;

class MySQLTest extends Base
{
    public static ?Database $database = null;

    // TODO@kodumbeats hacky way to identify adapters for tests
    // Remove once all methods are implemented
    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "mysql";
    }

    /**
     *
     * @return int
     */
    public static function getUsedIndexes(): int
    {
        return MySQL::getCountOfDefaultIndexes();
    }

    /**
     * @return Database
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'mysql';
        $dbPort = '3307';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MySQL::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();

        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new MySQL($pdo), $cache);
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }
}
