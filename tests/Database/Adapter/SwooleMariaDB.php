<?php

namespace Utopia\Tests\Adapter;

use Faker\Factory;
use PDO;
use Redis;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Tests\Base;

class SwooleMariaDB extends Base
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
        return "mariadb";
    }

    /**
     * @return Database
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'mariadb';
        $dbPort = '3306';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new MariaDB($pdo), $cache);
        $schema = 'shmuel';

        $database->setDefaultDatabase($schema);
        $name = 'myapp_'.uniqid();
        $database->setNamespace($name);

        if ($database->exists($database->getDefaultDatabase())) {
            $database->delete($database->getDefaultDatabase());
        }
        $database->create();

        // reclaim resources
        $database = null;
        $pdo = null;
        // create PDO pool for coroutines
        $pool = new PDOPool(
            (new PDOConfig())
                ->withHost('mariadb')
                ->withPort(3306)
                ->withDbName($schema)
                ->withCharset('utf8mb4')
                ->withUsername('root')
                ->withPassword('password'),
            128
        );

        $pdo = $pool->get();

        $database = new Database(new MariaDB($pdo), $cache);
        $database->setDefaultDatabase($schema);
        $database->setNamespace($name);

        return self::$database = $database;
    }
}
