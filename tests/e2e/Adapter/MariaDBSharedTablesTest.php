<?php

namespace Tests\E2E\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;

class MariaDBSharedTablesTest extends Base
{
    protected static ?Database $database = null;
    protected static string $namespace;

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
    public static function getDatabase(bool $fresh = false): Database
    {
        if (!is_null(self::$database) && !$fresh) {
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
        $database->setDatabase('utopiaTests');
        $database->setSharedTables(true);
        $database->setTenant(999);
        $database->setNamespace(static::$namespace = '');

        if ($database->exists('utopiaTests')) {
            $database->delete('utopiaTests');
        }

        $database->create();

        return self::$database = $database;
    }
}
