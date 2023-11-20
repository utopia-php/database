<?php

namespace Tests\E2E\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;

class SQLiteTest extends Base
{
    public static ?Database $database = null;
    protected static string $namespace;

    // Remove once all methods are implemented
    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "sqlite";
    }

    /**
     *
     * @return int
     */
    public static function getUsedIndexes(): int
    {
        return SQLite::getCountOfDefaultIndexes();
    }

    /**
     * @return Database
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $db = __DIR__."/database.sql";

        if (file_exists($db)) {
            unlink($db);
        }

        $dsn = $db;
        //$dsn = 'memory'; // Overwrite for fast tests
        $pdo = new PDO("sqlite:" . $dsn, null, null, SQLite::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis');
        $redis->flushAll();

        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new SQLite($pdo), $cache);
        $database->setDatabase('utopiaTests');
        $database->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }
}
