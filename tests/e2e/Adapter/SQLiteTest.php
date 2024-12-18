<?php

namespace Tests\E2E\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;

class SQLiteTest extends Base
{
    public static ?Database $database = null;
    protected static ?PDO $pdo = null;
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

        try {
            $redis = new Redis();
            $redis->connect('redis', 6379);
            $redis->flushAll();
            $cache = new Cache(new RedisAdapter($redis));
        } catch (\Exception $e) {
            $cache = new Cache(new None());
        }

        $database = new Database(new SQLite($pdo), $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;
        return self::$database = $database;
    }

    protected static function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = "`" . self::getDatabase()->getNamespace() . "_" . $collection . "`";
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        self::$pdo->exec($sql);

        return true;
    }

    protected static function deleteIndex(string $collection, string $index): bool
    {
        $index = "`".self::getDatabase()->getNamespace()."_".self::getDatabase()->getTenant()."_{$collection}_{$index}`";
        $sql = "DROP INDEX {$index}";

        self::$pdo->exec($sql);

        return true;
    }
}
