<?php

namespace Tests\E2E\Adapter\SharedTables;

use Redis;
use Tests\E2E\Adapter\Base;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\PDO;

class SQLiteTest extends Base
{
    public static ?Database $database = null;
    public static ?PDO $pdo = null;
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

        $redis = new Redis();
        $redis->connect('redis');
        $redis->flushAll();

        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new SQLite($pdo), $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setSharedTables(true)
            ->setTenant(999)
            ->setNamespace(static::$namespace = '');

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
