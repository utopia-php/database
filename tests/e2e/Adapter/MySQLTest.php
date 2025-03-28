<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Database;
use Utopia\Database\Exception;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Limit;
use Utopia\Database\PDO;

class MySQLTest extends Base
{
    public static ?Database $database = null;
    protected static ?PDO $pdo = null;
    protected static string $namespace;

    /**
     * @return Database
     * @throws Duplicate
     * @throws Exception
     * @throws Limit
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
        $sqlTable = "`" . self::getDatabase()->getDatabase() . "`.`" . self::getDatabase()->getNamespace() . "_" . $collection . "`";
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        self::$pdo->exec($sql);

        return true;
    }

    protected static function deleteIndex(string $collection, string $index): bool
    {
        $sqlTable = "`" . self::getDatabase()->getDatabase() . "`.`" . self::getDatabase()->getNamespace() . "_" . $collection . "`";
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        self::$pdo->exec($sql);

        return true;
    }
}
