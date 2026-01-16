<?php

namespace Tests\E2E\Adapter;

use Redis;
use ReflectionClass;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Database;
use Utopia\Database\Exception;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Limit;
use Utopia\Database\PDO;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Pools\Adapter\Stack;

class PoolTest extends Base
{
    public static ?Database $database = null;

    /**
     * @var UtopiaPool<MySQL>
     */
    protected static UtopiaPool $pool;
    protected static string $namespace;

    /**
     * @return Database
     * @throws Exception
     * @throws Duplicate
     * @throws Limit
     */
    public function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $pool = new UtopiaPool(new Stack(), 'mysql', 10, function () {
            $dbHost = 'mysql';
            $dbPort = '3307';
            $dbUser = 'root';
            $dbPass = 'password';

            return new MySQL(new PDO(
                dsn: "mysql:host={$dbHost};port={$dbPort};charset=utf8mb4",
                username: $dbUser,
                password: $dbPass,
                config: MySQL::getPDOAttributes(),
            ));
        });

        $database = new Database(new Pool($pool), $cache);

        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pool = $pool;

        return self::$database = $database;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = "`" . $this->getDatabase()->getDatabase() . "`.`" . $this->getDatabase()->getNamespace() . "_" . $collection . "`";
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        self::$pool->use(function (Adapter $adapter) use ($sql) {
            // Hack to get adapter PDO reference
            $class = new ReflectionClass($adapter);
            $property = $class->getProperty('pdo');
            $property->setAccessible(true);
            $pdo = $property->getValue($adapter);
            $pdo->exec($sql);
        });

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $sqlTable = "`" . $this->getDatabase()->getDatabase() . "`.`" . $this->getDatabase()->getNamespace() . "_" . $collection . "`";
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        self::$pool->use(function (Adapter $adapter) use ($sql) {
            // Hack to get adapter PDO reference
            $class = new ReflectionClass($adapter);
            $property = $class->getProperty('pdo');
            $property->setAccessible(true);
            $pdo = $property->getValue($adapter);
            $pdo->exec($sql);
        });

        return true;
    }
}
