<?php

namespace Utopia\Tests\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Redis;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Utopia\App;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Tests\Base;

class SwooleMariaDB extends TestCase
{
    public static ?Database $database = null;

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
        global $pdo;
        $cache = new Cache(new NoCache());
        return new Database(new MariaDB($pdo), $cache);
    }

    public function testPing(): void
    {
        $this->assertEquals(true, static::getDatabase()->ping());
    }

}
