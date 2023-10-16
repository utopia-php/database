<?php

namespace Utopia\Tests\Adapter;

use Utopia\Cache\Adapter\None;
use Utopia\Database\Database;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\ProxyMariaDB;
use Utopia\Tests\Base;

class ProxyMariaDBTest extends Base
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
        return "proxy-mariadb";
    }

    /**
     * @return Database
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $database = new Database(new ProxyMariaDB('http://proxy/v1', 'test-secret', 'default'), new Cache(new None()));
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_' . uniqid());

        if ($database->exists('utopiaTests')) {
            $database->delete('utopiaTests');
        }

        $database->create();

        return self::$database = $database;
    }
}
