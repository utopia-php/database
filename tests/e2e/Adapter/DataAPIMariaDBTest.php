<?php

namespace Tests\E2E\Adapter;

use Utopia\Cache\Adapter\None;
use Utopia\Database\Database;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\DataAPIMariaDB;

class DataAPIMariaDBTest extends Base
{
    public static ?Database $database = null;

    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "database-proxy-mariadb";
    }

    /**
     * @return Database
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $database = new Database(new DataAPIMariaDB('http://database-proxy/v1', 'test-secret', 'default'), new Cache(new None()));
        $database->setDatabase('utopiaTests');
        $database->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists('utopiaTests')) {
            $database->delete('utopiaTests');
        }

        $database->create();

        return self::$database = $database;
    }
}
