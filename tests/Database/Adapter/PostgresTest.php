<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Utopia\Database\Database;
use Utopia\Database\Adapter\Postgres;
use Utopia\Tests\Base;

class PostgresTest extends Base
{
    /**
     * @var Database
     */
    static $database = null;

    /**
     * @reture Adapter
     */
    static function getDatabase(): Database
    {
        if(!is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'postgres';
        $dbPort = '5432';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};", $dbUser, $dbPass, array(
            PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4',
            PDO::ATTR_TIMEOUT => 3, // Seconds
            PDO::ATTR_PERSISTENT => true
        ));

        // Connection settings
        $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);   // Return arrays
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);        // Handle all errors with exceptions

        $database = new Database(new Postgres($pdo));
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }
}