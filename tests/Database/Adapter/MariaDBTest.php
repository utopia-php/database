<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Tests\Base;

class MariaDBTest extends Base
{
    /**
     * @var Database
     */
    static $database = null;

    /**
     * @return Adapter
     */
    static function getDatabase(): Database
    {
        if(!is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'mariadb';
        $dbPort = '3306';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, [
            PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4',
            PDO::ATTR_TIMEOUT => 3, // Seconds
            PDO::ATTR_PERSISTENT => true,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $database = new Database(new MariaDB($pdo));
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }
}