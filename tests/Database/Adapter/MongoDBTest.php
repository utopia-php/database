<?php

namespace Utopia\Tests\Adapter;

use MongoDB\Client;
use PDO;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Tests\Base;

class MongoDBTest extends Base
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

        $client = new Client('mongodb://mongo/',
            [
                'username' => 'root',
                'password' => 'example',
            ],
        );

        $database = new Database(new MongoDB($client));
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }
}