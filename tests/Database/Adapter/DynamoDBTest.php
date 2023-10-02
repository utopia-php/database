<?php

namespace Utopia\Tests\Adapter;

use Exception;
use Redis;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\DynamoDB;
use Utopia\Database\Database;
use Aws\DynamoDb\DynamoDbClient as Client;
use Utopia\Tests\Base;

class DynamoDBTest extends Base
{
    public static ?Database $database = null;


    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "dynamodb";
    }

    /**
     * @return Database
     * @throws Exception
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $schema = 'utopiaTests'; // same as $this->testDatabase
        $client = new Client(['region' => 'us-west-2', 'version' => 'latest', 'endpoint' => 'http://dynamodb:8012']);

        $database = new Database(new DynamoDB($client), $cache);
        $database->setDefaultDatabase($schema);
        $database->setNamespace('myapp_' . uniqid());

        if ($database->exists('utopiaTests')) {
            $database->delete('utopiaTests');
        }

        $database->create();

        return self::$database = $database;
    }

}
