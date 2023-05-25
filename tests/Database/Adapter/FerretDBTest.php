<?php

namespace Utopia\Tests\Adapter;

use Exception;
use Redis;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\Ferret;
use Utopia\Database\Database;
use Utopia\Mongo\Client;

class FerretDBTest extends TestCase
{
    public static ?Database $database = null;


    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "ferretdb";
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
        $client = new Client(
            $schema,
            'ferretdb',
            27017,
            '',
            '',
            false
        );

        $database = new Database(new Ferret($client), $cache);
        $database->setDefaultDatabase($schema);
        $database->setNamespace('myapp_' . uniqid());

        if ($database->exists('utopiaTests')) {
            $database->delete('utopiaTests');
        }

        $database->create();

        return self::$database = $database;
    }

    public function testTrue(): void
    {
        $this->assertTrue(true);
    }
}
