<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Redis;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Tests\Base;

class MariaDBTest extends Base
{
    /**
     * @var Database
     */
    static $database = null;

    // TODO@kodumbeats hacky way to identify adapters for tests
    // Remove once all methods are implemented
    /**
     * Return name of adapter
     *
     * @return string
     */
    static function getAdapterName(): string
    {
        return "mariadb";
    }

    /**
     * @return Database
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

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new MariaDB($pdo), $cache);
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }

    public function testCreateDocuments()
    {
        static::getDatabase()->createCollection('documents');

        // $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'string', Database::VAR_STRING, 128, true));
        // $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'integer', Database::VAR_INTEGER, 0, true));
        // $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'bigint', Database::VAR_INTEGER, 8, true));
        // $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'float', Database::VAR_FLOAT, 0, true));
        // $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'boolean', Database::VAR_BOOLEAN, 0, true));
        // $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'colors', Database::VAR_STRING, 32, true, null, true, true));
        // $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'empty', Database::VAR_STRING, 32, false, null, true, true));
        // $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'with-dash', Database::VAR_STRING, 128, false, null));


        // // Create an array of documents with random attributes. Dont use the createDocument function
        // $documents = [];

        // for ($i = 0; $i < 5; $i++) {
        //     $documents[] = new Document([
        //         '$permissions' => [
        //             Permission::read(Role::any()),
        //             Permission::create(Role::any()),
        //             Permission::update(Role::any()),
        //             Permission::delete(Role::any()),
        //         ],
        //         'string' => 'text📝',
        //         'integer' => 5,
        //         'bigint' => 8589934592, // 2^33
        //         'float' => 5.55,
        //         'boolean' => true,
        //         'colors' => ['pink', 'green', 'blue'],
        //         'empty' => [],
        //         'with-dash' => 'Works',
        //     ]);
        // }

        // static::getDatabase()->createDocuments('documents', $documents);

        // $document = $documents[0];

        // $this->assertNotEmpty(true, $document->getId());
        // $this->assertIsString($document->getAttribute('string'));
        // $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
        // $this->assertIsInt($document->getAttribute('integer'));
        // $this->assertEquals(5, $document->getAttribute('integer'));
        // $this->assertIsInt($document->getAttribute('bigint'));
        // $this->assertEquals(8589934592, $document->getAttribute('bigint'));
        // $this->assertIsFloat($document->getAttribute('float'));
        // $this->assertEquals(5.55, $document->getAttribute('float'));
        // $this->assertIsBool($document->getAttribute('boolean'));
        // $this->assertEquals(true, $document->getAttribute('boolean'));
        // $this->assertIsArray($document->getAttribute('colors'));
        // $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));
        // $this->assertEquals([], $document->getAttribute('empty'));
        // $this->assertEquals('Works', $document->getAttribute('with-dash'));
    }
}