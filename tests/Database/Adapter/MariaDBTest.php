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

        $database->create();

        return self::$database = $database;
    }


    public function testCreateDocuments()
    {
        $count = 100000;
        $collection = 'testCreateDocuments';

        static::getDatabase()->createCollection($collection);

        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'bigint', Database::VAR_INTEGER, 8, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'boolean', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'colors', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'empty', Database::VAR_STRING, 32, false, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'with-dash', Database::VAR_STRING, 128, false, null));


        // Create an array of documents with random attributes. Dont use the createDocument function
        $documents = [];

        for ($i = 0; $i < $count; $i++) {
            $documents[] = new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'string' => 'text📝',
                'integer' => 5,
                'bigint' => 8589934592, // 2^33
                'float' => 5.55,
                'boolean' => true,
                'colors' => ['pink', 'green', 'blue'],
                'empty' => [],
                'with-dash' => 'Works',
            ]);
        }

        $res = static::getDatabase()->createDocuments($collection, $documents, 14000);

        $this->assertEquals($count, count($res));

        foreach ($res as $document) {
            $this->assertNotEmpty(true, $document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(8589934592, $document->getAttribute('bigint'));
            $this->assertIsFloat($document->getAttribute('float'));
            $this->assertEquals(5.55, $document->getAttribute('float'));
            $this->assertIsBool($document->getAttribute('boolean'));
            $this->assertEquals(true, $document->getAttribute('boolean'));
            $this->assertIsArray($document->getAttribute('colors'));
            $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));
            $this->assertEquals([], $document->getAttribute('empty'));
            $this->assertEquals('Works', $document->getAttribute('with-dash'));
        }
    }

    // public function testUpdateDocuments(Document $document)
    // {
    //     $document
    //         ->setAttribute('string', 'text📝 updated')
    //         ->setAttribute('integer', 6)
    //         ->setAttribute('float', 5.56)
    //         ->setAttribute('boolean', false)
    //         ->setAttribute('colors', 'red', Document::SET_TYPE_APPEND)
    //         ->setAttribute('with-dash', 'Works');

    //     $new = $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);

    //     $this->assertNotEmpty(true, $new->getId());
    //     $this->assertIsString($new->getAttribute('string'));
    //     $this->assertEquals('text📝 updated', $new->getAttribute('string'));
    //     $this->assertIsInt($new->getAttribute('integer'));
    //     $this->assertEquals(6, $new->getAttribute('integer'));
    //     $this->assertIsFloat($new->getAttribute('float'));
    //     $this->assertEquals(5.56, $new->getAttribute('float'));
    //     $this->assertIsBool($new->getAttribute('boolean'));
    //     $this->assertEquals(false, $new->getAttribute('boolean'));
    //     $this->assertIsArray($new->getAttribute('colors'));
    //     $this->assertEquals(['pink', 'green', 'blue', 'red'], $new->getAttribute('colors'));
    //     $this->assertEquals('Works', $new->getAttribute('with-dash'));

    //     $oldPermissions = $document->getPermissions();

    //     $new
    //         ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
    //         ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND)
    //         ->setAttribute('$permissions', Permission::update(Role::guests()), Document::SET_TYPE_APPEND)
    //         ->setAttribute('$permissions', Permission::delete(Role::guests()), Document::SET_TYPE_APPEND);

    //     $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

    //     $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

    //     $this->assertContains('guests', $new->getRead());
    //     $this->assertContains('guests', $new->getWrite());
    //     $this->assertContains('guests', $new->getCreate());
    //     $this->assertContains('guests', $new->getUpdate());
    //     $this->assertContains('guests', $new->getDelete());

    //     $new->setAttribute('$permissions', $oldPermissions);

    //     $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

    //     $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

    //     $this->assertNotContains('guests', $new->getRead());
    //     $this->assertNotContains('guests', $new->getWrite());
    //     $this->assertNotContains('guests', $new->getCreate());
    //     $this->assertNotContains('guests', $new->getUpdate());
    //     $this->assertNotContains('guests', $new->getDelete());

    //     return $document;
    // }
}