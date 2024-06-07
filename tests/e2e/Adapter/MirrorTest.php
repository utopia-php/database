<?php

namespace Tests\E2E\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Exception\Authorization;
use Utopia\Database\Exception\Conflict;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Mirror;

class MirrorTest extends Base
{
    protected static ?Mirror $database = null;
    protected static Database $source;
    protected static Database $destination;

    protected static string $namespace;

    /**
     * @throws \RedisException
     * @throws Exception
     */
    protected static function getDatabase(bool $fresh = false): Mirror
    {
        if (!is_null(self::$database) && !$fresh) {
            return self::$database;
        }

        $dbHost = 'mariadb';
        $dbPort = '3306';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());
        $redis = new Redis();
        $redis->connect('redis');
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        self::$source = new Database(new MariaDB($pdo), $cache);

        $mirrorHost = 'mariadb-mirror';
        $mirrorPort = '3306';
        $mirrorUser = 'root';
        $mirrorPass = 'password';

        $mirrorPdo = new PDO("mysql:host={$mirrorHost};port={$mirrorPort};charset=utf8mb4", $mirrorUser, $mirrorPass, MariaDB::getPDOAttributes());

        self::$destination = new Database(new MariaDB($mirrorPdo), $cache);

        $database = new Mirror(self::$source, self::$destination);

        $database
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }

    protected static function getAdapterName(): string
    {
        return "Mirror";
    }

    /**
     * @throws Exception
     * @throws \RedisException
     */
    public function testGetMirrorSource(): void
    {
        $database = self::getDatabase();
        $source = $database->getSource();
        $this->assertInstanceOf(Database::class, $source);
        $this->assertEquals(self::$source, $source);
    }

    /**
     * @throws Exception
     * @throws \RedisException
     */
    public function testGetMirrorDestination(): void
    {
        $database = self::getDatabase();
        $destination = $database->getDestination();
        $this->assertInstanceOf(Database::class, $destination);
        $this->assertEquals(self::$destination, $destination);
    }

    /**
     * @throws Limit
     * @throws Duplicate
     * @throws Exception
     * @throws \RedisException
     */
    public function testCreateMirroredCollection(): void
    {
        $database = self::getDatabase();

        $database->createCollection('testCreateMirroredCollection');

        // Assert collection exists in both databases
        $this->assertFalse($database->getSource()->getCollection('testCreateMirroredCollection')->isEmpty());
        $this->assertFalse($database->getDestination()->getCollection('testCreateMirroredCollection')->isEmpty());
    }

    /**
     * @throws Limit
     * @throws Duplicate
     * @throws \RedisException
     * @throws Conflict
     * @throws Exception
     */
    public function testUpdateMirroredCollection(): void
    {
        $database = self::getDatabase();

        $database->createCollection('testUpdateMirroredCollection', permissions: [
            Permission::read(Role::any()),
        ]);

        $collection = $database->getCollection('testUpdateMirroredCollection');

        $database->updateCollection(
            'testUpdateMirroredCollection',
            [
                Permission::read(Role::users()),
            ],
            $collection->getAttribute('documentSecurity')
        );

        // Asset both databases have updated the collection
        $this->assertEquals(
            [Permission::read(Role::users())],
            $database->getSource()->getCollection('testUpdateMirroredCollection')->getPermissions()
        );

        $this->assertEquals(
            [Permission::read(Role::users())],
            $database->getDestination()->getCollection('testUpdateMirroredCollection')->getPermissions()
        );
    }

    public function testDeleteMirroredCollection(): void
    {
        $database = self::getDatabase();

        $database->createCollection('testDeleteMirroredCollection');

        $database->deleteCollection('testDeleteMirroredCollection');

        // Assert collection is deleted in both databases
        $this->assertTrue($database->getSource()->getCollection('testDeleteMirroredCollection')->isEmpty());
        $this->assertTrue($database->getDestination()->getCollection('testDeleteMirroredCollection')->isEmpty());
    }

    /**
     * @throws Authorization
     * @throws Duplicate
     * @throws \RedisException
     * @throws Limit
     * @throws Structure
     * @throws Exception
     */
    public function testCreateMirroredDocument(): void
    {
        $database = self::getDatabase();

        $database->createCollection('testCreateMirroredDocument', attributes: [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'required' => true,
                'size' => Database::LENGTH_KEY,
            ]),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false);

        $document = $database->createDocument('testCreateMirroredDocument', new Document([
            'name' => 'Jake',
            '$permissions' => []
        ]));

        // Assert document is created in both databases
        $this->assertEquals(
            $document,
            $database->getSource()->getDocument('testCreateMirroredDocument', $document->getId())
        );

        $this->assertEquals(
            $document,
            $database->getDestination()->getDocument('testCreateMirroredDocument', $document->getId())
        );
    }

    /**
     * @throws Authorization
     * @throws Duplicate
     * @throws \RedisException
     * @throws Conflict
     * @throws Limit
     * @throws Structure
     * @throws Exception
     */
    public function testUpdateMirroredDocument(): void
    {
        $database = self::getDatabase();

        $database->createCollection('testUpdateMirroredDocument', attributes: [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'required' => true,
                'size' => Database::LENGTH_KEY,
            ]),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ], documentSecurity: false);

        $document = $database->createDocument('testUpdateMirroredDocument', new Document([
            'name' => 'Jake',
            '$permissions' => []
        ]));

        $document = $database->updateDocument(
            'testUpdateMirroredDocument',
            $document->getId(),
            $document->setAttribute('name', 'John')
        );

        // Assert document is updated in both databases
        $this->assertEquals(
            $document,
            $database->getSource()->getDocument('testUpdateMirroredDocument', $document->getId())
        );

        $this->assertEquals(
            $document,
            $database->getDestination()->getDocument('testUpdateMirroredDocument', $document->getId())
        );
    }

    public function testDeleteMirroredDocument(): void
    {
        $database = self::getDatabase();

        $database->createCollection('testDeleteMirroredDocument', attributes: [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'required' => true,
                'size' => Database::LENGTH_KEY,
            ]),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::delete(Role::any()),
        ], documentSecurity: false);

        $document = $database->createDocument('testDeleteMirroredDocument', new Document([
            'name' => 'Jake',
            '$permissions' => []
        ]));

        $database->deleteDocument('testDeleteMirroredDocument', $document->getId());

        // Assert document is deleted in both databases
        $this->assertTrue($database->getSource()->getDocument('testDeleteMirroredDocument', $document->getId())->isEmpty());
        $this->assertTrue($database->getDestination()->getDocument('testDeleteMirroredDocument', $document->getId())->isEmpty());
    }
}
