<?php

namespace Tests\E2E\Adapter;

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
use Utopia\Database\PDO;

class MirrorTest extends Base
{
    protected static ?Mirror $database = null;
    protected static ?PDO $destinationPdo = null;
    protected static ?PDO $sourcePdo = null;
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

        self::$sourcePdo = $pdo;
        self::$source = new Database(new MariaDB($pdo), $cache);

        $mirrorHost = 'mariadb-mirror';
        $mirrorPort = '3306';
        $mirrorUser = 'root';
        $mirrorPass = 'password';

        $mirrorPdo = new PDO("mysql:host={$mirrorHost};port={$mirrorPort};charset=utf8mb4", $mirrorUser, $mirrorPass, MariaDB::getPDOAttributes());

        $mirrorRedis = new Redis();
        $mirrorRedis->connect('redis-mirror');
        $mirrorRedis->flushAll();
        $mirrorCache = new Cache(new RedisAdapter($mirrorRedis));

        self::$destinationPdo = $mirrorPdo;
        self::$destination = new Database(new MariaDB($mirrorPdo), $mirrorCache);

        $database = new Mirror(self::$source, self::$destination);

        // Handle cases where the source and destination databases are not in sync because of previous tests
        if ($database->getSource()->exists('schema1')) {
            $database->getSource()->setDatabase('schema1')->delete();
        }
        if ($database->getDestination()->exists('schema1')) {
            $database->getDestination()->setDatabase('schema1')->delete();
        }
        if ($database->getSource()->exists('schema2')) {
            $database->getSource()->setDatabase('schema2')->delete();
        }
        if ($database->getDestination()->exists('schema2')) {
            $database->getDestination()->setDatabase('schema2')->delete();
        }
        if ($database->getSource()->exists('sharedTables')) {
            $database->getSource()->setDatabase('sharedTables')->delete();
        }
        if ($database->getDestination()->exists('sharedTables')) {
            $database->getDestination()->setDatabase('sharedTables')->delete();
        }

        $database
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
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

    protected static function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = "`" . self::$source->getDatabase() . "`.`" . self::$source->getNamespace() . "_" . $collection . "`";
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        self::$sourcePdo->exec($sql);

        $sqlTable = "`" . self::$destination->getDatabase() . "`.`" . self::$destination->getNamespace() . "_" . $collection . "`";
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        self::$destinationPdo->exec($sql);

        return true;
    }

    protected static function deleteIndex(string $collection, string $index): bool
    {
        $sqlTable = "`" . self::$source->getDatabase() . "`.`" . self::$source->getNamespace() . "_" . $collection . "`";
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        self::$sourcePdo->exec($sql);

        $sqlTable = "`" . self::$destination->getDatabase() . "`.`" . self::$destination->getNamespace() . "_" . $collection . "`";
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        self::$destinationPdo->exec($sql);

        return true;
    }
}
