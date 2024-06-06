<?php

namespace Tests\E2E\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;
use Utopia\Database\Exception;
use Utopia\Database\Exception\Conflict;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Limit;
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
    public function testGetSource(): void
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
    public function testGetDestination(): void
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
    public function testCreateCollection(): void
    {
        $database = self::getDatabase();

        $database->createCollection('testCreateCollection');

        // Assert collection exists in both databases
        $this->assertFalse($database->getSource()->getCollection('testCreateCollection')->isEmpty());
        $this->assertFalse($database->getDestination()->getCollection('testCreateCollection')->isEmpty());
    }

    /**
     * @throws Limit
     * @throws Duplicate
     * @throws \RedisException
     * @throws Conflict
     * @throws Exception
     */
    public function testUpdateCollection(): void
    {
        $database = self::getDatabase();

        $database->createCollection('testUpdateCollection', permissions: [
            Permission::read(Role::any()),
        ]);

        $collection = $database->getCollection('testUpdateCollection');

        $database->updateCollection(
            'testUpdateCollection',
            [
                Permission::read(Role::users()),
            ],
            $collection->getAttribute('documentSecurity')
        );

        // Asset both databases have updated the collection
        $this->assertEquals(
            [Permission::read(Role::users())],
            $database->getSource()->getCollection('testUpdateCollection')->getPermissions()
        );

        $this->assertEquals(
            [Permission::read(Role::users())],
            $database->getDestination()->getCollection('testUpdateCollection')->getPermissions()
        );
    }
}
