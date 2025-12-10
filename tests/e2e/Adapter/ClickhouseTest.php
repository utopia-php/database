<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Clickhouse;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

class ClickhouseTest extends TestCase
{
    private static ?Database $database = null;
    private static ?Authorization $authorization = null;
    private static string $namespace;

    protected function setUp(): void
    {
        if (is_null(self::$authorization)) {
            self::$authorization = new Authorization();
        }
        self::$authorization->addRole('any');
    }

    protected function tearDown(): void
    {
        self::$authorization?->reset();
    }

    protected function getDatabase(bool $fresh = false): Database
    {
        if (!is_null(self::$database) && !$fresh) {
            return self::$database;
        }

        $adapter = new Clickhouse(
            endpoint: 'http://clickhouse:8123',
            username: 'default',
            password: 'password',
            database: 'utopiaTests'
        );

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database($adapter, $cache);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace(self::$namespace = 'ch_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }

    public function testPing(): void
    {
        $this->assertTrue($this->getDatabase()->ping());
    }

    public function testCreateCollectionAndCrud(): void
    {
        $db = $this->getDatabase(true);
        $collection = 'movies';

        $db->createCollection($collection);
        $this->assertTrue($db->exists($db->getDatabase(), $collection));

        $doc = new Document([
            '$id' => 'movie1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::write(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'title' => 'Inception',
            'year' => 2010,
        ]);

        $created = $db->createDocument($collection, $doc);
        $this->assertEquals('movie1', $created->getId());

        $fetched = $db->getDocument($collection, 'movie1');
        $this->assertFalse($fetched->isEmpty());
        $this->assertEquals('Inception', $fetched->getAttribute('title'));

        $found = $db->find($collection);
        $this->assertCount(1, $found);

        $this->assertEquals(1, $db->count($collection));

        $this->assertTrue($db->deleteDocument($collection, 'movie1'));
        $this->assertTrue($db->getDocument($collection, 'movie1')->isEmpty());

        $db->deleteCollection($collection);
        $this->assertFalse($db->exists($db->getDatabase(), $collection));
    }

    public function testCollectionSize(): void
    {
        $db = $this->getDatabase(true);
        $collection = 'sizes';

        $db->createCollection($collection);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        for ($i = 0; $i < 5; $i++) {
            $db->createDocument($collection, new Document([
                '$id' => 'doc' . $i,
                '$permissions' => $permissions,
                'value' => str_repeat('a', 50),
            ]));
        }

        $size = $db->getSizeOfCollection($collection);
        $this->assertGreaterThan(0, $size);

        $db->deleteCollection($collection);
    }
}
