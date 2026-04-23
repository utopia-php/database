<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Attribute;
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
use Utopia\Query\Schema\ColumnType;

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
    protected function getDatabase(bool $fresh = false): Mirror
    {
        if (! is_null(self::$database) && ! $fresh) {
            return self::$database;
        }

        $dbHost = 'mariadb';
        $dbPort = '3306';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis');
        $redis->select(5);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        self::$sourcePdo = $pdo;
        self::$source = new Database(new MariaDB($pdo), $cache);

        $mirrorHost = 'mariadb-mirror';
        $mirrorPort = '3306';
        $mirrorUser = 'root';
        $mirrorPass = 'password';

        $mirrorPdo = new PDO("mysql:host={$mirrorHost};port={$mirrorPort};charset=utf8mb4", $mirrorUser, $mirrorPass, MariaDB::getPDOAttributes());

        $mirrorRedis = new Redis();
        $mirrorRedis->connect('redis-mirror');
        $mirrorRedis->select(5);
        $mirrorCache = new Cache((new RedisAdapter($mirrorRedis))->setMaxRetries(3));

        self::$destinationPdo = $mirrorPdo;
        self::$destination = new Database(new MariaDB($mirrorPdo), $mirrorCache);

        $database = new Mirror(self::$source, self::$destination);

        $token = static::getTestToken();
        $schemas = [
            $this->testDatabase,
            'schema1_'.$token,
            'schema2_'.$token,
            'sharedTables_'.$token,
            'sharedTablesTenantPerDocument_'.$token,
        ];

        /**
         * Handle cases where the source and destination databases are not in sync because of previous tests
         */
        assert(self::$authorization !== null);
        foreach ($schemas as $schema) {
            if ($database->getSource()->exists($schema)) {
                $database->getSource()->setAuthorization(self::$authorization);
                $database->getSource()->setDatabase($schema)->delete();
            }
            $destination = $database->getDestination();
            if ($destination !== null && $destination->exists($schema)) {
                $destination->setAuthorization(self::$authorization);
                $destination->setDatabase($schema)->delete();
            }
        }

        $database
            ->setDatabase($this->testDatabase)
            ->setAuthorization(self::$authorization)
            ->setNamespace(static::$namespace = 'myapp_'.uniqid());

        $database->create();

        return self::$database = $database;
    }

    /**
     * @throws Exception
     * @throws \RedisException
     */
    public function test_get_mirror_source(): void
    {
        $database = $this->getDatabase();
        $source = $database->getSource();
        $this->assertInstanceOf(Database::class, $source);
        $this->assertEquals(self::$source, $source);
    }

    /**
     * @throws Exception
     * @throws \RedisException
     */
    public function test_get_mirror_destination(): void
    {
        $database = $this->getDatabase();
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
    public function test_create_mirrored_collection(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('testCreateMirroredCollection');

        // Assert collection exists in both databases
        $this->assertFalse($database->getSource()->getCollection('testCreateMirroredCollection')->isEmpty());
        $destination = $database->getDestination();
        $this->assertNotNull($destination);
        $this->assertFalse($destination->getCollection('testCreateMirroredCollection')->isEmpty());
    }

    /**
     * @throws Limit
     * @throws Duplicate
     * @throws \RedisException
     * @throws Conflict
     * @throws Exception
     */
    public function test_update_mirrored_collection(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('testUpdateMirroredCollection', permissions: [
            Permission::read(Role::any()),
        ]);

        $collection = $database->getCollection('testUpdateMirroredCollection');

        $database->updateCollection(
            'testUpdateMirroredCollection',
            [
                Permission::read(Role::users()),
            ],
            (bool) $collection->getAttribute('documentSecurity')
        );

        // Asset both databases have updated the collection
        $this->assertEquals(
            [Permission::read(Role::users())],
            $database->getSource()->getCollection('testUpdateMirroredCollection')->getPermissions()
        );

        $destination = $database->getDestination();
        $this->assertNotNull($destination);
        $this->assertEquals(
            [Permission::read(Role::users())],
            $destination->getCollection('testUpdateMirroredCollection')->getPermissions()
        );
    }

    public function test_delete_mirrored_collection(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('testDeleteMirroredCollection');

        $database->deleteCollection('testDeleteMirroredCollection');

        // Assert collection is deleted in both databases
        $this->assertTrue($database->getSource()->getCollection('testDeleteMirroredCollection')->isEmpty());
        $destination = $database->getDestination();
        $this->assertNotNull($destination);
        $this->assertTrue($destination->getCollection('testDeleteMirroredCollection')->isEmpty());
    }

    /**
     * @throws Authorization
     * @throws Duplicate
     * @throws \RedisException
     * @throws Limit
     * @throws Structure
     * @throws Exception
     */
    public function test_create_mirrored_document(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('testCreateMirroredDocument', attributes: [
            new Attribute(key: 'name', type: ColumnType::String, size: Database::LENGTH_KEY, required: true),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false);

        $document = $database->createDocument('testCreateMirroredDocument', new Document([
            'name' => 'Jake',
            '$permissions' => [],
        ]));

        // Assert document is created in both databases
        $this->assertEquals(
            $document,
            $database->getSource()->getDocument('testCreateMirroredDocument', $document->getId())
        );

        $destination = $database->getDestination();
        $this->assertNotNull($destination);
        $this->assertEquals(
            $document,
            $destination->getDocument('testCreateMirroredDocument', $document->getId())
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
    public function test_update_mirrored_document(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('testUpdateMirroredDocument', attributes: [
            new Attribute(key: 'name', type: ColumnType::String, size: Database::LENGTH_KEY, required: true),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ], documentSecurity: false);

        $document = $database->createDocument('testUpdateMirroredDocument', new Document([
            'name' => 'Jake',
            '$permissions' => [],
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

        $destination = $database->getDestination();
        $this->assertNotNull($destination);
        $this->assertEquals(
            $document,
            $destination->getDocument('testUpdateMirroredDocument', $document->getId())
        );
    }

    public function test_delete_mirrored_document(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('testDeleteMirroredDocument', attributes: [
            new Attribute(key: 'name', type: ColumnType::String, size: Database::LENGTH_KEY, required: true),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::delete(Role::any()),
        ], documentSecurity: false);

        $document = $database->createDocument('testDeleteMirroredDocument', new Document([
            'name' => 'Jake',
            '$permissions' => [],
        ]));

        $database->deleteDocument('testDeleteMirroredDocument', $document->getId());

        // Assert document is deleted in both databases
        $this->assertTrue($database->getSource()->getDocument('testDeleteMirroredDocument', $document->getId())->isEmpty());
        $destination = $database->getDestination();
        $this->assertNotNull($destination);
        $this->assertTrue($destination->getDocument('testDeleteMirroredDocument', $document->getId())->isEmpty());
    }

    public function testCreateDocumentsSkipDuplicatesBackfillsDestination(): void
    {
        $database = $this->getDatabase();
        $collection = 'mirrorSkipDup';

        $database->createCollection($collection, attributes: [
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

        // Seed the SOURCE only (bypass the mirror) with the row we want to
        // skipDuplicates over later. Destination intentionally does NOT have it —
        // this simulates an in-flight backfill where the collection is marked
        // 'upgraded' (schema mirrored) but not every row has reached destination.
        $database->getSource()->createDocument($collection, new Document([
            '$id' => 'dup',
            'name' => 'Original',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
            ],
        ]));

        $this->assertSame(
            'Original',
            $database->getSource()->getDocument($collection, 'dup')->getAttribute('name')
        );
        $this->assertTrue(
            $database->getDestination()->getDocument($collection, 'dup')->isEmpty()
        );

        $database->skipDuplicates(fn () => $database->createDocuments($collection, [
            new Document([
                '$id' => 'dup',
                'name' => 'WouldBe',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                ],
            ]),
            new Document([
                '$id' => 'fresh',
                'name' => 'Fresh',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                ],
            ]),
        ]));

        // Source: INSERT IGNORE — 'dup' is a no-op, keeps 'Original'.
        $this->assertSame(
            'Original',
            $database->getSource()->getDocument($collection, 'dup')->getAttribute('name')
        );
        $this->assertSame(
            'Fresh',
            $database->getSource()->getDocument($collection, 'fresh')->getAttribute('name')
        );

        // Destination: 'dup' is NOT a duplicate there, so destination's own
        // INSERT IGNORE inserts it. This prevents permanent divergence when
        // destination is still catching up on rows that already exist on source.
        $this->assertSame(
            'WouldBe',
            $database->getDestination()->getDocument($collection, 'dup')->getAttribute('name'),
            'Source-skipped doc must still insert on destination when absent there'
        );
        $this->assertSame(
            'Fresh',
            $database->getDestination()->getDocument($collection, 'fresh')->getAttribute('name')
        );
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '`'.self::$source->getDatabase().'`.`'.self::$source->getNamespace().'_'.$collection.'`';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        assert(self::$sourcePdo !== null);
        self::$sourcePdo->exec($sql);

        $sqlTable = '`'.self::$destination->getDatabase().'`.`'.self::$destination->getNamespace().'_'.$collection.'`';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        assert(self::$destinationPdo !== null);
        self::$destinationPdo->exec($sql);

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $sqlTable = '`'.self::$source->getDatabase().'`.`'.self::$source->getNamespace().'_'.$collection.'`';
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        assert(self::$sourcePdo !== null);
        self::$sourcePdo->exec($sql);

        $sqlTable = '`'.self::$destination->getDatabase().'`.`'.self::$destination->getNamespace().'_'.$collection.'`';
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        assert(self::$destinationPdo !== null);
        self::$destinationPdo->exec($sql);

        return true;
    }
}
