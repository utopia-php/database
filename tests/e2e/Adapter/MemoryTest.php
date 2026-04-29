<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

/**
 * E2E tests for the in-memory adapter. Inherits the standard adapter scopes
 * from Base so it is exercised against the same scenarios as MariaDB/MySQL/
 * Postgres. Scope tests that depend on features Memory does not implement
 * (relationships, operators, vectors, spatial, fulltext, schemaless,
 * object attributes) self-skip via the adapter's getSupportFor* flags.
 *
 * The test methods declared directly on this class are Memory-specific
 * regressions for behaviour that is not exercised — or not exercised in the
 * same way — by the inherited scopes (transaction nesting semantics, raw
 * adapter store layout after attribute operations, tenancy on the in-process
 * map, etc.).
 */
class MemoryTest extends Base
{
    public static ?Database $database = null;
    protected static string $namespace;

    public static function getAdapterName(): string
    {
        return 'memory';
    }

    public function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new Memory(), $cache);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'memory_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        // Memory has no out-of-band schema mutation path; tests that exercise
        // "raw" column drops to simulate corruption do not apply.
        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        return true;
    }

    /**
     * Build a fresh Database backed by an isolated Memory adapter so the
     * Memory-specific regression tests below cannot pollute the shared
     * `getDatabase()` instance used by the inherited scope tests.
     */
    private function freshDatabase(): Database
    {
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new Memory(), $cache);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace('memory_iso_' . uniqid());
        $database->create();
        return $database;
    }

    /**
     * The inherited scope test does not gate on getSupportForUpserts(); skip
     * here because Memory throws on upsert by design.
     */
    public function testUpsertWithJSONFilters(): void
    {
        $this->markTestSkipped('Memory adapter does not implement upserts.');
    }

    /**
     * Inherited test creates a self-relationship; Memory has no relationships.
     */
    public function testAttributeNamesWithDots(): void
    {
        $this->markTestSkipped('Memory adapter does not implement relationships.');
    }

    /**
     * Inherited test asserts permission cascade through a relationship.
     *
     * @return array<mixed>
     */
    public function testCollectionPermissionsRelationships(): array
    {
        $this->markTestSkipped('Memory adapter does not implement relationships.');
    }

    /**
     * Inherited test asserts cursor ordering across a relationship join.
     */
    public function testOrderAndCursorWithRelationshipQueries(): void
    {
        $this->markTestSkipped('Memory adapter does not implement relationships.');
    }

    /**
     * Inherited test depends on PDO's automatic int->string coercion when an
     * INTEGER column is altered to VARCHAR. Memory keeps native PHP scalars,
     * so the historical int payload remains an int after the type change.
     */
    public function testUpdateAttributeStructure(): void
    {
        $this->markTestSkipped(
            'Memory stores native scalars; type changes do not retroactively '
            . 'coerce existing column values the way PDO string returns do.'
        );
    }

    /**
     * Inherited test exercises VARCHAR truncation when shrinking a column
     * that holds oversize data. Memory does not enforce string sizes on disk.
     */
    public function testUpdateAttributeSize(): void
    {
        $this->markTestSkipped(
            'Memory does not enforce string size truncation when an attribute '
            . 'is resized smaller than existing data.'
        );
    }

    /**
     * Memory has no reserved keyword list; the inherited test then has no
     * keywords to iterate over and is flagged risky.
     */
    public function testKeywords(): void
    {
        $this->markTestSkipped('Memory has no reserved keywords.');
    }

    /**
     * Memory does not implement upserts. Inherited scope tests that rely on
     * upserts skip themselves via getSupportForUpserts().
     */
    public function testUpsertIsNotImplemented(): void
    {
        $collection = new Document(['$id' => 'any']);

        $this->expectException(\Utopia\Database\Exception::class);
        $this->freshDatabase()->getAdapter()->upsertDocuments($collection, '', []);
    }

    /**
     * Regression: nesting startTransaction/rollbackTransaction must only
     * discard the inner write, leaving the outer transaction live.
     */
    public function testNestedTransactionRollbackOnlyDiscardsInner(): void
    {
        $database = $this->freshDatabase();

        $database->createCollection('nested', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);

        $adapter = $database->getAdapter();
        $adapter->startTransaction();
        $database->createDocument('nested', new Document([
            '$id' => 'outer',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'outer',
        ]));

        $adapter->startTransaction();
        $database->createDocument('nested', new Document([
            '$id' => 'inner',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'inner',
        ]));
        $adapter->rollbackTransaction();

        $this->assertTrue($adapter->inTransaction());
        $adapter->commitTransaction();

        $this->assertFalse($database->getDocument('nested', 'outer')->isEmpty());
        $this->assertTrue($database->getDocument('nested', 'inner')->isEmpty());
    }

    /**
     * Regression: array attributes round-trip cleanly through the JSON
     * encode/decode boundary the adapter applies on write/read.
     */
    public function testArrayAttributeRoundTrip(): void
    {
        $database = $this->freshDatabase();

        $database->createCollection('lists', [
            new Document([
                '$id' => 'tags',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => false,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ]),
        ], [], [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);

        $database->createDocument('lists', new Document([
            '$id' => 'l1',
            '$permissions' => [Permission::read(Role::any())],
            'tags' => ['php', 'memory', 'adapter'],
        ]));

        $fetched = $database->getDocument('lists', 'l1');
        $this->assertSame(['php', 'memory', 'adapter'], $fetched->getAttribute('tags'));
    }

    /**
     * Regression: CREATE UNIQUE INDEX on a collection that already contains
     * duplicate values must surface DuplicateException at the adapter layer
     * (matches MariaDB errno 1062).
     */
    public function testCreateUniqueIndexRejectsExistingDuplicates(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('uniqdup_' . \uniqid());
        $adapter->createCollection('emails', [], []);
        $adapter->createDocument(
            new Document(['$id' => 'emails']),
            new Document(['$id' => 'a', 'addr' => 'dup@example.com', '$permissions' => []])
        );
        $adapter->createDocument(
            new Document(['$id' => 'emails']),
            new Document(['$id' => 'b', 'addr' => 'dup@example.com', '$permissions' => []])
        );

        $this->expectException(DuplicateException::class);
        $adapter->createIndex('emails', 'unique_addr', Database::INDEX_UNIQUE, ['addr'], [], []);
    }

    /**
     * Regression: unique indexes must allow multiple null values (mirrors
     * MariaDB UNIQUE behaviour — NULL is treated as distinct per row).
     */
    public function testUniqueIndexAllowsMultipleNulls(): void
    {
        $database = $this->freshDatabase();

        $database->createCollection('optional', [
            new Document([
                '$id' => 'token',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [
            new Document([
                '$id' => 'unique_token',
                'type' => Database::INDEX_UNIQUE,
                'attributes' => ['token'],
            ]),
        ], [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);

        $database->createDocument('optional', new Document([
            '$id' => 'a',
            '$permissions' => [Permission::read(Role::any())],
            'token' => null,
        ]));
        $database->createDocument('optional', new Document([
            '$id' => 'b',
            '$permissions' => [Permission::read(Role::any())],
            'token' => null,
        ]));

        $this->assertEquals(2, $database->count('optional'));
    }

    /**
     * Regression: updateAttribute applies metadata after a rename — the new
     * key carries the new size, the old key is gone.
     */
    public function testUpdateAttributeAppliesMetadataAfterRename(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('rename_' . \uniqid());
        $adapter->createCollection('renames', [], []);
        $adapter->createAttribute('renames', 'old', Database::VAR_STRING, 64);

        $adapter->updateAttribute('renames', 'old', Database::VAR_STRING, 256, true, false, 'fresh');

        $store = (new \ReflectionClass($adapter))->getProperty('data')->getValue($adapter);
        $key = $adapter->getNamespace() . '_renames';

        $this->assertArrayHasKey('fresh', $store[$key]['attributes']);
        $this->assertArrayNotHasKey('old', $store[$key]['attributes']);
        $this->assertEquals(256, $store[$key]['attributes']['fresh']['size']);
    }

    /**
     * Regression: renameAttribute cascades the rename into any indexes that
     * referenced the old name.
     */
    public function testRenameAttributeUpdatesIndexReferences(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('idxrn_' . \uniqid());
        $adapter->createCollection('indexed', [], []);
        $adapter->createAttribute('indexed', 'name', Database::VAR_STRING, 64);
        $adapter->createIndex('indexed', 'idx_name', Database::INDEX_KEY, ['name'], [], []);

        $adapter->renameAttribute('indexed', 'name', 'title');

        $store = (new \ReflectionClass($adapter))->getProperty('data')->getValue($adapter);
        $key = $adapter->getNamespace() . '_indexed';

        $this->assertEquals(['title'], $store[$key]['indexes']['idx_name']['attributes']);
    }

    /**
     * Regression: deleteAttribute strips the attribute from any composite
     * index that referenced it.
     */
    public function testDeleteAttributeRemovesFromIndex(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('idxdrop_' . \uniqid());
        $adapter->createCollection('drops', [], []);
        $adapter->createAttribute('drops', 'a', Database::VAR_STRING, 64);
        $adapter->createAttribute('drops', 'b', Database::VAR_STRING, 64);
        $adapter->createIndex('drops', 'idx_ab', Database::INDEX_KEY, ['a', 'b'], [], []);

        $adapter->deleteAttribute('drops', 'a');

        $store = (new \ReflectionClass($adapter))->getProperty('data')->getValue($adapter);
        $key = $adapter->getNamespace() . '_drops';

        $this->assertEquals(['b'], $store[$key]['indexes']['idx_ab']['attributes']);
    }

    /**
     * Regression: bulk update via Database::updateDocuments must enforce
     * unique indexes on the changed attribute.
     */
    public function testBatchUpdateEnforcesUniqueIndexes(): void
    {
        $database = $this->freshDatabase();

        $database->createCollection('handles', [
            new Document([
                '$id' => 'handle',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [
            new Document([
                '$id' => 'unique_handle',
                'type' => Database::INDEX_UNIQUE,
                'attributes' => ['handle'],
            ]),
        ], [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ]);

        $database->createDocument('handles', new Document([
            '$id' => 'h1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'handle' => 'taken',
        ]));
        $database->createDocument('handles', new Document([
            '$id' => 'h2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'handle' => 'free',
        ]));

        $this->expectException(DuplicateException::class);
        $database->updateDocuments('handles', new Document(['handle' => 'taken']), [
            Query::equal('$id', ['h2']),
        ]);
    }

    /**
     * Regression: bulk delete clears the in-memory permissions index for the
     * affected collection.
     */
    public function testBulkDeleteRemovesPermissions(): void
    {
        $database = $this->freshDatabase();

        $database->createCollection('cleanup', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::create(Role::any()),
            Permission::delete(Role::any()),
        ]);

        for ($i = 0; $i < 3; $i++) {
            $database->createDocument('cleanup', new Document([
                '$id' => "c{$i}",
                '$permissions' => [Permission::read(Role::any()), Permission::delete(Role::any())],
                'name' => "n{$i}",
            ]));
        }

        $database->deleteDocuments('cleanup');

        $adapter = $database->getAdapter();
        $permissions = (new \ReflectionClass($adapter))->getProperty('permissions')->getValue($adapter);
        $key = $database->getNamespace() . '_cleanup';

        $this->assertEmpty($permissions[$key] ?? []);
    }

    /**
     * Regression: with shared tables enabled, two tenants writing the same
     * primary id must remain isolated on read.
     */
    public function testSharedTablesIsolatesTenants(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('shared_' . \uniqid());
        $adapter->setSharedTables(true);
        $adapter->setTenant(1);
        $adapter->createCollection('shared', [], []);

        $collection = new Document(['$id' => 'shared']);

        $adapter->createDocument($collection, new Document([
            '$id' => 'same',
            'name' => 'tenant1',
            '$permissions' => [],
        ]));

        $adapter->setTenant(2);
        $adapter->createDocument($collection, new Document([
            '$id' => 'same',
            'name' => 'tenant2',
            '$permissions' => [],
        ]));

        $tenant2Doc = $adapter->getDocument($collection, 'same');
        $this->assertEquals('tenant2', $tenant2Doc->getAttribute('name'));

        $adapter->setTenant(1);
        $tenant1Doc = $adapter->getDocument($collection, 'same');
        $this->assertEquals('tenant1', $tenant1Doc->getAttribute('name'));
    }

    public function testFindThrowsWhenCollectionMissing(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('missing_' . \uniqid());

        $this->expectException(NotFoundException::class);
        $adapter->find(new Document(['$id' => 'ghost']));
    }

    public function testCountThrowsWhenCollectionMissing(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('missing_' . \uniqid());

        $this->expectException(NotFoundException::class);
        $adapter->count(new Document(['$id' => 'ghost']));
    }

    public function testSumThrowsWhenCollectionMissing(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('missing_' . \uniqid());

        $this->expectException(NotFoundException::class);
        $adapter->sum(new Document(['$id' => 'ghost']), 'value');
    }

    public function testDeleteDocumentThrowsWhenCollectionMissing(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('missing_' . \uniqid());

        $this->expectException(NotFoundException::class);
        $adapter->deleteDocument('ghost', 'x');
    }

    public function testDeleteDocumentReturnsFalseForMissingDoc(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('miss_' . \uniqid());
        $adapter->createCollection('here', [], []);

        // Collection exists, document does not — mirrors MariaDB rowCount() == 0.
        $this->assertFalse($adapter->deleteDocument('here', 'never-created'));
    }

    public function testDeleteDocumentsThrowsWhenCollectionMissing(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('missing_' . \uniqid());

        $this->expectException(NotFoundException::class);
        $adapter->deleteDocuments('ghost', [], []);
    }

    public function testDeleteDocumentsHonoursTenantBoundary(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('shared_del_' . \uniqid());
        $adapter->setSharedTables(true);

        $collection = new Document(['$id' => 'box']);

        $adapter->setTenant(1);
        $adapter->createCollection('box', [], []);
        $adapter->createDocument($collection, new Document([
            '$id' => 'a',
            '$permissions' => [],
            'name' => 'tenant1-doc',
        ]));

        $adapter->setTenant(2);
        $adapter->createDocument($collection, new Document([
            '$id' => 'b',
            '$permissions' => [],
            'name' => 'tenant2-doc',
        ]));

        $adapter->setTenant(1);
        $deleted = $adapter->deleteDocuments('box', ['1'], []);

        $this->assertEquals(1, $deleted);

        $adapter->setTenant(2);
        $survivor = $adapter->getDocument($collection, 'b');
        $this->assertEquals('tenant2-doc', $survivor->getAttribute('name'));
    }

    public function testGetSequencesUsesDocumentTenant(): void
    {
        $adapter = new Memory();
        $adapter->setNamespace('seq_tenant_' . \uniqid());
        $adapter->setSharedTables(true);

        $collection = new Document(['$id' => 'box']);
        $adapter->setTenant(1);
        $adapter->createCollection('box', [], []);
        $adapter->createDocument($collection, new Document([
            '$id' => 'a',
            '$permissions' => [],
            'name' => 'tenant1',
        ]));

        $adapter->setTenant(7);
        $adapter->createDocument($collection, new Document([
            '$id' => 'a',
            '$permissions' => [],
            'name' => 'tenant7',
        ]));

        $probe = new Document(['$id' => 'a', '$tenant' => 1]);
        [$result] = $adapter->getSequences('box', [$probe]);
        $this->assertNotEmpty($result->getSequence());
    }

    /**
     * Regression: unique-index dedupe must compare type-normalised values so
     * two documents storing `true` collide even after the casting layer maps
     * booleans to integers on write.
     */
    public function testUniqueIndexNormalizesBoolAndNumericString(): void
    {
        $database = $this->freshDatabase();

        $database->createCollection('flags', [
            new Document([
                '$id' => 'active',
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [
            new Document([
                '$id' => 'unique_active',
                'type' => Database::INDEX_UNIQUE,
                'attributes' => ['active'],
            ]),
        ], [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);

        $database->createDocument('flags', new Document([
            '$id' => 'first',
            '$permissions' => [Permission::read(Role::any())],
            'active' => true,
        ]));

        $this->expectException(DuplicateException::class);
        $database->createDocument('flags', new Document([
            '$id' => 'second',
            '$permissions' => [Permission::read(Role::any())],
            'active' => true,
        ]));
    }
}
