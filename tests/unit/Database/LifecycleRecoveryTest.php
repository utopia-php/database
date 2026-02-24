<?php

namespace Tests\Unit\Database;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoneAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\PDO;

/**
 * Tests for the lifecycle recovery code in Database.php.
 *
 * These tests verify that schema modification methods correctly:
 * - Recover from partial failures (schema-only orphans)
 * - Suppress NotFoundException on delete when schema is already gone
 * - Roll back schema changes when metadata persistence fails
 * - Handle idempotent retries after partial failures
 *
 * Uses SQLite in-memory for fast, infrastructure-free testing.
 * Relationship tests use twoWay:false since SQLite can't execute
 * multi-statement ALTER TABLE needed for two-way columns.
 */
class LifecycleRecoveryTest extends TestCase
{
    private static ?Database $database = null;
    private static ?FailableAdapter $adapter = null;
    private static ?PDO $pdo = null;

    /**
     * Get or create the shared Database instance backed by SQLite in-memory.
     */
    private function getDatabase(): Database
    {
        if (self::$database !== null) {
            return self::$database;
        }

        $pdo = new PDO('sqlite::memory:', null, null, [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            \PDO::ATTR_EMULATE_PREPARES => true,
            \PDO::ATTR_STRINGIFY_FETCHES => true,
        ]);

        self::$pdo = $pdo;
        self::$adapter = new FailableAdapter($pdo);

        $cache = new Cache(new NoneAdapter());
        $database = new Database(self::$adapter, $cache);
        $database
            ->setDatabase('test')
            ->setNamespace('recovery_test');

        $database->create();

        self::$database = $database;
        return $database;
    }

    private function getAdapter(): FailableAdapter
    {
        $this->getDatabase();
        return self::$adapter;
    }

    /**
     * Helper: create a fresh collection for isolated testing.
     */
    private function freshCollection(string $name): void
    {
        $db = $this->getDatabase();
        if ($db->getCollection($name)->isEmpty() === false) {
            $db->deleteCollection($name);
        }
        $db->createCollection($name);
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->getAdapter()->clearFailures();
        $this->getAdapter()->resetCallCounts();
    }

    // ========================================================================
    // createAttribute
    // ========================================================================

    public function testCreateAttributeSuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('attr_success');

        $result = $db->createAttribute('attr_success', 'name', Database::VAR_STRING, 128, true);
        $this->assertTrue($result);

        $collection = $db->getCollection('attr_success');
        $found = false;
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            if ($attr->getId() === 'name') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'Attribute should exist in metadata after creation');
    }

    public function testCreateAttributeOrphanRecovery(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('attr_orphan');

        // Create column directly via adapter (no metadata)
        $this->getAdapter()->createAttribute('attr_orphan', 'orphaned_col', Database::VAR_STRING, 128, true, false, false);

        // Database.createAttribute should detect orphan and recover
        $result = $db->createAttribute('attr_orphan', 'orphaned_col', Database::VAR_STRING, 128, true);
        $this->assertTrue($result);

        // Verify metadata was created
        $collection = $db->getCollection('attr_orphan');
        $found = false;
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            if ($attr->getId() === 'orphaned_col') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'Orphaned attribute should have metadata after recovery');
    }

    public function testCreateAttributeTrueDuplicateThrows(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('attr_true_dup');

        $db->createAttribute('attr_true_dup', 'existing', Database::VAR_STRING, 128, true);

        $this->expectException(DuplicateException::class);
        $db->createAttribute('attr_true_dup', 'existing', Database::VAR_STRING, 128, true);
    }

    public function testCreateAttributeRollbackOnMetadataFailure(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('attr_rollback');

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);

        try {
            $db->createAttribute('attr_rollback', 'doomed', Database::VAR_STRING, 128, true);
            $this->fail('Should have thrown DatabaseException');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('metadata', \strtolower($e->getMessage()));
        }

        // The column should have been cleaned up (rollback)
        // Verify by successfully creating it again
        $adapter->clearFailures();
        $result = $db->createAttribute('attr_rollback', 'doomed', Database::VAR_STRING, 128, true);
        $this->assertTrue($result, 'Attribute should be creatable after failed attempt was rolled back');
    }

    public function testCreateAttributeSuppressesDuplicateWithoutMigrationFlag(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('shared_no_migrate');

        // Create column directly (orphan)
        $this->getAdapter()->createAttribute('shared_no_migrate', 'shared_col', Database::VAR_STRING, 128, true, false, false);

        // NOT in shared tables mode, NOT migrating — should still recover
        $this->assertFalse($this->getAdapter()->getSharedTables());
        $this->assertFalse($db->isMigrating());

        $result = $db->createAttribute('shared_no_migrate', 'shared_col', Database::VAR_STRING, 128, true);
        $this->assertTrue($result, 'Orphan recovery should work regardless of shared tables / migration mode');
    }

    // ========================================================================
    // createAttributes (batch)
    // ========================================================================

    // Note: testCreateAttributesBatchSuccess is skipped because SQLite does not support
    // batch ALTER TABLE ADD COLUMN (comma-separated columns in a single statement).

    public function testCreateAttributesBatchOrphanRecoveryViaAdapterDuplicate(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('attrs_batch_orphan');

        // First, create both columns normally
        $db->createAttribute('attrs_batch_orphan', 'col_x', Database::VAR_STRING, 128, true);
        $db->createAttribute('attrs_batch_orphan', 'col_y', Database::VAR_INTEGER, 0, false);

        // Delete just the metadata (simulating orphan state)
        $db->deleteAttribute('attrs_batch_orphan', 'col_x');
        $db->deleteAttribute('attrs_batch_orphan', 'col_y');

        // Now re-create via adapter to have schema-only columns, and inject
        // DuplicateException to test the inner catch suppression
        $adapter = $this->getAdapter();
        $adapter->createAttribute('attrs_batch_orphan', 'col_x', Database::VAR_STRING, 128, true, false, false);
        $adapter->createAttribute('attrs_batch_orphan', 'col_y', Database::VAR_INTEGER, 0, true, false, false);

        // Inject DuplicateException for createAttributes (the batch adapter call)
        // to test the inner catch suppression
        $adapter->failOnNext('createAttributes', new DuplicateException('Simulated duplicate'));

        $result = $db->createAttributes('attrs_batch_orphan', [
            ['$id' => 'col_x', 'type' => Database::VAR_STRING, 'size' => 128, 'required' => true, 'default' => null, 'signed' => true, 'array' => false, 'format' => null, 'formatOptions' => [], 'filters' => []],
            ['$id' => 'col_y', 'type' => Database::VAR_INTEGER, 'size' => 0, 'required' => false, 'default' => null, 'signed' => true, 'array' => false, 'format' => null, 'formatOptions' => [], 'filters' => []],
        ]);
        $this->assertTrue($result);

        $collection = $db->getCollection('attrs_batch_orphan');
        $names = [];
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            $names[] = $attr->getId();
        }
        $this->assertContains('col_x', $names);
        $this->assertContains('col_y', $names);
    }

    // ========================================================================
    // deleteAttribute
    // ========================================================================

    public function testDeleteAttributeSuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_attr_ok');
        $db->createAttribute('del_attr_ok', 'to_delete', Database::VAR_STRING, 128, false);

        $result = $db->deleteAttribute('del_attr_ok', 'to_delete');
        $this->assertTrue($result);

        $collection = $db->getCollection('del_attr_ok');
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            $this->assertNotEquals('to_delete', $attr->getId());
        }
    }

    public function testDeleteAttributeSchemaAlreadyGone(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_attr_gone');
        $db->createAttribute('del_attr_gone', 'ghost', Database::VAR_STRING, 128, false);

        // Delete column directly at adapter level
        $this->getAdapter()->deleteAttribute('del_attr_gone', 'ghost');

        // Database.deleteAttribute should still succeed (NotFoundException suppressed)
        $result = $db->deleteAttribute('del_attr_gone', 'ghost');
        $this->assertTrue($result);

        // Verify metadata is cleaned up
        $collection = $db->getCollection('del_attr_gone');
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            $this->assertNotEquals('ghost', $attr->getId());
        }
    }

    public function testDeleteAttributeRollbackOnMetadataFailure(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_attr_rollback');
        $db->createAttribute('del_attr_rollback', 'precious', Database::VAR_STRING, 128, false);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);

        try {
            $db->deleteAttribute('del_attr_rollback', 'precious');
            $this->fail('Should have thrown DatabaseException');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('metadata', \strtolower($e->getMessage()));
        }

        // Column should have been recreated by rollback
        $adapter->clearFailures();
        $collection = $db->getCollection('del_attr_rollback');
        $found = false;
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            if ($attr->getId() === 'precious') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'Attribute metadata should still exist after rollback');

        // Verify the column is functional by inserting a document
        $doc = $db->createDocument('del_attr_rollback', new Document([
            '$id' => 'test_rollback',
            '$permissions' => [],
            'precious' => 'still here',
        ]));
        $this->assertEquals('still here', $doc->getAttribute('precious'));
    }

    public function testDeleteNonExistentAttributeThrows(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_nonexist');

        $this->expectException(DatabaseException::class);
        $db->deleteAttribute('del_nonexist', 'nonexistent');
    }

    // ========================================================================
    // createIndex
    // ========================================================================

    public function testCreateIndexSuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idx_success');
        $db->createAttribute('idx_success', 'name', Database::VAR_STRING, 128, true);

        $result = $db->createIndex('idx_success', 'idx_name', Database::INDEX_KEY, ['name']);
        $this->assertTrue($result);

        $collection = $db->getCollection('idx_success');
        $found = false;
        foreach ($collection->getAttribute('indexes', []) as $index) {
            if ($index->getId() === 'idx_name') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'Index should exist in metadata');
    }

    public function testCreateIndexOrphanRecovery(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idx_orphan');
        $db->createAttribute('idx_orphan', 'field', Database::VAR_STRING, 128, true);

        // Create index directly via adapter (no metadata)
        $this->getAdapter()->createIndex('idx_orphan', 'orphan_idx', Database::INDEX_KEY, ['field'], [], [], ['string']);

        // Database.createIndex should detect orphan and recover
        $result = $db->createIndex('idx_orphan', 'orphan_idx', Database::INDEX_KEY, ['field']);
        $this->assertTrue($result);

        // Verify metadata was created
        $collection = $db->getCollection('idx_orphan');
        $found = false;
        foreach ($collection->getAttribute('indexes', []) as $index) {
            if ($index->getId() === 'orphan_idx') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'Orphaned index should have metadata after recovery');
    }

    public function testCreateIndexTrueDuplicateThrows(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idx_dup');
        $db->createAttribute('idx_dup', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('idx_dup', 'dup_idx', Database::INDEX_KEY, ['col']);

        $this->expectException(DuplicateException::class);
        $db->createIndex('idx_dup', 'dup_idx', Database::INDEX_KEY, ['col']);
    }

    // ========================================================================
    // deleteIndex
    // ========================================================================

    public function testDeleteIndexSuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_idx_ok');
        $db->createAttribute('del_idx_ok', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('del_idx_ok', 'del_me', Database::INDEX_KEY, ['col']);

        $result = $db->deleteIndex('del_idx_ok', 'del_me');
        $this->assertTrue($result);

        $collection = $db->getCollection('del_idx_ok');
        foreach ($collection->getAttribute('indexes', []) as $index) {
            $this->assertNotEquals('del_me', $index->getId());
        }
    }

    public function testDeleteIndexSchemaAlreadyGone(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_idx_gone');
        $db->createAttribute('del_idx_gone', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('del_idx_gone', 'ghost_idx', Database::INDEX_KEY, ['col']);

        // Delete index directly at adapter level
        $this->getAdapter()->deleteIndex('del_idx_gone', 'ghost_idx');

        // Database.deleteIndex should still succeed
        $result = $db->deleteIndex('del_idx_gone', 'ghost_idx');
        $this->assertTrue($result);

        // Verify metadata is cleaned up
        $collection = $db->getCollection('del_idx_gone');
        foreach ($collection->getAttribute('indexes', []) as $index) {
            $this->assertNotEquals('ghost_idx', $index->getId());
        }
    }

    public function testDeleteIndexRollbackOnMetadataFailure(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_idx_rollback');
        $db->createAttribute('del_idx_rollback', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('del_idx_rollback', 'keep_me', Database::INDEX_KEY, ['col']);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);

        try {
            $db->deleteIndex('del_idx_rollback', 'keep_me');
            $this->fail('Should have thrown DatabaseException');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('metadata', \strtolower($e->getMessage()));
        }

        // Verify index metadata still exists
        $adapter->clearFailures();
        $collection = $db->getCollection('del_idx_rollback');
        $found = false;
        foreach ($collection->getAttribute('indexes', []) as $index) {
            if ($index->getId() === 'keep_me') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'Index metadata should still exist after rollback');
    }

    public function testDeleteNonExistentIndexThrows(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_idx_nonexist');

        $this->expectException(NotFoundException::class);
        $db->deleteIndex('del_idx_nonexist', 'nonexistent');
    }

    // ========================================================================
    // createCollection
    // ========================================================================

    public function testCreateCollectionSuccess(): void
    {
        $db = $this->getDatabase();
        $result = $db->createCollection('coll_success');
        $this->assertInstanceOf(Document::class, $result);

        $collection = $db->getCollection('coll_success');
        $this->assertFalse($collection->isEmpty());

        $db->deleteCollection('coll_success');
    }

    public function testCreateCollectionOrphanRecoveryViaDuplicateException(): void
    {
        $db = $this->getDatabase();
        $adapter = $this->getAdapter();

        // Inject DuplicateException from adapter (simulating what a proper adapter
        // would throw when table already exists but metadata doesn't)
        $adapter->failOnNext('createCollection', new DuplicateException('Table already exists'));

        $result = $db->createCollection('coll_orphan_dup');
        $this->assertInstanceOf(Document::class, $result);

        $collection = $db->getCollection('coll_orphan_dup');
        $this->assertFalse($collection->isEmpty(), 'Collection should have metadata after DuplicateException recovery');

        $db->deleteCollection('coll_orphan_dup');
    }

    public function testCreateCollectionTrueDuplicateThrows(): void
    {
        $db = $this->getDatabase();
        $db->createCollection('coll_dup');

        try {
            $db->createCollection('coll_dup');
            $this->fail('Should have thrown DuplicateException');
        } catch (DuplicateException $e) {
            $this->assertStringContainsString('already exists', $e->getMessage());
        }

        $db->deleteCollection('coll_dup');
    }

    public function testCreateCollectionRollbackOnMetadataFailure(): void
    {
        $db = $this->getDatabase();

        $adapter = $this->getAdapter();
        $adapter->failMetadataCreates(10);

        try {
            $db->createCollection('coll_rollback');
            $this->fail('Should have thrown DatabaseException');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('metadata', \strtolower($e->getMessage()));
        }

        // Should be able to create it cleanly now (rollback cleaned up the table)
        $adapter->clearFailures();
        $result = $db->createCollection('coll_rollback');
        $this->assertInstanceOf(Document::class, $result);

        $db->deleteCollection('coll_rollback');
    }

    // ========================================================================
    // deleteCollection
    // ========================================================================

    public function testDeleteCollectionSuccess(): void
    {
        $db = $this->getDatabase();
        $db->createCollection('del_coll_ok');

        $result = $db->deleteCollection('del_coll_ok');
        $this->assertTrue($result);

        $collection = $db->getCollection('del_coll_ok');
        $this->assertTrue($collection->isEmpty());
    }

    public function testDeleteCollectionSchemaAlreadyGone(): void
    {
        $db = $this->getDatabase();
        $db->createCollection('del_coll_gone');

        // Drop table directly via adapter
        $this->getAdapter()->deleteCollection('del_coll_gone');

        // Database.deleteCollection should still succeed
        $result = $db->deleteCollection('del_coll_gone');
        $this->assertTrue($result);

        $collection = $db->getCollection('del_coll_gone');
        $this->assertTrue($collection->isEmpty());
    }

    public function testDeleteCollectionRollbackOnMetadataFailure(): void
    {
        $db = $this->getDatabase();
        $db->createCollection('del_coll_rollback');
        $db->createAttribute('del_coll_rollback', 'name', Database::VAR_STRING, 128, false);

        $adapter = $this->getAdapter();
        $adapter->failMetadataDeletes(10);

        try {
            $db->deleteCollection('del_coll_rollback');
            $this->fail('Should have thrown DatabaseException');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('metadata', \strtolower($e->getMessage()));
        }

        // Table should have been recreated by rollback — verify metadata still exists
        $adapter->clearFailures();
        $collection = $db->getCollection('del_coll_rollback');
        $this->assertFalse($collection->isEmpty(), 'Collection metadata should still exist after rollback');

        $db->deleteCollection('del_coll_rollback');
    }

    // ========================================================================
    // renameAttribute
    // ========================================================================

    public function testRenameAttributeSuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('rename_attr_ok');
        $db->createAttribute('rename_attr_ok', 'old_name', Database::VAR_STRING, 128, false);

        $result = $db->renameAttribute('rename_attr_ok', 'old_name', 'new_name');
        $this->assertTrue($result);

        $collection = $db->getCollection('rename_attr_ok');
        $names = [];
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            $names[] = $attr->getId();
        }
        $this->assertContains('new_name', $names);
        $this->assertNotContains('old_name', $names);
    }

    public function testRenameAttributeRollbackOnMetadataFailure(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('rename_attr_rb');
        $db->createAttribute('rename_attr_rb', 'stable', Database::VAR_STRING, 128, false);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);

        try {
            $db->renameAttribute('rename_attr_rb', 'stable', 'unstable');
            $this->fail('Should have thrown DatabaseException');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('metadata', \strtolower($e->getMessage()));
        }

        // Rollback should have reversed the rename
        $adapter->clearFailures();
        $collection = $db->getCollection('rename_attr_rb');
        $names = [];
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            $names[] = $attr->getId();
        }
        $this->assertContains('stable', $names, 'Attribute should be back to original name after rollback');
        $this->assertNotContains('unstable', $names);
    }

    // ========================================================================
    // renameIndex
    // ========================================================================

    public function testRenameIndexSuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('rename_idx_ok');
        $db->createAttribute('rename_idx_ok', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('rename_idx_ok', 'old_idx', Database::INDEX_KEY, ['col']);

        $result = $db->renameIndex('rename_idx_ok', 'old_idx', 'new_idx');
        $this->assertTrue($result);

        $collection = $db->getCollection('rename_idx_ok');
        $names = [];
        foreach ($collection->getAttribute('indexes', []) as $index) {
            $names[] = $index->getId();
        }
        $this->assertContains('new_idx', $names);
        $this->assertNotContains('old_idx', $names);
    }

    public function testRenameIndexRollbackOnMetadataFailure(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('rename_idx_rb');
        $db->createAttribute('rename_idx_rb', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('rename_idx_rb', 'keep_idx', Database::INDEX_KEY, ['col']);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);

        try {
            $db->renameIndex('rename_idx_rb', 'keep_idx', 'gone_idx');
            $this->fail('Should have thrown DatabaseException');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('metadata', \strtolower($e->getMessage()));
        }

        // Rollback should have reversed the rename
        $adapter->clearFailures();
        $collection = $db->getCollection('rename_idx_rb');
        $names = [];
        foreach ($collection->getAttribute('indexes', []) as $index) {
            $names[] = $index->getId();
        }
        $this->assertContains('keep_idx', $names, 'Index should be back to original name after rollback');
        $this->assertNotContains('gone_idx', $names);
    }

    public function testRenameIndexOrphanRecoveryViaReverseProbe(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('rename_idx_orphan');
        $db->createAttribute('rename_idx_orphan', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('rename_idx_orphan', 'probe_old', Database::INDEX_KEY, ['col']);

        // Rename directly at adapter level (orphan: schema renamed, metadata not)
        $this->getAdapter()->renameIndex('rename_idx_orphan', 'probe_old', 'probe_new');

        // Database.renameIndex should detect the orphan via reverse-probe and recover
        $result = $db->renameIndex('rename_idx_orphan', 'probe_old', 'probe_new');
        $this->assertTrue($result);

        // Verify metadata is updated
        $collection = $db->getCollection('rename_idx_orphan');
        $names = [];
        foreach ($collection->getAttribute('indexes', []) as $index) {
            $names[] = $index->getId();
        }
        $this->assertContains('probe_new', $names);
        $this->assertNotContains('probe_old', $names);
    }

    // ========================================================================
    // createRelationship (one-way only — SQLite limitation)
    // ========================================================================

    public function testCreateRelationshipOneWaySuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('rel_parent');
        $this->freshCollection('rel_child');

        $result = $db->createRelationship(
            collection: 'rel_parent',
            relatedCollection: 'rel_child',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: false,
            id: 'child_link',
            twoWayKey: 'parent_link'
        );
        $this->assertTrue($result);

        $parent = $db->getCollection('rel_parent');
        $found = false;
        foreach ($parent->getAttribute('attributes', []) as $attr) {
            if ($attr->getId() === 'child_link') {
                $found = true;
                $this->assertEquals(Database::VAR_RELATIONSHIP, $attr->getAttribute('type'));
                break;
            }
        }
        $this->assertTrue($found, 'Relationship attribute should exist in parent collection');
    }

    public function testCreateRelationshipOrphanRecoveryViaInjectedDuplicate(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('rel_orphan_p');
        $this->freshCollection('rel_orphan_c');

        $adapter = $this->getAdapter();

        // First create the physical relationship columns in schema (orphan state)
        $adapter->createRelationship(
            'rel_orphan_p', 'rel_orphan_c',
            Database::RELATION_ONE_TO_ONE, false,
            'orphan_link', 'orphan_back'
        );

        // Now inject DuplicateException so the Database-level call catches it
        // and proceeds to metadata + index creation (columns already exist)
        $adapter->failOnNext('createRelationship', new DuplicateException('Relationship columns already exist'));

        $result = $db->createRelationship(
            collection: 'rel_orphan_p',
            relatedCollection: 'rel_orphan_c',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: false,
            id: 'orphan_link',
            twoWayKey: 'orphan_back'
        );
        $this->assertTrue($result);

        // Verify metadata was created despite DuplicateException
        $parent = $db->getCollection('rel_orphan_p');
        $found = false;
        foreach ($parent->getAttribute('attributes', []) as $attr) {
            if ($attr->getId() === 'orphan_link') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'Orphaned relationship should have metadata after recovery');
    }

    // ========================================================================
    // deleteRelationship (one-way)
    // ========================================================================

    public function testDeleteRelationshipOneWaySuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_rel_p');
        $this->freshCollection('del_rel_c');
        $db->createRelationship(
            collection: 'del_rel_p',
            relatedCollection: 'del_rel_c',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: false,
            id: 'del_link',
            twoWayKey: 'del_back'
        );

        $result = $db->deleteRelationship('del_rel_p', 'del_link');
        $this->assertTrue($result);

        $parent = $db->getCollection('del_rel_p');
        foreach ($parent->getAttribute('attributes', []) as $attr) {
            $this->assertNotEquals('del_link', $attr->getId());
        }
    }

    public function testDeleteRelationshipSchemaAlreadyGone(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('del_rel_gone_p');
        $this->freshCollection('del_rel_gone_c');
        $db->createRelationship(
            collection: 'del_rel_gone_p',
            relatedCollection: 'del_rel_gone_c',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: false,
            id: 'ghost_link',
            twoWayKey: 'ghost_back'
        );

        // Inject NotFoundException to simulate schema already being gone
        // (SQLite can't drop columns with indexes directly, so we simulate)
        $this->getAdapter()->failOnNext('deleteRelationship', new NotFoundException('Relationship already deleted'));

        // Database.deleteRelationship should still succeed via NotFoundException catch
        $result = $db->deleteRelationship('del_rel_gone_p', 'ghost_link');
        $this->assertTrue($result);

        $parent = $db->getCollection('del_rel_gone_p');
        foreach ($parent->getAttribute('attributes', []) as $attr) {
            $this->assertNotEquals('ghost_link', $attr->getId());
        }
    }

    // ========================================================================
    // updateRelationship (one-way)
    // ========================================================================

    public function testUpdateRelationshipRenameSuccess(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('upd_rel_p');
        $this->freshCollection('upd_rel_c');
        $db->createRelationship(
            collection: 'upd_rel_p',
            relatedCollection: 'upd_rel_c',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: false,
            id: 'orig_key',
            twoWayKey: 'orig_back'
        );

        $result = $db->updateRelationship(
            collection: 'upd_rel_p',
            id: 'orig_key',
            newKey: 'renamed_key',
        );
        $this->assertTrue($result);

        $parent = $db->getCollection('upd_rel_p');
        $names = [];
        foreach ($parent->getAttribute('attributes', []) as $attr) {
            $names[] = $attr->getId();
        }
        $this->assertContains('renamed_key', $names);
        $this->assertNotContains('orig_key', $names);
    }

    // ========================================================================
    // Idempotent retries — verify operations succeed after prior partial failure
    // ========================================================================

    public function testCreateAttributeIdempotentRetry(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idempotent_attr');

        $adapter = $this->getAdapter();

        // First attempt: schema succeeds but metadata fails
        $adapter->failMetadataUpdates(10);
        try {
            $db->createAttribute('idempotent_attr', 'retry_col', Database::VAR_STRING, 128, true);
            $this->fail('Should have thrown');
        } catch (DatabaseException $e) {
            // Expected
        }

        // Second attempt: should detect orphan and recover
        $adapter->clearFailures();
        $result = $db->createAttribute('idempotent_attr', 'retry_col', Database::VAR_STRING, 128, true);
        $this->assertTrue($result, 'Retry after failure should succeed via orphan recovery');

        $collection = $db->getCollection('idempotent_attr');
        $found = false;
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            if ($attr->getId() === 'retry_col') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found);
    }

    public function testCreateIndexIdempotentRetry(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idempotent_idx');
        $db->createAttribute('idempotent_idx', 'col', Database::VAR_STRING, 128, true);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);
        try {
            $db->createIndex('idempotent_idx', 'retry_idx', Database::INDEX_KEY, ['col']);
            $this->fail('Should have thrown');
        } catch (DatabaseException $e) {
            // Expected
        }

        $adapter->clearFailures();
        $result = $db->createIndex('idempotent_idx', 'retry_idx', Database::INDEX_KEY, ['col']);
        $this->assertTrue($result, 'Retry after failure should succeed via orphan recovery');
    }

    public function testCreateCollectionIdempotentRetry(): void
    {
        $db = $this->getDatabase();

        $adapter = $this->getAdapter();
        $adapter->failMetadataCreates(10);
        try {
            $db->createCollection('idempotent_coll');
            $this->fail('Should have thrown');
        } catch (DatabaseException $e) {
            // Expected
        }

        // Second attempt: table exists, metadata doesn't — inject DuplicateException
        // to simulate what a proper adapter would throw
        $adapter->clearFailures();
        $adapter->failOnNext('createCollection', new DuplicateException('Table already exists'));
        $result = $db->createCollection('idempotent_coll');
        $this->assertInstanceOf(Document::class, $result);

        $db->deleteCollection('idempotent_coll');
    }

    public function testDeleteAttributeIdempotentRetry(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idempotent_del_attr');
        $db->createAttribute('idempotent_del_attr', 'doomed', Database::VAR_STRING, 128, false);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);
        try {
            $db->deleteAttribute('idempotent_del_attr', 'doomed');
            $this->fail('Should have thrown');
        } catch (DatabaseException $e) {
            // Expected
        }

        $adapter->clearFailures();
        $result = $db->deleteAttribute('idempotent_del_attr', 'doomed');
        $this->assertTrue($result, 'Retry after rollback should succeed');

        $collection = $db->getCollection('idempotent_del_attr');
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            $this->assertNotEquals('doomed', $attr->getId());
        }
    }

    public function testDeleteIndexIdempotentRetry(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idempotent_del_idx');
        $db->createAttribute('idempotent_del_idx', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('idempotent_del_idx', 'doomed_idx', Database::INDEX_KEY, ['col']);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);
        try {
            $db->deleteIndex('idempotent_del_idx', 'doomed_idx');
            $this->fail('Should have thrown');
        } catch (DatabaseException $e) {
            // Expected
        }

        $adapter->clearFailures();
        $result = $db->deleteIndex('idempotent_del_idx', 'doomed_idx');
        $this->assertTrue($result, 'Retry after rollback should succeed');
    }

    public function testRenameAttributeIdempotentRetry(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idempotent_rename');
        $db->createAttribute('idempotent_rename', 'old_col', Database::VAR_STRING, 128, false);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);
        try {
            $db->renameAttribute('idempotent_rename', 'old_col', 'new_col');
            $this->fail('Should have thrown');
        } catch (DatabaseException $e) {
            // Expected
        }

        // Column was renamed then rolled back — retry should succeed
        $adapter->clearFailures();
        $result = $db->renameAttribute('idempotent_rename', 'old_col', 'new_col');
        $this->assertTrue($result, 'Retry after rollback should succeed');

        $collection = $db->getCollection('idempotent_rename');
        $names = [];
        foreach ($collection->getAttribute('attributes', []) as $attr) {
            $names[] = $attr->getId();
        }
        $this->assertContains('new_col', $names);
    }

    public function testRenameIndexIdempotentRetry(): void
    {
        $db = $this->getDatabase();
        $this->freshCollection('idempotent_rename_idx');
        $db->createAttribute('idempotent_rename_idx', 'col', Database::VAR_STRING, 128, true);
        $db->createIndex('idempotent_rename_idx', 'old_idx', Database::INDEX_KEY, ['col']);

        $adapter = $this->getAdapter();
        $adapter->failMetadataUpdates(10);
        try {
            $db->renameIndex('idempotent_rename_idx', 'old_idx', 'new_idx');
            $this->fail('Should have thrown');
        } catch (DatabaseException $e) {
            // Expected
        }

        $adapter->clearFailures();
        $result = $db->renameIndex('idempotent_rename_idx', 'old_idx', 'new_idx');
        $this->assertTrue($result, 'Retry after rollback should succeed');
    }

    // ========================================================================
    // FailableAdapter — verify test infrastructure works
    // ========================================================================

    public function testFailableAdapterTracksCallCounts(): void
    {
        $db = $this->getDatabase();
        $adapter = $this->getAdapter();
        $adapter->resetCallCounts();

        $this->freshCollection('tracking_test');
        $db->createAttribute('tracking_test', 'tracked', Database::VAR_STRING, 128, false);

        $this->assertGreaterThan(0, $adapter->getCallCount('createAttribute'));
        $this->assertGreaterThan(0, $adapter->getCallCount('createCollection'));
    }

    public function testFailableAdapterInjectsDuplicateException(): void
    {
        $adapter = $this->getAdapter();
        $adapter->failOnNext('createAttribute', new DuplicateException('Injected'));

        try {
            $adapter->createAttribute('nonexistent', 'col', Database::VAR_STRING, 128, true, false, false);
            $this->fail('Should have thrown');
        } catch (DuplicateException $e) {
            $this->assertEquals('Injected', $e->getMessage());
        }
    }

    public function testFailableAdapterQueuesMultipleFailures(): void
    {
        $adapter = $this->getAdapter();
        $adapter->failOnNext('deleteIndex', new NotFoundException('First'));
        $adapter->failOnNext('deleteIndex', new DatabaseException('Second'));

        try {
            $adapter->deleteIndex('x', 'y');
            $this->fail('First call should throw');
        } catch (NotFoundException $e) {
            $this->assertEquals('First', $e->getMessage());
        }

        try {
            $adapter->deleteIndex('x', 'y');
            $this->fail('Second call should throw');
        } catch (DatabaseException $e) {
            $this->assertEquals('Second', $e->getMessage());
        }
    }
}
