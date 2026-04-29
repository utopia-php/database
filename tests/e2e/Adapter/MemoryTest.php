<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Tests for the in-memory adapter. This suite intentionally sticks to the
 * basic CRUD + query surface the adapter supports. Relationships, operators,
 * spatial types, vectors, fulltext and regex are deliberately not implemented
 * and are verified to throw.
 */
class MemoryTest extends TestCase
{
    protected Database $database;
    protected Authorization $authorization;

    protected function setUp(): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole('any');

        $database = new Database(new Memory(), new Cache(new MemoryCache()));
        $database
            ->setAuthorization($this->authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace('memory_' . \uniqid());

        $database->create();

        $this->database = $database;
    }

    public function testDatabaseLifecycle(): void
    {
        $this->assertTrue($this->database->exists());
        $this->database->delete();
        $this->assertFalse($this->database->exists());
    }

    public function testCreateAndDeleteCollection(): void
    {
        $collection = $this->database->createCollection('posts', [
            new Document([
                '$id' => 'title',
                'type' => Database::VAR_STRING,
                'size' => 128,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        $this->assertEquals('posts', $collection->getId());
        $this->assertTrue($this->database->exists(null, 'posts'));

        $this->database->deleteCollection('posts');
        $this->assertFalse($this->database->exists(null, 'posts'));
    }

    public function testAttributeCrud(): void
    {
        $this->database->createCollection('books');

        $this->assertTrue($this->database->createAttribute('books', 'title', Database::VAR_STRING, 128, true));
        $this->assertTrue($this->database->createAttribute('books', 'pages', Database::VAR_INTEGER, 0, true));

        $updated = $this->database->updateAttribute('books', 'title', Database::VAR_STRING, 256);
        $this->assertEquals(256, $updated->getAttribute('size'));
        $this->assertTrue($this->database->renameAttribute('books', 'title', 'heading'));
        $this->assertTrue($this->database->deleteAttribute('books', 'heading'));
    }

    public function testIndexCrud(): void
    {
        $this->database->createCollection('widgets');
        $this->database->createAttribute('widgets', 'name', Database::VAR_STRING, 128, true);
        $this->database->createAttribute('widgets', 'count', Database::VAR_INTEGER, 0, true);

        $this->assertTrue(
            $this->database->createIndex('widgets', 'idx_name', Database::INDEX_KEY, ['name'])
        );
        $this->assertTrue(
            $this->database->createIndex('widgets', 'unique_count', Database::INDEX_UNIQUE, ['count'])
        );
        $this->assertTrue($this->database->renameIndex('widgets', 'idx_name', 'idx_name_renamed'));
        $this->assertTrue($this->database->deleteIndex('widgets', 'idx_name_renamed'));
    }

    public function testFulltextIndexIsNotImplemented(): void
    {
        $this->database->createCollection('articles');
        $this->database->createAttribute('articles', 'body', Database::VAR_STRING, 1024, true);

        $this->expectException(DatabaseException::class);
        $this->database->createIndex('articles', 'body_idx', Database::INDEX_FULLTEXT, ['body']);
    }

    public function testDocumentCrud(): void
    {
        $this->database->createCollection('notes', [
            new Document([
                '$id' => 'title',
                'type' => Database::VAR_STRING,
                'size' => 128,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'body',
                'type' => Database::VAR_STRING,
                'size' => 4096,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $created = $this->database->createDocument('notes', new Document([
            '$id' => 'note1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'title' => 'Hello',
            'body' => 'World',
        ]));

        $this->assertEquals('note1', $created->getId());
        $this->assertNotEmpty($created->getSequence());

        $fetched = $this->database->getDocument('notes', 'note1');
        $this->assertEquals('Hello', $fetched->getAttribute('title'));

        $fetched->setAttribute('title', 'Hello Updated');
        $updated = $this->database->updateDocument('notes', 'note1', $fetched);
        $this->assertEquals('Hello Updated', $updated->getAttribute('title'));

        $this->assertTrue($this->database->deleteDocument('notes', 'note1'));
        $this->assertTrue($this->database->getDocument('notes', 'note1')->isEmpty());
    }

    public function testDuplicateIdThrows(): void
    {
        $this->database->createCollection('labels');
        $this->database->createAttribute('labels', 'name', Database::VAR_STRING, 64, true);

        $this->database->createDocument('labels', new Document([
            '$id' => 'a',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'x',
        ]));

        $this->expectException(DuplicateException::class);
        $this->database->createDocument('labels', new Document([
            '$id' => 'a',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'y',
        ]));
    }

    public function testUniqueIndexEnforcement(): void
    {
        $this->database->createCollection('users', [
            new Document([
                '$id' => 'email',
                'type' => Database::VAR_STRING,
                'size' => 128,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [
            new Document([
                '$id' => 'unique_email',
                'type' => Database::INDEX_UNIQUE,
                'attributes' => ['email'],
            ]),
        ]);

        $this->database->createDocument('users', new Document([
            '$id' => 'u1',
            '$permissions' => [Permission::read(Role::any())],
            'email' => 'a@example.com',
        ]));

        $this->expectException(DuplicateException::class);
        $this->database->createDocument('users', new Document([
            '$id' => 'u2',
            '$permissions' => [Permission::read(Role::any())],
            'email' => 'a@example.com',
        ]));
    }

    public function testFindWithBasicQueries(): void
    {
        $this->seedNumbers();

        $results = $this->database->find('numbers', [Query::greaterThan('value', 5)]);
        $values = \array_map(fn (Document $d) => $d->getAttribute('value'), $results);
        \sort($values);
        $this->assertEquals([6, 7, 8, 9, 10], $values);

        $results = $this->database->find('numbers', [Query::between('value', 3, 5)]);
        $this->assertCount(3, $results);

        $results = $this->database->find('numbers', [Query::equal('category', ['even'])]);
        $this->assertCount(5, $results);

        $results = $this->database->find('numbers', [Query::notEqual('category', 'even')]);
        $this->assertCount(5, $results);

        $results = $this->database->find('numbers', [Query::isNull('tag')]);
        $this->assertCount(10, $results);
    }

    public function testFindStartsWithEndsWith(): void
    {
        $this->database->createCollection('names', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        foreach (['alpha', 'alphabet', 'beta', 'gamma', 'delta'] as $n) {
            $this->database->createDocument('names', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'name' => $n,
            ]));
        }

        $starts = $this->database->find('names', [Query::startsWith('name', 'alpha')]);
        $this->assertCount(2, $starts);

        $ends = $this->database->find('names', [Query::endsWith('name', 'a')]);
        $this->assertCount(4, $ends);
    }

    public function testOrderAndLimitAndOffset(): void
    {
        $this->seedNumbers();

        $results = $this->database->find('numbers', [
            Query::orderAsc('value'),
            Query::limit(3),
        ]);
        $this->assertEquals([1, 2, 3], \array_map(fn ($d) => $d->getAttribute('value'), $results));

        $results = $this->database->find('numbers', [
            Query::orderDesc('value'),
            Query::limit(3),
        ]);
        $this->assertEquals([10, 9, 8], \array_map(fn ($d) => $d->getAttribute('value'), $results));

        $results = $this->database->find('numbers', [
            Query::orderAsc('value'),
            Query::limit(3),
            Query::offset(3),
        ]);
        $this->assertEquals([4, 5, 6], \array_map(fn ($d) => $d->getAttribute('value'), $results));
    }

    public function testCountAndSum(): void
    {
        $this->seedNumbers();

        $this->assertEquals(10, $this->database->count('numbers'));
        $this->assertEquals(55, $this->database->sum('numbers', 'value'));
        $this->assertEquals(30, $this->database->sum('numbers', 'value', [Query::equal('category', ['even'])]));
    }

    public function testBatchCreateAndDelete(): void
    {
        $this->database->createCollection('tags', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        $docs = [];
        for ($i = 0; $i < 5; $i++) {
            $docs[] = new Document([
                '$id' => "tag{$i}",
                '$permissions' => [Permission::read(Role::any()), Permission::delete(Role::any())],
                'name' => "tag-{$i}",
            ]);
        }
        $created = $this->database->createDocuments('tags', $docs);
        $this->assertEquals(5, $created);
        $this->assertEquals(5, $this->database->count('tags'));

        $deleted = $this->database->deleteDocuments('tags');
        $this->assertEquals(5, $deleted);
        $this->assertEquals(0, $this->database->count('tags'));
    }

    public function testIncreaseDocumentAttribute(): void
    {
        $this->database->createCollection('counters', [
            new Document([
                '$id' => 'count',
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        $this->database->createDocument('counters', new Document([
            '$id' => 'c1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => 1,
        ]));

        $this->database->increaseDocumentAttribute('counters', 'c1', 'count', 4);
        $fetched = $this->database->getDocument('counters', 'c1');
        $this->assertEquals(5, $fetched->getAttribute('count'));

        $this->database->decreaseDocumentAttribute('counters', 'c1', 'count', 2);
        $fetched = $this->database->getDocument('counters', 'c1');
        $this->assertEquals(3, $fetched->getAttribute('count'));
    }

    public function testPermissionsFilterResults(): void
    {
        $this->database->createCollection('items', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        // Public readable
        $this->database->createDocument('items', new Document([
            '$id' => 'public',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'public',
        ]));

        // Only user:alice readable
        $this->database->createDocument('items', new Document([
            '$id' => 'private',
            '$permissions' => [Permission::read(Role::user('alice'))],
            'name' => 'private',
        ]));

        // With default 'any' role we should see only the public doc
        $results = $this->database->find('items');
        $this->assertCount(1, $results);
        $this->assertEquals('public', $results[0]->getId());

        // Add alice role and both docs show up
        $this->authorization->addRole('user:alice');
        $results = $this->database->find('items');
        $this->assertCount(2, $results);

        // Skipping auth lists everything
        $this->authorization->removeRole('user:alice');
        $results = $this->authorization->skip(fn () => $this->database->find('items'));
        $this->assertCount(2, $results);
    }

    public function testTransactionCommit(): void
    {
        $this->database->createCollection('tx', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        $this->database->withTransaction(function () {
            $this->database->createDocument('tx', new Document([
                '$id' => 'd1',
                '$permissions' => [Permission::read(Role::any())],
                'name' => 'first',
            ]));
        });

        $this->assertEquals(1, $this->database->count('tx'));
    }

    public function testTransactionRollback(): void
    {
        $this->database->createCollection('txr', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        try {
            $this->database->withTransaction(function () {
                $this->database->createDocument('txr', new Document([
                    '$id' => 'd1',
                    '$permissions' => [Permission::read(Role::any())],
                    'name' => 'first',
                ]));

                throw new \RuntimeException('force rollback');
            });
        } catch (\RuntimeException) {
            // expected
        }

        $this->assertEquals(0, $this->database->count('txr'));
    }

    public function testRelationshipsAreNotImplemented(): void
    {
        $this->database->createCollection('posts');
        $this->database->createCollection('authors');

        $this->expectException(DatabaseException::class);
        $this->database->getAdapter()->createRelationship('posts', 'authors', Database::RELATION_ONE_TO_ONE);
    }

    public function testUpsertIsNotImplemented(): void
    {
        $collection = new Document(['$id' => 'any']);
        $this->expectException(DatabaseException::class);
        $this->database->getAdapter()->upsertDocuments($collection, '', []);
    }

    public function testNestedTransactionRollbackOnlyDiscardsInner(): void
    {
        $this->database->createCollection('nested', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        $adapter = $this->database->getAdapter();
        $adapter->startTransaction();
        $this->database->createDocument('nested', new Document([
            '$id' => 'outer',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'outer',
        ]));

        $adapter->startTransaction();
        $this->database->createDocument('nested', new Document([
            '$id' => 'inner',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'inner',
        ]));
        $adapter->rollbackTransaction();

        $this->assertTrue($adapter->inTransaction());
        $adapter->commitTransaction();

        $this->assertFalse($this->database->getDocument('nested', 'outer')->isEmpty());
        $this->assertTrue($this->database->getDocument('nested', 'inner')->isEmpty());
    }

    public function testArrayAttributeRoundTrip(): void
    {
        $this->database->createCollection('lists', [
            new Document([
                '$id' => 'tags',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => false,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ]),
        ]);

        $this->database->createDocument('lists', new Document([
            '$id' => 'l1',
            '$permissions' => [Permission::read(Role::any())],
            'tags' => ['php', 'memory', 'adapter'],
        ]));

        $fetched = $this->database->getDocument('lists', 'l1');
        $this->assertSame(['php', 'memory', 'adapter'], $fetched->getAttribute('tags'));
    }

    public function testCreateUniqueIndexRejectsExistingDuplicates(): void
    {
        // MariaDB rejects CREATE UNIQUE INDEX with errno 1062 when existing rows
        // contain duplicates; the adapter surfaces that as DuplicateException
        // and Database::createIndex silently treats it as an "orphan" index.
        // Memory mirrors that contract — DuplicateException at the adapter level.
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

    public function testUniqueIndexAllowsMultipleNulls(): void
    {
        $this->database->createCollection('optional', [
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
        ]);

        $this->database->createDocument('optional', new Document([
            '$id' => 'a',
            '$permissions' => [Permission::read(Role::any())],
            'token' => null,
        ]));
        $this->database->createDocument('optional', new Document([
            '$id' => 'b',
            '$permissions' => [Permission::read(Role::any())],
            'token' => null,
        ]));

        $this->assertEquals(2, $this->database->count('optional'));
    }

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

    public function testBatchUpdateEnforcesUniqueIndexes(): void
    {
        $this->database->createCollection('handles', [
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
        ]);

        $this->database->createDocument('handles', new Document([
            '$id' => 'h1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'handle' => 'taken',
        ]));
        $this->database->createDocument('handles', new Document([
            '$id' => 'h2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'handle' => 'free',
        ]));

        $this->expectException(DuplicateException::class);
        $this->database->updateDocuments('handles', new Document(['handle' => 'taken']), [
            Query::equal('$id', ['h2']),
        ]);
    }

    public function testBulkDeleteRemovesPermissions(): void
    {
        $this->database->createCollection('cleanup', [
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
            $this->database->createDocument('cleanup', new Document([
                '$id' => "c{$i}",
                '$permissions' => [Permission::read(Role::any()), Permission::delete(Role::any())],
                'name' => "n{$i}",
            ]));
        }

        $this->database->deleteDocuments('cleanup');

        $adapter = $this->database->getAdapter();
        $permissions = (new \ReflectionClass($adapter))->getProperty('permissions')->getValue($adapter);
        $key = $this->database->getNamespace() . '_cleanup';

        $this->assertEmpty($permissions[$key] ?? []);
    }

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

        // Tenant 1 and 2 each own a single row whose auto-incrementing _id
        // sequences may collide. Asking tenant 1 to delete sequence 1 must not
        // touch tenant 2's row, even though they share the underlying map.
        $adapter->setTenant(1);
        $deleted = $adapter->deleteDocuments('box', ['1'], []);

        $this->assertEquals(1, $deleted);

        $adapter->setTenant(2);
        $survivor = $adapter->getDocument($collection, 'b');
        $this->assertEquals('tenant2-doc', $survivor->getAttribute('name'));
    }

    public function testFindAppliesSelectProjection(): void
    {
        $this->database->createCollection('proj', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'secret',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        $this->database->createDocument('proj', new Document([
            '$id' => 'p1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'visible',
            'secret' => 'hidden',
        ]));

        $results = $this->database->find('proj', [Query::select(['name'])]);
        $this->assertCount(1, $results);
        $this->assertEquals('visible', $results[0]->getAttribute('name'));
        $this->assertNull($results[0]->getAttribute('secret'));
        // Internals always survive.
        $this->assertEquals('p1', $results[0]->getId());
        $this->assertNotEmpty($results[0]->getSequence());
    }

    public function testFindDefaultOrderingIsSequenceAscending(): void
    {
        $this->seedNumbers();

        // No explicit order: results should follow insertion (clustered _id) order.
        $results = $this->database->find('numbers', [Query::limit(3)]);
        $values = \array_map(fn (Document $d) => $d->getAttribute('value'), $results);
        $this->assertEquals([1, 2, 3], $values);
    }

    public function testCountHonoursMaxZero(): void
    {
        $this->seedNumbers();
        // LIMIT 0 returns zero rows on MariaDB.
        $this->assertEquals(0, $this->database->count('numbers', [], 0));
    }

    public function testSumHonoursMaxZero(): void
    {
        $this->seedNumbers();
        $this->assertEquals(0, $this->database->sum('numbers', 'value', [], 0));
    }

    public function testIncreaseSilentlyNoopsOnBoundViolation(): void
    {
        $this->database->createCollection('clamped', [
            new Document([
                '$id' => 'count',
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        $this->database->createDocument('clamped', new Document([
            '$id' => 'c1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => 5,
        ]));

        // Bound violated: MariaDB's UPDATE matches zero rows but still returns true.
        $this->assertTrue(
            $this->database->getAdapter()->increaseDocumentAttribute(
                'clamped',
                'c1',
                'count',
                10,
                (new \DateTime())->format('Y-m-d H:i:s.u'),
                null,
                10,
            )
        );

        $fetched = $this->database->getDocument('clamped', 'c1');
        $this->assertEquals(5, $fetched->getAttribute('count'));
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

        // Adapter currently scoped to tenant 7; ask for sequence of a doc that
        // claims tenant 1 — must use the document's tenant, not the adapter's.
        $probe = new Document(['$id' => 'a', '$tenant' => 1]);
        [$result] = $adapter->getSequences('box', [$probe]);
        $this->assertNotEmpty($result->getSequence());
    }

    public function testRandomOrderingShufflesResults(): void
    {
        $this->seedNumbers();

        // Random order is non-deterministic; we just verify the path returns
        // the same set of rows without blowing up usort's transitivity.
        $results = $this->database->find('numbers', [Query::orderRandom()]);
        $this->assertCount(10, $results);
    }

    public function testUniqueIndexNormalizesBoolAndNumericString(): void
    {
        $this->database->createCollection('flags', [
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
        ]);

        $this->database->createDocument('flags', new Document([
            '$id' => 'first',
            '$permissions' => [Permission::read(Role::any())],
            'active' => true,
        ]));

        // Document attr is bool true; the casting layer may normalise this on
        // disk — adapter must compare type-normalised values to catch the dup.
        $this->expectException(DuplicateException::class);
        $this->database->createDocument('flags', new Document([
            '$id' => 'second',
            '$permissions' => [Permission::read(Role::any())],
            'active' => true,
        ]));
    }

    protected function seedNumbers(): void
    {
        $this->database->createCollection('numbers', [
            new Document([
                '$id' => 'value',
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'category',
                'type' => Database::VAR_STRING,
                'size' => 32,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'tag',
                'type' => Database::VAR_STRING,
                'size' => 32,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ]);

        for ($i = 1; $i <= 10; $i++) {
            $this->database->createDocument('numbers', new Document([
                '$id' => 'n' . $i,
                '$permissions' => [Permission::read(Role::any())],
                'value' => $i,
                'category' => ($i % 2 === 0) ? 'even' : 'odd',
                'tag' => null,
            ]));
        }
    }
}
