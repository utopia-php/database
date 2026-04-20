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
