<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

trait VectorTests
{
    public function testVectorAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Test that vector attributes can only be created on PostgreSQL
        $database->createCollection('vectorCollection');

        // Create a vector attribute with 3 dimensions
        $database->createAttribute('vectorCollection', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create a vector attribute with 128 dimensions
        $database->createAttribute('vectorCollection', new Attribute(key: 'large_embedding', type: ColumnType::Vector, size: 128, required: false, default: null));

        // Verify the attributes were created
        $collection = $database->getCollection('vectorCollection');
        $attributes = $collection->getAttribute('attributes');

        $embeddingAttr = null;
        $largeEmbeddingAttr = null;

        foreach ($attributes as $attr) {
            if ($attr['key'] === 'embedding') {
                $embeddingAttr = $attr;
            } elseif ($attr['key'] === 'large_embedding') {
                $largeEmbeddingAttr = $attr;
            }
        }

        $this->assertNotNull($embeddingAttr);
        $this->assertNotNull($largeEmbeddingAttr);
        $this->assertEquals(ColumnType::Vector->value, $embeddingAttr['type']);
        $this->assertEquals(3, $embeddingAttr['size']);
        $this->assertEquals(ColumnType::Vector->value, $largeEmbeddingAttr['type']);
        $this->assertEquals(128, $largeEmbeddingAttr['size']);

        // Cleanup
        $database->deleteCollection('vectorCollection');
    }



    public function testVectorDocuments(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorDocuments');
        $database->createAttribute('vectorDocuments', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorDocuments', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create documents with vector data
        $doc1 = $database->createDocument('vectorDocuments', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Document 1',
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $doc2 = $database->createDocument('vectorDocuments', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Document 2',
            'embedding' => [0.0, 1.0, 0.0],
        ]));

        $doc3 = $database->createDocument('vectorDocuments', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Document 3',
            'embedding' => [0.0, 0.0, 1.0],
        ]));

        $this->assertNotEmpty($doc1->getId());
        $this->assertNotEmpty($doc2->getId());
        $this->assertNotEmpty($doc3->getId());

        $this->assertEquals([1.0, 0.0, 0.0], $doc1->getAttribute('embedding'));
        $this->assertEquals([0.0, 1.0, 0.0], $doc2->getAttribute('embedding'));
        $this->assertEquals([0.0, 0.0, 1.0], $doc3->getAttribute('embedding'));

        // Cleanup
        $database->deleteCollection('vectorDocuments');
    }

    public function testVectorQueries(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorQueries');
        $database->createAttribute('vectorQueries', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorQueries', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create test documents with read permissions
        $doc1 = $database->createDocument('vectorQueries', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Test 1',
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $doc2 = $database->createDocument('vectorQueries', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Test 2',
            'embedding' => [0.0, 1.0, 0.0],
        ]));

        $doc3 = $database->createDocument('vectorQueries', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Test 3',
            'embedding' => [0.5, 0.5, 0.0],
        ]));

        // Verify documents were created
        $this->assertNotEmpty($doc1->getId());
        $this->assertNotEmpty($doc2->getId());
        $this->assertNotEmpty($doc3->getId());

        // Test without vector queries first
        $allDocs = $database->find('vectorQueries');
        $this->assertCount(3, $allDocs, 'Should have 3 documents in collection');

        // Test vector dot product query
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id'),
        ]);

        $this->assertCount(3, $results);

        // Test vector cosine distance query
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id'),
        ]);

        $this->assertCount(3, $results);

        // Test vector euclidean distance query
        $results = $database->find('vectorQueries', [
            Query::vectorEuclidean('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id'),
        ]);

        $this->assertCount(3, $results);

        // Test vector queries with limit - should return only top results
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(2),
        ]);

        $this->assertCount(2, $results);
        // The most similar vector should be the one closest to [1.0, 0.0, 0.0]
        $this->assertEquals('Test 1', $results[0]->getAttribute('name'));

        // Test vector query with limit of 1
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [0.0, 1.0, 0.0]),
            Query::limit(1),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('Test 2', $results[0]->getAttribute('name'));

        // Test vector query combined with other filters
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [0.5, 0.5, 0.0]),
            Query::notEqual('name', 'Test 1'),
        ]);

        $this->assertCount(2, $results);
        // Should not contain Test 1
        foreach ($results as $result) {
            $this->assertNotEquals('Test 1', $result->getAttribute('name'));
        }

        // Test vector query with specific name filter
        $results = $database->find('vectorQueries', [
            Query::vectorEuclidean('embedding', [0.7, 0.7, 0.0]),
            Query::equal('name', ['Test 3']),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('Test 3', $results[0]->getAttribute('name'));

        // Test vector query with offset - skip first result
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [0.5, 0.5, 0.0]),
            Query::limit(2),
            Query::offset(1),
        ]);

        $this->assertCount(2, $results);
        // Should skip the most similar result

        // Test empty result with impossible filter combination
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::equal('name', ['Test 2']),
            Query::equal('name', ['Test 3']),  // Impossible condition
        ]);

        $this->assertCount(0, $results);

        // Test vector query with secondary ordering
        // Vector similarity takes precedence, name DESC is secondary
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [0.4, 0.6, 0.0]),
            Query::orderDesc('name'),
            Query::limit(2),
        ]);

        $this->assertCount(2, $results);
        // Results should be ordered primarily by vector similarity
        // The vector [0.4, 0.6, 0.0] is most similar to Test 2 [0.0, 1.0, 0.0]
        // and Test 3 [0.5, 0.5, 0.0] using dot product
        // Test 2 dot product: 0.4*0.0 + 0.6*1.0 + 0.0*0.0 = 0.6
        // Test 3 dot product: 0.4*0.5 + 0.6*0.5 + 0.0*0.0 = 0.5
        // So Test 2 should come first (higher dot product with negative inner product operator)
        $this->assertEquals('Test 2', $results[0]->getAttribute('name'));

        // Cleanup
        $database->deleteCollection('vectorQueries');
    }


    public function testVectorIndexes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorIndexes');
        $database->createAttribute('vectorIndexes', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create different types of vector indexes
        // Euclidean distance index (L2 distance)
        $database->createIndex('vectorIndexes', new Index(key: 'embedding_euclidean', type: IndexType::HnswEuclidean, attributes: ['embedding']));

        // Cosine distance index
        $database->createIndex('vectorIndexes', new Index(key: 'embedding_cosine', type: IndexType::HnswCosine, attributes: ['embedding']));

        // Inner product (dot product) index
        $database->createIndex('vectorIndexes', new Index(key: 'embedding_dot', type: IndexType::HnswDot, attributes: ['embedding']));

        // Verify indexes were created
        $collection = $database->getCollection('vectorIndexes');
        $indexes = $collection->getAttribute('indexes');

        $this->assertCount(3, $indexes);

        // Test that queries work with indexes
        $database->createDocument('vectorIndexes', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $database->createDocument('vectorIndexes', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [0.0, 1.0, 0.0],
        ]));

        // Query should use the appropriate index based on the operator
        $results = $database->find('vectorIndexes', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(1),
        ]);

        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vectorIndexes');
    }



    public function testVectorWithNullAndEmpty(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorNullEmpty');
        $database->createAttribute('vectorNullEmpty', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: false)); // Not required

        // Test with null vector (should work for non-required attribute)
        $doc1 = $database->createDocument('vectorNullEmpty', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => null,
        ]));

        $this->assertNull($doc1->getAttribute('embedding'));

        // Test with empty array (should fail)
        try {
            $database->createDocument('vectorNullEmpty', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'embedding' => [],
            ]));
            $this->fail('Should have thrown exception for empty vector');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric values', strtolower($e->getMessage()));
        }

        // Cleanup
        $database->deleteCollection('vectorNullEmpty');
    }

    public function testLargeVectors(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Test with maximum allowed dimensions (16000 for pgvector)
        $database->createCollection('vectorLarge');
        $database->createAttribute('vectorLarge', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 1536, required: true)); // Common embedding size

        // Create a large vector
        $largeVector = array_fill(0, 1536, 0.1);
        $largeVector[0] = 1.0; // Make first element different

        $doc = $database->createDocument('vectorLarge', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => $largeVector,
        ]));

        $this->assertCount(1536, $doc->getAttribute('embedding'));
        $this->assertEquals(1.0, $doc->getAttribute('embedding')[0]);

        // Test vector search on large vectors
        $searchVector = array_fill(0, 1536, 0.0);
        $searchVector[0] = 1.0;

        $results = $database->find('vectorLarge', [
            Query::vectorCosine('embedding', $searchVector),
        ]);

        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vectorLarge');
    }

    public function testVectorUpdates(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorUpdates');
        $database->createAttribute('vectorUpdates', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create initial document
        $doc = $database->createDocument('vectorUpdates', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $this->assertEquals([1.0, 0.0, 0.0], $doc->getAttribute('embedding'));

        // Update the vector
        $updated = $database->updateDocument('vectorUpdates', $doc->getId(), new Document([
            'embedding' => [0.0, 1.0, 0.0],
        ]));

        $this->assertEquals([0.0, 1.0, 0.0], $updated->getAttribute('embedding'));

        // Test partial update (should replace entire vector)
        $updated2 = $database->updateDocument('vectorUpdates', $doc->getId(), new Document([
            'embedding' => [0.5, 0.5, 0.5],
        ]));

        $this->assertEquals([0.5, 0.5, 0.5], $updated2->getAttribute('embedding'));

        // Cleanup
        $database->deleteCollection('vectorUpdates');
    }

    public function testMultipleVectorAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('multiVector');
        $database->createAttribute('multiVector', new Attribute(key: 'embedding1', type: ColumnType::Vector, size: 3, required: true));
        $database->createAttribute('multiVector', new Attribute(key: 'embedding2', type: ColumnType::Vector, size: 5, required: true));
        $database->createAttribute('multiVector', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        // Create documents with multiple vector attributes
        $doc1 = $database->createDocument('multiVector', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Doc 1',
            'embedding1' => [1.0, 0.0, 0.0],
            'embedding2' => [1.0, 0.0, 0.0, 0.0, 0.0],
        ]));

        $doc2 = $database->createDocument('multiVector', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Doc 2',
            'embedding1' => [0.0, 1.0, 0.0],
            'embedding2' => [0.0, 1.0, 0.0, 0.0, 0.0],
        ]));

        // Query by first vector
        $results = $database->find('multiVector', [
            Query::vectorCosine('embedding1', [1.0, 0.0, 0.0]),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('Doc 1', $results[0]->getAttribute('name'));

        // Query by second vector
        $results = $database->find('multiVector', [
            Query::vectorCosine('embedding2', [0.0, 1.0, 0.0, 0.0, 0.0]),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('Doc 2', $results[0]->getAttribute('name'));

        // Cleanup
        $database->deleteCollection('multiVector');
    }

    public function testVectorQueriesWithPagination(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorPagination');
        $database->createAttribute('vectorPagination', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));
        $database->createAttribute('vectorPagination', new Attribute(key: 'index', type: ColumnType::Integer, size: 0, required: true));

        // Create 10 documents
        for ($i = 0; $i < 10; $i++) {
            $database->createDocument('vectorPagination', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'index' => $i,
                'embedding' => [
                    cos($i * M_PI / 10),
                    sin($i * M_PI / 10),
                    0.0,
                ],
            ]));
        }

        // Test pagination with vector queries
        $searchVector = [1.0, 0.0, 0.0];

        // First page
        $page1 = $database->find('vectorPagination', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(3),
            Query::offset(0),
        ]);

        $this->assertCount(3, $page1);

        // Second page
        $page2 = $database->find('vectorPagination', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(3),
            Query::offset(3),
        ]);

        $this->assertCount(3, $page2);

        // Ensure different documents
        $page1Ids = array_map(fn ($doc) => $doc->getId(), $page1);
        $page2Ids = array_map(fn ($doc) => $doc->getId(), $page2);
        $this->assertEmpty(array_intersect($page1Ids, $page2Ids));

        // Test with cursor pagination
        $firstBatch = $database->find('vectorPagination', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(5),
        ]);

        $this->assertCount(5, $firstBatch);

        $lastDoc = $firstBatch[4];
        $nextBatch = $database->find('vectorPagination', [
            Query::vectorCosine('embedding', $searchVector),
            Query::cursorAfter($lastDoc),
            Query::limit(5),
        ]);

        $this->assertCount(5, $nextBatch);
        $this->assertNotEquals($lastDoc->getId(), $nextBatch[0]->getId());

        // Cleanup
        $database->deleteCollection('vectorPagination');
    }

    public function testCombinedVectorAndTextSearch(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorTextSearch');
        $database->createAttribute('vectorTextSearch', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorTextSearch', new Attribute(key: 'category', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute('vectorTextSearch', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create fulltext index for title
        $database->createIndex('vectorTextSearch', new Index(key: 'title_fulltext', type: IndexType::Fulltext, attributes: ['title']));

        // Create test documents
        $docs = [
            ['title' => 'Machine Learning Basics', 'category' => 'AI', 'embedding' => [1.0, 0.0, 0.0]],
            ['title' => 'Deep Learning Advanced', 'category' => 'AI', 'embedding' => [0.9, 0.1, 0.0]],
            ['title' => 'Web Development Guide', 'category' => 'Web', 'embedding' => [0.0, 1.0, 0.0]],
            ['title' => 'Database Design', 'category' => 'Data', 'embedding' => [0.0, 0.0, 1.0]],
            ['title' => 'AI Ethics', 'category' => 'AI', 'embedding' => [0.8, 0.2, 0.0]],
        ];

        foreach ($docs as $doc) {
            $database->createDocument('vectorTextSearch', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                ...$doc,
            ]));
        }

        // Combine vector search with category filter
        $results = $database->find('vectorTextSearch', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::equal('category', ['AI']),
            Query::limit(2),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('AI', $results[0]->getAttribute('category'));
        $this->assertEquals('Machine Learning Basics', $results[0]->getAttribute('title'));

        // Combine vector search with text search
        $results = $database->find('vectorTextSearch', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::search('title', 'Learning'),
            Query::limit(5),
        ]);

        $this->assertCount(2, $results);
        foreach ($results as $result) {
            $this->assertStringContainsString('Learning', $result->getAttribute('title'));
        }

        // Complex query with multiple filters
        $results = $database->find('vectorTextSearch', [
            Query::vectorEuclidean('embedding', [0.5, 0.5, 0.0]),
            Query::notEqual('category', ['Web']),
            Query::limit(3),
        ]);

        $this->assertCount(3, $results);
        foreach ($results as $result) {
            $this->assertNotEquals('Web', $result->getAttribute('category'));
        }

        // Cleanup
        $database->deleteCollection('vectorTextSearch');
    }

    public function testVectorSpecialFloatValues(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorSpecialFloats');
        $database->createAttribute('vectorSpecialFloats', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Test with very small values (near zero)
        $doc1 = $database->createDocument('vectorSpecialFloats', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1e-10, 1e-10, 1e-10],
        ]));

        $this->assertNotNull($doc1->getId());

        // Test with very large values
        $doc2 = $database->createDocument('vectorSpecialFloats', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1e10, 1e10, 1e10],
        ]));

        $this->assertNotNull($doc2->getId());

        // Test with negative values
        $doc3 = $database->createDocument('vectorSpecialFloats', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [-1.0, -0.5, -0.1],
        ]));

        $this->assertNotNull($doc3->getId());

        // Test with mixed sign values
        $doc4 = $database->createDocument('vectorSpecialFloats', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [-1.0, 0.0, 1.0],
        ]));

        $this->assertNotNull($doc4->getId());

        // Query with negative vector
        $results = $database->find('vectorSpecialFloats', [
            Query::vectorCosine('embedding', [-1.0, -1.0, -1.0]),
        ]);

        $this->assertGreaterThan(0, count($results));

        // Cleanup
        $database->deleteCollection('vectorSpecialFloats');
    }

    public function testVectorIndexPerformance(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorPerf');
        $database->createAttribute('vectorPerf', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 128, required: true));
        $database->createAttribute('vectorPerf', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        // Create documents
        $numDocs = 100;
        for ($i = 0; $i < $numDocs; $i++) {
            $vector = [];
            for ($j = 0; $j < 128; $j++) {
                $vector[] = sin($i * $j * 0.01);
            }

            $database->createDocument('vectorPerf', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => "Doc $i",
                'embedding' => $vector,
            ]));
        }

        // Query without index
        $searchVector = array_fill(0, 128, 0.5);

        $startTime = microtime(true);
        $results1 = $database->find('vectorPerf', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(10),
        ]);
        $timeWithoutIndex = microtime(true) - $startTime;

        $this->assertCount(10, $results1);

        // Create HNSW index
        $database->createIndex('vectorPerf', new Index(key: 'embedding_hnsw', type: IndexType::HnswCosine, attributes: ['embedding']));

        // Query with index (should be faster for larger datasets)
        $startTime = microtime(true);
        $results2 = $database->find('vectorPerf', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(10),
        ]);
        $timeWithIndex = microtime(true) - $startTime;

        $this->assertCount(10, $results2);

        // Results should be the same
        $this->assertEquals(
            array_map(fn ($d) => $d->getId(), $results1),
            array_map(fn ($d) => $d->getId(), $results2)
        );

        // Cleanup
        $database->deleteCollection('vectorPerf');
    }


    public function testVectorNormalization(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorNorm');
        $database->createAttribute('vectorNorm', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create documents with normalized and non-normalized vectors
        $doc1 = $database->createDocument('vectorNorm', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0], // Already normalized
        ]));

        $doc2 = $database->createDocument('vectorNorm', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [3.0, 4.0, 0.0], // Not normalized (magnitude = 5)
        ]));

        // Cosine similarity should work regardless of normalization
        $results = $database->find('vectorNorm', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
        ]);

        $this->assertCount(2, $results);

        // For cosine similarity, [3, 4, 0] has similarity 3/5 = 0.6 with [1, 0, 0]
        // So [1, 0, 0] should be first (similarity = 1.0)
        $this->assertEquals([1.0, 0.0, 0.0], $results[0]->getAttribute('embedding'));

        // Cleanup
        $database->deleteCollection('vectorNorm');
    }

    public function testVectorWithInfinityValues(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorInfinity');
        $database->createAttribute('vectorInfinity', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Test with INF value - should fail
        try {
            $database->createDocument('vectorInfinity', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'embedding' => [INF, 0.0, 0.0],
            ]));
            $this->fail('Should have thrown exception for INF value');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }

        // Test with -INF value - should fail
        try {
            $database->createDocument('vectorInfinity', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'embedding' => [-INF, 0.0, 0.0],
            ]));
            $this->fail('Should have thrown exception for -INF value');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }

        // Cleanup
        $database->deleteCollection('vectorInfinity');
    }

    public function testVectorWithNaNValues(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorNaN');
        $database->createAttribute('vectorNaN', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Test with NaN value - should fail
        try {
            $database->createDocument('vectorNaN', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'embedding' => [NAN, 0.0, 0.0],
            ]));
            $this->fail('Should have thrown exception for NaN value');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }

        // Cleanup
        $database->deleteCollection('vectorNaN');
    }






    public function testVectorWithRelationships(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create parent collection with vectors
        $database->createCollection('vectorParent');
        $database->createAttribute('vectorParent', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorParent', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create child collection
        $database->createCollection('vectorChild');
        $database->createAttribute('vectorChild', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));
        $database->createRelationship(new Relationship(
            collection: 'vectorChild',
            relatedCollection: 'vectorParent',
            type: RelationType::ManyToOne,
            twoWay: true,
            key: 'parent',
            twoWayKey: 'children',
        ));

        // Create parent documents with vectors
        $parent1 = $database->createDocument('vectorParent', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Parent 1',
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $parent2 = $database->createDocument('vectorParent', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Parent 2',
            'embedding' => [0.0, 1.0, 0.0],
        ]));

        // Create child documents
        $child1 = $database->createDocument('vectorChild', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'title' => 'Child 1',
            'parent' => $parent1->getId(),
        ]));

        $child2 = $database->createDocument('vectorChild', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'title' => 'Child 2',
            'parent' => $parent2->getId(),
        ]));

        // Query parents by vector similarity
        $results = $database->find('vectorParent', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('Parent 1', $results[0]->getAttribute('name'));

        // Verify relationships are intact
        $parent1Fetched = $database->getDocument('vectorParent', $parent1->getId());
        $children = $parent1Fetched->getAttribute('children');
        $this->assertCount(1, $children);
        $this->assertEquals('Child 1', $children[0]->getAttribute('title'));

        // Query with vector and relationship filter combined
        $results = $database->find('vectorParent', [
            Query::vectorCosine('embedding', [0.5, 0.5, 0.0]),
            Query::equal('name', ['Parent 1']),
        ]);

        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vectorChild');
        $database->deleteCollection('vectorParent');
    }

    public function testVectorWithTwoWayRelationships(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create two collections with two-way relationship and vectors
        $database->createCollection('vectorAuthors');
        $database->createAttribute('vectorAuthors', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorAuthors', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        $database->createCollection('vectorBooks');
        $database->createAttribute('vectorBooks', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorBooks', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));
        $database->createRelationship(new Relationship(
            collection: 'vectorBooks',
            relatedCollection: 'vectorAuthors',
            type: RelationType::ManyToOne,
            twoWay: true,
            key: 'author',
            twoWayKey: 'books',
        ));

        // Create documents
        $author = $database->createDocument('vectorAuthors', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Author 1',
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $book1 = $database->createDocument('vectorBooks', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'title' => 'Book 1',
            'embedding' => [0.9, 0.1, 0.0],
            'author' => $author->getId(),
        ]));

        $book2 = $database->createDocument('vectorBooks', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'title' => 'Book 2',
            'embedding' => [0.8, 0.2, 0.0],
            'author' => $author->getId(),
        ]));

        // Query books by vector similarity
        $results = $database->find('vectorBooks', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(1),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('Book 1', $results[0]->getAttribute('title'));

        // Query authors and verify relationship
        $authorFetched = $database->getDocument('vectorAuthors', $author->getId());
        $books = $authorFetched->getAttribute('books');
        $this->assertCount(2, $books);

        // Cleanup
        $database->deleteCollection('vectorBooks');
        $database->deleteCollection('vectorAuthors');
    }

    public function testVectorAllZeros(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorZeros');
        $database->createAttribute('vectorZeros', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create document with all-zeros vector
        $doc = $database->createDocument('vectorZeros', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [0.0, 0.0, 0.0],
        ]));

        $this->assertEquals([0.0, 0.0, 0.0], $doc->getAttribute('embedding'));

        // Create another document with non-zero vector
        $doc2 = $database->createDocument('vectorZeros', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        // Query with zero vector - cosine similarity should handle gracefully
        $results = $database->find('vectorZeros', [
            Query::vectorCosine('embedding', [0.0, 0.0, 0.0]),
        ]);

        // Should return documents, though similarity may be undefined
        $this->assertGreaterThan(0, count($results));

        // Query with non-zero vector against zero vectors
        $results = $database->find('vectorZeros', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
        ]);

        $this->assertCount(2, $results);

        // Cleanup
        $database->deleteCollection('vectorZeros');
    }


    public function testDeleteVectorAttribute(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorDeleteAttr');
        $database->createAttribute('vectorDeleteAttr', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorDeleteAttr', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create document with vector
        $doc = $database->createDocument('vectorDeleteAttr', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Test',
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $this->assertNotNull($doc->getAttribute('embedding'));

        // Delete the vector attribute
        $result = $database->deleteAttribute('vectorDeleteAttr', 'embedding');
        $this->assertTrue($result);

        // Verify attribute is gone
        $collection = $database->getCollection('vectorDeleteAttr');
        $attributes = $collection->getAttribute('attributes');
        foreach ($attributes as $attr) {
            $this->assertNotEquals('embedding', $attr['key']);
        }

        // Fetch document - should not have embedding anymore
        $docFetched = $database->getDocument('vectorDeleteAttr', $doc->getId());
        $this->assertNull($docFetched->getAttribute('embedding', null));

        // Cleanup
        $database->deleteCollection('vectorDeleteAttr');
    }

    public function testDeleteAttributeWithVectorIndexes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorDeleteIndexedAttr');
        $database->createAttribute('vectorDeleteIndexedAttr', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create multiple indexes on the vector attribute
        $database->createIndex('vectorDeleteIndexedAttr', new Index(key: 'idx1', type: IndexType::HnswCosine, attributes: ['embedding']));
        $database->createIndex('vectorDeleteIndexedAttr', new Index(key: 'idx2', type: IndexType::HnswEuclidean, attributes: ['embedding']));

        // Create document
        $database->createDocument('vectorDeleteIndexedAttr', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        // Delete the attribute - should also delete indexes
        $result = $database->deleteAttribute('vectorDeleteIndexedAttr', 'embedding');
        $this->assertTrue($result);

        // Verify indexes are gone
        $collection = $database->getCollection('vectorDeleteIndexedAttr');
        $indexes = $collection->getAttribute('indexes');
        $this->assertCount(0, $indexes);

        // Cleanup
        $database->deleteCollection('vectorDeleteIndexedAttr');
    }



    public function testVectorCursorBeforePagination(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorCursorBefore');
        $database->createAttribute('vectorCursorBefore', new Attribute(key: 'index', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute('vectorCursorBefore', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create 10 documents
        for ($i = 0; $i < 10; $i++) {
            $database->createDocument('vectorCursorBefore', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'index' => $i,
                'embedding' => [1.0 - ($i * 0.05), $i * 0.05, 0.0],
            ]));
        }

        // Get first 5 results
        $firstBatch = $database->find('vectorCursorBefore', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(5),
        ]);

        $this->assertCount(5, $firstBatch);

        // Get results before the 4th document (backward pagination)
        $fourthDoc = $firstBatch[3];
        $beforeBatch = $database->find('vectorCursorBefore', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::cursorBefore($fourthDoc),
            Query::limit(3),
        ]);

        // Should get the 3 documents before the 4th one
        $this->assertCount(3, $beforeBatch);
        $beforeIds = array_map(fn ($d) => $d->getId(), $beforeBatch);
        $this->assertNotContains($fourthDoc->getId(), $beforeIds);

        // Cleanup
        $database->deleteCollection('vectorCursorBefore');
    }

    public function testVectorBackwardPagination(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorBackward');
        $database->createAttribute('vectorBackward', new Attribute(key: 'value', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute('vectorBackward', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create documents
        for ($i = 0; $i < 20; $i++) {
            $database->createDocument('vectorBackward', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'value' => $i,
                'embedding' => [cos($i * 0.1), sin($i * 0.1), 0.0],
            ]));
        }

        // Get last batch
        $allResults = $database->find('vectorBackward', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(20),
        ]);

        // Navigate backwards from the end
        $lastDoc = $allResults[19];
        $backwardBatch = $database->find('vectorBackward', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::cursorBefore($lastDoc),
            Query::limit(5),
        ]);

        $this->assertCount(5, $backwardBatch);

        // Continue backward pagination
        $firstOfBackward = $backwardBatch[0];
        $moreBackward = $database->find('vectorBackward', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::cursorBefore($firstOfBackward),
            Query::limit(5),
        ]);

        // Should get at least some results (may be less than 5 due to cursor position)
        $this->assertGreaterThan(0, count($moreBackward));
        $this->assertLessThanOrEqual(5, count($moreBackward));

        // Cleanup
        $database->deleteCollection('vectorBackward');
    }

    public function testVectorDimensionUpdate(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorDimUpdate');
        $database->createAttribute('vectorDimUpdate', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create document
        $doc = $database->createDocument('vectorDimUpdate', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $this->assertCount(3, $doc->getAttribute('embedding'));

        // Try to update attribute dimensions - should fail (immutable)
        try {
            $database->updateAttribute('vectorDimUpdate', 'embedding', ColumnType::Vector->value, 5, true);
            $this->fail('Should not allow changing vector dimensions');
        } catch (\Throwable $e) {
            // Expected - dimension changes not allowed (either validation or database error)
            $this->assertTrue(true);
        }

        // Cleanup
        $database->deleteCollection('vectorDimUpdate');
    }


    public function testVectorConcurrentUpdates(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorConcurrent');
        $database->createAttribute('vectorConcurrent', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));
        $database->createAttribute('vectorConcurrent', new Attribute(key: 'version', type: ColumnType::Integer, size: 0, required: true));

        // Create initial document
        $doc = $database->createDocument('vectorConcurrent', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
            'version' => 1,
        ]));

        // Simulate concurrent updates
        $update1 = $database->updateDocument('vectorConcurrent', $doc->getId(), new Document([
            'embedding' => [0.0, 1.0, 0.0],
            'version' => 2,
        ]));

        $update2 = $database->updateDocument('vectorConcurrent', $doc->getId(), new Document([
            'embedding' => [0.0, 0.0, 1.0],
            'version' => 3,
        ]));

        // Last update should win
        $final = $database->getDocument('vectorConcurrent', $doc->getId());
        $this->assertEquals([0.0, 0.0, 1.0], $final->getAttribute('embedding'));
        $this->assertEquals(3, $final->getAttribute('version'));

        // Cleanup
        $database->deleteCollection('vectorConcurrent');
    }

    public function testDeleteVectorIndexes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorDeleteIdx');
        $database->createAttribute('vectorDeleteIdx', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create index
        $database->createIndex('vectorDeleteIdx', new Index(key: 'idx_cosine', type: IndexType::HnswCosine, attributes: ['embedding']));

        // Verify index exists
        $collection = $database->getCollection('vectorDeleteIdx');
        $indexes = $collection->getAttribute('indexes');
        $this->assertCount(1, $indexes);

        // Create documents
        $database->createDocument('vectorDeleteIdx', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        // Delete index
        $result = $database->deleteIndex('vectorDeleteIdx', 'idx_cosine');
        $this->assertTrue($result);

        // Verify index is gone
        $collection = $database->getCollection('vectorDeleteIdx');
        $indexes = $collection->getAttribute('indexes');
        $this->assertCount(0, $indexes);

        // Queries should still work (without index optimization)
        $results = $database->find('vectorDeleteIdx', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
        ]);

        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vectorDeleteIdx');
    }

    public function testMultipleVectorIndexes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorMultiIdx');
        $database->createAttribute('vectorMultiIdx', new Attribute(key: 'embedding1', type: ColumnType::Vector, size: 3, required: true));
        $database->createAttribute('vectorMultiIdx', new Attribute(key: 'embedding2', type: ColumnType::Vector, size: 3, required: true));

        // Create multiple indexes on different vector attributes
        $database->createIndex('vectorMultiIdx', new Index(key: 'idx1_cosine', type: IndexType::HnswCosine, attributes: ['embedding1']));
        $database->createIndex('vectorMultiIdx', new Index(key: 'idx2_euclidean', type: IndexType::HnswEuclidean, attributes: ['embedding2']));

        // Verify both indexes exist
        $collection = $database->getCollection('vectorMultiIdx');
        $indexes = $collection->getAttribute('indexes');
        $this->assertCount(2, $indexes);

        // Create document
        $database->createDocument('vectorMultiIdx', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding1' => [1.0, 0.0, 0.0],
            'embedding2' => [0.0, 1.0, 0.0],
        ]));

        // Query using first index
        $results = $database->find('vectorMultiIdx', [
            Query::vectorCosine('embedding1', [1.0, 0.0, 0.0]),
        ]);
        $this->assertCount(1, $results);

        // Query using second index
        $results = $database->find('vectorMultiIdx', [
            Query::vectorEuclidean('embedding2', [0.0, 1.0, 0.0]),
        ]);
        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vectorMultiIdx');
    }


    public function testVectorQueryWithoutIndex(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorNoIndex');
        $database->createAttribute('vectorNoIndex', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create documents without any index
        $database->createDocument('vectorNoIndex', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $database->createDocument('vectorNoIndex', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [0.0, 1.0, 0.0],
        ]));

        // Queries should still work (sequential scan)
        $results = $database->find('vectorNoIndex', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
        ]);

        $this->assertCount(2, $results);

        // Cleanup
        $database->deleteCollection('vectorNoIndex');
    }

    public function testVectorQueryEmpty(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorEmptyQuery');
        $database->createAttribute('vectorEmptyQuery', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // No documents in collection
        $results = $database->find('vectorEmptyQuery', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
        ]);

        $this->assertCount(0, $results);

        // Cleanup
        $database->deleteCollection('vectorEmptyQuery');
    }

    public function testSingleDimensionVector(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorSingleDim');
        $database->createAttribute('vectorSingleDim', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 1, required: true));

        // Create documents with single-dimension vectors
        $doc1 = $database->createDocument('vectorSingleDim', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0],
        ]));

        $doc2 = $database->createDocument('vectorSingleDim', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [0.5],
        ]));

        $this->assertEquals([1.0], $doc1->getAttribute('embedding'));
        $this->assertEquals([0.5], $doc2->getAttribute('embedding'));

        // Query with single dimension
        $results = $database->find('vectorSingleDim', [
            Query::vectorCosine('embedding', [1.0]),
        ]);

        $this->assertCount(2, $results);

        // Cleanup
        $database->deleteCollection('vectorSingleDim');
    }

    public function testVectorLongResultSet(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorLongResults');
        $database->createAttribute('vectorLongResults', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create 100 documents
        for ($i = 0; $i < 100; $i++) {
            $database->createDocument('vectorLongResults', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'embedding' => [
                    sin($i * 0.1),
                    cos($i * 0.1),
                    sin($i * 0.05),
                ],
            ]));
        }

        // Query all results
        $results = $database->find('vectorLongResults', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(100),
        ]);

        $this->assertCount(100, $results);

        // Cleanup
        $database->deleteCollection('vectorLongResults');
    }

    public function testMultipleVectorQueriesOnSameCollection(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorMultiQuery');
        $database->createAttribute('vectorMultiQuery', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create documents
        for ($i = 0; $i < 10; $i++) {
            $database->createDocument('vectorMultiQuery', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'embedding' => [
                    cos($i * M_PI / 10),
                    sin($i * M_PI / 10),
                    0.0,
                ],
            ]));
        }

        // Execute multiple different vector queries
        $results1 = $database->find('vectorMultiQuery', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(5),
        ]);

        $results2 = $database->find('vectorMultiQuery', [
            Query::vectorEuclidean('embedding', [0.0, 1.0, 0.0]),
            Query::limit(5),
        ]);

        $results3 = $database->find('vectorMultiQuery', [
            Query::vectorDot('embedding', [0.5, 0.5, 0.0]),
            Query::limit(5),
        ]);

        // All should return results
        $this->assertCount(5, $results1);
        $this->assertCount(5, $results2);
        $this->assertCount(5, $results3);

        // Results should be different based on query vector
        $this->assertNotEquals(
            $results1[0]->getId(),
            $results2[0]->getId()
        );

        // Cleanup
        $database->deleteCollection('vectorMultiQuery');
    }


    public function testVectorLargeValues(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorLargeVals');
        $database->createAttribute('vectorLargeVals', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Test with very large float values (but not INF)
        $doc = $database->createDocument('vectorLargeVals', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1e38, -1e38, 1e37],
        ]));

        $this->assertNotNull($doc->getId());

        // Query should work
        $results = $database->find('vectorLargeVals', [
            Query::vectorCosine('embedding', [1e38, -1e38, 1e37]),
        ]);

        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vectorLargeVals');
    }

    public function testVectorPrecisionLoss(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorPrecision');
        $database->createAttribute('vectorPrecision', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create vector with high precision values
        $highPrecision = [0.123456789012345, 0.987654321098765, 0.555555555555555];
        $doc = $database->createDocument('vectorPrecision', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => $highPrecision,
        ]));

        // Retrieve and check precision (may have some loss)
        $retrieved = $doc->getAttribute('embedding');
        $this->assertCount(3, $retrieved);

        // Values should be close to original (allowing for float precision)
        for ($i = 0; $i < 3; $i++) {
            $this->assertEqualsWithDelta($highPrecision[$i], $retrieved[$i], 0.0001);
        }

        // Cleanup
        $database->deleteCollection('vectorPrecision');
    }

    public function testVector16000DimensionsBoundary(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Test exactly 16000 dimensions (pgvector limit)
        $database->createCollection('vector16000');
        $database->createAttribute('vector16000', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 16000, required: true));

        // Create a vector with exactly 16000 dimensions
        $largeVector = array_fill(0, 16000, 0.1);
        $largeVector[0] = 1.0;

        $doc = $database->createDocument('vector16000', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => $largeVector,
        ]));

        $this->assertCount(16000, $doc->getAttribute('embedding'));

        // Query should work
        $searchVector = array_fill(0, 16000, 0.0);
        $searchVector[0] = 1.0;

        $results = $database->find('vector16000', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(1),
        ]);

        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vector16000');
    }

    public function testVectorLargeDatasetIndexBuild(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorLargeDataset');
        $database->createAttribute('vectorLargeDataset', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 128, required: true));

        // Create 200 documents
        for ($i = 0; $i < 200; $i++) {
            $vector = [];
            for ($j = 0; $j < 128; $j++) {
                $vector[] = sin(($i + $j) * 0.01);
            }

            $database->createDocument('vectorLargeDataset', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'embedding' => $vector,
            ]));
        }

        // Create index on large dataset
        $database->createIndex('vectorLargeDataset', new Index(key: 'idx_hnsw', type: IndexType::HnswCosine, attributes: ['embedding']));

        // Verify queries work
        $searchVector = array_fill(0, 128, 0.5);
        $results = $database->find('vectorLargeDataset', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(10),
        ]);

        $this->assertCount(10, $results);

        // Cleanup
        $database->deleteCollection('vectorLargeDataset');
    }

    public function testVectorFilterDisabled(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorFilterDisabled');
        $database->createAttribute('vectorFilterDisabled', new Attribute(key: 'status', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute('vectorFilterDisabled', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create documents
        $database->createDocument('vectorFilterDisabled', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'status' => 'active',
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $database->createDocument('vectorFilterDisabled', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'status' => 'disabled',
            'embedding' => [0.9, 0.1, 0.0],
        ]));

        $database->createDocument('vectorFilterDisabled', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'status' => 'active',
            'embedding' => [0.8, 0.2, 0.0],
        ]));

        // Query with filter excluding disabled
        $results = $database->find('vectorFilterDisabled', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::notEqual('status', ['disabled']),
        ]);

        $this->assertCount(2, $results);
        foreach ($results as $doc) {
            $this->assertEquals('active', $doc->getAttribute('status'));
        }

        // Cleanup
        $database->deleteCollection('vectorFilterDisabled');
    }

    public function testVectorFilterOverride(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorFilterOverride');
        $database->createAttribute('vectorFilterOverride', new Attribute(key: 'category', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute('vectorFilterOverride', new Attribute(key: 'priority', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute('vectorFilterOverride', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        // Create documents
        for ($i = 0; $i < 5; $i++) {
            $database->createDocument('vectorFilterOverride', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'category' => $i < 3 ? 'A' : 'B',
                'priority' => $i,
                'embedding' => [1.0 - ($i * 0.1), $i * 0.1, 0.0],
            ]));
        }

        // Query with multiple filters
        $results = $database->find('vectorFilterOverride', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::equal('category', ['A']),
            Query::greaterThan('priority', 0),
            Query::limit(2),
        ]);

        // Should get category A documents with priority > 0
        $this->assertCount(2, $results);
        foreach ($results as $doc) {
            $this->assertEquals('A', $doc->getAttribute('category'));
            $this->assertGreaterThan(0, $doc->getAttribute('priority'));
        }

        // Cleanup
        $database->deleteCollection('vectorFilterOverride');
    }

    public function testMultipleFiltersOnVectorAttribute(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorMultiFilters');
        $database->createAttribute('vectorMultiFilters', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorMultiFilters', new Attribute(key: 'embedding1', type: ColumnType::Vector, size: 3, required: true));
        $database->createAttribute('vectorMultiFilters', new Attribute(key: 'embedding2', type: ColumnType::Vector, size: 3, required: true));

        // Create documents
        $database->createDocument('vectorMultiFilters', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Doc 1',
            'embedding1' => [1.0, 0.0, 0.0],
            'embedding2' => [0.0, 1.0, 0.0],
        ]));

        // Try to use multiple vector queries - should reject
        try {
            $database->find('vectorMultiFilters', [
                Query::vectorCosine('embedding1', [1.0, 0.0, 0.0]),
                Query::vectorCosine('embedding2', [0.0, 1.0, 0.0]),
            ]);
            $this->fail('Should not allow multiple vector queries');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('multiple vector queries', strtolower($e->getMessage()));
        }

        // Cleanup
        $database->deleteCollection('vectorMultiFilters');
    }

    public function testVectorQueryInNestedQuery(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorNested');
        $database->createAttribute('vectorNested', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('vectorNested', new Attribute(key: 'embedding1', type: ColumnType::Vector, size: 3, required: true));
        $database->createAttribute('vectorNested', new Attribute(key: 'embedding2', type: ColumnType::Vector, size: 3, required: true));

        // Create document
        $database->createDocument('vectorNested', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Doc 1',
            'embedding1' => [1.0, 0.0, 0.0],
            'embedding2' => [0.0, 1.0, 0.0],
        ]));

        // Try to use vector query in nested OR clause with another vector query - should reject
        try {
            $database->find('vectorNested', [
                Query::vectorCosine('embedding1', [1.0, 0.0, 0.0]),
                Query::or([
                    Query::vectorCosine('embedding2', [0.0, 1.0, 0.0]),
                    Query::equal('name', ['Doc 1']),
                ]),
            ]);
            $this->fail('Should not allow multiple vector queries across nested queries');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('multiple vector queries', strtolower($e->getMessage()));
        }

        // Cleanup
        $database->deleteCollection('vectorNested');
    }

    public function testVectorQueryCount(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorCount');
        $database->createAttribute('vectorCount', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        $database->createDocument('vectorCount', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $count = $database->count('vectorCount', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
        ]);

        $this->assertEquals(1, $count);

        $database->deleteCollection('vectorCount');
    }

    public function testVectorQuerySum(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorSum');
        $database->createAttribute('vectorSum', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));
        $database->createAttribute('vectorSum', new Attribute(key: 'value', type: ColumnType::Integer, size: 0, required: true));

        // Create documents with different values
        $database->createDocument('vectorSum', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
            'value' => 10,
        ]));

        $database->createDocument('vectorSum', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [0.0, 1.0, 0.0],
            'value' => 20,
        ]));

        $database->createDocument('vectorSum', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'embedding' => [0.5, 0.5, 0.0],
            'value' => 30,
        ]));

        // Test sum with vector query - should sum all matching documents
        $sum = $database->sum('vectorSum', 'value', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
        ]);

        $this->assertEquals(60, $sum);

        // Test sum with vector query and filter combined
        $sum = $database->sum('vectorSum', 'value', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::greaterThan('value', 15),
        ]);

        $this->assertEquals(50, $sum);

        $database->deleteCollection('vectorSum');
    }

    public function testVectorUpsert(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('vectorUpsert');
        $database->createAttribute('vectorUpsert', new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3, required: true));

        $insertedDoc = $database->upsertDocument('vectorUpsert', new Document([
            '$id' => 'vectorUpsert',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $this->assertEquals([1.0, 0.0, 0.0], $insertedDoc->getAttribute('embedding'));

        $insertedDoc = $database->getDocument('vectorUpsert', 'vectorUpsert');
        $this->assertEquals([1.0, 0.0, 0.0], $insertedDoc->getAttribute('embedding'));

        $updatedDoc = $database->upsertDocument('vectorUpsert', new Document([
            '$id' => 'vectorUpsert',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'embedding' => [2.0, 0.0, 0.0],
        ]));

        $this->assertEquals([2.0, 0.0, 0.0], $updatedDoc->getAttribute('embedding'));

        $updatedDoc = $database->getDocument('vectorUpsert', 'vectorUpsert');
        $this->assertEquals([2.0, 0.0, 0.0], $updatedDoc->getAttribute('embedding'));

        $database->deleteCollection('vectorUpsert');
    }
}
