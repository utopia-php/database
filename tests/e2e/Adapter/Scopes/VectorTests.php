<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait VectorTests
{
    public function testVectorAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        // Test that vector attributes can only be created on PostgreSQL
        $database->createCollection('vectorCollection');

        // Create a vector attribute with 3 dimensions
        $database->createAttribute('vectorCollection', 'embedding', Database::VAR_VECTOR, 3, true);

        // Create a vector attribute with 128 dimensions
        $database->createAttribute('vectorCollection', 'large_embedding', Database::VAR_VECTOR, 128, false, null);

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
        $this->assertEquals(Database::VAR_VECTOR, $embeddingAttr['type']);
        $this->assertEquals(3, $embeddingAttr['size']);
        $this->assertEquals(Database::VAR_VECTOR, $largeEmbeddingAttr['type']);
        $this->assertEquals(128, $largeEmbeddingAttr['size']);

        // Cleanup
        $database->deleteCollection('vectorCollection');
    }

    public function testVectorInvalidDimensions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorErrorCollection');

        // Test invalid dimensions
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector dimensions must be a positive integer');
        $database->createAttribute('vectorErrorCollection', 'bad_embedding', Database::VAR_VECTOR, 0, true);

        // Cleanup
        $database->deleteCollection('vectorErrorCollection');
    }

    public function testVectorTooManyDimensions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorLimitCollection');

        // Test too many dimensions (pgvector limit is 16000)
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector dimensions cannot exceed 16000');
        $database->createAttribute('vectorLimitCollection', 'huge_embedding', Database::VAR_VECTOR, 16001, true);

        // Cleanup
        $database->deleteCollection('vectorLimitCollection');
    }

    public function testVectorDocuments(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorDocuments');
        $database->createAttribute('vectorDocuments', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('vectorDocuments', 'embedding', Database::VAR_VECTOR, 3, true);

        // Create documents with vector data
        $doc1 = $database->createDocument('vectorDocuments', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'name' => 'Document 1',
            'embedding' => [1.0, 0.0, 0.0]
        ]));

        $doc2 = $database->createDocument('vectorDocuments', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'name' => 'Document 2',
            'embedding' => [0.0, 1.0, 0.0]
        ]));

        $doc3 = $database->createDocument('vectorDocuments', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'name' => 'Document 3',
            'embedding' => [0.0, 0.0, 1.0]
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

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorQueries');
        $database->createAttribute('vectorQueries', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('vectorQueries', 'embedding', Database::VAR_VECTOR, 3, true);

        // Create test documents with read permissions
        $doc1 = $database->createDocument('vectorQueries', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'name' => 'Test 1',
            'embedding' => [1.0, 0.0, 0.0]
        ]));

        $doc2 = $database->createDocument('vectorQueries', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'name' => 'Test 2',
            'embedding' => [0.0, 1.0, 0.0]
        ]));

        $doc3 = $database->createDocument('vectorQueries', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'name' => 'Test 3',
            'embedding' => [0.5, 0.5, 0.0]
        ]));

        // Verify documents were created
        $this->assertNotEmpty($doc1->getId());
        $this->assertNotEmpty($doc2->getId());
        $this->assertNotEmpty($doc3->getId());

        // Test without vector queries first
        $allDocs = $database->find('vectorQueries');
        $this->assertCount(3, $allDocs, "Should have 3 documents in collection");

        // Test vector dot product query
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id')
        ]);

        $this->assertCount(3, $results);

        // Test vector cosine distance query
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id')
        ]);

        $this->assertCount(3, $results);

        // Test vector euclidean distance query
        $results = $database->find('vectorQueries', [
            Query::vectorEuclidean('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id')
        ]);

        $this->assertCount(3, $results);

        // Test vector queries with limit - should return only top results
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(2)
        ]);

        $this->assertCount(2, $results);
        // The most similar vector should be the one closest to [1.0, 0.0, 0.0]
        $this->assertEquals('Test 1', $results[0]->getAttribute('name'));

        // Test vector query with limit of 1
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [0.0, 1.0, 0.0]),
            Query::limit(1)
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('Test 2', $results[0]->getAttribute('name'));

        // Test vector query combined with other filters
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [0.5, 0.5, 0.0]),
            Query::notEqual('name', 'Test 1')
        ]);

        $this->assertCount(2, $results);
        // Should not contain Test 1
        foreach ($results as $result) {
            $this->assertNotEquals('Test 1', $result->getAttribute('name'));
        }

        // Test vector query with specific name filter
        $results = $database->find('vectorQueries', [
            Query::vectorEuclidean('embedding', [0.7, 0.7, 0.0]),
            Query::equal('name', ['Test 3'])
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('Test 3', $results[0]->getAttribute('name'));

        // Test vector query with offset - skip first result
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [0.5, 0.5, 0.0]),
            Query::limit(2),
            Query::offset(1)
        ]);

        $this->assertCount(2, $results);
        // Should skip the most similar result

        // Test empty result with impossible filter combination
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::equal('name', ['Test 2']),
            Query::equal('name', ['Test 3'])  // Impossible condition
        ]);

        $this->assertCount(0, $results);

        // Test vector query with secondary ordering
        // Vector similarity takes precedence, name DESC is secondary
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [0.4, 0.6, 0.0]),
            Query::orderDesc('name'),
            Query::limit(2)
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

    public function testVectorQueryValidation(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorValidation');
        $database->createAttribute('vectorValidation', 'embedding', Database::VAR_VECTOR, 3, true);
        $database->createAttribute('vectorValidation', 'name', Database::VAR_STRING, 255, true);

        // Test that vector queries fail on non-vector attributes
        $this->expectException(DatabaseException::class);
        $database->find('vectorValidation', [
            Query::vectorDot('name', [1.0, 0.0, 0.0])
        ]);

        // Cleanup
        $database->deleteCollection('vectorValidation');
    }

    public function testVectorIndexes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorIndexes');
        $database->createAttribute('vectorIndexes', 'embedding', Database::VAR_VECTOR, 3, true);

        // Create different types of vector indexes
        // Euclidean distance index (L2 distance)
        $database->createIndex('vectorIndexes', 'embedding_euclidean', Database::INDEX_HNSW_EUCLIDEAN, ['embedding']);

        // Cosine distance index
        $database->createIndex('vectorIndexes', 'embedding_cosine', Database::INDEX_HNSW_COSINE, ['embedding']);

        // Inner product (dot product) index
        $database->createIndex('vectorIndexes', 'embedding_dot', Database::INDEX_HNSW_DOT, ['embedding']);

        // Verify indexes were created
        $collection = $database->getCollection('vectorIndexes');
        $indexes = $collection->getAttribute('indexes');

        $this->assertCount(3, $indexes);

        // Test that queries work with indexes
        $database->createDocument('vectorIndexes', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [1.0, 0.0, 0.0]
        ]));

        $database->createDocument('vectorIndexes', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [0.0, 1.0, 0.0]
        ]));

        // Query should use the appropriate index based on the operator
        $results = $database->find('vectorIndexes', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::limit(1)
        ]);

        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vectorIndexes');
    }

    public function testVectorDimensionMismatch(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorDimMismatch');
        $database->createAttribute('vectorDimMismatch', 'embedding', Database::VAR_VECTOR, 3, true);

        // Test creating document with wrong dimension count
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessageMatches('/must be an array of 3 numeric values/');

        $database->createDocument('vectorDimMismatch', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [1.0, 0.0] // Only 2 dimensions, expects 3
        ]));

        // Cleanup
        $database->deleteCollection('vectorDimMismatch');
    }

    public function testVectorWithInvalidDataTypes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorInvalidTypes');
        $database->createAttribute('vectorInvalidTypes', 'embedding', Database::VAR_VECTOR, 3, true);

        // Test with string values in vector
        try {
            $database->createDocument('vectorInvalidTypes', new Document([
                '$permissions' => [
                    Permission::read(Role::any())
                ],
                'embedding' => ['one', 'two', 'three']
            ]));
            $this->fail('Should have thrown exception for non-numeric vector values');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric values', strtolower($e->getMessage()));
        }

        // Test with mixed types
        try {
            $database->createDocument('vectorInvalidTypes', new Document([
                '$permissions' => [
                    Permission::read(Role::any())
                ],
                'embedding' => [1.0, 'two', 3.0]
            ]));
            $this->fail('Should have thrown exception for mixed type vector values');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric values', strtolower($e->getMessage()));
        }

        // Cleanup
        $database->deleteCollection('vectorInvalidTypes');
    }

    public function testVectorWithNullAndEmpty(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorNullEmpty');
        $database->createAttribute('vectorNullEmpty', 'embedding', Database::VAR_VECTOR, 3, false); // Not required

        // Test with null vector (should work for non-required attribute)
        $doc1 = $database->createDocument('vectorNullEmpty', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => null
        ]));

        $this->assertNull($doc1->getAttribute('embedding'));

        // Test with empty array (should fail)
        try {
            $database->createDocument('vectorNullEmpty', new Document([
                '$permissions' => [
                    Permission::read(Role::any())
                ],
                'embedding' => []
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

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        // Test with maximum allowed dimensions (16000 for pgvector)
        $database->createCollection('vectorLarge');
        $database->createAttribute('vectorLarge', 'embedding', Database::VAR_VECTOR, 1536, true); // Common embedding size

        // Create a large vector
        $largeVector = array_fill(0, 1536, 0.1);
        $largeVector[0] = 1.0; // Make first element different

        $doc = $database->createDocument('vectorLarge', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => $largeVector
        ]));

        $this->assertCount(1536, $doc->getAttribute('embedding'));
        $this->assertEquals(1.0, $doc->getAttribute('embedding')[0]);

        // Test vector search on large vectors
        $searchVector = array_fill(0, 1536, 0.0);
        $searchVector[0] = 1.0;

        $results = $database->find('vectorLarge', [
            Query::vectorCosine('embedding', $searchVector)
        ]);

        $this->assertCount(1, $results);

        // Cleanup
        $database->deleteCollection('vectorLarge');
    }

    public function testVectorUpdates(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorUpdates');
        $database->createAttribute('vectorUpdates', 'embedding', Database::VAR_VECTOR, 3, true);

        // Create initial document
        $doc = $database->createDocument('vectorUpdates', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ],
            'embedding' => [1.0, 0.0, 0.0]
        ]));

        $this->assertEquals([1.0, 0.0, 0.0], $doc->getAttribute('embedding'));

        // Update the vector
        $updated = $database->updateDocument('vectorUpdates', $doc->getId(), new Document([
            'embedding' => [0.0, 1.0, 0.0]
        ]));

        $this->assertEquals([0.0, 1.0, 0.0], $updated->getAttribute('embedding'));

        // Test partial update (should replace entire vector)
        $updated2 = $database->updateDocument('vectorUpdates', $doc->getId(), new Document([
            'embedding' => [0.5, 0.5, 0.5]
        ]));

        $this->assertEquals([0.5, 0.5, 0.5], $updated2->getAttribute('embedding'));

        // Cleanup
        $database->deleteCollection('vectorUpdates');
    }

    public function testMultipleVectorAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('multiVector');
        $database->createAttribute('multiVector', 'embedding1', Database::VAR_VECTOR, 3, true);
        $database->createAttribute('multiVector', 'embedding2', Database::VAR_VECTOR, 5, true);
        $database->createAttribute('multiVector', 'name', Database::VAR_STRING, 255, true);

        // Create documents with multiple vector attributes
        $doc1 = $database->createDocument('multiVector', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'name' => 'Doc 1',
            'embedding1' => [1.0, 0.0, 0.0],
            'embedding2' => [1.0, 0.0, 0.0, 0.0, 0.0]
        ]));

        $doc2 = $database->createDocument('multiVector', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'name' => 'Doc 2',
            'embedding1' => [0.0, 1.0, 0.0],
            'embedding2' => [0.0, 1.0, 0.0, 0.0, 0.0]
        ]));

        // Query by first vector
        $results = $database->find('multiVector', [
            Query::vectorCosine('embedding1', [1.0, 0.0, 0.0])
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('Doc 1', $results[0]->getAttribute('name'));

        // Query by second vector
        $results = $database->find('multiVector', [
            Query::vectorCosine('embedding2', [0.0, 1.0, 0.0, 0.0, 0.0])
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

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorPagination');
        $database->createAttribute('vectorPagination', 'embedding', Database::VAR_VECTOR, 3, true);
        $database->createAttribute('vectorPagination', 'index', Database::VAR_INTEGER, 0, true);

        // Create 10 documents
        for ($i = 0; $i < 10; $i++) {
            $database->createDocument('vectorPagination', new Document([
                '$permissions' => [
                    Permission::read(Role::any())
                ],
                'index' => $i,
                'embedding' => [
                    cos($i * M_PI / 10),
                    sin($i * M_PI / 10),
                    0.0
                ]
            ]));
        }

        // Test pagination with vector queries
        $searchVector = [1.0, 0.0, 0.0];

        // First page
        $page1 = $database->find('vectorPagination', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(3),
            Query::offset(0)
        ]);

        $this->assertCount(3, $page1);

        // Second page
        $page2 = $database->find('vectorPagination', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(3),
            Query::offset(3)
        ]);

        $this->assertCount(3, $page2);

        // Ensure different documents
        $page1Ids = array_map(fn ($doc) => $doc->getId(), $page1);
        $page2Ids = array_map(fn ($doc) => $doc->getId(), $page2);
        $this->assertEmpty(array_intersect($page1Ids, $page2Ids));

        // Test with cursor pagination
        $firstBatch = $database->find('vectorPagination', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(5)
        ]);

        $this->assertCount(5, $firstBatch);

        $lastDoc = $firstBatch[4];
        $nextBatch = $database->find('vectorPagination', [
            Query::vectorCosine('embedding', $searchVector),
            Query::cursorAfter($lastDoc),
            Query::limit(5)
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

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorTextSearch');
        $database->createAttribute('vectorTextSearch', 'title', Database::VAR_STRING, 255, true);
        $database->createAttribute('vectorTextSearch', 'category', Database::VAR_STRING, 50, true);
        $database->createAttribute('vectorTextSearch', 'embedding', Database::VAR_VECTOR, 3, true);

        // Create fulltext index for title
        $database->createIndex('vectorTextSearch', 'title_fulltext', Database::INDEX_FULLTEXT, ['title']);

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
                    Permission::read(Role::any())
                ],
                ...$doc
            ]));
        }

        // Combine vector search with category filter
        $results = $database->find('vectorTextSearch', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::equal('category', ['AI']),
            Query::limit(2)
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('AI', $results[0]->getAttribute('category'));
        $this->assertEquals('Machine Learning Basics', $results[0]->getAttribute('title'));

        // Combine vector search with text search
        $results = $database->find('vectorTextSearch', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::search('title', 'Learning'),
            Query::limit(5)
        ]);

        $this->assertCount(2, $results);
        foreach ($results as $result) {
            $this->assertStringContainsString('Learning', $result->getAttribute('title'));
        }

        // Complex query with multiple filters
        $results = $database->find('vectorTextSearch', [
            Query::vectorEuclidean('embedding', [0.5, 0.5, 0.0]),
            Query::notEqual('category', ['Web']),
            Query::limit(3)
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

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorSpecialFloats');
        $database->createAttribute('vectorSpecialFloats', 'embedding', Database::VAR_VECTOR, 3, true);

        // Test with very small values (near zero)
        $doc1 = $database->createDocument('vectorSpecialFloats', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [1e-10, 1e-10, 1e-10]
        ]));

        $this->assertNotNull($doc1->getId());

        // Test with very large values
        $doc2 = $database->createDocument('vectorSpecialFloats', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [1e10, 1e10, 1e10]
        ]));

        $this->assertNotNull($doc2->getId());

        // Test with negative values
        $doc3 = $database->createDocument('vectorSpecialFloats', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [-1.0, -0.5, -0.1]
        ]));

        $this->assertNotNull($doc3->getId());

        // Test with mixed sign values
        $doc4 = $database->createDocument('vectorSpecialFloats', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [-1.0, 0.0, 1.0]
        ]));

        $this->assertNotNull($doc4->getId());

        // Query with negative vector
        $results = $database->find('vectorSpecialFloats', [
            Query::vectorCosine('embedding', [-1.0, -1.0, -1.0])
        ]);

        $this->assertGreaterThan(0, count($results));

        // Cleanup
        $database->deleteCollection('vectorSpecialFloats');
    }

    public function testVectorIndexPerformance(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorPerf');
        $database->createAttribute('vectorPerf', 'embedding', Database::VAR_VECTOR, 128, true);
        $database->createAttribute('vectorPerf', 'name', Database::VAR_STRING, 255, true);

        // Create documents
        $numDocs = 100;
        for ($i = 0; $i < $numDocs; $i++) {
            $vector = [];
            for ($j = 0; $j < 128; $j++) {
                $vector[] = sin($i * $j * 0.01);
            }

            $database->createDocument('vectorPerf', new Document([
                '$permissions' => [
                    Permission::read(Role::any())
                ],
                'name' => "Doc $i",
                'embedding' => $vector
            ]));
        }

        // Query without index
        $searchVector = array_fill(0, 128, 0.5);

        $startTime = microtime(true);
        $results1 = $database->find('vectorPerf', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(10)
        ]);
        $timeWithoutIndex = microtime(true) - $startTime;

        $this->assertCount(10, $results1);

        // Create HNSW index
        $database->createIndex('vectorPerf', 'embedding_hnsw', Database::INDEX_HNSW_COSINE, ['embedding']);

        // Query with index (should be faster for larger datasets)
        $startTime = microtime(true);
        $results2 = $database->find('vectorPerf', [
            Query::vectorCosine('embedding', $searchVector),
            Query::limit(10)
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

    public function testVectorQueryValidationExtended(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorValidation2');
        $database->createAttribute('vectorValidation2', 'embedding', Database::VAR_VECTOR, 3, true);
        $database->createAttribute('vectorValidation2', 'text', Database::VAR_STRING, 255, true);

        $database->createDocument('vectorValidation2', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'text' => 'Test',
            'embedding' => [1.0, 0.0, 0.0]
        ]));

        // Test vector query with wrong dimension count
        try {
            $database->find('vectorValidation2', [
                Query::vectorCosine('embedding', [1.0, 0.0]) // Wrong dimension
            ]);
            $this->fail('Should have thrown exception for dimension mismatch');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('elements', strtolower($e->getMessage()));
        }

        // Test vector query on non-vector attribute
        try {
            $database->find('vectorValidation2', [
                Query::vectorCosine('text', [1.0, 0.0, 0.0])
            ]);
            $this->fail('Should have thrown exception for non-vector attribute');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('vector', strtolower($e->getMessage()));
        }

        // Cleanup
        $database->deleteCollection('vectorValidation2');
    }

    public function testVectorNormalization(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForVectors()) {
            $this->expectNotToPerformAssertions();
        }

        $database->createCollection('vectorNorm');
        $database->createAttribute('vectorNorm', 'embedding', Database::VAR_VECTOR, 3, true);

        // Create documents with normalized and non-normalized vectors
        $doc1 = $database->createDocument('vectorNorm', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [1.0, 0.0, 0.0] // Already normalized
        ]));

        $doc2 = $database->createDocument('vectorNorm', new Document([
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'embedding' => [3.0, 4.0, 0.0] // Not normalized (magnitude = 5)
        ]));

        // Cosine similarity should work regardless of normalization
        $results = $database->find('vectorNorm', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0])
        ]);

        $this->assertCount(2, $results);

        // For cosine similarity, [3, 4, 0] has similarity 3/5 = 0.6 with [1, 0, 0]
        // So [1, 0, 0] should be first (similarity = 1.0)
        $this->assertEquals([1.0, 0.0, 0.0], $results[0]->getAttribute('embedding'));

        // Cleanup
        $database->deleteCollection('vectorNorm');
    }
}
