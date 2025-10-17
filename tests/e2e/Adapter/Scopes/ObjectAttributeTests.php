<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait ObjectAttributeTests
{
    public function testObjectAttribute(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Skip test if adapter doesn't support JSONB
        if (!$database->getAdapter()->getSupportForObject()) {
            $this->markTestSkipped('Adapter does not support object attributes');
        }

        $collectionId = ID::unique();
        $database->createCollection($collectionId);

        // Create object attribute
        $this->assertEquals(true, $database->createAttribute($collectionId, 'meta', Database::TYPE_OBJECT, 0, false));

        // Test 1: Create and read document with object attribute
        $doc1 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc1',
            '$permissions' => [Permission::read(Role::any()),Permission::update(Role::any())],
            'meta' => [
                'age' => 25,
                'skills' => ['react', 'node'],
                'user' => [
                    'info' => [
                        'country' => 'IN'
                    ]
                ]
            ]
        ]));

        $this->assertIsArray($doc1->getAttribute('meta'));
        $this->assertEquals(25, $doc1->getAttribute('meta')['age']);
        $this->assertEquals(['react', 'node'], $doc1->getAttribute('meta')['skills']);
        $this->assertEquals('IN', $doc1->getAttribute('meta')['user']['info']['country']);

        // Test 2: Query::equal with simple key-value pair
        $results = $database->find($collectionId, [
            Query::equal('meta', [['age' => 25]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc1', $results[0]->getId());

        // Test 3: Query::equal with nested JSON
        $results = $database->find($collectionId, [
            Query::equal('meta', [[
                'user' => [
                    'info' => [
                        'country' => 'IN'
                    ]
                ]
            ]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc1', $results[0]->getId());

        // Test 4: Query::contains for array element
        $results = $database->find($collectionId, [
            Query::contains('meta', [['skills' => 'react']])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc1', $results[0]->getId());

        // Test 5: Create another document with different values
        $doc2 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc2',
            '$permissions' => [Permission::read(Role::any()),Permission::update(Role::any())],
            'meta' => [
                'age' => 30,
                'skills' => ['python', 'java'],
                'user' => [
                    'info' => [
                        'country' => 'US'
                    ]
                ]
            ]
        ]));

        // Test 6: Query should return only doc1
        $results = $database->find($collectionId, [
            Query::equal('meta', [['age' => 25]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc1', $results[0]->getId());

        // Test 7: Query for doc2
        $results = $database->find($collectionId, [
            Query::equal('meta', [[
                'user' => [
                    'info' => [
                        'country' => 'US'
                    ]
                ]
            ]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc2', $results[0]->getId());

        // Test 8: Update document
        $updatedDoc = $database->updateDocument($collectionId, 'doc1', new Document([
            '$id' => 'doc1',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'age' => 26,
                'skills' => ['react', 'node', 'typescript'],
                'user' => [
                    'info' => [
                        'country' => 'CA'
                    ]
                ]
            ]
        ]));

        $this->assertEquals(26, $updatedDoc->getAttribute('meta')['age']);
        $this->assertEquals(['react', 'node', 'typescript'], $updatedDoc->getAttribute('meta')['skills']);
        $this->assertEquals('CA', $updatedDoc->getAttribute('meta')['user']['info']['country']);

        // Test 9: Query updated document
        $results = $database->find($collectionId, [
            Query::equal('meta', [['age' => 26]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc1', $results[0]->getId());

        // Test 10: Query with multiple conditions using contains
        $results = $database->find($collectionId, [
            Query::contains('meta', [['skills' => 'typescript']])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc1', $results[0]->getId());

        // Test 11: Negative test - query that shouldn't match
        $results = $database->find($collectionId, [
            Query::equal('meta', [['age' => 99]])
        ]);
        $this->assertCount(0, $results);

        // Test 11d: notEqual on scalar inside object should exclude doc1
        $results = $database->find($collectionId, [
            Query::notEqual('meta', [['age' => 26]])
        ]);
        // Should return doc2 only
        $this->assertCount(1, $results);
        $this->assertEquals('doc2', $results[0]->getId());

        // Test 11e: notEqual on nested object should exclude doc1
        $results = $database->find($collectionId, [
            Query::notEqual('meta', [[
                'user' => [
                    'info' => [
                        'country' => 'CA'
                    ]
                ]
            ]])
        ]);
        // Should return doc2 only
        $this->assertCount(1, $results);
        $this->assertEquals('doc2', $results[0]->getId());

        // Test 11a: Test getDocument by ID
        $fetchedDoc = $database->getDocument($collectionId, 'doc1');
        $this->assertEquals('doc1', $fetchedDoc->getId());
        $this->assertIsArray($fetchedDoc->getAttribute('meta'));
        $this->assertEquals(26, $fetchedDoc->getAttribute('meta')['age']);
        $this->assertEquals(['react', 'node', 'typescript'], $fetchedDoc->getAttribute('meta')['skills']);
        $this->assertEquals('CA', $fetchedDoc->getAttribute('meta')['user']['info']['country']);

        // Test 11b: Test Query::select to limit returned attributes
        $results = $database->find($collectionId, [
            Query::select(['$id', 'meta']),
            Query::equal('meta', [['age' => 26]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc1', $results[0]->getId());
        $this->assertIsArray($results[0]->getAttribute('meta'));
        $this->assertEquals(26, $results[0]->getAttribute('meta')['age']);

        // Test 11c: Test Query::select with only $id (exclude meta)
        $results = $database->find($collectionId, [
            Query::select(['$id']),
            Query::equal('meta', [['age' => 30]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc2', $results[0]->getId());
        // Meta should not be present when not selected
        $this->assertEmpty($results[0]->getAttribute('meta'));

        // Test 12: Test with null value
        $doc3 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc3',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => null
        ]));
        $this->assertNull($doc3->getAttribute('meta'));

        // Test 13: Test with empty object
        $doc4 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc4',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => []
        ]));
        $this->assertIsArray($doc4->getAttribute('meta'));
        $this->assertEmpty($doc4->getAttribute('meta'));

        // Test 14: Test deeply nested structure (5 levels)
        $doc5 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc5',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'level1' => [
                    'level2' => [
                        'level3' => [
                            'level4' => [
                                'level5' => 'deep_value'
                            ]
                        ]
                    ]
                ]
            ]
        ]));
        $this->assertEquals('deep_value', $doc5->getAttribute('meta')['level1']['level2']['level3']['level4']['level5']);

        // Test 15: Query deeply nested structure
        $results = $database->find($collectionId, [
            Query::equal('meta', [[
                'level1' => [
                    'level2' => [
                        'level3' => [
                            'level4' => [
                                'level5' => 'deep_value'
                            ]
                        ]
                    ]
                ]
            ]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc5', $results[0]->getId());

        // Test 16: Query partial nested path
        $results = $database->find($collectionId, [
            Query::equal('meta', [[
                'level1' => [
                    'level2' => [
                        'level3' => [
                            'level4' => [
                                'level5' => 'deep_value'
                            ]
                        ]
                    ]
                ]
            ]])
        ]);
        $this->assertCount(1, $results);

        // Test 17: Test with mixed data types
        $doc6 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc6',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'string' => 'text',
                'number' => 42,
                'float' => 3.14,
                'boolean' => true,
                'null_value' => null,
                'array' => [1, 2, 3],
                'object' => ['key' => 'value']
            ]
        ]));
        $this->assertEquals('text', $doc6->getAttribute('meta')['string']);
        $this->assertEquals(42, $doc6->getAttribute('meta')['number']);
        $this->assertEquals(3.14, $doc6->getAttribute('meta')['float']);
        $this->assertTrue($doc6->getAttribute('meta')['boolean']);
        $this->assertNull($doc6->getAttribute('meta')['null_value']);

        // Test 18: Query with boolean value
        $results = $database->find($collectionId, [
            Query::equal('meta', [['boolean' => true]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc6', $results[0]->getId());

        // Test 19: Query with numeric value
        $results = $database->find($collectionId, [
            Query::equal('meta', [['number' => 42]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc6', $results[0]->getId());

        // Test 20: Query with float value
        $results = $database->find($collectionId, [
            Query::equal('meta', [['float' => 3.14]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc6', $results[0]->getId());

        // Test 21: Test contains with multiple array elements
        $doc7 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc7',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'tags' => ['php', 'javascript', 'python', 'go', 'rust']
            ]
        ]));
        $results = $database->find($collectionId, [
            Query::contains('meta', [['tags' => 'rust']])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc7', $results[0]->getId());

        // Test 22: Test contains with numeric array element
        $doc8 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc8',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'scores' => [85, 90, 95, 100]
            ]
        ]));
        $results = $database->find($collectionId, [
            Query::contains('meta', [['scores' => 95]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc8', $results[0]->getId());

        // Test 23: Negative test - contains query that shouldn't match
        $results = $database->find($collectionId, [
            Query::contains('meta', [['tags' => 'kotlin']])
        ]);
        $this->assertCount(0, $results);

        // Test 23b: notContains should exclude doc7 (which has 'rust')
        $results = $database->find($collectionId, [
            Query::notContains('meta', [['tags' => 'rust']])
        ]);
        // Should not include doc7; returns others (at least doc1, doc2, ...)
        $this->assertGreaterThanOrEqual(1, count($results));
        foreach ($results as $doc) {
            if ($doc->getId() === 'doc7') {
                $this->fail('doc7 should not be returned by notContains for rust');
            }
        }

        // Test 24: Test complex nested array within object
        $doc9 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc9',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'projects' => [
                    [
                        'name' => 'Project A',
                        'technologies' => ['react', 'node'],
                        'active' => true
                    ],
                    [
                        'name' => 'Project B',
                        'technologies' => ['vue', 'python'],
                        'active' => false
                    ]
                ],
                'company' => 'TechCorp'
            ]
        ]));
        $this->assertIsArray($doc9->getAttribute('meta')['projects']);
        $this->assertCount(2, $doc9->getAttribute('meta')['projects']);
        $this->assertEquals('Project A', $doc9->getAttribute('meta')['projects'][0]['name']);

        // Test 25: Query using equal with nested key
        $results = $database->find($collectionId, [
            Query::equal('meta', [['company' => 'TechCorp']])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc9', $results[0]->getId());

        // Test 25b: Query the entire array structure using equal
        $results = $database->find($collectionId, [
            Query::equal('meta', [[
                'projects' => [
                    [
                        'name' => 'Project A',
                        'technologies' => ['react', 'node'],
                        'active' => true
                    ],
                    [
                        'name' => 'Project B',
                        'technologies' => ['vue', 'python'],
                        'active' => false
                    ]
                ]
            ]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc9', $results[0]->getId());

        // Test 26: Test with special characters in values
        $doc10 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc10',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'description' => 'Test with "quotes" and \'apostrophes\'',
                'emoji' => '🚀🎉',
                'symbols' => '@#$%^&*()'
            ]
        ]));
        $this->assertEquals('Test with "quotes" and \'apostrophes\'', $doc10->getAttribute('meta')['description']);
        $this->assertEquals('🚀🎉', $doc10->getAttribute('meta')['emoji']);

        // Test 27: Query with special characters
        $results = $database->find($collectionId, [
            Query::equal('meta', [['emoji' => '🚀🎉']])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc10', $results[0]->getId());

        // Test 28: Test equal query with complete object match
        $doc11 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc11',
            '$permissions' => [Permission::read(Role::any()),Permission::update(Role::any())],
            'meta' => [
                'config' => [
                    'theme' => 'dark',
                    'language' => 'en'
                ]
            ]
        ]));
        $results = $database->find($collectionId, [
            Query::equal('meta', [['config' => ['theme' => 'dark', 'language' => 'en']]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc11', $results[0]->getId());

        // Test 29: Negative test - partial object match should still work (containment)
        $results = $database->find($collectionId, [
            Query::equal('meta', [['config' => ['theme' => 'dark']]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc11', $results[0]->getId());

        // Test 30: Test updating to empty object
        $updatedDoc11 = $database->updateDocument($collectionId, 'doc11', new Document([
            '$id' => 'doc11',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => []
        ]));
        $this->assertIsArray($updatedDoc11->getAttribute('meta'));
        $this->assertEmpty($updatedDoc11->getAttribute('meta'));

        // Test 31: Test with nested arrays of primitives
        $doc12 = $database->createDocument($collectionId, new Document([
            '$id' => 'doc12',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'matrix' => [
                    [1, 2, 3],
                    [4, 5, 6],
                    [7, 8, 9]
                ]
            ]
        ]));
        $this->assertIsArray($doc12->getAttribute('meta')['matrix']);
        $this->assertEquals([1, 2, 3], $doc12->getAttribute('meta')['matrix'][0]);

        // Test 32: Contains query with nested array
        $results = $database->find($collectionId, [
            Query::contains('meta', [['matrix' => [[4, 5, 6]]]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc12', $results[0]->getId());

        // Test 33: Test getDocument with various documents
        $fetchedDoc6 = $database->getDocument($collectionId, 'doc6');
        $this->assertEquals('doc6', $fetchedDoc6->getId());
        $this->assertEquals('text', $fetchedDoc6->getAttribute('meta')['string']);
        $this->assertEquals(42, $fetchedDoc6->getAttribute('meta')['number']);
        $this->assertTrue($fetchedDoc6->getAttribute('meta')['boolean']);

        $fetchedDoc10 = $database->getDocument($collectionId, 'doc10');
        $this->assertEquals('🚀🎉', $fetchedDoc10->getAttribute('meta')['emoji']);
        $this->assertEquals('Test with "quotes" and \'apostrophes\'', $fetchedDoc10->getAttribute('meta')['description']);

        // Test 34: Test Query::select with complex nested structures
        $results = $database->find($collectionId, [
            Query::select(['$id', '$permissions', 'meta']),
            Query::equal('meta', [[
                'level1' => [
                    'level2' => [
                        'level3' => [
                            'level4' => [
                                'level5' => 'deep_value'
                            ]
                        ]
                    ]
                ]
            ]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc5', $results[0]->getId());
        $this->assertEquals('deep_value', $results[0]->getAttribute('meta')['level1']['level2']['level3']['level4']['level5']);

        // Test 35: Test selecting multiple documents and verifying object attributes
        $allDocs = $database->find($collectionId, [
            Query::select(['$id', 'meta']),
            Query::limit(25)
        ]);
        $this->assertGreaterThan(10, count($allDocs));

        // Verify that each document with meta has proper structure
        foreach ($allDocs as $doc) {
            $meta = $doc->getAttribute('meta');
            if ($meta !== null && $meta !== []) {
                $this->assertIsArray($meta, "Document {$doc->getId()} should have array meta");
            }
        }

        // Test 36: Test Query::select with only meta attribute
        $results = $database->find($collectionId, [
            Query::select(['meta']),
            Query::equal('meta', [['tags' => ['php', 'javascript', 'python', 'go', 'rust']]])
        ]);
        $this->assertCount(1, $results);
        $this->assertIsArray($results[0]->getAttribute('meta'));
        $this->assertEquals(['php', 'javascript', 'python', 'go', 'rust'], $results[0]->getAttribute('meta')['tags']);

        // Clean up
        $database->deleteCollection($collectionId);
    }

    public function testObjectAttributeGinIndex(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Skip test if adapter doesn't support JSONB
        if (!$database->getAdapter()->getSupportForObject()) {
            $this->markTestSkipped('Adapter does not support object attributes');
        }

        $collectionId = ID::unique();
        $database->createCollection($collectionId);

        // Create object attribute
        $this->assertEquals(true, $database->createAttribute($collectionId, 'data', Database::TYPE_OBJECT, 0, false));

        // Test 1: Create GIN index on object attribute
        $ginIndex = $database->createIndex($collectionId, 'idx_data_gin', Database::INDEX_GIN, ['data']);
        $this->assertTrue($ginIndex);

        // Test 2: Create documents with JSONB data
        $doc1 = $database->createDocument($collectionId, new Document([
            '$id' => 'gin1',
            '$permissions' => [Permission::read(Role::any())],
            'data' => [
                'tags' => ['php', 'javascript', 'python'],
                'config' => [
                    'env' => 'production',
                    'debug' => false
                ],
                'version' => '1.0.0'
            ]
        ]));

        $doc2 = $database->createDocument($collectionId, new Document([
            '$id' => 'gin2',
            '$permissions' => [Permission::read(Role::any())],
            'data' => [
                'tags' => ['java', 'kotlin', 'scala'],
                'config' => [
                    'env' => 'development',
                    'debug' => true
                ],
                'version' => '2.0.0'
            ]
        ]));

        // Test 3: Query with equal on indexed JSONB column
        $results = $database->find($collectionId, [
            Query::equal('data', [['env' => 'production']])
        ]);
        $this->assertCount(0, $results); // Note: GIN index doesn't make equal queries work differently

        // Test 4: Query with contains on indexed JSONB column
        $results = $database->find($collectionId, [
            Query::contains('data', [['tags' => 'php']])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('gin1', $results[0]->getId());

        // Test 5: Verify GIN index improves performance for containment queries
        $results = $database->find($collectionId, [
            Query::contains('data', [['tags' => 'kotlin']])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('gin2', $results[0]->getId());

        // Test 6: Try to create GIN index on non-object attribute (should fail)
        $database->createAttribute($collectionId, 'name', Database::VAR_STRING, 255, false);

        $exceptionThrown = false;
        try {
            $database->createIndex($collectionId, 'idx_name_gin', Database::INDEX_GIN, ['name']);
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(IndexException::class, $e);
            $this->assertStringContainsString('GIN index can only be created on object attributes', $e->getMessage());
        }
        $this->assertTrue($exceptionThrown, 'Expected Index exception for GIN index on non-object attribute');

        // Test 7: Try to create GIN index on multiple attributes (should fail)
        $database->createAttribute($collectionId, 'metadata', Database::TYPE_OBJECT, 0, false);

        $exceptionThrown = false;
        try {
            $database->createIndex($collectionId, 'idx_multi_gin', Database::INDEX_GIN, ['data', 'metadata']);
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(IndexException::class, $e);
            $this->assertStringContainsString('GIN index can be created on a single object attribute', $e->getMessage());
        }
        $this->assertTrue($exceptionThrown, 'Expected Index exception for GIN index on multiple attributes');

        // Test 8: Try to create GIN index with orders (should fail)
        $exceptionThrown = false;
        try {
            $database->createIndex($collectionId, 'idx_ordered_gin', Database::INDEX_GIN, ['metadata'], [], [Database::ORDER_ASC]);
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(IndexException::class, $e);
            $this->assertStringContainsString('GIN indexes do not support explicit orders', $e->getMessage());
        }
        $this->assertTrue($exceptionThrown, 'Expected Index exception for GIN index with orders');

        // Clean up
        $database->deleteCollection($collectionId);
    }

    public function testObjectAttributeInvalidCases(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Skip test if adapter doesn't support JSONB
        if (!$database->getAdapter()->getSupportForObject()) {
            $this->markTestSkipped('Adapter does not support object attributes');
        }

        $collectionId = ID::unique();
        $database->createCollection($collectionId);

        // Create object attribute
        $this->assertEquals(true, $database->createAttribute($collectionId, 'meta', Database::TYPE_OBJECT, 0, false));

        // Test 1: Try to create document with string instead of object (should fail)
        $exceptionThrown = false;
        try {
            $database->createDocument($collectionId, new Document([
                '$id' => 'invalid1',
                '$permissions' => [Permission::read(Role::any())],
                'meta' => 'this is a string not an object'
            ]));
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(StructureException::class, $e);
        }
        $this->assertTrue($exceptionThrown, 'Expected Structure exception for string value');

        // Test 2: Try to create document with integer instead of object (should fail)
        $exceptionThrown = false;
        try {
            $database->createDocument($collectionId, new Document([
                '$id' => 'invalid2',
                '$permissions' => [Permission::read(Role::any())],
                'meta' => 12345
            ]));
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(StructureException::class, $e);
        }
        $this->assertTrue($exceptionThrown, 'Expected Structure exception for integer value');

        // Test 3: Try to create document with boolean instead of object (should fail)
        $exceptionThrown = false;
        try {
            $database->createDocument($collectionId, new Document([
                '$id' => 'invalid3',
                '$permissions' => [Permission::read(Role::any())],
                'meta' => true
            ]));
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(StructureException::class, $e);
        }
        $this->assertTrue($exceptionThrown, 'Expected Structure exception for boolean value');

        // Test 4: Create valid document for query tests
        $database->createDocument($collectionId, new Document([
            '$id' => 'valid1',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'name' => 'John',
                'age' => 30,
                'settings' => [
                    'notifications' => true,
                    'theme' => 'dark'
                ]
            ]
        ]));

        // Test 5: Query with non-matching nested structure
        $results = $database->find($collectionId, [
            Query::equal('meta', [['settings' => ['notifications' => false]]])
        ]);
        $this->assertCount(0, $results, 'Should not match when nested value differs');

        // Test 6: Query with non-existent key
        $results = $database->find($collectionId, [
            Query::equal('meta', [['nonexistent' => 'value']])
        ]);
        $this->assertCount(0, $results, 'Should not match non-existent keys');

        // Test 7: Contains query with non-matching array element
        $database->createDocument($collectionId, new Document([
            '$id' => 'valid2',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'fruits' => ['apple', 'banana', 'orange']
            ]
        ]));
        $results = $database->find($collectionId, [
            Query::contains('meta', [['fruits' => 'grape']])
        ]);
        $this->assertCount(0, $results, 'Should not match non-existent array element');

        // Test 8: Test order preservation in nested objects
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'order_test',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => [
                'z_last' => 'value',
                'a_first' => 'value',
                'm_middle' => 'value'
            ]
        ]));
        $meta = $doc->getAttribute('meta');
        $this->assertIsArray($meta);
        // Note: JSON objects don't guarantee key order, but we can verify all keys exist
        $this->assertArrayHasKey('z_last', $meta);
        $this->assertArrayHasKey('a_first', $meta);
        $this->assertArrayHasKey('m_middle', $meta);

        // Test 9: Test with very large nested structure
        $largeStructure = [];
        for ($i = 0; $i < 50; $i++) {
            $largeStructure["key_$i"] = [
                'id' => $i,
                'name' => "Item $i",
                'values' => range(1, 10)
            ];
        }
        $docLarge = $database->createDocument($collectionId, new Document([
            '$id' => 'large_structure',
            '$permissions' => [Permission::read(Role::any())],
            'meta' => $largeStructure
        ]));
        $this->assertIsArray($docLarge->getAttribute('meta'));
        $this->assertCount(50, $docLarge->getAttribute('meta'));

        // Test 10: Query within large structure
        $results = $database->find($collectionId, [
            Query::equal('meta', [['key_25' => ['id' => 25, 'name' => 'Item 25', 'values' => range(1, 10)]]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('large_structure', $results[0]->getId());

        // Test 11: Test getDocument with large structure
        $fetchedLargeDoc = $database->getDocument($collectionId, 'large_structure');
        $this->assertEquals('large_structure', $fetchedLargeDoc->getId());
        $this->assertIsArray($fetchedLargeDoc->getAttribute('meta'));
        $this->assertCount(50, $fetchedLargeDoc->getAttribute('meta'));
        $this->assertEquals(25, $fetchedLargeDoc->getAttribute('meta')['key_25']['id']);
        $this->assertEquals('Item 25', $fetchedLargeDoc->getAttribute('meta')['key_25']['name']);

        // Test 12: Test Query::select with valid document
        $results = $database->find($collectionId, [
            Query::select(['$id', 'meta']),
            Query::equal('meta', [['name' => 'John']])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('valid1', $results[0]->getId());
        $this->assertIsArray($results[0]->getAttribute('meta'));
        $this->assertEquals('John', $results[0]->getAttribute('meta')['name']);
        $this->assertEquals(30, $results[0]->getAttribute('meta')['age']);

        // Test 13: Test getDocument returns proper structure
        $fetchedValid1 = $database->getDocument($collectionId, 'valid1');
        $this->assertEquals('valid1', $fetchedValid1->getId());
        $this->assertIsArray($fetchedValid1->getAttribute('meta'));
        $this->assertEquals('John', $fetchedValid1->getAttribute('meta')['name']);
        $this->assertTrue($fetchedValid1->getAttribute('meta')['settings']['notifications']);
        $this->assertEquals('dark', $fetchedValid1->getAttribute('meta')['settings']['theme']);

        // Test 14: Test Query::select excluding meta
        $results = $database->find($collectionId, [
            Query::select(['$id', '$permissions']),
            Query::equal('meta', [['fruits' => ['apple', 'banana', 'orange']]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('valid2', $results[0]->getId());
        // Meta should be empty when not selected
        $this->assertEmpty($results[0]->getAttribute('meta'));

        // Test 15: Test getDocument with non-existent ID returns empty document
        $nonExistent = $database->getDocument($collectionId, 'does_not_exist');
        $this->assertTrue($nonExistent->isEmpty());

        // Test 16: with multiple json
        $defaultSettings = ['config' => ['theme' => 'light', 'lang' => 'en']];
        $this->assertEquals(true, $database->createAttribute($collectionId, 'settings', Database::TYPE_OBJECT, 0, false, $defaultSettings));
        $database->createDocument($collectionId, new Document(['$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($collectionId, new Document(['settings' => ['config' => ['theme' => 'dark', 'lang' => 'en']],'$permissions' => [Permission::read(Role::any())]]));
        $results = $database->find($collectionId, [
            Query::equal('settings', [['config' => ['theme' => 'light']],['config' => ['theme' => 'dark']]])
        ]);
        $this->assertCount(2, $results);

        $results = $database->find($collectionId, [
            // Containment: both documents have config.lang == 'en'
            Query::contains('settings', [['config' => ['lang' => 'en']]])
        ]);
        $this->assertCount(2, $results);

        // Clean up
        $database->deleteCollection($collectionId);
    }

    public function testObjectAttributeDefaults(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Skip test if adapter doesn't support JSONB
        if (!$database->getAdapter()->getSupportForObject()) {
            $this->markTestSkipped('Adapter does not support object attributes');
        }

        $collectionId = ID::unique();
        $database->createCollection($collectionId);

        // 1) Default empty object
        $this->assertEquals(true, $database->createAttribute($collectionId, 'metaDefaultEmpty', Database::TYPE_OBJECT, 0, false, []));

        // 2) Default nested object
        $defaultSettings = ['config' => ['theme' => 'light', 'lang' => 'en']];
        $this->assertEquals(true, $database->createAttribute($collectionId, 'settings', Database::TYPE_OBJECT, 0, false, $defaultSettings));

        // 3) Required without default (should fail when missing)
        $this->assertEquals(true, $database->createAttribute($collectionId, 'profile', Database::TYPE_OBJECT, 0, true, null));

        // 4) Required with default (should auto-populate)
        $this->assertEquals(true, $database->createAttribute($collectionId, 'profile2', Database::TYPE_OBJECT, 0, false, ['name' => 'anon']));

        // 5) Explicit null default
        $this->assertEquals(true, $database->createAttribute($collectionId, 'misc', Database::TYPE_OBJECT, 0, false, null));

        // Create document missing all above attributes
        $exceptionThrown = false;
        try {
            $doc = $database->createDocument($collectionId, new Document([
                '$id' => 'def1',
                '$permissions' => [Permission::read(Role::any())],
            ]));
            // Should not reach here because 'profile' is required and missing
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(StructureException::class, $e);
        }
        $this->assertTrue($exceptionThrown, 'Expected Structure exception for missing required object attribute');

        // Create document providing required 'profile' but omit others to test defaults
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'def2',
            '$permissions' => [Permission::read(Role::any())],
            'profile' => ['name' => 'provided'],
        ]));

        // metaDefaultEmpty should default to []
        $this->assertIsArray($doc->getAttribute('metaDefaultEmpty'));
        $this->assertEmpty($doc->getAttribute('metaDefaultEmpty'));

        // settings should default to nested object
        $this->assertIsArray($doc->getAttribute('settings'));
        $this->assertEquals('light', $doc->getAttribute('settings')['config']['theme']);
        $this->assertEquals('en', $doc->getAttribute('settings')['config']['lang']);

        // profile provided explicitly
        $this->assertEquals('provided', $doc->getAttribute('profile')['name']);

        // profile2 required with default should be auto-populated
        $this->assertIsArray($doc->getAttribute('profile2'));
        $this->assertEquals('anon', $doc->getAttribute('profile2')['name']);

        // misc explicit null default remains null when omitted
        $this->assertNull($doc->getAttribute('misc'));

        // Query defaults work
        $results = $database->find($collectionId, [
            Query::equal('settings', [['config' => ['theme' => 'light']]])
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('def2', $results[0]->getId());

        // Clean up
        $database->deleteCollection($collectionId);
    }
}
