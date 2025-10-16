<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\Query;

trait OperatorTests
{
    public function testUpdateWithOperators(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create test collection with various attribute types
        $collectionId = 'test_operators';
        $database->createCollection($collectionId);

        $database->createAttribute($collectionId, 'count', Database::VAR_INTEGER, 0, false, 0);
        $database->createAttribute($collectionId, 'score', Database::VAR_FLOAT, 0, false, 0.0);
        $database->createAttribute($collectionId, 'tags', Database::VAR_STRING, 50, false, null, true, true);
        $database->createAttribute($collectionId, 'numbers', Database::VAR_INTEGER, 0, false, null, true, true);
        $database->createAttribute($collectionId, 'name', Database::VAR_STRING, 100, false, 'test');

        // Create test document
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => 10,
            'score' => 15.5,
            'tags' => ['initial', 'tag'],
            'numbers' => [1, 2, 3],
            'name' => 'Test Document'
        ]));

        // Test increment operator
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'count' => Operator::increment(5)
        ]));
        $this->assertEquals(15, $updated->getAttribute('count'));

        // Test decrement operator
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'count' => Operator::decrement(3)
        ]));
        $this->assertEquals(12, $updated->getAttribute('count'));

        // Test increment with float
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'score' => Operator::increment(2.5)
        ]));
        $this->assertEquals(18.0, $updated->getAttribute('score'));

        // Test append operator
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'tags' => Operator::arrayAppend(['new', 'appended'])
        ]));
        $this->assertEquals(['initial', 'tag', 'new', 'appended'], $updated->getAttribute('tags'));

        // Test prepend operator
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'tags' => Operator::arrayPrepend(['first'])
        ]));
        $this->assertEquals(['first', 'initial', 'tag', 'new', 'appended'], $updated->getAttribute('tags'));

        // Test insert operator
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'numbers' => Operator::arrayInsert(1, 99)
        ]));
        $this->assertEquals([1, 99, 2, 3], $updated->getAttribute('numbers'));

        // Test multiple operators in one update
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'count' => Operator::increment(8),
            'score' => Operator::decrement(3.0),
            'numbers' => Operator::arrayAppend([4, 5]),
            'name' => 'Updated Name' // Regular update mixed with operators
        ]));

        $this->assertEquals(20, $updated->getAttribute('count'));
        $this->assertEquals(15.0, $updated->getAttribute('score'));
        $this->assertEquals([1, 99, 2, 3, 4, 5], $updated->getAttribute('numbers'));
        $this->assertEquals('Updated Name', $updated->getAttribute('name'));

        // Test edge cases

        // Test increment with default value (1)
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'count' => Operator::increment() // Should increment by 1
        ]));
        $this->assertEquals(21, $updated->getAttribute('count'));

        // Test insert at beginning (index 0)
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'numbers' => Operator::arrayInsert(0, 0)
        ]));
        $this->assertEquals([0, 1, 99, 2, 3, 4, 5], $updated->getAttribute('numbers'));

        // Test insert at end
        $numbers = $updated->getAttribute('numbers');
        $lastIndex = count($numbers);
        $updated = $database->updateDocument($collectionId, 'test_doc', new Document([
            'numbers' => Operator::arrayInsert($lastIndex, 100)
        ]));
        $this->assertEquals([0, 1, 99, 2, 3, 4, 5, 100], $updated->getAttribute('numbers'));

        $database->deleteCollection($collectionId);
    }

    public function testUpdateDocumentsWithOperators(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create test collection
        $collectionId = 'test_batch_operators';
        $database->createCollection($collectionId);

        $database->createAttribute($collectionId, 'count', Database::VAR_INTEGER, 0, false, 0);
        $database->createAttribute($collectionId, 'tags', Database::VAR_STRING, 50, false, null, true, true);
        $database->createAttribute($collectionId, 'category', Database::VAR_STRING, 50, true);

        // Create multiple test documents
        $docs = [];
        for ($i = 1; $i <= 3; $i++) {
            $docs[] = $database->createDocument($collectionId, new Document([
                '$id' => "doc_{$i}",
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                'count' => $i * 10,
                'tags' => ["tag_{$i}"],
                'category' => 'test'
            ]));
        }

        // Test updateDocuments with operators
        $count = $database->updateDocuments(
            $collectionId,
            new Document([
                'count' => Operator::increment(5),
                'tags' => Operator::arrayAppend(['batch_updated']),
                'category' => 'updated' // Regular update mixed with operators
            ])
        );

        $this->assertEquals(3, $count);

        // Verify all documents were updated
        $updated = $database->find($collectionId);
        $this->assertCount(3, $updated);

        foreach ($updated as $doc) {
            $originalCount = (int) str_replace('doc_', '', $doc->getId()) * 10;
            $this->assertEquals($originalCount + 5, $doc->getAttribute('count'));
            $this->assertContains('batch_updated', $doc->getAttribute('tags'));
            $this->assertEquals('updated', $doc->getAttribute('category'));
        }

        // Test with query filters
        $count = $database->updateDocuments(
            $collectionId,
            new Document([
                'count' => Operator::increment(10)
            ]),
            [Query::equal('$id', ['doc_1', 'doc_2'])]
        );

        $this->assertEquals(2, $count);

        // Verify only filtered documents were updated
        $doc1 = $database->getDocument($collectionId, 'doc_1');
        $doc2 = $database->getDocument($collectionId, 'doc_2');
        $doc3 = $database->getDocument($collectionId, 'doc_3');

        $this->assertEquals(25, $doc1->getAttribute('count')); // 10 + 5 + 10
        $this->assertEquals(35, $doc2->getAttribute('count')); // 20 + 5 + 10
        $this->assertEquals(35, $doc3->getAttribute('count')); // 30 + 5 (not updated in second batch)

        $database->deleteCollection($collectionId);
    }

    public function testUpdateDocumentsWithAllOperators(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create comprehensive test collection
        $collectionId = 'test_all_operators_bulk';
        $database->createCollection($collectionId);

        // Create attributes for all operator types
        $database->createAttribute($collectionId, 'counter', Database::VAR_INTEGER, 0, false, 10);
        $database->createAttribute($collectionId, 'score', Database::VAR_FLOAT, 0, false, 5.0);
        $database->createAttribute($collectionId, 'multiplier', Database::VAR_FLOAT, 0, false, 2.0);
        $database->createAttribute($collectionId, 'divisor', Database::VAR_FLOAT, 0, false, 100.0);
        $database->createAttribute($collectionId, 'remainder', Database::VAR_INTEGER, 0, false, 20);
        $database->createAttribute($collectionId, 'power_val', Database::VAR_FLOAT, 0, false, 2.0);
        $database->createAttribute($collectionId, 'title', Database::VAR_STRING, 255, false, 'Title');
        $database->createAttribute($collectionId, 'content', Database::VAR_STRING, 500, false, 'old content');
        $database->createAttribute($collectionId, 'tags', Database::VAR_STRING, 50, false, null, true, true);
        $database->createAttribute($collectionId, 'categories', Database::VAR_STRING, 50, false, null, true, true);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);
        $database->createAttribute($collectionId, 'duplicates', Database::VAR_STRING, 50, false, null, true, true);
        $database->createAttribute($collectionId, 'active', Database::VAR_BOOLEAN, 0, false, false);
        $database->createAttribute($collectionId, 'last_update', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']);
        $database->createAttribute($collectionId, 'next_update', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']);
        $database->createAttribute($collectionId, 'now_field', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']);

        // Create test documents
        $docs = [];
        for ($i = 1; $i <= 3; $i++) {
            $docs[] = $database->createDocument($collectionId, new Document([
                '$id' => "bulk_doc_{$i}",
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                'counter' => $i * 10,
                'score' => $i * 1.5,
                'multiplier' => $i * 1.0,
                'divisor' => $i * 50.0,
                'remainder' => $i * 7,
                'power_val' => $i + 1.0,
                'title' => "Title {$i}",
                'content' => "old content {$i}",
                'tags' => ["tag_{$i}", "common"],
                'categories' => ["cat_{$i}", "test"],
                'items' => ["item_{$i}", "shared", "item_{$i}"],
                'duplicates' => ["a", "b", "a", "c", "b", "d"],
                'active' => $i % 2 === 0,
                'last_update' => DateTime::addSeconds(new \DateTime(), -86400),
                'next_update' => DateTime::addSeconds(new \DateTime(), 86400)
            ]));
        }

        // Test bulk update with ALL operators
        $count = $database->updateDocuments(
            $collectionId,
            new Document([
                'counter' => Operator::increment(5, 50),           // Math with limit
                'score' => Operator::decrement(0.5, 0),            // Math with limit
                'multiplier' => Operator::multiply(2, 100),        // Math with limit
                'divisor' => Operator::divide(2, 10),              // Math with limit
                'remainder' => Operator::modulo(5),                // Math
                'power_val' => Operator::power(2, 100),            // Math with limit
                'title' => Operator::concat(' - Updated'),         // String
                'content' => Operator::replace('old', 'new'),      // String
                'tags' => Operator::arrayAppend(['bulk']),         // Array
                'categories' => Operator::arrayPrepend(['priority']), // Array
                'items' => Operator::arrayRemove('shared'),        // Array
                'duplicates' => Operator::arrayUnique(),           // Array
                'active' => Operator::toggle(),                    // Boolean
                'last_update' => Operator::dateAddDays(1),         // Date
                'next_update' => Operator::dateSubDays(1),         // Date
                'now_field' => Operator::dateSetNow()              // Date
            ])
        );

        $this->assertEquals(3, $count);

        // Verify all operators worked correctly
        $updated = $database->find($collectionId, [Query::orderAsc('$id')]);
        $this->assertCount(3, $updated);

        // Check bulk_doc_1
        $doc1 = $updated[0];
        $this->assertEquals(15, $doc1->getAttribute('counter'));          // 10 + 5
        $this->assertEquals(1.0, $doc1->getAttribute('score'));           // 1.5 - 0.5
        $this->assertEquals(2.0, $doc1->getAttribute('multiplier'));      // 1.0 * 2
        $this->assertEquals(25.0, $doc1->getAttribute('divisor'));        // 50.0 / 2
        $this->assertEquals(2, $doc1->getAttribute('remainder'));         // 7 % 5
        $this->assertEquals(4.0, $doc1->getAttribute('power_val'));       // 2^2
        $this->assertEquals('Title 1 - Updated', $doc1->getAttribute('title'));
        $this->assertEquals('new content 1', $doc1->getAttribute('content'));
        $this->assertContains('bulk', $doc1->getAttribute('tags'));
        $this->assertContains('priority', $doc1->getAttribute('categories'));
        $this->assertNotContains('shared', $doc1->getAttribute('items'));
        $this->assertCount(4, $doc1->getAttribute('duplicates')); // Should have unique values
        $this->assertEquals(true, $doc1->getAttribute('active'));         // Was false, toggled to true

        // Check bulk_doc_2
        $doc2 = $updated[1];
        $this->assertEquals(25, $doc2->getAttribute('counter'));          // 20 + 5
        $this->assertEquals(2.5, $doc2->getAttribute('score'));           // 3.0 - 0.5
        $this->assertEquals(4.0, $doc2->getAttribute('multiplier'));      // 2.0 * 2
        $this->assertEquals(50.0, $doc2->getAttribute('divisor'));        // 100.0 / 2
        $this->assertEquals(4, $doc2->getAttribute('remainder'));         // 14 % 5
        $this->assertEquals(9.0, $doc2->getAttribute('power_val'));       // 3^2
        $this->assertEquals('Title 2 - Updated', $doc2->getAttribute('title'));
        $this->assertEquals('new content 2', $doc2->getAttribute('content'));
        $this->assertEquals(false, $doc2->getAttribute('active'));        // Was true, toggled to false

        // Check bulk_doc_3
        $doc3 = $updated[2];
        $this->assertEquals(35, $doc3->getAttribute('counter'));          // 30 + 5
        $this->assertEquals(4.0, $doc3->getAttribute('score'));           // 4.5 - 0.5
        $this->assertEquals(6.0, $doc3->getAttribute('multiplier'));      // 3.0 * 2
        $this->assertEquals(75.0, $doc3->getAttribute('divisor'));        // 150.0 / 2
        $this->assertEquals(1, $doc3->getAttribute('remainder'));         // 21 % 5
        $this->assertEquals(16.0, $doc3->getAttribute('power_val'));      // 4^2
        $this->assertEquals('Title 3 - Updated', $doc3->getAttribute('title'));
        $this->assertEquals('new content 3', $doc3->getAttribute('content'));
        $this->assertEquals(true, $doc3->getAttribute('active'));         // Was false, toggled to true

        // Verify date operations worked (just check they're not null and are strings)
        $this->assertNotNull($doc1->getAttribute('last_update'));
        $this->assertNotNull($doc1->getAttribute('next_update'));
        $this->assertNotNull($doc1->getAttribute('now_field'));

        $database->deleteCollection($collectionId);
    }

    public function testUpdateDocumentsOperatorsWithQueries(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create test collection
        $collectionId = 'test_operators_with_queries';
        $database->createCollection($collectionId);

        $database->createAttribute($collectionId, 'category', Database::VAR_STRING, 50, true);
        $database->createAttribute($collectionId, 'count', Database::VAR_INTEGER, 0, false, 0);
        $database->createAttribute($collectionId, 'score', Database::VAR_FLOAT, 0, false, 0.0);
        $database->createAttribute($collectionId, 'active', Database::VAR_BOOLEAN, 0, false, false);

        // Create test documents
        for ($i = 1; $i <= 5; $i++) {
            $database->createDocument($collectionId, new Document([
                '$id' => "query_doc_{$i}",
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                'category' => $i <= 3 ? 'A' : 'B',
                'count' => $i * 10,
                'score' => $i * 1.5,
                'active' => $i % 2 === 0
            ]));
        }

        // Test 1: Update only category A documents
        $count = $database->updateDocuments(
            $collectionId,
            new Document([
                'count' => Operator::increment(100),
                'score' => Operator::multiply(2)
            ]),
            [Query::equal('category', ['A'])]
        );

        $this->assertEquals(3, $count);

        // Verify only category A documents were updated
        $categoryA = $database->find($collectionId, [Query::equal('category', ['A']), Query::orderAsc('$id')]);
        $categoryB = $database->find($collectionId, [Query::equal('category', ['B']), Query::orderAsc('$id')]);

        $this->assertEquals(110, $categoryA[0]->getAttribute('count')); // 10 + 100
        $this->assertEquals(120, $categoryA[1]->getAttribute('count')); // 20 + 100
        $this->assertEquals(130, $categoryA[2]->getAttribute('count')); // 30 + 100
        $this->assertEquals(40, $categoryB[0]->getAttribute('count'));  // Not updated
        $this->assertEquals(50, $categoryB[1]->getAttribute('count'));  // Not updated

        // Test 2: Update only documents with count < 50
        $count = $database->updateDocuments(
            $collectionId,
            new Document([
                'active' => Operator::toggle(),
                'score' => Operator::multiply(10)
            ]),
            [Query::lessThan('count', 50)]
        );

        // Only doc_4 (count=40) matches, doc_5 has count=50 which is not < 50
        $this->assertEquals(1, $count);

        $doc4 = $database->getDocument($collectionId, 'query_doc_4');
        $this->assertEquals(false, $doc4->getAttribute('active')); // Was true, now false
        // Doc_4 initial score: 4*1.5 = 6.0
        // Category B so not updated in first batch
        // Second update: 6.0 * 10 = 60.0
        $this->assertEquals(60.0, $doc4->getAttribute('score'));

        // Verify doc_5 was not updated
        $doc5 = $database->getDocument($collectionId, 'query_doc_5');
        $this->assertEquals(false, $doc5->getAttribute('active')); // Still false
        $this->assertEquals(7.5, $doc5->getAttribute('score'));    // Still 5*1.5=7.5 (category B, not updated)

        $database->deleteCollection($collectionId);
    }

    public function testOperatorErrorHandling(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create test collection
        $collectionId = 'test_operator_errors';
        $database->createCollection($collectionId);

        $database->createAttribute($collectionId, 'text_field', Database::VAR_STRING, 100, true);
        $database->createAttribute($collectionId, 'number_field', Database::VAR_INTEGER, 0, true);
        $database->createAttribute($collectionId, 'array_field', Database::VAR_STRING, 50, false, null, true, true);

        // Create test document
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'error_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'text_field' => 'hello',
            'number_field' => 42,
            'array_field' => ['item1', 'item2']
        ]));

        // Test increment on non-numeric field
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage("Cannot apply increment to non-numeric field 'text_field'");

        $database->updateDocument($collectionId, 'error_test_doc', new Document([
            'text_field' => Operator::increment(1)
        ]));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayErrorHandling(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create test collection
        $collectionId = 'test_array_operator_errors';
        $database->createCollection($collectionId);

        $database->createAttribute($collectionId, 'text_field', Database::VAR_STRING, 100, true);
        $database->createAttribute($collectionId, 'array_field', Database::VAR_STRING, 50, false, null, true, true);

        // Create test document
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'array_error_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'text_field' => 'hello',
            'array_field' => ['item1', 'item2']
        ]));

        // Test append on non-array field
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage("Cannot apply arrayAppend to non-array field 'text_field'");

        $database->updateDocument($collectionId, 'array_error_test_doc', new Document([
            'text_field' => Operator::arrayAppend(['new_item'])
        ]));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorInsertErrorHandling(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create test collection
        $collectionId = 'test_insert_operator_errors';
        $database->createCollection($collectionId);

        $database->createAttribute($collectionId, 'array_field', Database::VAR_STRING, 50, false, null, true, true);

        // Create test document
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'insert_error_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'array_field' => ['item1', 'item2']
        ]));

        // Test insert with negative index
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage("Insert index must be a non-negative integer");

        $database->updateDocument($collectionId, 'insert_error_test_doc', new Document([
            'array_field' => Operator::arrayInsert(-1, 'new_item')
        ]));

        $database->deleteCollection($collectionId);
    }

    /**
     * Comprehensive edge case tests for operator validation failures
     */
    public function testOperatorValidationEdgeCases(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create comprehensive test collection
        $collectionId = 'test_operator_edge_cases';
        $database->createCollection($collectionId);

        // Create various attribute types for testing
        $database->createAttribute($collectionId, 'string_field', Database::VAR_STRING, 100, false, 'default');
        $database->createAttribute($collectionId, 'int_field', Database::VAR_INTEGER, 0, false, 10);
        $database->createAttribute($collectionId, 'float_field', Database::VAR_FLOAT, 0, false, 1.5);
        $database->createAttribute($collectionId, 'bool_field', Database::VAR_BOOLEAN, 0, false, false);
        $database->createAttribute($collectionId, 'array_field', Database::VAR_STRING, 50, false, null, true, true);
        $database->createAttribute($collectionId, 'date_field', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']);

        // Create test document
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'edge_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'string_field' => 'hello',
            'int_field' => 42,
            'float_field' => 3.14,
            'bool_field' => true,
            'array_field' => ['a', 'b', 'c'],
            'date_field' => '2023-01-01 00:00:00'
        ]));

        // Test: Math operator on string field
        try {
            $database->updateDocument($collectionId, 'edge_test_doc', new Document([
                'string_field' => Operator::increment(5)
            ]));
            $this->fail('Expected exception for increment on string field');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString("Cannot apply increment to non-numeric field 'string_field'", $e->getMessage());
        }

        // Test: String operator on numeric field
        try {
            $database->updateDocument($collectionId, 'edge_test_doc', new Document([
                'int_field' => Operator::concat(' suffix')
            ]));
            $this->fail('Expected exception for concat on integer field');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString("Cannot apply concat", $e->getMessage());
        }

        // Test: Array operator on non-array field
        try {
            $database->updateDocument($collectionId, 'edge_test_doc', new Document([
                'string_field' => Operator::arrayAppend(['new'])
            ]));
            $this->fail('Expected exception for arrayAppend on string field');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString("Cannot apply arrayAppend to non-array field 'string_field'", $e->getMessage());
        }

        // Test: Boolean operator on non-boolean field
        try {
            $database->updateDocument($collectionId, 'edge_test_doc', new Document([
                'int_field' => Operator::toggle()
            ]));
            $this->fail('Expected exception for toggle on integer field');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString("Cannot apply toggle to non-boolean field 'int_field'", $e->getMessage());
        }

        // Test: Date operator on non-date field
        try {
            $database->updateDocument($collectionId, 'edge_test_doc', new Document([
                'string_field' => Operator::dateAddDays(5)
            ]));
            $this->fail('Expected exception for dateAddDays on string field');
        } catch (DatabaseException $e) {
            // Date operators check if string can be parsed as date
            $this->assertStringContainsString("Invalid date format in field 'string_field'", $e->getMessage());
        }

        $database->deleteCollection($collectionId);
    }

    public function testOperatorDivisionModuloByZero(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_division_zero';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'number', Database::VAR_FLOAT, 0, false, 100.0);

        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'zero_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'number' => 100.0
        ]));

        // Test: Division by zero
        try {
            $database->updateDocument($collectionId, 'zero_test_doc', new Document([
                'number' => Operator::divide(0)
            ]));
            $this->fail('Expected exception for division by zero');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString("Division by zero is not allowed", $e->getMessage());
        }

        // Test: Modulo by zero
        try {
            $database->updateDocument($collectionId, 'zero_test_doc', new Document([
                'number' => Operator::modulo(0)
            ]));
            $this->fail('Expected exception for modulo by zero');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString("Modulo by zero is not allowed", $e->getMessage());
        }

        // Test: Valid division
        $updated = $database->updateDocument($collectionId, 'zero_test_doc', new Document([
            'number' => Operator::divide(2)
        ]));
        $this->assertEquals(50.0, $updated->getAttribute('number'));

        // Test: Valid modulo
        $updated = $database->updateDocument($collectionId, 'zero_test_doc', new Document([
            'number' => Operator::modulo(7)
        ]));
        $this->assertEquals(1.0, $updated->getAttribute('number')); // 50 % 7 = 1

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayInsertOutOfBounds(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_array_insert_bounds';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'bounds_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['a', 'b', 'c'] // Length = 3
        ]));

        // Test: Insert at out of bounds index
        try {
            $database->updateDocument($collectionId, 'bounds_test_doc', new Document([
                'items' => Operator::arrayInsert(10, 'new') // Index 10 > length 3
            ]));
            $this->fail('Expected exception for out of bounds insert');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString("Insert index 10 is out of bounds for array of length 3", $e->getMessage());
        }

        // Test: Insert at valid index (end)
        $updated = $database->updateDocument($collectionId, 'bounds_test_doc', new Document([
            'items' => Operator::arrayInsert(3, 'd') // Insert at end
        ]));
        $this->assertEquals(['a', 'b', 'c', 'd'], $updated->getAttribute('items'));

        // Test: Insert at valid index (middle)
        $updated = $database->updateDocument($collectionId, 'bounds_test_doc', new Document([
            'items' => Operator::arrayInsert(2, 'x') // Insert at index 2
        ]));
        $this->assertEquals(['a', 'b', 'x', 'c', 'd'], $updated->getAttribute('items'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorValueLimits(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_operator_limits';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'counter', Database::VAR_INTEGER, 0, false, 10);
        $database->createAttribute($collectionId, 'score', Database::VAR_FLOAT, 0, false, 5.0);

        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'limits_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 10,
            'score' => 5.0
        ]));

        // Test: Increment with max limit
        $updated = $database->updateDocument($collectionId, 'limits_test_doc', new Document([
            'counter' => Operator::increment(100, 50) // Increment by 100 but max is 50
        ]));
        $this->assertEquals(50, $updated->getAttribute('counter')); // Should be capped at 50

        // Test: Decrement with min limit
        $updated = $database->updateDocument($collectionId, 'limits_test_doc', new Document([
            'score' => Operator::decrement(10, 0) // Decrement score by 10 but min is 0
        ]));
        $this->assertEquals(0, $updated->getAttribute('score')); // Should be capped at 0

        // Test: Multiply with max limit
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'limits_test_doc2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 10,
            'score' => 5.0
        ]));

        $updated = $database->updateDocument($collectionId, 'limits_test_doc2', new Document([
            'counter' => Operator::multiply(10, 75) // 10 * 10 = 100, but max is 75
        ]));
        $this->assertEquals(75, $updated->getAttribute('counter')); // Should be capped at 75

        // Test: Power with max limit
        $updated = $database->updateDocument($collectionId, 'limits_test_doc2', new Document([
            'score' => Operator::power(3, 100) // 5^3 = 125, but max is 100
        ]));
        $this->assertEquals(100, $updated->getAttribute('score')); // Should be capped at 100

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayFilterValidation(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_array_filter';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'numbers', Database::VAR_INTEGER, 0, false, null, true, true);
        $database->createAttribute($collectionId, 'tags', Database::VAR_STRING, 50, false, null, true, true);

        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'filter_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'numbers' => [1, 2, 3, 4, 5],
            'tags' => ['apple', 'banana', 'cherry']
        ]));

        // Test: Filter with equals condition on numbers
        $updated = $database->updateDocument($collectionId, 'filter_test_doc', new Document([
            'numbers' => Operator::arrayFilter('equals', 3) // Keep only 3
        ]));
        $this->assertEquals([3], $updated->getAttribute('numbers'));

        // Test: Filter with not-equals condition on strings
        $updated = $database->updateDocument($collectionId, 'filter_test_doc', new Document([
            'tags' => Operator::arrayFilter('notEquals', 'banana') // Remove 'banana'
        ]));
        $this->assertEquals(['apple', 'cherry'], $updated->getAttribute('tags'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorReplaceValidation(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_replace';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'text', Database::VAR_STRING, 255, false, 'default text');
        $database->createAttribute($collectionId, 'number', Database::VAR_INTEGER, 0, false, 0);

        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'replace_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'text' => 'The quick brown fox',
            'number' => 42
        ]));

        // Test: Valid replace operation
        $updated = $database->updateDocument($collectionId, 'replace_test_doc', new Document([
            'text' => Operator::replace('quick', 'slow')
        ]));
        $this->assertEquals('The slow brown fox', $updated->getAttribute('text'));

        // Test: Replace on non-string field
        try {
            $database->updateDocument($collectionId, 'replace_test_doc', new Document([
                'number' => Operator::replace('4', '5')
            ]));
            $this->fail('Expected exception for replace on integer field');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString("Cannot apply replace to non-string field 'number'", $e->getMessage());
        }

        // Test: Replace with empty string
        $updated = $database->updateDocument($collectionId, 'replace_test_doc', new Document([
            'text' => Operator::replace('slow', '')
        ]));
        $this->assertEquals('The  brown fox', $updated->getAttribute('text')); // Two spaces where 'slow' was

        $database->deleteCollection($collectionId);
    }

    public function testOperatorNullValueHandling(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_null_handling';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'nullable_int', Database::VAR_INTEGER, 0, false, null, false, false);
        $database->createAttribute($collectionId, 'nullable_string', Database::VAR_STRING, 100, false, null, false, false);
        $database->createAttribute($collectionId, 'nullable_bool', Database::VAR_BOOLEAN, 0, false, null, false, false);

        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'null_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'nullable_int' => null,
            'nullable_string' => null,
            'nullable_bool' => null
        ]));

        // Test: Increment on null numeric field (should treat as 0)
        $updated = $database->updateDocument($collectionId, 'null_test_doc', new Document([
            'nullable_int' => Operator::increment(5)
        ]));
        $this->assertEquals(5, $updated->getAttribute('nullable_int'));

        // Test: Concat on null string field (should treat as empty string)
        $updated = $database->updateDocument($collectionId, 'null_test_doc', new Document([
            'nullable_string' => Operator::concat('hello')
        ]));
        $this->assertEquals('hello', $updated->getAttribute('nullable_string'));

        // Test: Toggle on null boolean field (should treat as false)
        $updated = $database->updateDocument($collectionId, 'null_test_doc', new Document([
            'nullable_bool' => Operator::toggle()
        ]));
        $this->assertEquals(true, $updated->getAttribute('nullable_bool'));

        // Test operators on non-null values
        $updated = $database->updateDocument($collectionId, 'null_test_doc', new Document([
            'nullable_int' => Operator::multiply(2)  // 5 * 2 = 10
        ]));
        $this->assertEquals(10, $updated->getAttribute('nullable_int'));

        $updated = $database->updateDocument($collectionId, 'null_test_doc', new Document([
            'nullable_string' => Operator::replace('hello', 'hi')
        ]));
        $this->assertEquals('hi', $updated->getAttribute('nullable_string'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorComplexScenarios(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_complex_operators';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'stats', Database::VAR_INTEGER, 0, false, null, true, true);
        $database->createAttribute($collectionId, 'metadata', Database::VAR_STRING, 100, false, null, true, true);
        $database->createAttribute($collectionId, 'score', Database::VAR_FLOAT, 0, false, 0.0);
        $database->createAttribute($collectionId, 'name', Database::VAR_STRING, 255, false, '');

        // Create document with complex data
        $doc = $database->createDocument($collectionId, new Document([
            '$id' => 'complex_test_doc',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'stats' => [10, 20, 20, 30, 20, 40],
            'metadata' => ['key1', 'key2', 'key3'],
            'score' => 50.0,
            'name' => 'Test'
        ]));

        // Test: Multiple operations on same array
        $updated = $database->updateDocument($collectionId, 'complex_test_doc', new Document([
            'stats' => Operator::arrayUnique() // Should remove duplicate 20s
        ]));
        $stats = $updated->getAttribute('stats');
        $this->assertCount(4, $stats); // [10, 20, 30, 40]
        $this->assertEquals([10, 20, 30, 40], $stats);

        // Test: Array intersection
        $updated = $database->updateDocument($collectionId, 'complex_test_doc', new Document([
            'stats' => Operator::arrayIntersect([20, 30, 50]) // Keep only 20 and 30
        ]));
        $this->assertEquals([20, 30], $updated->getAttribute('stats'));

        // Test: Array difference
        $doc2 = $database->createDocument($collectionId, new Document([
            '$id' => 'complex_test_doc2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'stats' => [1, 2, 3, 4, 5],
            'metadata' => ['a', 'b', 'c'],
            'score' => 100.0,
            'name' => 'Test2'
        ]));

        $updated = $database->updateDocument($collectionId, 'complex_test_doc2', new Document([
            'stats' => Operator::arrayDiff([2, 4, 6]) // Remove 2 and 4
        ]));
        $this->assertEquals([1, 3, 5], $updated->getAttribute('stats'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorIncrement(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_increment_operator';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'count', Database::VAR_INTEGER, 0, false, 0);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => 5
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'count' => Operator::increment(3)
        ]));

        $this->assertEquals(8, $updated->getAttribute('count'));

        // Edge case: null value
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => null
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'count' => Operator::increment(3)
        ]));

        $this->assertEquals(3, $updated->getAttribute('count'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorStringConcat(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_string_concat_operator';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'title', Database::VAR_STRING, 255, false, '');

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Hello'
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'title' => Operator::concat(' World')
        ]));

        $this->assertEquals('Hello World', $updated->getAttribute('title'));

        // Edge case: null value
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => null
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'title' => Operator::concat('Test')
        ]));

        $this->assertEquals('Test', $updated->getAttribute('title'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorModulo(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_modulo_operator';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'number', Database::VAR_INTEGER, 0, false, 0);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'number' => 10
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'number' => Operator::modulo(3)
        ]));

        $this->assertEquals(1, $updated->getAttribute('number'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorToggle(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_toggle_operator';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'active', Database::VAR_BOOLEAN, 0, false, false);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'active' => false
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'active' => Operator::toggle()
        ]));

        $this->assertEquals(true, $updated->getAttribute('active'));

        // Test toggle again
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'active' => Operator::toggle()
        ]));

        $this->assertEquals(false, $updated->getAttribute('active'));

        $database->deleteCollection($collectionId);
    }


    public function testOperatorArrayUnique(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'test_array_unique_operator';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['a', 'b', 'a', 'c', 'b']
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayUnique()
        ]));

        $result = $updated->getAttribute('items');
        $this->assertCount(3, $result);
        $this->assertContains('a', $result);
        $this->assertContains('b', $result);
        $this->assertContains('c', $result);

        $database->deleteCollection($collectionId);
    }

    // Comprehensive Operator Tests

    public function testOperatorIncrementComprehensive(): void
    {
        $database = static::getDatabase();

        // Setup collection
        $collectionId = 'operator_increment_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'count', Database::VAR_INTEGER, 0, false);
        $database->createAttribute($collectionId, 'score', Database::VAR_FLOAT, 0, false);
        $database->createAttribute($collectionId, 'text', Database::VAR_STRING, 255, false);

        // Success case - integer
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => 5
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'count' => Operator::increment(3)
        ]));

        $this->assertEquals(8, $updated->getAttribute('count'));

        // Success case - with max limit
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'count' => Operator::increment(5, 10)
        ]));
        $this->assertEquals(10, $updated->getAttribute('count')); // Should cap at 10

        // Success case - float
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'score' => 2.5
        ]));

        $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
            'score' => Operator::increment(1.5)
        ]));
        $this->assertEquals(4.0, $updated->getAttribute('score'));

        // Edge case: null value
        $doc3 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => null
        ]));
        $updated = $database->updateDocument($collectionId, $doc3->getId(), new Document([
            'count' => Operator::increment(5)
        ]));
        $this->assertEquals(5, $updated->getAttribute('count'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorDecrementComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_decrement_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'count', Database::VAR_INTEGER, 0, false);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => 10
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'count' => Operator::decrement(3)
        ]));

        $this->assertEquals(7, $updated->getAttribute('count'));

        // Success case - with min limit
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'count' => Operator::decrement(10, 5)
        ]));
        $this->assertEquals(5, $updated->getAttribute('count')); // Should stop at min 5

        // Edge case: null value
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => null
        ]));
        $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
            'count' => Operator::decrement(3)
        ]));
        $this->assertEquals(-3, $updated->getAttribute('count'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorMultiplyComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_multiply_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'value', Database::VAR_FLOAT, 0, false);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'value' => 4.0
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'value' => Operator::multiply(2.5)
        ]));

        $this->assertEquals(10.0, $updated->getAttribute('value'));

        // Success case - with max limit
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'value' => Operator::multiply(3, 20)
        ]));
        $this->assertEquals(20.0, $updated->getAttribute('value')); // Should cap at 20

        $database->deleteCollection($collectionId);
    }

    public function testOperatorDivideComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_divide_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'value', Database::VAR_FLOAT, 0, false);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'value' => 10.0
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'value' => Operator::divide(2)
        ]));

        $this->assertEquals(5.0, $updated->getAttribute('value'));

        // Success case - with min limit
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'value' => Operator::divide(10, 2)
        ]));
        $this->assertEquals(2.0, $updated->getAttribute('value')); // Should stop at min 2

        $database->deleteCollection($collectionId);
    }

    public function testOperatorModuloComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_modulo_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'number', Database::VAR_INTEGER, 0, false);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'number' => 10
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'number' => Operator::modulo(3)
        ]));

        $this->assertEquals(1, $updated->getAttribute('number'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorPowerComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_power_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'number', Database::VAR_FLOAT, 0, false);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'number' => 2
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'number' => Operator::power(3)
        ]));

        $this->assertEquals(8, $updated->getAttribute('number'));

        // Success case - with max limit
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'number' => Operator::power(4, 50)
        ]));
        $this->assertEquals(50, $updated->getAttribute('number')); // Should cap at 50

        $database->deleteCollection($collectionId);
    }

    public function testOperatorStringConcatComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_concat_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'text', Database::VAR_STRING, 255, false);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'text' => 'Hello'
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'text' => Operator::concat(' World')
        ]));

        $this->assertEquals('Hello World', $updated->getAttribute('text'));

        // Edge case: null value
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'text' => null
        ]));
        $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
            'text' => Operator::concat('Test')
        ]));
        $this->assertEquals('Test', $updated->getAttribute('text'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorReplaceComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_replace_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'text', Database::VAR_STRING, 255, false);

        // Success case - single replacement
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'text' => 'Hello World'
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'text' => Operator::replace('World', 'Universe')
        ]));

        $this->assertEquals('Hello Universe', $updated->getAttribute('text'));

        // Success case - multiple occurrences
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'text' => 'test test test'
        ]));

        $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
            'text' => Operator::replace('test', 'demo')
        ]));

        $this->assertEquals('demo demo demo', $updated->getAttribute('text'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayAppendComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_append_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'tags', Database::VAR_STRING, 50, false, null, true, true);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'tags' => ['initial']
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'tags' => Operator::arrayAppend(['new', 'items'])
        ]));

        $this->assertEquals(['initial', 'new', 'items'], $updated->getAttribute('tags'));

        // Edge case: empty array
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'tags' => []
        ]));
        $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
            'tags' => Operator::arrayAppend(['first'])
        ]));
        $this->assertEquals(['first'], $updated->getAttribute('tags'));

        // Edge case: null array
        $doc3 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'tags' => null
        ]));
        $updated = $database->updateDocument($collectionId, $doc3->getId(), new Document([
            'tags' => Operator::arrayAppend(['test'])
        ]));
        $this->assertEquals(['test'], $updated->getAttribute('tags'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayPrependComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_prepend_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['existing']
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayPrepend(['first', 'second'])
        ]));

        $this->assertEquals(['first', 'second', 'existing'], $updated->getAttribute('items'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayInsertComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_insert_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'numbers', Database::VAR_INTEGER, 0, false, null, true, true);

        // Success case - middle insertion
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'numbers' => [1, 2, 4]
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => Operator::arrayInsert(2, 3)
        ]));

        $this->assertEquals([1, 2, 3, 4], $updated->getAttribute('numbers'));

        // Success case - beginning insertion
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => Operator::arrayInsert(0, 0)
        ]));

        $this->assertEquals([0, 1, 2, 3, 4], $updated->getAttribute('numbers'));

        // Success case - end insertion
        $numbers = $updated->getAttribute('numbers');
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => Operator::arrayInsert(count($numbers), 5)
        ]));

        $this->assertEquals([0, 1, 2, 3, 4, 5], $updated->getAttribute('numbers'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayRemoveComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_remove_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        // Success case - single occurrence
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['a', 'b', 'c']
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayRemove('b')
        ]));

        $this->assertEquals(['a', 'c'], $updated->getAttribute('items'));

        // Success case - multiple occurrences
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['x', 'y', 'x', 'z', 'x']
        ]));

        $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
            'items' => Operator::arrayRemove('x')
        ]));

        $this->assertEquals(['y', 'z'], $updated->getAttribute('items'));

        // Success case - non-existent value
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayRemove('nonexistent')
        ]));

        $this->assertEquals(['a', 'c'], $updated->getAttribute('items')); // Should remain unchanged

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayUniqueComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_unique_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        // Success case - with duplicates
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['a', 'b', 'a', 'c', 'b', 'a']
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayUnique()
        ]));

        $result = $updated->getAttribute('items');
        sort($result); // Sort for consistent comparison
        $this->assertEquals(['a', 'b', 'c'], $result);

        // Success case - no duplicates
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['x', 'y', 'z']
        ]));

        $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
            'items' => Operator::arrayUnique()
        ]));

        $this->assertEquals(['x', 'y', 'z'], $updated->getAttribute('items'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayIntersectComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_intersect_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['a', 'b', 'c', 'd']
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayIntersect(['b', 'c', 'e'])
        ]));

        $result = $updated->getAttribute('items');
        sort($result);
        $this->assertEquals(['b', 'c'], $result);

        // Success case - no intersection
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayIntersect(['x', 'y', 'z'])
        ]));

        $this->assertEquals([], $updated->getAttribute('items'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayDiffComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_diff_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['a', 'b', 'c', 'd']
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayDiff(['b', 'd'])
        ]));

        $result = $updated->getAttribute('items');
        sort($result);
        $this->assertEquals(['a', 'c'], $result);

        // Success case - empty diff array
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayDiff([])
        ]));

        $result = $updated->getAttribute('items');
        sort($result);
        $this->assertEquals(['a', 'c'], $result); // Should remain unchanged

        $database->deleteCollection($collectionId);
    }

    public function testOperatorArrayFilterComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_filter_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'numbers', Database::VAR_INTEGER, 0, false, null, true, true);
        $database->createAttribute($collectionId, 'mixed', Database::VAR_STRING, 50, false, null, true, true);

        // Success case - equals condition
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'numbers' => [1, 2, 3, 2, 4],
            'mixed' => ['a', 'b', null, 'c', null]
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => Operator::arrayFilter('equals', 2)
        ]));

        $this->assertEquals([2, 2], $updated->getAttribute('numbers'));

        // Success case - notNull condition
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'mixed' => Operator::arrayFilter('notNull')
        ]));

        $this->assertEquals(['a', 'b', 'c'], $updated->getAttribute('mixed'));

        // Success case - greaterThan condition (reset array first)
        $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => [1, 2, 3, 2, 4]
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => Operator::arrayFilter('greaterThan', 2)
        ]));

        $this->assertEquals([3, 4], $updated->getAttribute('numbers'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorToggleComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_toggle_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'active', Database::VAR_BOOLEAN, 0, false);

        // Success case - true to false
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'active' => true
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'active' => Operator::toggle()
        ]));

        $this->assertEquals(false, $updated->getAttribute('active'));

        // Success case - false to true
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'active' => Operator::toggle()
        ]));

        $this->assertEquals(true, $updated->getAttribute('active'));

        // Success case - null to true
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'active' => null
        ]));

        $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
            'active' => Operator::toggle()
        ]));

        $this->assertEquals(true, $updated->getAttribute('active'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorDateAddDaysComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_date_add_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'date', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']);

        // Success case - positive days
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'date' => '2023-01-01 00:00:00'
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'date' => Operator::dateAddDays(5)
        ]));

        $this->assertEquals('2023-01-06T00:00:00.000+00:00', $updated->getAttribute('date'));

        // Success case - negative days (subtracting)
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'date' => Operator::dateAddDays(-3)
        ]));

        $this->assertEquals('2023-01-03T00:00:00.000+00:00', $updated->getAttribute('date'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorDateSubDaysComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_date_sub_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'date', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'date' => '2023-01-10 00:00:00'
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'date' => Operator::dateSubDays(3)
        ]));

        $this->assertEquals('2023-01-07T00:00:00.000+00:00', $updated->getAttribute('date'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorDateSetNowComprehensive(): void
    {
        $database = static::getDatabase();

        $collectionId = 'operator_date_now_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'timestamp', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']);

        // Success case
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'timestamp' => '2020-01-01 00:00:00'
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'timestamp' => Operator::dateSetNow()
        ]));

        $result = $updated->getAttribute('timestamp');
        $this->assertNotEmpty($result);

        // Verify it's a recent timestamp (within last minute)
        $now = new \DateTime();
        $resultDate = new \DateTime($result);
        $diff = $now->getTimestamp() - $resultDate->getTimestamp();
        $this->assertLessThan(60, $diff); // Should be within 60 seconds

        $database->deleteCollection($collectionId);
    }


    public function testMixedOperators(): void
    {
        $database = static::getDatabase();

        $collectionId = 'mixed_operators_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'count', Database::VAR_INTEGER, 0, false);
        $database->createAttribute($collectionId, 'score', Database::VAR_FLOAT, 0, false);
        $database->createAttribute($collectionId, 'tags', Database::VAR_STRING, 50, false, null, true, true);
        $database->createAttribute($collectionId, 'name', Database::VAR_STRING, 255, false);
        $database->createAttribute($collectionId, 'active', Database::VAR_BOOLEAN, 0, false);

        // Test multiple operators in one update
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'count' => 5,
            'score' => 10.0,
            'tags' => ['initial'],
            'name' => 'Test',
            'active' => false
        ]));

        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'count' => Operator::increment(3),
            'score' => Operator::multiply(1.5),
            'tags' => Operator::arrayAppend(['new', 'item']),
            'name' => Operator::concat(' Document'),
            'active' => Operator::toggle()
        ]));

        $this->assertEquals(8, $updated->getAttribute('count'));
        $this->assertEquals(15.0, $updated->getAttribute('score'));
        $this->assertEquals(['initial', 'new', 'item'], $updated->getAttribute('tags'));
        $this->assertEquals('Test Document', $updated->getAttribute('name'));
        $this->assertEquals(true, $updated->getAttribute('active'));

        $database->deleteCollection($collectionId);
    }

    public function testOperatorsBatch(): void
    {
        $database = static::getDatabase();

        $collectionId = 'batch_operators_test';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'count', Database::VAR_INTEGER, 0, false);
        $database->createAttribute($collectionId, 'category', Database::VAR_STRING, 50, false);

        // Create multiple documents
        $docs = [];
        for ($i = 1; $i <= 3; $i++) {
            $docs[] = $database->createDocument($collectionId, new Document([
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                'count' => $i * 5,
                'category' => 'test'
            ]));
        }

        // Test updateDocuments with operators
        $updateCount = $database->updateDocuments($collectionId, new Document([
            'count' => Operator::increment(10)
        ]), [
            Query::equal('category', ['test'])
        ]);

        $this->assertEquals(3, $updateCount);

        // Fetch the updated documents to verify the operator worked
        $updated = $database->find($collectionId, [
            Query::equal('category', ['test']),
            Query::orderAsc('count')
        ]);
        $this->assertCount(3, $updated);
        $this->assertEquals(15, $updated[0]->getAttribute('count')); // 5 + 10
        $this->assertEquals(20, $updated[1]->getAttribute('count')); // 10 + 10
        $this->assertEquals(25, $updated[2]->getAttribute('count')); // 15 + 10

        $database->deleteCollection($collectionId);
    }

    /**
     * Test ARRAY_INSERT at beginning of array
     *
     * This test verifies that inserting at index 0 actually adds the element
     */
    public function testArrayInsertAtBeginning(): void
    {
        $database = static::getDatabase();

        $collectionId = 'test_array_insert_beginning';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['second', 'third', 'fourth']
        ]));

        $this->assertEquals(['second', 'third', 'fourth'], $doc->getAttribute('items'));

        // Attempt to insert at index 0
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayInsert(0, 'first')
        ]));

        // Refetch to get the actual database value
        $refetched = $database->getDocument($collectionId, $doc->getId());

        // Should insert 'first' at index 0, shifting existing elements
        $this->assertEquals(
            ['first', 'second', 'third', 'fourth'],
            $refetched->getAttribute('items'),
            'ARRAY_INSERT should insert element at index 0'
        );

        $database->deleteCollection($collectionId);
    }

    /**
     * Test ARRAY_INSERT at middle of array
     *
     * This test verifies that inserting at index 2 in a 5-element array works
     */
    public function testArrayInsertAtMiddle(): void
    {
        $database = static::getDatabase();

        $collectionId = 'test_array_insert_middle';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_INTEGER, 0, false, null, true, true);

        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => [1, 2, 4, 5, 6]
        ]));

        $this->assertEquals([1, 2, 4, 5, 6], $doc->getAttribute('items'));

        // Attempt to insert at index 2 (middle position)
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayInsert(2, 3)
        ]));

        // Refetch to get the actual database value
        $refetched = $database->getDocument($collectionId, $doc->getId());

        // Should insert 3 at index 2, shifting remaining elements
        $this->assertEquals(
            [1, 2, 3, 4, 5, 6],
            $refetched->getAttribute('items'),
            'ARRAY_INSERT should insert element at index 2'
        );

        $database->deleteCollection($collectionId);
    }

    /**
     * Test ARRAY_INSERT at end of array
     *
     * This test verifies that inserting at the last index (end of array) works
     */
    public function testArrayInsertAtEnd(): void
    {
        $database = static::getDatabase();

        $collectionId = 'test_array_insert_end';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'items', Database::VAR_STRING, 50, false, null, true, true);

        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'items' => ['apple', 'banana', 'cherry']
        ]));

        $this->assertEquals(['apple', 'banana', 'cherry'], $doc->getAttribute('items'));

        // Attempt to insert at end (index = length)
        $items = $doc->getAttribute('items');
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'items' => Operator::arrayInsert(count($items), 'date')
        ]));

        // Refetch to get the actual database value
        $refetched = $database->getDocument($collectionId, $doc->getId());

        // Should insert 'date' at end of array
        $this->assertEquals(
            ['apple', 'banana', 'cherry', 'date'],
            $refetched->getAttribute('items'),
            'ARRAY_INSERT should insert element at end of array'
        );

        $database->deleteCollection($collectionId);
    }

    /**
     * Test ARRAY_INSERT with multiple operations
     *
     * This test verifies that multiple sequential insert operations work correctly
     */
    public function testArrayInsertMultipleOperations(): void
    {
        $database = static::getDatabase();

        $collectionId = 'test_array_insert_multiple';
        $database->createCollection($collectionId);
        $database->createAttribute($collectionId, 'numbers', Database::VAR_INTEGER, 0, false, null, true, true);

        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'numbers' => [1, 3, 5]
        ]));

        $this->assertEquals([1, 3, 5], $doc->getAttribute('numbers'));

        // First insert: add 2 at index 1
        $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => Operator::arrayInsert(1, 2)
        ]));

        // Refetch to get the actual database value
        $refetched = $database->getDocument($collectionId, $doc->getId());

        // Should insert 2 at index 1
        $this->assertEquals(
            [1, 2, 3, 5],
            $refetched->getAttribute('numbers'),
            'First ARRAY_INSERT should work'
        );

        // Second insert: add 4 at index 3
        $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => Operator::arrayInsert(3, 4)
        ]));

        // Refetch to get the actual database value
        $refetched = $database->getDocument($collectionId, $doc->getId());

        // Should insert 4 at index 3
        $this->assertEquals(
            [1, 2, 3, 4, 5],
            $refetched->getAttribute('numbers'),
            'Second ARRAY_INSERT should work'
        );

        // Third insert: add 0 at beginning
        $database->updateDocument($collectionId, $doc->getId(), new Document([
            'numbers' => Operator::arrayInsert(0, 0)
        ]));

        // Refetch to get the actual database value
        $refetched = $database->getDocument($collectionId, $doc->getId());

        // Should insert 0 at index 0
        $this->assertEquals(
            [0, 1, 2, 3, 4, 5],
            $refetched->getAttribute('numbers'),
            'Third ARRAY_INSERT should work'
        );

        $database->deleteCollection($collectionId);
    }

    /**
     * Bug #6: Post-Operator Validation Missing
     * Test that INCREMENT operator can exceed maximum value constraint
     *
     * The database validates document structure BEFORE operators are applied (line 4912 in Database.php),
     * but not AFTER. This test creates a document with an integer field that has a max constraint,
     * then uses INCREMENT to push the value beyond that maximum. The operation should fail with a
     * validation error, but currently succeeds because post-operator validation is missing.
     */
    public function testOperatorIncrementExceedsMaxValue(): void
    {
        $database = static::getDatabase();

        $collectionId = 'test_increment_max_violation';
        $database->createCollection($collectionId);

        // Create an integer attribute with a maximum value of 100
        // Using size=4 (signed int) with max constraint through Range validator
        $database->createAttribute($collectionId, 'score', Database::VAR_INTEGER, 4, false, 0, false, false);

        // Get the collection to verify attribute was created
        $collection = $database->getCollection($collectionId);
        $attributes = $collection->getAttribute('attributes', []);
        $scoreAttr = null;
        foreach ($attributes as $attr) {
            if ($attr['$id'] === 'score') {
                $scoreAttr = $attr;
                break;
            }
        }

        // Create a document with score at 95 (within valid range)
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'score' => 95
        ]));

        $this->assertEquals(95, $doc->getAttribute('score'));

        // Test case 1: Small increment that stays within INT_MAX should work
        $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
            'score' => Operator::increment(5)
        ]));
        // Refetch to get the actual computed value
        $updated = $database->getDocument($collectionId, $doc->getId());
        $this->assertEquals(100, $updated->getAttribute('score'));

        // Test case 2: Increment that would exceed Database::INT_MAX (2147483647)
        // This is the bug - the operator will create a value > INT_MAX which should be rejected
        // but post-operator validation is missing
        $doc2 = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'score' => Database::INT_MAX - 10 // Start near the maximum
        ]));

        $this->assertEquals(Database::INT_MAX - 10, $doc2->getAttribute('score'));

        // BUG EXPOSED: This increment will push the value beyond Database::INT_MAX
        // It should throw a StructureException for exceeding the integer range,
        // but currently succeeds because validation happens before operator application
        try {
            $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
                'score' => Operator::increment(20) // Will result in INT_MAX + 10
            ]));

            // Refetch to get the actual computed value from the database
            $refetched = $database->getDocument($collectionId, $doc2->getId());
            $finalScore = $refetched->getAttribute('score');

            // Document the bug: The value should not exceed INT_MAX
            $this->assertLessThanOrEqual(
                Database::INT_MAX,
                $finalScore,
                "BUG EXPOSED: INCREMENT pushed score to {$finalScore}, exceeding INT_MAX (" . Database::INT_MAX . "). Post-operator validation is missing!"
            );
        } catch (StructureException $e) {
            // This is the CORRECT behavior - validation should catch the constraint violation
            $this->assertStringContainsString('invalid type', $e->getMessage());
        }

        $database->deleteCollection($collectionId);
    }

    /**
     * Bug #6: Post-Operator Validation Missing
     * Test that CONCAT operator can exceed maximum string length
     *
     * This test creates a string attribute with a maximum length constraint,
     * then uses CONCAT to make the string longer than allowed. The operation should fail,
     * but currently succeeds because validation only happens before operators are applied.
     */
    public function testOperatorConcatExceedsMaxLength(): void
    {
        $database = static::getDatabase();

        $collectionId = 'test_concat_length_violation';
        $database->createCollection($collectionId);

        // Create a string attribute with max length of 20 characters
        $database->createAttribute($collectionId, 'title', Database::VAR_STRING, 20, false, '');

        // Create a document with a 15-character title (within limit)
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Hello World'  // 11 characters
        ]));

        $this->assertEquals('Hello World', $doc->getAttribute('title'));
        $this->assertEquals(11, strlen($doc->getAttribute('title')));

        // BUG EXPOSED: Concat a 15-character string to make total length 26 (exceeds max of 20)
        // This should throw a StructureException for exceeding max length,
        // but currently succeeds because validation only checks the input, not the result
        try {
            $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
                'title' => Operator::concat(' - Extended Title')  // Adding 18 chars = 29 total
            ]));

            // Refetch to get the actual computed value from the database
            $refetched = $database->getDocument($collectionId, $doc->getId());
            $finalTitle = $refetched->getAttribute('title');
            $finalLength = strlen($finalTitle);

            // Document the bug: The resulting string should not exceed 20 characters
            $this->assertLessThanOrEqual(
                20,
                $finalLength,
                "BUG EXPOSED: CONCAT created string of length {$finalLength} ('{$finalTitle}'), exceeding max length of 20. Post-operator validation is missing!"
            );
        } catch (StructureException $e) {
            // This is the CORRECT behavior - validation should catch the length violation
            $this->assertStringContainsString('invalid type', $e->getMessage());
        }

        $database->deleteCollection($collectionId);
    }

    /**
     * Bug #6: Post-Operator Validation Missing
     * Test that MULTIPLY operator can create values outside allowed range
     *
     * This test shows that multiplying a float can exceed the maximum allowed value
     * for the field type, bypassing schema constraints.
     */
    public function testOperatorMultiplyViolatesRange(): void
    {
        $database = static::getDatabase();

        $collectionId = 'test_multiply_range_violation';
        $database->createCollection($collectionId);

        // Create a signed integer attribute (max value = Database::INT_MAX = 2147483647)
        $database->createAttribute($collectionId, 'quantity', Database::VAR_INTEGER, 4, false, 1, false, false);

        // Create a document with quantity that when multiplied will exceed INT_MAX
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'quantity' => 1000000000  // 1 billion
        ]));

        $this->assertEquals(1000000000, $doc->getAttribute('quantity'));

        // BUG EXPOSED: Multiply by 10 to get 10 billion, which exceeds INT_MAX (2.147 billion)
        // This should throw a StructureException for exceeding the integer range,
        // but currently may succeed or cause overflow because validation is missing
        try {
            $updated = $database->updateDocument($collectionId, $doc->getId(), new Document([
                'quantity' => Operator::multiply(10)  // 1,000,000,000 * 10 = 10,000,000,000 > INT_MAX
            ]));

            // Refetch to get the actual computed value from the database
            $refetched = $database->getDocument($collectionId, $doc->getId());
            $finalQuantity = $refetched->getAttribute('quantity');

            // Document the bug: The value should not exceed INT_MAX
            $this->assertLessThanOrEqual(
                Database::INT_MAX,
                $finalQuantity,
                "BUG EXPOSED: MULTIPLY created value {$finalQuantity}, exceeding INT_MAX (" . Database::INT_MAX . "). Post-operator validation is missing!"
            );

            // Also verify the value didn't overflow into negative (integer overflow behavior)
            $this->assertGreaterThan(
                0,
                $finalQuantity,
                "BUG EXPOSED: MULTIPLY caused integer overflow to {$finalQuantity}. Post-operator validation should prevent this!"
            );
        } catch (StructureException $e) {
            // This is the CORRECT behavior - validation should catch the range violation
            $this->assertStringContainsString('invalid type', $e->getMessage());
        }

        $database->deleteCollection($collectionId);
    }

    /**
     * Bug #6: Post-Operator Validation Missing
     * Test that ARRAY_APPEND can add items that violate array item constraints
     *
     * This test creates an integer array attribute and uses ARRAY_APPEND to add a string,
     * which should fail type validation but currently succeeds in some cases.
     */
    public function testOperatorArrayAppendViolatesItemConstraints(): void
    {
        $database = static::getDatabase();

        $collectionId = 'test_array_item_type_violation';
        $database->createCollection($collectionId);

        // Create an array attribute for integers with max value constraint
        // Each item should be an integer within the valid range
        $database->createAttribute($collectionId, 'numbers', Database::VAR_INTEGER, 4, false, null, true, true);

        // Create a document with valid integer array
        $doc = $database->createDocument($collectionId, new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'numbers' => [10, 20, 30]
        ]));

        $this->assertEquals([10, 20, 30], $doc->getAttribute('numbers'));

        // Test case 1: Append integers that exceed INT_MAX
        // BUG EXPOSED: These values exceed the constraint but validation is not applied post-operator
        try {
            // Create a fresh document for this test
            $doc2 = $database->createDocument($collectionId, new Document([
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                'numbers' => [100, 200]
            ]));

            // Try to append values that would exceed INT_MAX
            $hugeValue = Database::INT_MAX + 1000;  // Exceeds integer maximum

            $updated = $database->updateDocument($collectionId, $doc2->getId(), new Document([
                'numbers' => Operator::arrayAppend([$hugeValue])
            ]));

            // Refetch to get the actual computed value from the database
            $refetched = $database->getDocument($collectionId, $doc2->getId());
            $finalNumbers = $refetched->getAttribute('numbers');
            $lastNumber = end($finalNumbers);

            // Document the bug: Array items should not exceed INT_MAX
            $this->assertLessThanOrEqual(
                Database::INT_MAX,
                $lastNumber,
                "BUG EXPOSED: ARRAY_APPEND added value {$lastNumber} exceeding INT_MAX (" . Database::INT_MAX . "). Post-operator validation is missing!"
            );
        } catch (StructureException $e) {
            // This is the CORRECT behavior - validation should catch the constraint violation
            $this->assertStringContainsString('invalid type', $e->getMessage());
        } catch (TypeException $e) {
            // Also acceptable - type validation catches the issue
            $this->assertStringContainsString('Invalid', $e->getMessage());
        }

        // Test case 2: Append multiple items where at least one violates constraints
        try {
            $doc3 = $database->createDocument($collectionId, new Document([
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                'numbers' => [1, 2, 3]
            ]));

            // Append a mix of valid and invalid values
            // The last value exceeds INT_MAX
            $mixedValues = [40, 50, Database::INT_MAX + 100];

            $updated = $database->updateDocument($collectionId, $doc3->getId(), new Document([
                'numbers' => Operator::arrayAppend($mixedValues)
            ]));

            // Refetch to get the actual computed value from the database
            $refetched = $database->getDocument($collectionId, $doc3->getId());
            $finalNumbers = $refetched->getAttribute('numbers');

            // Document the bug: ALL array items should be validated
            foreach ($finalNumbers as $num) {
                $this->assertLessThanOrEqual(
                    Database::INT_MAX,
                    $num,
                    "BUG EXPOSED: ARRAY_APPEND added invalid value {$num} exceeding INT_MAX (" . Database::INT_MAX . "). Post-operator validation is missing!"
                );
            }
        } catch (StructureException $e) {
            // This is the CORRECT behavior
            $this->assertStringContainsString('invalid type', $e->getMessage());
        } catch (TypeException $e) {
            // Also acceptable
            $this->assertStringContainsString('Invalid', $e->getMessage());
        }

        $database->deleteCollection($collectionId);
    }
}
