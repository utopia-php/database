<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Throwable;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

trait SchemalessTests
{
    public function testSchemalessDocumentOperation(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid('schemaless');
        $database->createCollection($colName);
        $database->createAttribute($colName, 'key', Database::VAR_STRING, 50, true);
        $database->createAttribute($colName, 'value', Database::VAR_STRING, 50, false, 'value');

        $permissions = [Permission::read(Role::any()), Permission::write(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];

        // Valid documents without any predefined attributes
        $docs = [
            new Document(['$id' => 'doc1', '$permissions' => $permissions, 'freeA' => 'doc1']),
            new Document(['$id' => 'doc2', '$permissions' => $permissions, 'freeB' => 'test']),
            new Document(['$id' => 'doc3', '$permissions' => $permissions]),
        ];
        $this->assertEquals(3, $database->createDocuments($colName, $docs));

        // Any extra attributes should be allowed (fully schemaless)
        $docs = [
            new Document(['$id' => 'doc11', 'title' => 'doc1', '$permissions' => $permissions]),
            new Document(['$id' => 'doc21', 'moviename' => 'doc2', 'moviedescription' => 'test', '$permissions' => $permissions]),
            new Document(['$id' => 'doc31', '$permissions' => $permissions]),
        ];

        $createdDocs = $database->createDocuments($colName, $docs);
        $this->assertEquals(3, $createdDocs);

        // Create a single document with extra attribute as well
        $single = $database->createDocument($colName, new Document(['$id' => 'docS', 'extra' => 'yes', '$permissions' => $permissions]));
        $this->assertEquals('docS', $single->getId());
        $this->assertEquals('yes', $single->getAttribute('extra'));

        $found = $database->find($colName);
        $this->assertCount(7, $found);
        $doc11 = $database->getDocument($colName, 'doc11');
        $this->assertEquals('doc1', $doc11->getAttribute('title'));

        $doc21 = $database->getDocument($colName, 'doc21');
        $this->assertEquals('doc2', $doc21->getAttribute('moviename'));
        $this->assertEquals('test', $doc21->getAttribute('moviedescription'));

        $updated = $database->updateDocument($colName, 'doc31', new Document(['moviename' => 'updated']));
        $this->assertEquals('updated', $updated->getAttribute('moviename'));

        $this->assertTrue($database->deleteDocument($colName, 'doc21'));
        $deleted = $database->getDocument($colName, 'doc21');
        $this->assertTrue($deleted->isEmpty());
        $remaining = $database->find($colName);
        $this->assertCount(6, $remaining);

        // Bulk update: set a new extra attribute on all remaining docs
        $modified = $database->updateDocuments($colName, new Document(['bulkExtra' => 'yes']));
        $this->assertEquals(6, $modified);
        $all = $database->find($colName);
        foreach ($all as $doc) {
            $this->assertEquals('yes', $doc->getAttribute('bulkExtra'));
        }

        // Upsert: create new and update existing with extra attributes preserved
        $upserts = [
            new Document(['$id' => 'docU1', 'extraU' => 1, '$permissions' => $permissions]),
            new Document(['$id' => 'doc1', 'extraU' => 2, '$permissions' => $permissions]),
        ];
        $countUpserts = $database->upsertDocuments($colName, $upserts);
        $this->assertEquals(2, $countUpserts);
        $docU1 = $database->getDocument($colName, 'docU1');
        $this->assertEquals(1, $docU1->getAttribute('extraU'));
        $doc1AfterUpsert = $database->getDocument($colName, 'doc1');
        $this->assertEquals(2, $doc1AfterUpsert->getAttribute('extraU'));

        // Increase/Decrease numeric attribute: add numeric attribute and mutate it
        $docS = $database->getDocument($colName, 'docS');
        $this->assertEquals(0, $docS->getAttribute('counter'));
        $docS = $database->increaseDocumentAttribute($colName, 'docS', 'counter', 5);
        $this->assertEquals(5, $docS->getAttribute('counter'));
        $docS = $database->decreaseDocumentAttribute($colName, 'docS', 'counter', 3);
        $this->assertEquals(2, $docS->getAttribute('counter'));

        $deletedByCounter = $database->deleteDocuments($colName, [Query::equal('counter', [2])]);
        $this->assertEquals(1, $deletedByCounter);

        $deletedCount = $database->deleteDocuments($colName, [Query::startsWith('$id', 'doc')]);
        $this->assertEquals(6, $deletedCount);
        $postDelete = $database->find($colName);
        $this->assertCount(0, $postDelete);

        $database->deleteCollection($colName);
    }

    public function testSchemalessDocumentInvalidInteralAttributeValidation(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // test to ensure internal attributes are checked during creating schemaless document
        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid('schemaless');
        $database->createCollection($colName);
        try {
            $docs = [
                new Document(['$id' => true, 'freeA' => 'doc1']),
                new Document(['$id' => true, 'freeB' => 'test']),
                new Document(['$id' => true]),
            ];
            $database->createDocuments($colName, $docs);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        try {
            $docs = [
                new Document(['$createdAt' => true, 'freeA' => 'doc1']),
                new Document(['$updatedAt' => true, 'freeB' => 'test']),
                new Document(['$permissions' => 12]),
            ];
            $database->createDocuments($colName, $docs);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        $database->deleteCollection($colName);

    }

    public function testSchemalessSelectionOnUnknownAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid('schemaless');
        $database->createCollection($colName);
        $permissions = [Permission::read(Role::any()), Permission::write(Role::any()), Permission::update(Role::any())];
        $docs = [
            new Document(['$id' => 'doc1', '$permissions' => $permissions, 'freeA' => 'doc1']),
            new Document(['$id' => 'doc2', '$permissions' => $permissions, 'freeB' => 'test']),
            new Document(['$id' => 'doc3', '$permissions' => $permissions]),
        ];
        $this->assertEquals(3, $database->createDocuments($colName, $docs));

        $docA = $database->getDocument($colName, 'doc1', [Query::select(['freeA'])]);
        $this->assertEquals('doc1', $docA->getAttribute('freeA'));

        $docC = $database->getDocument($colName, 'doc1', [Query::select(['freeC'])]);
        $this->assertNull($docC->getAttribute('freeC'));

        $docs = $database->find($colName, [Query::equal('$id', ['doc1','doc2']),Query::select(['freeC'])]);
        foreach ($docs as $doc) {
            $this->assertNull($doc->getAttribute('freeC'));
            // since not selected
            $this->assertArrayNotHasKey('freeA', $doc->getAttributes());
            $this->assertArrayNotHasKey('freeB', $doc->getAttributes());
        }

        $docA = $database->find($colName, [
            Query::equal('$id', ['doc1']),
            Query::select(['freeA'])
        ]);
        $this->assertEquals('doc1', $docA[0]->getAttribute('freeA'));

        $docC = $database->find($colName, [
            Query::equal('$id', ['doc1']),
            Query::select(['freeC'])
        ]);
        $this->assertArrayNotHasKey('freeC', $docC[0]->getAttributes());
    }

    public function testSchemalessIncrement(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid("schemaless_increment");
        $database->createCollection($colName);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        $docs = [
            new Document(['$id' => 'doc1', '$permissions' => $permissions, 'counter' => 10, 'score' => 5.5]),
            new Document(['$id' => 'doc2', '$permissions' => $permissions, 'counter' => 20, 'points' => 100]),
            new Document(['$id' => 'doc3', '$permissions' => $permissions, 'value' => 0]),
        ];
        $this->assertEquals(3, $database->createDocuments($colName, $docs));

        $doc1 = $database->increaseDocumentAttribute($colName, 'doc1', 'counter', 5);
        $this->assertEquals(15, $doc1->getAttribute('counter'));
        $this->assertEquals(5.5, $doc1->getAttribute('score'));

        $doc1 = $database->increaseDocumentAttribute($colName, 'doc1', 'score', 2.3);
        $this->assertEquals(7.8, $doc1->getAttribute('score'));

        $doc2 = $database->increaseDocumentAttribute($colName, 'doc2', 'points', 50);
        $this->assertEquals(150, $doc2->getAttribute('points'));

        $doc3 = $database->increaseDocumentAttribute($colName, 'doc3', 'newCounter', 1);
        $this->assertEquals(1, $doc3->getAttribute('newCounter'));
        $this->assertEquals(0, $doc3->getAttribute('value'));

        try {
            $database->increaseDocumentAttribute($colName, 'doc1', 'counter', 10, 20);
            $this->assertEquals(20, $database->getDocument($colName, 'doc1')->getAttribute('counter'));
        } catch (\Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        }

        $allDocs = $database->find($colName);
        $this->assertCount(3, $allDocs);

        $database->deleteCollection($colName);
    }

    public function testSchemalessDecrement(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid("schemaless_decrement");
        $database->createCollection($colName);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        $docs = [
            new Document(['$id' => 'doc1', '$permissions' => $permissions, 'counter' => 100, 'balance' => 250.75]),
            new Document(['$id' => 'doc2', '$permissions' => $permissions, 'score' => 50, 'extraData' => 'preserved']),
        ];
        $this->assertEquals(2, $database->createDocuments($colName, $docs));

        $doc1 = $database->decreaseDocumentAttribute($colName, 'doc1', 'counter', 25);
        $this->assertEquals(75, $doc1->getAttribute('counter'));
        $this->assertEquals(250.75, $doc1->getAttribute('balance'));

        $doc1 = $database->decreaseDocumentAttribute($colName, 'doc1', 'balance', 50.25);
        $this->assertEquals(200.5, $doc1->getAttribute('balance'));

        $doc2 = $database->decreaseDocumentAttribute($colName, 'doc2', 'score', 15);
        $this->assertEquals(35, $doc2->getAttribute('score'));
        $this->assertEquals('preserved', $doc2->getAttribute('extraData'));

        try {
            $database->decreaseDocumentAttribute($colName, 'doc2', 'score', 40, 0);
            $this->fail('Expected LimitException not thrown');
        } catch (\Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        }

        $doc2 = $database->decreaseDocumentAttribute($colName, 'doc2', 'score', 50);
        $this->assertEquals(-15, $doc2->getAttribute('score'));

        $retrievedDoc1 = $database->getDocument($colName, 'doc1');
        $this->assertEquals(75, $retrievedDoc1->getAttribute('counter'));
        $this->assertEquals(200.5, $retrievedDoc1->getAttribute('balance'));

        $database->deleteCollection($colName);
    }

    public function testSchemalessUpdateDocumentWithQuery(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid("schemaless_update");
        $database->createCollection($colName);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        $docs = [
            new Document(['$id' => 'doc1', '$permissions' => $permissions, 'type' => 'user', 'status' => 'active', 'score' => 100]),
            new Document(['$id' => 'doc2', '$permissions' => $permissions, 'type' => 'admin', 'status' => 'active', 'level' => 5]),
            new Document(['$id' => 'doc3', '$permissions' => $permissions, 'type' => 'user', 'status' => 'inactive', 'score' => 50]),
            new Document(['$id' => 'doc4', '$permissions' => $permissions, 'type' => 'user', 'status' => 'pending', 'newField' => 'test']),
        ];
        $this->assertEquals(4, $database->createDocuments($colName, $docs));

        $updatedDoc = $database->updateDocument($colName, 'doc1', new Document([
            'status' => 'updated',
            'lastModified' => '2023-01-01',
            'newAttribute' => 'added'
        ]));

        $this->assertEquals('updated', $updatedDoc->getAttribute('status'));
        $this->assertEquals('2023-01-01', $updatedDoc->getAttribute('lastModified'));
        $this->assertEquals('added', $updatedDoc->getAttribute('newAttribute'));
        $this->assertArrayNotHasKey('score', $updatedDoc);
        $this->assertArrayNotHasKey('type', $updatedDoc);

        $retrievedDoc = $database->getDocument($colName, 'doc1');
        $this->assertEquals('updated', $retrievedDoc->getAttribute('status'));
        $this->assertEquals('added', $retrievedDoc->getAttribute('newAttribute'));

        $updatedDoc2 = $database->updateDocument($colName, 'doc2', new Document([
            'customField1' => 'value1',
            'customField2' => 42,
            'customField3' => ['array', 'of', 'values']
        ]));

        $this->assertEquals('value1', $updatedDoc2->getAttribute('customField1'));
        $this->assertEquals(42, $updatedDoc2->getAttribute('customField2'));
        $this->assertEquals(['array', 'of', 'values'], $updatedDoc2->getAttribute('customField3'));
        $this->assertArrayNotHasKey('type', $updatedDoc2);

        $database->deleteCollection($colName);
    }

    public function testSchemalessDeleteDocumentWithQuery(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid("schemaless_delete");
        $database->createCollection($colName);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        $docs = [
            new Document(['$id' => 'doc1', '$permissions' => $permissions, 'category' => 'temp', 'priority' => 1]),
            new Document(['$id' => 'doc2', '$permissions' => $permissions, 'category' => 'permanent', 'priority' => 5]),
            new Document(['$id' => 'doc3', '$permissions' => $permissions, 'category' => 'temp', 'priority' => 3]),
        ];
        $this->assertEquals(3, $database->createDocuments($colName, $docs));

        $result = $database->deleteDocument($colName, 'doc1');
        $this->assertTrue($result);

        $deletedDoc = $database->getDocument($colName, 'doc1');
        $this->assertTrue($deletedDoc->isEmpty());

        $remainingDocs = $database->find($colName);
        $this->assertCount(2, $remainingDocs);

        $tempDocs = $database->find($colName, [Query::equal('category', ['temp'])]);
        $this->assertCount(1, $tempDocs);
        $this->assertEquals('doc3', $tempDocs[0]->getId());

        $database->deleteCollection($colName);
    }

    public function testSchemalessUpdateDocumentsWithQuery(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid("schemaless_bulk_update");
        $database->createCollection($colName);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        $docs = [];
        for ($i = 1; $i <= 10; $i++) {
            $docs[] = new Document([
                '$id' => "doc{$i}",
                '$permissions' => $permissions,
                'type' => $i <= 5 ? 'typeA' : 'typeB',
                'status' => 'pending',
                'score' => $i * 10,
                'customField' => "value{$i}"
            ]);
        }
        $this->assertEquals(10, $database->createDocuments($colName, $docs));

        $updatedCount = $database->updateDocuments($colName, new Document([
            'status' => 'processed',
            'processedAt' => '2023-01-01',
            'newBulkField' => 'bulk_value'
        ]), [Query::equal('type', ['typeA'])]);

        $this->assertEquals(5, $updatedCount);

        $processedDocs = $database->find($colName, [Query::equal('status', ['processed'])]);
        $this->assertCount(5, $processedDocs);

        foreach ($processedDocs as $doc) {
            $this->assertEquals('typeA', $doc->getAttribute('type'));
            $this->assertEquals('processed', $doc->getAttribute('status'));
            $this->assertEquals('2023-01-01', $doc->getAttribute('processedAt'));
            $this->assertEquals('bulk_value', $doc->getAttribute('newBulkField'));
            $this->assertNotNull($doc->getAttribute('score'));
            $this->assertNotNull($doc->getAttribute('customField'));
        }

        $pendingDocs = $database->find($colName, [Query::equal('status', ['pending'])]);
        $this->assertCount(5, $pendingDocs);

        foreach ($pendingDocs as $doc) {
            $this->assertEquals('typeB', $doc->getAttribute('type'));
            $this->assertEquals('pending', $doc->getAttribute('status'));
            $this->assertNull($doc->getAttribute('processedAt'));
            $this->assertNull($doc->getAttribute('newBulkField'));
        }

        $highScoreCount = $database->updateDocuments($colName, new Document([
            'tier' => 'premium'
        ]), [Query::greaterThan('score', 70)]);

        $this->assertEquals(3, $highScoreCount); // docs 8, 9, 10

        $premiumDocs = $database->find($colName, [Query::equal('tier', ['premium'])]);
        $this->assertCount(3, $premiumDocs);

        $allUpdateCount = $database->updateDocuments($colName, new Document([
            'globalFlag' => true,
            'lastUpdate' => '2023-12-31'
        ]));

        $this->assertEquals(10, $allUpdateCount);

        $allDocs = $database->find($colName);
        $this->assertCount(10, $allDocs);

        foreach ($allDocs as $doc) {
            $this->assertTrue($doc->getAttribute('globalFlag'));
            $this->assertEquals('2023-12-31', $doc->getAttribute('lastUpdate'));
        }

        $database->deleteCollection($colName);
    }

    public function testSchemalessDeleteDocumentsWithQuery(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid("schemaless_bulk_delete");
        $database->createCollection($colName);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        $docs = [];
        for ($i = 1; $i <= 15; $i++) {
            $docs[] = new Document([
                '$id' => "doc{$i}",
                '$permissions' => $permissions,
                'category' => $i <= 5 ? 'temp' : ($i <= 10 ? 'archive' : 'active'),
                'priority' => $i % 3, // 0, 1, or 2
                'score' => $i * 5,
                'tags' => ["tag{$i}", 'common'],
                'metadata' => ['created' => "2023-01-{$i}"]
            ]);
        }
        $this->assertEquals(15, $database->createDocuments($colName, $docs));

        $deletedCount = $database->deleteDocuments($colName, [Query::equal('category', ['temp'])]);
        $this->assertEquals(5, $deletedCount);

        $remainingDocs = $database->find($colName);
        $this->assertCount(10, $remainingDocs);

        $tempDocs = $database->find($colName, [Query::equal('category', ['temp'])]);
        $this->assertCount(0, $tempDocs);

        $highScoreDeleted = $database->deleteDocuments($colName, [Query::greaterThan('score', 50)]);
        $this->assertEquals(5, $highScoreDeleted); // docs 11-15

        $remainingAfterScore = $database->find($colName);
        $this->assertCount(5, $remainingAfterScore); // docs 6-10 remain

        foreach ($remainingAfterScore as $doc) {
            $this->assertLessThanOrEqual(50, $doc->getAttribute('score'));
            $this->assertEquals('archive', $doc->getAttribute('category'));
        }

        $multiConditionDeleted = $database->deleteDocuments($colName, [
            Query::equal('category', ['archive']),
            Query::equal('priority', [1])
        ]);
        $this->assertEquals(2, $multiConditionDeleted); // docs 7 and 10

        $finalRemaining = $database->find($colName);
        $this->assertCount(3, $finalRemaining); // docs 6, 8, 9

        foreach ($finalRemaining as $doc) {
            $this->assertEquals('archive', $doc->getAttribute('category'));
            $this->assertNotEquals(1, $doc->getAttribute('priority'));
        }

        $allDeleted = $database->deleteDocuments($colName);
        $this->assertEquals(3, $allDeleted);

        $emptyResult = $database->find($colName);
        $this->assertCount(0, $emptyResult);

        $database->deleteCollection($colName);
    }

    public function testSchemalessOperationsWithCallback(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $colName = uniqid("schemaless_callbacks");
        $database->createCollection($colName);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        $docs = [];
        for ($i = 1; $i <= 8; $i++) {
            $docs[] = new Document([
                '$id' => "doc{$i}",
                '$permissions' => $permissions,
                'group' => $i <= 4 ? 'A' : 'B',
                'value' => $i * 10,
                'customData' => "data{$i}"
            ]);
        }
        $this->assertEquals(8, $database->createDocuments($colName, $docs));

        $updateResults = [];
        $updateCount = $database->updateDocuments(
            $colName,
            new Document(['processed' => true, 'timestamp' => '2023-01-01']),
            [Query::equal('group', ['A'])],
            onNext: function ($doc) use (&$updateResults) {
                $updateResults[] = $doc->getId();
            }
        );

        $this->assertEquals(4, $updateCount);
        $this->assertCount(4, $updateResults);
        $this->assertContains('doc1', $updateResults);
        $this->assertContains('doc2', $updateResults);
        $this->assertContains('doc3', $updateResults);
        $this->assertContains('doc4', $updateResults);

        $processedDocs = $database->find($colName, [Query::equal('processed', [true])]);
        $this->assertCount(4, $processedDocs);

        $deleteResults = [];
        $deleteCount = $database->deleteDocuments(
            $colName,
            [Query::greaterThan('value', 50)],
            onNext: function ($doc) use (&$deleteResults) {
                $deleteResults[] = [
                    'id' => $doc->getId(),
                    'value' => $doc->getAttribute('value'),
                    'customData' => $doc->getAttribute('customData')
                ];
            }
        );

        $this->assertEquals(3, $deleteCount); // docs 6, 7, 8
        $this->assertCount(3, $deleteResults);

        foreach ($deleteResults as $result) {
            $this->assertGreaterThan(50, $result['value']);
            $this->assertStringStartsWith('data', $result['customData']);
        }

        $remainingDocs = $database->find($colName);
        $this->assertCount(5, $remainingDocs); // docs 1-5

        foreach ($remainingDocs as $doc) {
            $this->assertLessThanOrEqual(50, $doc->getAttribute('value'));
        }

        $database->deleteCollection($colName);
    }

    public function testSchemalessIndexCreateListDelete(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Schemaless adapter still supports defining attributes/indexes metadata
        $col = uniqid('sl_idx');
        $database->createCollection($col);

        $database->createDocument($col, new Document([
            '$id' => 'a',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 't1',
            'rank' => 1,
        ]));
        $database->createDocument($col, new Document([
            '$id' => 'b',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 't2',
            'rank' => 2,
        ]));

        $this->assertTrue($database->createIndex($col, 'idx_title_unique', Database::INDEX_UNIQUE, ['title'], [128], [Database::ORDER_ASC]));
        $this->assertTrue($database->createIndex($col, 'idx_rank_key', Database::INDEX_KEY, ['rank'], [0], [Database::ORDER_ASC]));

        $collection = $database->getCollection($col);
        $indexes = $collection->getAttribute('indexes');
        $this->assertCount(2, $indexes);
        $ids = array_map(fn ($i) => $i['$id'], $indexes);
        $this->assertContains('idx_rank_key', $ids);
        $this->assertContains('idx_title_unique', $ids);

        $this->assertTrue($database->deleteIndex($col, 'idx_rank_key'));
        $collection = $database->getCollection($col);
        $this->assertCount(1, $collection->getAttribute('indexes'));
        $this->assertEquals('idx_title_unique', $collection->getAttribute('indexes')[0]['$id']);

        $this->assertTrue($database->deleteIndex($col, 'idx_title_unique'));
        $database->deleteCollection($col);
    }

    public function testSchemalessIndexDuplicatePrevention(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = uniqid('sl_idx_dup');
        $database->createCollection($col);

        $database->createDocument($col, new Document([
            '$id' => 'a',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'x'
        ]));

        $this->assertTrue($database->createIndex($col, 'duplicate', Database::INDEX_KEY, ['name'], [0], [Database::ORDER_ASC]));

        try {
            $database->createIndex($col, 'duplicate', Database::INDEX_KEY, ['name'], [0], [Database::ORDER_ASC]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $database->deleteCollection($col);
    }

    public function testSchemalessPermissions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = uniqid('sl_perms');
        $database->createCollection($col);

        // Create with permissive read only
        $doc = $database->createDocument($col, new Document([
            '$id' => 'd1',
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'field' => 'value'
        ]));

        $this->assertFalse($doc->isEmpty());

        // Without roles, cannot read
        Authorization::cleanRoles();
        $empty = $database->getDocument($col, 'd1');
        $this->assertTrue($empty->isEmpty());

        // With any role, can read
        Authorization::setRole(Role::any()->toString());
        $fetched = $database->getDocument($col, 'd1');
        $this->assertEquals('value', $fetched->getAttribute('field'));

        // Attempt update without update permission
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());
        try {
            $database->updateDocument($col, 'd1', new Document(['field' => 'updated']));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }

        // Grant update permission and update
        Authorization::skip(function () use ($database, $col) {
            $database->updateDocument($col, 'd1', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ]
            ]));
        });

        $updated = $database->updateDocument($col, 'd1', new Document(['field' => 'updated']));
        $this->assertEquals('updated', $updated->getAttribute('field'));

        // Creating without any roles should fail
        Authorization::cleanRoles();
        try {
            $database->createDocument($col, new Document([
                'field' => 'x'
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }

        $database->deleteCollection($col);
        Authorization::cleanRoles();
    }

    public function testSchemalessInternalAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = uniqid('sl_internal_full');
        $database->createCollection($col);

        Authorization::setRole(Role::any()->toString());

        $doc = $database->createDocument($col, new Document([
            '$id' => 'i1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'alpha',
        ]));

        $this->assertEquals('i1', $doc->getId());
        $this->assertEquals($col, $doc->getCollection());
        $this->assertNotEmpty($doc->getSequence());
        $this->assertNotEmpty($doc->getAttribute('$createdAt'));
        $this->assertNotEmpty($doc->getAttribute('$updatedAt'));
        $perms = $doc->getPermissions();
        $this->assertGreaterThanOrEqual(1, count($perms));
        $this->assertContains(Permission::read(Role::any()), $perms);
        $this->assertContains(Permission::update(Role::any()), $perms);
        $this->assertContains(Permission::delete(Role::any()), $perms);

        $selected = $database->getDocument($col, 'i1', [
            Query::select(['name', '$id', '$sequence', '$collection', '$createdAt', '$updatedAt', '$permissions'])
        ]);
        $this->assertEquals('alpha', $selected->getAttribute('name'));
        $this->assertArrayHasKey('$id', $selected);
        $this->assertArrayHasKey('$sequence', $selected);
        $this->assertArrayHasKey('$collection', $selected);
        $this->assertArrayHasKey('$createdAt', $selected);
        $this->assertArrayHasKey('$updatedAt', $selected);
        $this->assertArrayHasKey('$permissions', $selected);

        $found = $database->find($col, [
            Query::equal('$id', ['i1']),
            Query::select(['$id', '$sequence', '$collection', '$createdAt', '$updatedAt', '$permissions'])
        ]);
        $this->assertCount(1, $found);
        $this->assertArrayHasKey('$id', $found[0]);
        $this->assertArrayHasKey('$sequence', $found[0]);
        $this->assertArrayHasKey('$collection', $found[0]);
        $this->assertArrayHasKey('$createdAt', $found[0]);
        $this->assertArrayHasKey('$updatedAt', $found[0]);
        $this->assertArrayHasKey('$permissions', $found[0]);

        $seq = $doc->getSequence();
        $bySeq = $database->find($col, [Query::equal('$sequence', [$seq])]);
        $this->assertCount(1, $bySeq);
        $this->assertEquals('i1', $bySeq[0]->getId());

        $createdAtBefore = $doc->getAttribute('$createdAt');
        $updatedAtBefore = $doc->getAttribute('$updatedAt');
        $updated = $database->updateDocument($col, 'i1', new Document(['name' => 'beta']));
        $this->assertEquals('beta', $updated->getAttribute('name'));
        $this->assertEquals($createdAtBefore, $updated->getAttribute('$createdAt'));
        $this->assertNotEquals($updatedAtBefore, $updated->getAttribute('$updatedAt'));

        $changed = $database->updateDocument($col, 'i1', new Document(['$id' => 'i1-new']));
        $this->assertEquals('i1-new', $changed->getId());
        $refetched = $database->getDocument($col, 'i1-new');
        $this->assertEquals('i1-new', $refetched->getId());

        try {
            $database->updateDocument($col, 'i1-new', new Document(['$permissions' => 'invalid']));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof StructureException);
        }

        $database->setPreserveDates(true);
        $customCreated = '2000-01-01T00:00:00.000+00:00';
        $customUpdated = '2000-01-02T00:00:00.000+00:00';
        $d2 = $database->createDocument($col, new Document([
            '$id' => 'i2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$createdAt' => $customCreated,
            '$updatedAt' => $customUpdated,
            'v' => 1
        ]));
        $this->assertEquals($customCreated, $d2->getAttribute('$createdAt'));
        $this->assertEquals($customUpdated, $d2->getAttribute('$updatedAt'));

        $newUpdated = '2000-01-03T00:00:00.000+00:00';
        $d2u = $database->updateDocument($col, 'i2', new Document([
            'v' => 2,
            '$updatedAt' => $newUpdated
        ]));
        $this->assertEquals($customCreated, $d2u->getAttribute('$createdAt'));
        $this->assertEquals($newUpdated, $d2u->getAttribute('$updatedAt'));
        $database->setPreserveDates(false);

        $database->deleteCollection($col);
        Authorization::cleanRoles();
    }

    public function testSchemalessDates(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = uniqid('sl_dates');
        $database->createCollection($col);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        // Seed deterministic date strings
        $createdAt1 = '2000-01-01T10:00:00.000+00:00';
        $updatedAt1 = '2000-01-02T11:11:11.000+00:00';
        $curDate1   = '2000-01-05T05:05:05.000+00:00';

        // createDocument with preserved dates
        $doc1 = $database->withPreserveDates(function () use ($database, $col, $permissions, $createdAt1, $updatedAt1, $curDate1) {
            return $database->createDocument($col, new Document([
                '$id' => 'd1',
                '$permissions' => $permissions,
                '$createdAt' => $createdAt1,
                '$updatedAt' => $updatedAt1,
                'curDate' => $curDate1,
                'counter' => 0,
            ]));
        });

        $this->assertEquals('d1', $doc1->getId());
        $this->assertTrue(is_string($doc1->getAttribute('curDate')));
        $this->assertEquals($curDate1, $doc1->getAttribute('curDate'));
        $this->assertTrue(is_string($doc1->getAttribute('$createdAt')));
        $this->assertTrue(is_string($doc1->getAttribute('$updatedAt')));
        $this->assertEquals($createdAt1, $doc1->getAttribute('$createdAt'));
        $this->assertEquals($updatedAt1, $doc1->getAttribute('$updatedAt'));

        $fetched1 = $database->getDocument($col, 'd1');
        $this->assertEquals($curDate1, $fetched1->getAttribute('curDate'));
        $this->assertTrue(is_string($fetched1->getAttribute('curDate')));
        $this->assertTrue(is_string($fetched1->getAttribute('$createdAt')));
        $this->assertTrue(is_string($fetched1->getAttribute('$updatedAt')));
        $this->assertEquals($createdAt1, $fetched1->getAttribute('$createdAt'));
        $this->assertEquals($updatedAt1, $fetched1->getAttribute('$updatedAt'));

        // createDocuments with preserved dates
        $createdAt2 = '2001-02-03T04:05:06.000+00:00';
        $updatedAt2 = '2001-02-04T04:05:07.000+00:00';
        $curDate2   = '2001-02-05T06:07:08.000+00:00';

        $createdAt3 = '2002-03-04T05:06:07.000+00:00';
        $updatedAt3 = '2002-03-05T05:06:08.000+00:00';
        $curDate3   = '2002-03-06T07:08:09.000+00:00';

        $countCreated = $database->withPreserveDates(function () use ($database, $col, $permissions, $createdAt2, $updatedAt2, $curDate2, $createdAt3, $updatedAt3, $curDate3) {
            return $database->createDocuments($col, [
                new Document([
                    '$id' => 'd2',
                    '$permissions' => $permissions,
                    '$createdAt' => $createdAt2,
                    '$updatedAt' => $updatedAt2,
                    'curDate' => $curDate2,
                ]),
                new Document([
                    '$id' => 'd3',
                    '$permissions' => $permissions,
                    '$createdAt' => $createdAt3,
                    '$updatedAt' => $updatedAt3,
                    'curDate' => $curDate3,
                ]),
            ]);
        });
        $this->assertEquals(2, $countCreated);

        $fetched2 = $database->getDocument($col, 'd2');
        $this->assertEquals($curDate2, $fetched2->getAttribute('curDate'));
        $this->assertEquals($createdAt2, $fetched2->getAttribute('$createdAt'));
        $this->assertEquals($updatedAt2, $fetched2->getAttribute('$updatedAt'));

        $fetched3 = $database->getDocument($col, 'd3');
        $this->assertEquals($curDate3, $fetched3->getAttribute('curDate'));
        $this->assertEquals($createdAt3, $fetched3->getAttribute('$createdAt'));
        $this->assertEquals($updatedAt3, $fetched3->getAttribute('$updatedAt'));

        // updateDocument with preserved $updatedAt and custom date field
        $newCurDate1   = '2000-02-01T00:00:00.000+00:00';
        $newUpdatedAt1 = '2000-02-02T02:02:02.000+00:00';
        $updated1 = $database->withPreserveDates(function () use ($database, $col, $newCurDate1, $newUpdatedAt1) {
            return $database->updateDocument($col, 'd1', new Document([
                'curDate' => $newCurDate1,
                '$updatedAt' => $newUpdatedAt1,
            ]));
        });
        $this->assertEquals($newCurDate1, $updated1->getAttribute('curDate'));
        $this->assertEquals($newUpdatedAt1, $updated1->getAttribute('$updatedAt'));
        $refetched1 = $database->getDocument($col, 'd1');
        $this->assertEquals($newCurDate1, $refetched1->getAttribute('curDate'));
        $this->assertEquals($newUpdatedAt1, $refetched1->getAttribute('$updatedAt'));

        // updateDocuments with preserved $updatedAt over a subset
        $bulkCurDate   = '2001-01-01T00:00:00.000+00:00';
        $bulkUpdatedAt = '2001-01-02T00:00:00.000+00:00';
        $updatedCount = $database->withPreserveDates(function () use ($database, $col, $bulkCurDate, $bulkUpdatedAt) {
            return $database->updateDocuments(
                $col,
                new Document([
                    'curDate' => $bulkCurDate,
                    '$updatedAt' => $bulkUpdatedAt,
                ]),
                [Query::equal('$id', ['d2', 'd3'])]
            );
        });
        $this->assertEquals(2, $updatedCount);
        $afterBulk2 = $database->getDocument($col, 'd2');
        $afterBulk3 = $database->getDocument($col, 'd3');
        $this->assertEquals($bulkCurDate, $afterBulk2->getAttribute('curDate'));
        $this->assertEquals($bulkUpdatedAt, $afterBulk2->getAttribute('$updatedAt'));
        $this->assertEquals($bulkCurDate, $afterBulk3->getAttribute('curDate'));
        $this->assertEquals($bulkUpdatedAt, $afterBulk3->getAttribute('$updatedAt'));

        // upsertDocument: create new then update existing with preserved dates
        $createdAt4 = '2003-03-03T03:03:03.000+00:00';
        $updatedAt4 = '2003-03-04T04:04:04.000+00:00';
        $curDate4   = '2003-03-05T05:05:05.000+00:00';
        $up1 = $database->withPreserveDates(function () use ($database, $col, $permissions, $createdAt4, $updatedAt4, $curDate4) {
            return $database->upsertDocument($col, new Document([
                '$id' => 'd4',
                '$permissions' => $permissions,
                '$createdAt' => $createdAt4,
                '$updatedAt' => $updatedAt4,
                'curDate' => $curDate4,
            ]));
        });
        $this->assertEquals('d4', $up1->getId());
        $this->assertEquals($curDate4, $up1->getAttribute('curDate'));
        $this->assertEquals($createdAt4, $up1->getAttribute('$createdAt'));
        $this->assertEquals($updatedAt4, $up1->getAttribute('$updatedAt'));

        $updatedAt4b = '2003-03-06T06:06:06.000+00:00';
        $curDate4b   = '2003-03-07T07:07:07.000+00:00';
        $up2 = $database->withPreserveDates(function () use ($database, $col, $updatedAt4b, $curDate4b) {
            return $database->upsertDocument($col, new Document([
                '$id' => 'd4',
                'curDate' => $curDate4b,
                '$updatedAt' => $updatedAt4b,
            ]));
        });
        $this->assertEquals($curDate4b, $up2->getAttribute('curDate'));
        $this->assertEquals($updatedAt4b, $up2->getAttribute('$updatedAt'));
        $refetched4 = $database->getDocument($col, 'd4');
        $this->assertEquals($curDate4b, $refetched4->getAttribute('curDate'));
        $this->assertEquals($updatedAt4b, $refetched4->getAttribute('$updatedAt'));

        // upsertDocuments: mix create and update with preserved dates
        $createdAt5 = '2004-04-01T01:01:01.000+00:00';
        $updatedAt5 = '2004-04-02T02:02:02.000+00:00';
        $curDate5   = '2004-04-03T03:03:03.000+00:00';
        $updatedAt2b = '2001-02-08T08:08:08.000+00:00';
        $curDate2b   = '2001-02-09T09:09:09.000+00:00';

        $upCount = $database->withPreserveDates(function () use ($database, $col, $permissions, $createdAt5, $updatedAt5, $curDate5, $updatedAt2b, $curDate2b) {
            return $database->upsertDocuments($col, [
                new Document([
                    '$id' => 'd5',
                    '$permissions' => $permissions,
                    '$createdAt' => $createdAt5,
                    '$updatedAt' => $updatedAt5,
                    'curDate' => $curDate5,
                ]),
                new Document([
                    '$id' => 'd2',
                    '$updatedAt' => $updatedAt2b,
                    'curDate' => $curDate2b,
                ]),
            ]);
        });
        $this->assertEquals(2, $upCount);

        $fetched5 = $database->getDocument($col, 'd5');
        $this->assertEquals($curDate5, $fetched5->getAttribute('curDate'));
        $this->assertEquals($createdAt5, $fetched5->getAttribute('$createdAt'));
        $this->assertEquals($updatedAt5, $fetched5->getAttribute('$updatedAt'));

        $fetched2b = $database->getDocument($col, 'd2');
        $this->assertEquals($curDate2b, $fetched2b->getAttribute('curDate'));
        $this->assertEquals($updatedAt2b, $fetched2b->getAttribute('$updatedAt'));

        // increase/decrease should not affect date types; ensure they remain strings
        $afterInc = $database->increaseDocumentAttribute($col, 'd1', 'counter', 5);
        $this->assertEquals(5, $afterInc->getAttribute('counter'));
        $this->assertTrue(is_string($afterInc->getAttribute('curDate')));
        $this->assertTrue(is_string($afterInc->getAttribute('$createdAt')));
        $this->assertTrue(is_string($afterInc->getAttribute('$updatedAt')));

        $afterIncFetched = $database->getDocument($col, 'd1');
        $this->assertEquals(5, $afterIncFetched->getAttribute('counter'));
        $this->assertTrue(is_string($afterIncFetched->getAttribute('curDate')));
        $this->assertTrue(is_string($afterIncFetched->getAttribute('$createdAt')));
        $this->assertTrue(is_string($afterIncFetched->getAttribute('$updatedAt')));

        $afterDec = $database->decreaseDocumentAttribute($col, 'd1', 'counter', 2);
        $this->assertEquals(3, $afterDec->getAttribute('counter'));
        $this->assertTrue(is_string($afterDec->getAttribute('curDate')));
        $this->assertTrue(is_string($afterDec->getAttribute('$createdAt')));
        $this->assertTrue(is_string($afterDec->getAttribute('$updatedAt')));

        $afterDecFetched = $database->getDocument($col, 'd1');
        $this->assertEquals(3, $afterDecFetched->getAttribute('counter'));
        $this->assertTrue(is_string($afterDecFetched->getAttribute('curDate')));
        $this->assertTrue(is_string($afterDecFetched->getAttribute('$createdAt')));
        $this->assertTrue(is_string($afterDecFetched->getAttribute('$updatedAt')));

        $database->deleteCollection($col);
    }

    public function testSchemalessRemoveAttributesByUpdate(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // in schemaless, if attributes are created and then if values are not provided then they are replaced with the default attribute automatically in the encode
        if ($database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = uniqid('sl_update_remove');
        $database->createCollection($col);

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ];

        $single = $database->createDocument($col, new Document(['$id' => 'docS', 'key' => 'single', 'extra' => 'yes', '$permissions' => $permissions]));
        $this->assertEquals('docS', $single->getId());
        $this->assertEquals('yes', $single->getAttribute('extra'));
        $this->assertEquals('single', $single->getAttribute('key'));

        // before removing attribute
        $doc = $database->getDocument($col, 'docS');
        $this->assertEquals('yes', $doc->getAttribute('extra'));
        $this->assertEquals('single', $doc->getAttribute('key'));

        // removing attribute
        $doc = $database->updateDocument($col, 'docS', new Document(['$id' => 'docS','key' => 'single2']));
        $this->assertEquals('single2', $doc->getAttribute('key'));
        $this->assertArrayNotHasKey('extra', $doc);

        $doc = $database->getDocument($col, 'docS');
        $this->assertEquals('single2', $doc->getAttribute('key'));
        $this->assertArrayNotHasKey('extra', $doc);

        $database->deleteCollection($col);
    }
}
