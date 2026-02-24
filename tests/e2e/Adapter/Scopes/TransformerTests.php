<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait TransformerTests
{
    public function testSetTransformer(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $transformer = function (Document $document, Document $collection, Database $db): Document {
            $document->setAttribute('transformed', true);
            return $document;
        };

        $result = $database->setTransformer($transformer);

        $this->assertInstanceOf(Database::class, $result);
        $this->assertSame($transformer, $database->getTransformer());

        // Cleanup
        $database->setTransformer(null);
    }

    public function testGetTransformerReturnsNull(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Ensure no transformer is set
        $database->setTransformer(null);

        $this->assertNull($database->getTransformer());
    }

    public function testTransformerWithGetDocument(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create collection
        $database->createCollection('transformerTestGet', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createAttribute('transformerTestGet', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('transformerTestGet', 'email', Database::VAR_STRING, 255, true);

        // Create a document first (without transformer)
        $created = $database->createDocument('transformerTestGet', new Document([
            '$id' => ID::unique(),
            'name' => 'Test User',
            'email' => 'test@example.com',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        // Set up transformer
        $transformerCalled = false;
        $receivedCollection = null;
        $receivedDatabase = null;

        $database->setTransformer(function (Document $document, Document $collection, Database $db) use (&$transformerCalled, &$receivedCollection, &$receivedDatabase): Document {
            $transformerCalled = true;
            $receivedCollection = $collection;
            $receivedDatabase = $db;
            $document->setAttribute('_transformed', true);
            $document->setAttribute('_customAttribute', 'added by transformer');
            return $document;
        });

        // Get document - transformer should be called
        $fetched = $database->getDocument('transformerTestGet', $created->getId());

        $this->assertTrue($transformerCalled, 'Transformer should be called');
        $this->assertNotNull($receivedCollection, 'Transformer should receive collection');
        $this->assertEquals('transformerTestGet', $receivedCollection->getId());
        $this->assertInstanceOf(Database::class, $receivedDatabase);
        $this->assertTrue($fetched->getAttribute('_transformed'));
        $this->assertEquals('added by transformer', $fetched->getAttribute('_customAttribute'));
        $this->assertEquals('Test User', $fetched->getAttribute('name'));

        // Cleanup
        $database->setTransformer(null);
        $database->deleteCollection('transformerTestGet');
    }

    public function testTransformerWithFind(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create collection
        $database->createCollection('transformerTestFind', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createAttribute('transformerTestFind', 'title', Database::VAR_STRING, 255, true);

        // Create documents without transformer
        $database->setTransformer(null);

        $database->createDocument('transformerTestFind', new Document([
            '$id' => ID::unique(),
            'title' => 'First Post',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->createDocument('transformerTestFind', new Document([
            '$id' => ID::unique(),
            'title' => 'Second Post',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        // Set up transformer that counts calls
        $callCount = 0;

        $database->setTransformer(function (Document $document, Document $collection, Database $db) use (&$callCount): Document {
            $callCount++;
            $document->setAttribute('_index', $callCount);
            return $document;
        });

        // Find documents
        $documents = $database->find('transformerTestFind', [Query::limit(10)]);

        $this->assertCount(2, $documents);
        $this->assertEquals(2, $callCount, 'Transformer should be called for each document');

        // Verify each document was transformed
        foreach ($documents as $doc) {
            $this->assertTrue($doc->attributeExists('_index'), 'Each document should have _index attribute');
        }

        // Cleanup
        $database->setTransformer(null);
        $database->deleteCollection('transformerTestFind');
    }

    public function testTransformerWithCreateDocument(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create collection
        $database->createCollection('transformerTestCreate', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createAttribute('transformerTestCreate', 'name', Database::VAR_STRING, 255, true);

        // Set up transformer
        $transformerCalled = false;

        $database->setTransformer(function (Document $document, Document $collection, Database $db) use (&$transformerCalled): Document {
            $transformerCalled = true;
            $document->setAttribute('_createdViaApi', true);
            return $document;
        });

        // Create document - transformer should be called on returned document
        $created = $database->createDocument('transformerTestCreate', new Document([
            '$id' => ID::unique(),
            'name' => 'New User',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $this->assertTrue($transformerCalled, 'Transformer should be called on create');
        $this->assertTrue($created->getAttribute('_createdViaApi'));
        $this->assertEquals('New User', $created->getAttribute('name'));

        // Cleanup
        $database->setTransformer(null);
        $database->deleteCollection('transformerTestCreate');
    }

    public function testTransformerWithUpdateDocument(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create collection
        $database->createCollection('transformerTestUpdate', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]);

        $database->createAttribute('transformerTestUpdate', 'status', Database::VAR_STRING, 50, true);

        // Create document without transformer
        $database->setTransformer(null);

        $created = $database->createDocument('transformerTestUpdate', new Document([
            '$id' => ID::unique(),
            'status' => 'pending',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        // Set up transformer for update
        $transformerCalled = false;

        $database->setTransformer(function (Document $document, Document $collection, Database $db) use (&$transformerCalled): Document {
            $transformerCalled = true;
            $document->setAttribute('_lastModifiedBy', 'system');
            return $document;
        });

        // Update document
        $updated = $database->updateDocument('transformerTestUpdate', $created->getId(), new Document([
            '$id' => $created->getId(),
            'status' => 'active',
        ]));

        $this->assertTrue($transformerCalled, 'Transformer should be called on update');
        $this->assertEquals('system', $updated->getAttribute('_lastModifiedBy'));
        $this->assertEquals('active', $updated->getAttribute('status'));

        // Cleanup
        $database->setTransformer(null);
        $database->deleteCollection('transformerTestUpdate');
    }

    public function testTransformerSkippedForEmptyDocument(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create collection
        $database->createCollection('transformerTestEmpty', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createAttribute('transformerTestEmpty', 'name', Database::VAR_STRING, 255, true);

        // Set up transformer
        $transformerCalled = false;

        $database->setTransformer(function (Document $document, Document $collection, Database $db) use (&$transformerCalled): Document {
            $transformerCalled = true;
            return $document;
        });

        // Try to get non-existent document
        $result = $database->getDocument('transformerTestEmpty', 'nonexistent-id');

        $this->assertTrue($result->isEmpty(), 'Document should be empty');
        $this->assertFalse($transformerCalled, 'Transformer should NOT be called for empty documents');

        // Cleanup
        $database->setTransformer(null);
        $database->deleteCollection('transformerTestEmpty');
    }

    public function testTransformerClearedWithNull(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create collection
        $database->createCollection('transformerTestClear', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createAttribute('transformerTestClear', 'value', Database::VAR_STRING, 255, true);

        // Create document
        $created = $database->createDocument('transformerTestClear', new Document([
            '$id' => ID::unique(),
            'value' => 'test',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        // Set transformer
        $database->setTransformer(function (Document $document, Document $collection, Database $db): Document {
            $document->setAttribute('_transformed', true);
            return $document;
        });

        // Verify transformer works
        $fetched = $database->getDocument('transformerTestClear', $created->getId());
        $this->assertTrue($fetched->getAttribute('_transformed'));

        // Clear transformer
        $database->setTransformer(null);

        // Verify transformer no longer runs
        $fetchedAgain = $database->getDocument('transformerTestClear', $created->getId());
        $this->assertNull($fetchedAgain->getAttribute('_transformed'));

        // Cleanup
        $database->deleteCollection('transformerTestClear');
    }

    public function testTransformerReceivesCorrectParameters(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Create collection
        $database->createCollection('transformerTestParams', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createAttribute('transformerTestParams', 'data', Database::VAR_STRING, 255, true);

        // Create document without transformer
        $database->setTransformer(null);

        $created = $database->createDocument('transformerTestParams', new Document([
            '$id' => ID::unique(),
            'data' => 'test value',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        // Set up transformer that captures parameters
        $capturedDocument = null;
        $capturedCollection = null;
        $capturedDatabase = null;

        $database->setTransformer(function (Document $document, Document $collection, Database $db) use (&$capturedDocument, &$capturedCollection, &$capturedDatabase): Document {
            $capturedDocument = $document;
            $capturedCollection = $collection;
            $capturedDatabase = $db;
            return $document;
        });

        // Fetch document
        $database->getDocument('transformerTestParams', $created->getId());

        // Verify parameters
        $this->assertInstanceOf(Document::class, $capturedDocument);
        $this->assertEquals($created->getId(), $capturedDocument->getId());
        $this->assertEquals('test value', $capturedDocument->getAttribute('data'));

        $this->assertInstanceOf(Document::class, $capturedCollection);
        $this->assertEquals('transformerTestParams', $capturedCollection->getId());

        $this->assertInstanceOf(Database::class, $capturedDatabase);
        $this->assertSame($database, $capturedDatabase);

        // Cleanup
        $database->setTransformer(null);
        $database->deleteCollection('transformerTestParams');
    }

    public function testTransformerMethodChaining(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $transformer = function (Document $document, Document $collection, Database $db): Document {
            return $document;
        };

        $result = $database->setTransformer($transformer);

        $this->assertInstanceOf(Database::class, $result);

        // Test chaining with other methods
        $result2 = $database
            ->setTransformer($transformer)
            ->setTransformer(null);

        $this->assertInstanceOf(Database::class, $result2);
        $this->assertNull($database->getTransformer());
    }
}
