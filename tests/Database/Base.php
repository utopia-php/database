<?php

namespace Utopia\Tests;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as ExceptionAuthorization;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

abstract class Base extends TestCase
{
    /**
     * @return Adapter
     */
    abstract static protected function getDatabase(): Database;

    /**
     * @return string
     */
    abstract static protected function getAdapterName(): string;

    /**
     * @return int
     */
    abstract static protected function getAdapterRowLimit(): int;

    public function setUp(): void
    {
        Authorization::setRole('role:all');
    }

    public function tearDown(): void
    {
        Authorization::reset();
    }

    protected string $testDatabase = 'utopiaTests';

    public function testCreateExistsDelete()
    {
        if (!static::getDatabase()->exists($this->testDatabase)) {
            $this->assertEquals(true, static::getDatabase()->create($this->testDatabase));
        }
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));
        $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->create($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
    }

    /**
     * @depends testCreateExistsDelete
     */
    public function testCreateListExistsDeleteCollection()
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors'));

        $this->assertCount(1, static::getDatabase()->listCollections());
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase, 'actors'));

        // Collection names should not be unique
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors2'));
        $this->assertCount(2, static::getDatabase()->listCollections());
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase, 'actors2'));
        $collection = static::getDatabase()->getCollection('actors2');
        $collection->setAttribute('name', 'actors'); // change name to one that exists
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->updateDocument($collection->getCollection(), $collection->getId(), $collection));
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors2')); // Delete collection when finished
        $this->assertCount(1, static::getDatabase()->listCollections());

        $this->assertEquals(false, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors'));
        $this->assertEquals(true, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase, 'actors'));
    }

    public function testCreateDeleteAttribute()
    {
        static::getDatabase()->createCollection('attributes');

        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string1', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string2', Database::VAR_STRING, 16383+1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string3', Database::VAR_STRING, 65535+1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string4', Database::VAR_STRING, 16777215+1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'bigint', Database::VAR_INTEGER, 8, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean', Database::VAR_BOOLEAN, 0, true));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(8, $collection->getAttribute('attributes'));

        // Array
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string_list', Database::VAR_STRING, 128, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer_list', Database::VAR_INTEGER, 0, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float_list', Database::VAR_FLOAT, 0, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean_list', Database::VAR_BOOLEAN, 0, true, null, true, true));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(12, $collection->getAttribute('attributes'));

        // Default values
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string_default', Database::VAR_STRING, 256, false, 'test'));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer_default', Database::VAR_INTEGER, 0, false, 1));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float_default', Database::VAR_FLOAT, 0, false, 1.5));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean_default', Database::VAR_BOOLEAN, 0, false, false));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(16, $collection->getAttribute('attributes'));

        // Delete
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string1'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string2'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string3'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string4'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'bigint'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(8, $collection->getAttribute('attributes'));

        // Delete Array
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean_list'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(4, $collection->getAttribute('attributes'));

        // Delete default
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean_default'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(0, $collection->getAttribute('attributes'));

        // Test for custom chars in ID
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as_5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as5dasdasdas_', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', '.as5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as.5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as5dasdasdas.', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', '-as5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as-5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as5dasdasdas-', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'socialAccountForYoutubeSubscribersss', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', '5f058a89258075f058a89258075f058t9214', Database::VAR_BOOLEAN, 0, true));

        // Using this collection to test invalid default values
        // static::getDatabase()->deleteCollection('attributes');
    }

    /**
     * Using phpunit dataProviders to check that all these combinations of types/defaults throw exceptions
     * https://phpunit.de/manual/3.7/en/writing-tests-for-phpunit.html#writing-tests-for-phpunit.data-providers
     */
    public function invalidDefaultValues()
    {
        return [
            [Database::VAR_STRING, 1],
            [Database::VAR_STRING, 1.5],
            [Database::VAR_STRING, false],
            [Database::VAR_INTEGER, "one"],
            [Database::VAR_INTEGER, 1.5],
            [Database::VAR_INTEGER, true],
            [Database::VAR_FLOAT, 1],
            [Database::VAR_FLOAT, "one"],
            [Database::VAR_FLOAT, false],
            [Database::VAR_BOOLEAN, 0],
            [Database::VAR_BOOLEAN, "false"],
            [Database::VAR_BOOLEAN, 0.5],
        ];
    }

    /**
     * @depends testCreateDeleteAttribute
     * @dataProvider invalidDefaultValues
     * @expectedException Exception
     */
    public function testInvalidDefaultValues($type, $default)
    {
        $this->expectException(\Exception::class);
        $this->assertEquals(false, static::getDatabase()->createAttribute('attributes', 'bad_default', $type, 256, true, $default));
    }

    /**
     * @depends testInvalidDefaultValues
     */
    public function testAttributeCaseInsensitivity()
    {
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'caseSensitive', Database::VAR_STRING, 128, true));
        $this->expectException(DuplicateException::class);
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'CaseSensitive', Database::VAR_STRING, 128, true));
    }

    /**
     * @depends testAttributeCaseInsensitivity
     */
    public function testIndexCaseInsensitivity()
    {
        $this->assertEquals(true, static::getDatabase()->createIndex('attributes', 'key_caseSensitive', Database::INDEX_KEY, ['caseSensitive'], [128]));
        $this->expectException(DuplicateException::class);
        $this->assertEquals(true, static::getDatabase()->createIndex('attributes', 'key_CaseSensitive', Database::INDEX_KEY, ['caseSensitive'], [128]));
    }

    /**
     * Ensure the collection is removed after use
     * 
     * @depends testIndexCaseInsensitivity
     */
    public function testCleanupAttributeTests()
    {
        static::getDatabase()->deleteCollection('attributes');
        $this->assertEquals(1,1);
    }

    /**
     * @depends testCreateDeleteAttribute
     * @expectedException Exception
     */
    public function testUnknownFormat()
    {
        $this->expectException(\Exception::class);
        $this->assertEquals(false, static::getDatabase()->createAttribute('attributes', 'bad_format', Database::VAR_STRING, 256, true, null, true, false, 'url'));
    }

    public function testCreateDeleteIndex()
    {
        static::getDatabase()->createCollection('indexes');

        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'order', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'boolean', Database::VAR_BOOLEAN, 0, true));

        // Indexes
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index2', Database::INDEX_KEY, ['float', 'integer'], [], [Database::ORDER_ASC, Database::ORDER_DESC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index3', Database::INDEX_KEY, ['integer', 'boolean'], [], [Database::ORDER_ASC, Database::ORDER_DESC, Database::ORDER_DESC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index4', Database::INDEX_UNIQUE, ['string'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'order', Database::INDEX_UNIQUE, ['order'], [128], [Database::ORDER_ASC]));
        
        $collection = static::getDatabase()->getCollection('indexes');
        $this->assertCount(5, $collection->getAttribute('indexes'));

        // Delete Indexes
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index2'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index3'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index4'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'order'));

        $collection = static::getDatabase()->getCollection('indexes');
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->deleteCollection('indexes');
    }

    public function testCreateCollectionWithSchema()
    {
        $attributes = [
            new Document([
                '$id' => 'attribute1',
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'attribute2',
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'attribute3',
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ];

        $indexes = [
            new Document([
                '$id' => 'index1',
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => 'index2',
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute2'],
                'lengths' => [],
                'orders' => ['DESC'],
            ]),
            new Document([
                '$id' => 'index3',
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute3', 'attribute2'],
                'lengths' => [],
                'orders' => ['DESC', 'ASC'],
            ]),
        ];

        $collection = static::getDatabase()->createCollection('withSchema', $attributes, $indexes);

        $this->assertEquals(false, $collection->isEmpty());
        $this->assertEquals('withSchema', $collection->getId());

        $this->assertIsArray($collection->getAttribute('attributes'));
        $this->assertCount(3, $collection->getAttribute('attributes'));
        $this->assertEquals('attribute1', $collection->getAttribute('attributes')[0]['$id']);
        $this->assertEquals(Database::VAR_STRING, $collection->getAttribute('attributes')[0]['type']);
        $this->assertEquals('attribute2', $collection->getAttribute('attributes')[1]['$id']);
        $this->assertEquals(Database::VAR_INTEGER, $collection->getAttribute('attributes')[1]['type']);
        $this->assertEquals('attribute3', $collection->getAttribute('attributes')[2]['$id']);
        $this->assertEquals(Database::VAR_BOOLEAN, $collection->getAttribute('attributes')[2]['type']);

        $this->assertIsArray($collection->getAttribute('indexes'));
        $this->assertCount(3, $collection->getAttribute('indexes'));
        $this->assertEquals('index1', $collection->getAttribute('indexes')[0]['$id']);
        $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[0]['type']);
        $this->assertEquals('index2', $collection->getAttribute('indexes')[1]['$id']);
        $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[1]['type']);
        $this->assertEquals('index3', $collection->getAttribute('indexes')[2]['$id']);
        $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[2]['type']);

        static::getDatabase()->deleteCollection('withSchema');
    }

    public function testCreateCollectionValidator()
    {
        $collections = [
            "validatorTest",
            "validator-test",
            "validator_test",
            "validator.test",
        ];

        $attributes = [
            new Document([
                '$id' => 'attribute1',
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'attribute-2',
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'attribute_3',
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'attribute.4',
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ];

        $indexes = [
            new Document([
                '$id' => 'index1',
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => 'index-2',
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute-2'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => 'index_3',
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute_3'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => 'index.4',
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute.4'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
        ];

        foreach ($collections as $id) {
            $collection = static::getDatabase()->createCollection($id, $attributes, $indexes);

            $this->assertEquals(false, $collection->isEmpty());
            $this->assertEquals($id, $collection->getId());

            $this->assertIsArray($collection->getAttribute('attributes'));
            $this->assertCount(4, $collection->getAttribute('attributes'));
            $this->assertEquals('attribute1', $collection->getAttribute('attributes')[0]['$id']);
            $this->assertEquals(Database::VAR_STRING, $collection->getAttribute('attributes')[0]['type']);
            $this->assertEquals('attribute-2', $collection->getAttribute('attributes')[1]['$id']);
            $this->assertEquals(Database::VAR_INTEGER, $collection->getAttribute('attributes')[1]['type']);
            $this->assertEquals('attribute_3', $collection->getAttribute('attributes')[2]['$id']);
            $this->assertEquals(Database::VAR_BOOLEAN, $collection->getAttribute('attributes')[2]['type']);
            $this->assertEquals('attribute.4', $collection->getAttribute('attributes')[3]['$id']);
            $this->assertEquals(Database::VAR_BOOLEAN, $collection->getAttribute('attributes')[3]['type']);

            $this->assertIsArray($collection->getAttribute('indexes'));
            $this->assertCount(4, $collection->getAttribute('indexes'));
            $this->assertEquals('index1', $collection->getAttribute('indexes')[0]['$id']);
            $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[0]['type']);
            $this->assertEquals('index-2', $collection->getAttribute('indexes')[1]['$id']);
            $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[1]['type']);
            $this->assertEquals('index_3', $collection->getAttribute('indexes')[2]['$id']);
            $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[2]['type']);
            $this->assertEquals('index.4', $collection->getAttribute('indexes')[3]['$id']);
            $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[3]['type']);

            static::getDatabase()->deleteCollection($id);
        }
    }

    public function testCreateDocument()
    {
        static::getDatabase()->createCollection('documents');

        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'bigint', Database::VAR_INTEGER, 8, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'boolean', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'colors', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'empty', Database::VAR_STRING, 32, false, null, true, true));

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
        ]));

        $this->assertNotEmpty(true, $document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
        $this->assertIsInt($document->getAttribute('integer'));
        $this->assertEquals(5, $document->getAttribute('integer'));
        $this->assertIsInt($document->getAttribute('bigint'));
        $this->assertEquals(8589934592, $document->getAttribute('bigint'));
        $this->assertIsFloat($document->getAttribute('float'));
        $this->assertEquals(5.55, $document->getAttribute('float'));
        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(true, $document->getAttribute('boolean'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));
        $this->assertEquals([], $document->getAttribute('empty'));

        return $document;
    }

    public function testCreateDocumentDefaults()
    {
        static::getDatabase()->createCollection('defaults');

        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'string', Database::VAR_STRING, 128, false, 'default'));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'integer', Database::VAR_INTEGER, 0, false, 1));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'float', Database::VAR_FLOAT, 0, false, 1.5));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'boolean', Database::VAR_BOOLEAN, 0, false, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'colors', Database::VAR_STRING, 32, false, ['red', 'green', 'blue'], true, true));

        $document = static::getDatabase()->createDocument('defaults', new Document([
            '$read' => ['role:all'],
            '$write' => ['role:all'],
        ]));

        $this->assertNotEmpty(true, $document->getId());

        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('default', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer'));
        $this->assertEquals(1, $document->getAttribute('integer'));
        $this->assertIsFloat($document->getAttribute('float'));
        $this->assertEquals(1.5, $document->getAttribute('float'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertCount(3, $document->getAttribute('colors'));
        $this->assertEquals('red', $document->getAttribute('colors')[0]);
        $this->assertEquals('green', $document->getAttribute('colors')[1]);
        $this->assertEquals('blue', $document->getAttribute('colors')[2]);

        // cleanup collection
        static::getDatabase()->deleteCollection('defaults');
    }

    /**
     * @depends testCreateDocument
     */
    public function testGetDocument(Document $document)
    {
        $document = static::getDatabase()->getDocument('documents', $document->getId());

        $this->assertNotEmpty(true, $document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer'));
        $this->assertEquals(5, $document->getAttribute('integer'));
        $this->assertIsFloat($document->getAttribute('float'));
        $this->assertEquals(5.55, $document->getAttribute('float'));
        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(true, $document->getAttribute('boolean'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testListDocumentSearch(Document $document)
    {
        static::getDatabase()->createIndex('documents', 'string', Database::INDEX_FULLTEXT, ['string']);
        static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['role:all'],
            '$write' => ['role:all'],
            'string' => '*test+alias@email-provider.com',
            'integer' => 0,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
        ]));

        /**
         * Allow reserved keywords for search
         */
        $documents = static::getDatabase()->find('documents', [
            new Query('string', Query::TYPE_SEARCH, ['*test+alias@email-provider.com']),
        ]);

        $this->assertEquals(1, count($documents));

        return $document;
    }

    /**
     * @depends testGetDocument
     */
    public function testUpdateDocument(Document $document)
    {
        $document
            ->setAttribute('string', 'text📝 updated')
            ->setAttribute('integer', 6)
            ->setAttribute('float', 5.56)
            ->setAttribute('boolean', false)
            ->setAttribute('colors', 'red', Document::SET_TYPE_APPEND)
        ;

        $new = $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);

        $this->assertNotEmpty(true, $new->getId());
        $this->assertIsString($new->getAttribute('string'));
        $this->assertEquals('text📝 updated', $new->getAttribute('string'));
        $this->assertIsInt($new->getAttribute('integer'));
        $this->assertEquals(6, $new->getAttribute('integer'));
        $this->assertIsFloat($new->getAttribute('float'));
        $this->assertEquals(5.56, $new->getAttribute('float'));
        $this->assertIsBool($new->getAttribute('boolean'));
        $this->assertEquals(false, $new->getAttribute('boolean'));
        $this->assertIsArray($new->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue', 'red'], $new->getAttribute('colors'));

        $oldRead = $document->getRead();
        $oldWrite = $document->getWrite();

        $new
            ->setAttribute('$read', 'role:guest', Document::SET_TYPE_APPEND)
            ->setAttribute('$write', 'role:guest', Document::SET_TYPE_APPEND)
        ;

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertContains('role:guest', $new->getRead());
        $this->assertContains('role:guest', $new->getWrite());

        $new
            ->setAttribute('$read', $oldRead)
            ->setAttribute('$write', $oldWrite)
        ;

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertNotContains('role:guest', $new->getRead());
        $this->assertNotContains('role:guest', $new->getWrite());

        return $document;
    }

    /**
     * @depends testUpdateDocument
     */
    public function testDeleteDocument(Document $document)
    {
        $result = $this->getDatabase()->deleteDocument($document->getCollection(), $document->getId());
        $document = $this->getDatabase()->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $result);
        $this->assertEquals(true, $document->isEmpty());
    }

    /**
     * @depends testUpdateDocument
     */
    public function testFind(Document $document)
    {
        static::getDatabase()->createCollection('movies');

        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'director', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'price', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'active', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'generes', Database::VAR_STRING, 32, true, null, true, true));

        static::getDatabase()->createDocument('movies', new Document([
            '$id' => 'frozen',
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Frozen II',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2019,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Captain America: The First Avenger',
            'director' => 'Joe Johnston',
            'year' => 2011,
            'price' => 25.94,
            'active' => true,
            'generes' => ['science fiction', 'action', 'comics'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Captain Marvel',
            'director' => 'Anna Boden & Ryan Fleck',
            'year' => 2019,
            'price' => 25.99,
            'active' => true,
            'generes' => ['science fiction', 'action', 'comics'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Work in Progress',
            'director' => 'TBD',
            'year' => 2025,
            'price' => 0.0,
            'active' => false,
            'generes' => [],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['userx'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Work in Progress 2',
            'director' => 'TBD',
            'year' => 2026,
            'price' => 0.0,
            'active' => false,
            'generes' => [],
        ]));

        /**
         * Check Basic
         */
        $documents = static::getDatabase()->find('movies');
        $movieDocuments = $documents;

        $this->assertEquals(5, count($documents));
        $this->assertNotEmpty($documents[0]->getId());
        $this->assertEquals('movies', $documents[0]->getCollection());
        $this->assertEquals(['role:all', 'user1', 'user2'], $documents[0]->getRead());
        $this->assertEquals(['role:all', 'user1x', 'user2x'], $documents[0]->getWrite());
        $this->assertEquals('Frozen', $documents[0]->getAttribute('name'));
        $this->assertEquals('Chris Buck & Jennifer Lee', $documents[0]->getAttribute('director'));
        $this->assertIsString($documents[0]->getAttribute('director'));
        $this->assertEquals(2013, $documents[0]->getAttribute('year'));
        $this->assertIsInt($documents[0]->getAttribute('year'));
        $this->assertEquals(39.50, $documents[0]->getAttribute('price'));
        $this->assertIsFloat($documents[0]->getAttribute('price'));
        $this->assertEquals(true, $documents[0]->getAttribute('active'));
        $this->assertIsBool($documents[0]->getAttribute('active'));
        $this->assertEquals(['animation', 'kids'], $documents[0]->getAttribute('generes'));
        $this->assertIsArray($documents[0]->getAttribute('generes'));

        // Alphabetical order
        $sortedDocuments = $movieDocuments;
        \usort($sortedDocuments, function($doc1, $doc2) {
            return strcmp($doc1['$id'], $doc2['$id']);
        });

        $firstDocumentId = $sortedDocuments[0]->getId();
        $lastDocumentId = $sortedDocuments[\count($sortedDocuments) - 1]->getId();

         /**
         * Check $id: Notice, this orders ID names alphabetically, not by internal numeric ID
         */
        $documents = static::getDatabase()->find('movies', [], 25, 0, ['$id'], [Database::ORDER_DESC]);
        $this->assertEquals($lastDocumentId, $documents[0]->getId());
        $documents = static::getDatabase()->find('movies', [], 25, 0, ['$id'], [Database::ORDER_ASC]);
        $this->assertEquals($firstDocumentId, $documents[0]->getId());

        /**
         * Check internal numeric ID sorting
         */
        $documents = static::getDatabase()->find('movies', [], 25, 0, [], [Database::ORDER_DESC]);
        $this->assertEquals($movieDocuments[\count($movieDocuments) - 1]->getId(), $documents[0]->getId());
        $documents = static::getDatabase()->find('movies', [], 25, 0, [], [Database::ORDER_ASC]);
        $this->assertEquals($movieDocuments[0]->getId(), $documents[0]->getId());


        /**
         * Check Permissions
         */
        Authorization::setRole('userx');

        $documents = static::getDatabase()->find('movies');

        $this->assertEquals(6, count($documents));

        /**
         * Check an Integer condition
         */
        $documents = static::getDatabase()->find('movies', [
            new Query('year', Query::TYPE_EQUAL, [2019]),
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);

        /**
         * Boolean condition
         */
        $documents = static::getDatabase()->find('movies', [
            new Query('active', Query::TYPE_EQUAL, [true]),
        ]);

        $this->assertEquals(4, count($documents));

        /**
         * String condition
         */
        $documents = static::getDatabase()->find('movies', [
            new Query('director', Query::TYPE_EQUAL, ['TBD']),
        ]);

        $this->assertEquals(2, count($documents));

        /**
         * Float condition
         */
        $documents = static::getDatabase()->find('movies', [
            new Query('price', Query::TYPE_LESSER, [26.00]),
            new Query('price', Query::TYPE_GREATER, [25.98]),
        ]);

        // TODO@kodumbeats hacky way to pass mariadb tests
        // Remove when $operator="contains" is supported
        if (static::getAdapterName() === "mongodb")
        {
            /**
             * Array contains condition
             */
            $documents = static::getDatabase()->find('movies', [
                new Query('generes', Query::TYPE_CONTAINS, ['comics']),
            ]);

            $this->assertEquals(2, count($documents));

            /**
             * Array contains OR condition
             */
            $documents = static::getDatabase()->find('movies', [
                new Query('generes', Query::TYPE_CONTAINS, ['comics', 'kids']),
            ]);

            $this->assertEquals(4, count($documents));
        }

        /**
         * Fulltext search
         */
        $success = static::getDatabase()->createIndex('movies', 'name', Database::INDEX_FULLTEXT, ['name']);
        $this->assertEquals(true, $success);

        $documents = static::getDatabase()->find('movies', [
            new Query('name', Query::TYPE_SEARCH, ['captain']),
        ]);

        $this->assertEquals(2, count($documents));

        /**
         * Fulltext search (wildcard)
         */
        // TODO: Looks like the MongoDB implementation is a bit more complex, skipping that for now.
        if (in_array(static::getAdapterName(), ['mysql', 'mariadb'])) {
            $documents = static::getDatabase()->find('movies', [
                new Query('name', Query::TYPE_SEARCH, ['cap']),
            ]);

            $this->assertEquals(2, count($documents));
        }

        /**
         * Multiple conditions
         */
        $documents = static::getDatabase()->find('movies', [
            new Query('director', Query::TYPE_EQUAL, ['TBD']),
            new Query('year', Query::TYPE_EQUAL, [2026]),
        ]);

        $this->assertEquals(1, count($documents));

        /**
         * Multiple conditions and OR values
         */
        $documents = static::getDatabase()->find('movies', [
            new Query('name', Query::TYPE_EQUAL, ['Frozen II', 'Captain Marvel']),
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);

        /**
         * $id condition
         */
        $documents = static::getDatabase()->find('movies', [
            new Query('$id', Query::TYPE_EQUAL, ['frozen']),
        ]);

        $this->assertEquals(1, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);

        /**
         * ORDER BY
         */
        $documents = static::getDatabase()->find('movies', [], 25, 0, ['price', 'name'], [Database::ORDER_DESC]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
        $this->assertEquals('Frozen II', $documents[1]['name']);
        $this->assertEquals('Captain Marvel', $documents[2]['name']);
        $this->assertEquals('Captain America: The First Avenger', $documents[3]['name']);
        $this->assertEquals('Work in Progress', $documents[4]['name']);
        $this->assertEquals('Work in Progress 2', $documents[5]['name']);

        /**
         * ORDER BY natural
         */
        $base = array_reverse(static::getDatabase()->find('movies', [], 25, 0));
        $documents = static::getDatabase()->find('movies', [], 25, 0, [], [Database::ORDER_DESC]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals($base[0]['name'], $documents[0]['name']);
        $this->assertEquals($base[1]['name'], $documents[1]['name']);
        $this->assertEquals($base[2]['name'], $documents[2]['name']);
        $this->assertEquals($base[3]['name'], $documents[3]['name']);
        $this->assertEquals($base[4]['name'], $documents[4]['name']);
        $this->assertEquals($base[5]['name'], $documents[5]['name']);

        /**
         * ORDER BY - Multiple attributes
         */
        $documents = static::getDatabase()->find('movies', [], 25, 0, ['price', 'name'], [Database::ORDER_DESC, Database::ORDER_DESC]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Frozen', $documents[1]['name']);
        $this->assertEquals('Captain Marvel', $documents[2]['name']);
        $this->assertEquals('Captain America: The First Avenger', $documents[3]['name']);
        $this->assertEquals('Work in Progress 2', $documents[4]['name']);
        $this->assertEquals('Work in Progress', $documents[5]['name']);

        /**
         * ORDER BY - After
         */
        $movies = static::getDatabase()->find('movies', [], 25, 0, [], []);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[1]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[3]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[4]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[5]);
        $this->assertEmpty(count($documents));

        /**
         * ORDER BY - Before
         */
        $movies = static::getDatabase()->find('movies', [], 25, 0, [], []);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[5], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[3], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[2], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[1], Database::CURSOR_BEFORE);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [], $movies[0], Database::CURSOR_BEFORE);
        $this->assertEmpty(count($documents));

        /**
         * ORDER BY - After by natural order
         */
        $movies = array_reverse(static::getDatabase()->find('movies', [], 25, 0, [], []));

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[1]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[3]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[4]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[5]);
        $this->assertEmpty(count($documents));

        /**
         * ORDER BY - Before by natural order
         */
        $movies = static::getDatabase()->find('movies', [], 25, 0, [], [Database::ORDER_DESC]);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[5], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[3], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[2], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[1], Database::CURSOR_BEFORE);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, [], [Database::ORDER_DESC], $movies[0], Database::CURSOR_BEFORE);
        $this->assertEmpty(count($documents));

        /**
         * ORDER BY - Single Attribute After
         */
        $movies = static::getDatabase()->find('movies', [], 25, 0, ['year'], [Database::ORDER_DESC]);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[1]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[3]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[4]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[5]);
        $this->assertEmpty(count($documents));

        /**
         * ORDER BY - Single Attribute Before
         */
        $movies = static::getDatabase()->find('movies', [], 25, 0, ['year'], [Database::ORDER_DESC]);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[5], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[3], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[2], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[1], Database::CURSOR_BEFORE);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['year'], [Database::ORDER_DESC], $movies[0], Database::CURSOR_BEFORE);
        $this->assertEmpty(count($documents));


        /**
         * ORDER BY - Multiple Attribute After
         */
        $movies = static::getDatabase()->find('movies', [], 25, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC]);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[1]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[3]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[4]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[5]);
        $this->assertEmpty(count($documents));

        /**
         * ORDER BY - Multiple Attribute Before
         */
        $movies = static::getDatabase()->find('movies', [], 25, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC]);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[5], Database::CURSOR_BEFORE);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[4], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[2], Database::CURSOR_BEFORE);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[1], Database::CURSOR_BEFORE);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [], 2, 0, ['price', 'year'], [Database::ORDER_DESC, Database::ORDER_ASC], $movies[0], Database::CURSOR_BEFORE);
        $this->assertEmpty(count($documents));

        /**
         * Limit
         */
        $documents = static::getDatabase()->find('movies', [], 4, 0, ['name']);

        $this->assertEquals(4, count($documents));
        $this->assertEquals('Captain America: The First Avenger', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
        $this->assertEquals('Frozen', $documents[2]['name']);
        $this->assertEquals('Frozen II', $documents[3]['name']);

        /**
         * Limit + Offset
         */
        $documents = static::getDatabase()->find('movies', [], 4, 2, ['name']);

        $this->assertEquals(4, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
        $this->assertEquals('Frozen II', $documents[1]['name']);
        $this->assertEquals('Work in Progress', $documents[2]['name']);
        $this->assertEquals('Work in Progress 2', $documents[3]['name']);

        /**
         * Test that OR queries are handled correctly
         */
        $documents = static::getDatabase()->find('movies', [
            new Query('director', Query::TYPE_EQUAL, ['TBD', 'Joe Johnston']),
            new Query('year', Query::TYPE_EQUAL, [2025]),
        ]);
        $this->assertEquals(1, count($documents));

        /**
         * ORDER BY - After Exception
         * Must be last assertion in test
         */
        $document = new Document([
            '$collection' => 'other collection'
        ]);

        $this->expectException(Exception::class);
        static::getDatabase()->find('movies', [], 2, 0, [], [], $document);
    }

    /**
     * @depends testFind
     */
    public function testFindOne()
    {
        $document = static::getDatabase()->findOne('movies', [], 2, ['name']);
        $this->assertEquals('Frozen', $document['name']);

        $document = static::getDatabase()->findOne('movies', [], 10);
        $this->assertEquals(false, $document);
    }

    /**
     * @depends testFind
     */
    public function testCount()
    {
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        
        $count = static::getDatabase()->count('movies', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(2, $count);
        
        Authorization::unsetRole('userx');
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(5, $count);
        
        Authorization::disable();
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        Authorization::reset();
        
        Authorization::disable();
        $count = static::getDatabase()->count('movies', [], 3);
        $this->assertEquals(3, $count);
        Authorization::reset();

        /**
         * Test that OR queries are handled correctly
         */
        Authorization::disable();
        $count = static::getDatabase()->count('movies', [
            new Query('director', Query::TYPE_EQUAL, ['TBD', 'Joe Johnston']),
            new Query('year', Query::TYPE_EQUAL, [2025]),
        ]);
        $this->assertEquals(1, $count);
        Authorization::reset();
    }

    /**
     * @depends testFind
     */
    public function testSum()
    {
        Authorization::setRole('userx');
        $sum = static::getDatabase()->sum('movies', 'year', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(2019+2019, $sum);
        $sum = static::getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013+2019+2011+2019+2025+2026, $sum);
        $sum = static::getDatabase()->sum('movies', 'price', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(round(39.50+25.99, 2), round($sum, 2));
        $sum = static::getDatabase()->sum('movies', 'price', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(round(39.50+25.99, 2), round($sum, 2));
        
        $sum = static::getDatabase()->sum('movies', 'year', [new Query('year', Query::TYPE_EQUAL, [2019])], 1);
        $this->assertEquals(2019, $sum);

        Authorization::unsetRole('userx');
        $sum = static::getDatabase()->sum('movies', 'year', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(2019+2019, $sum);
        $sum = static::getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013+2019+2011+2019+2025, $sum);
        $sum = static::getDatabase()->sum('movies', 'price', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(round(39.50+25.99, 2), round($sum, 2));
        $sum = static::getDatabase()->sum('movies', 'price', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(round(39.50+25.99, 2), round($sum, 2));
    }

    public function testEncodeDecode()
    {
        $collection = new Document([
            '$collection' => Database::METADATA,
            '$id' => 'users',
            'name' => 'Users',
            'attributes' => [
                [
                    '$id' => 'name',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 256,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'email',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 1024,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'status',
                    'type' => Database::VAR_INTEGER,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'password',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'passwordUpdate',
                    'type' => Database::VAR_INTEGER,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'registration',
                    'type' => Database::VAR_INTEGER,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'emailVerification',
                    'type' => Database::VAR_BOOLEAN,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'reset',
                    'type' => Database::VAR_BOOLEAN,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'prefs',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json']
                ],
                [
                    '$id' => 'sessions',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => 'tokens',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => 'memberships',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => 'roles',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 128,
                    'signed' => true,
                    'required' => false,
                    'array' => true,
                    'filters' => [],
                ],
                [
                    '$id' => 'tags',
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 128,
                    'signed' => true,
                    'required' => false,
                    'array' => true,
                    'filters' => ['json'],
                ],
            ],
            'indexes' => [
                [
                    '$id' => '_key_email',
                    'type' => Database::INDEX_UNIQUE,
                    'attributes' => ['email'],
                    'lengths' => [1024],
                    'orders' => [Database::ORDER_ASC],
                ]
            ],
        ]);

        $document = new Document([
            '$id' => '608fdbe51361a',
            '$read' => ['role:all'],
            '$write' => ['user:608fdbe51361a'],
            'email' => 'test@example.com',
            'emailVerification' => false,
            'status' => 1,
            'password' => 'randomhash',
            'passwordUpdate' => 1234,
            'registration' => 1234,
            'reset' => false,
            'name' => 'My Name',
            'prefs' => new \stdClass,
            'sessions' => [],
            'tokens' => [],
            'memberships' => [],
            'roles' => [
                'admin',
                'developer',
                'tester',
            ],
            'tags' => [
                ['$id' => '1', 'label' => 'x'],
                ['$id' => '2', 'label' => 'y'],
                ['$id' => '3', 'label' => 'z'],
            ],
        ]);

        $result = static::getDatabase()->encode($collection, $document);

        $this->assertEquals('608fdbe51361a', $result->getAttribute('$id'));
        $this->assertEquals(['role:all'], $result->getAttribute('$read'));
        $this->assertEquals(['user:608fdbe51361a'], $result->getAttribute('$write'));
        $this->assertEquals('test@example.com', $result->getAttribute('email'));
        $this->assertEquals(false, $result->getAttribute('emailVerification'));
        $this->assertEquals(1, $result->getAttribute('status'));
        $this->assertEquals('randomhash', $result->getAttribute('password'));
        $this->assertEquals(1234, $result->getAttribute('passwordUpdate'));
        $this->assertEquals(1234, $result->getAttribute('registration'));
        $this->assertEquals(false, $result->getAttribute('reset'));
        $this->assertEquals('My Name', $result->getAttribute('name'));
        $this->assertEquals('{}', $result->getAttribute('prefs'));
        $this->assertEquals('[]', $result->getAttribute('sessions'));
        $this->assertEquals('[]', $result->getAttribute('tokens'));
        $this->assertEquals('[]', $result->getAttribute('memberships'));
        $this->assertEquals(['admin','developer','tester',], $result->getAttribute('roles'));
        $this->assertEquals(['{"$id":"1","label":"x"}','{"$id":"2","label":"y"}','{"$id":"3","label":"z"}',], $result->getAttribute('tags'));

        $result = static::getDatabase()->decode($collection, $document);

        $this->assertEquals('608fdbe51361a', $result->getAttribute('$id'));
        $this->assertEquals(['role:all'], $result->getAttribute('$read'));
        $this->assertEquals(['user:608fdbe51361a'], $result->getAttribute('$write'));
        $this->assertEquals('test@example.com', $result->getAttribute('email'));
        $this->assertEquals(false, $result->getAttribute('emailVerification'));
        $this->assertEquals(1, $result->getAttribute('status'));
        $this->assertEquals('randomhash', $result->getAttribute('password'));
        $this->assertEquals(1234, $result->getAttribute('passwordUpdate'));
        $this->assertEquals(1234, $result->getAttribute('registration'));
        $this->assertEquals(false, $result->getAttribute('reset'));
        $this->assertEquals('My Name', $result->getAttribute('name'));
        $this->assertEquals([], $result->getAttribute('prefs'));
        $this->assertEquals([], $result->getAttribute('sessions'));
        $this->assertEquals([], $result->getAttribute('tokens'));
        $this->assertEquals([], $result->getAttribute('memberships'));
        $this->assertEquals(['admin','developer','tester',], $result->getAttribute('roles'));
        $this->assertEquals([
            new Document(['$id' => '1', 'label' => 'x']),
            new Document(['$id' => '2', 'label' => 'y']),
            new Document(['$id' => '3', 'label' => 'z']),
        ], $result->getAttribute('tags'));
    }

    /**
     * @depends testCreateDocument
     */
    public function testReadPermissionsSuccess(Document $document)
    {
        $document = static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['role:all'],
            '$write' => ['role:all'],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $this->assertEquals(false, $document->isEmpty());

        Authorization::cleanRoles();

        $document = static::getDatabase()->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $document->isEmpty());
        
        Authorization::setRole('role:all');

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testReadPermissionsFailure(Document $document)
    {
        $this->expectException(ExceptionAuthorization::class);

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['user1'],
            '$write' => ['user1'],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));           

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testWritePermissionsSuccess(Document $document)
    {
        $document = static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['role:all'],
            '$write' => ['role:all'],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $this->assertEquals(false, $document->isEmpty());

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testWritePermissionsFailure(Document $document)
    {
        $this->expectException(ExceptionAuthorization::class);

        Authorization::cleanRoles();

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['role:all'],
            '$write' => ['role:all'],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testWritePermissionsUpdateFailure(Document $document)
    {
        $this->expectException(ExceptionAuthorization::class);

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['role:all'],
            '$write' => ['role:all'],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        Authorization::cleanRoles();

        $document = static::getDatabase()->updateDocument('documents', $document->getId(), new Document([
            '$id' => $document->getId(),
            '$read' => ['role:all'],
            '$write' => ['role:all'],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        return $document;
    }

    public function testExceptionAttributeLimit()
    {
        if ($this->getDatabase()->getAttributeLimit() > 0) {
            // load the collection up to the limit
            $attributes = [];
            for ($i=0; $i < $this->getDatabase()->getAttributeLimit(); $i++) {
                $attributes[] = new Document([
                    '$id' => "test{$i}",
                    'type' => Database::VAR_INTEGER,
                    'size' => 0,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }
            $collection = static::getDatabase()->createCollection('attributeLimit', $attributes);

            $this->expectException(LimitException::class);
            $this->assertEquals(false, static::getDatabase()->createAttribute('attributeLimit', "breaking", Database::VAR_INTEGER, 0, true));
        } 

        // Default assertion for other adapters
        $this->assertEquals(1,1);
    }

    /**
     * @depends testExceptionAttributeLimit
     */
    public function testCheckAttributeCountLimit()
    {
        if ($this->getDatabase()->getAttributeLimit() > 0) {
            $collection = static::getDatabase()->getCollection('attributeLimit');

            // create same attribute in testExceptionAttributeLimit
            $attribute = new Document([
                    '$id' => 'breaking',
                    'type' => Database::VAR_INTEGER,
                    'size' => 0,
                    'required' => true,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
            ]);

            $this->expectException(LimitException::class);
            $this->assertEquals(false, static::getDatabase()->checkAttribute($collection, $attribute));
        }

        // Default assertion for other adapters
        $this->assertEquals(1,1);

    }

    /**
     * Using phpunit dataProviders to check that all these combinations of types/sizes throw exceptions
     * https://phpunit.de/manual/3.7/en/writing-tests-for-phpunit.html#writing-tests-for-phpunit.data-providers
     */
    public function rowWidthExceedsMaximum()
    {
        return [
            // These combinations of attributes gets exactly to the 64k limit
            // [$key, $stringSize, $stringCount, $intCount, $floatCount, $boolCount]
            // [0, 1024, 15, 0, 731, 3],
            // [1, 512, 31, 0, 0, 833],
            // [2, 256, 62, 128, 0, 305],
            // [3, 128, 125, 30, 24, 2],
            //
            // Taken 500 bytes off for tests
            [0, 1024, 15, 0, 606, 3],
            [1, 512, 31, 0, 0, 333],
            [2, 256, 62, 103, 0, 5],
            [3, 128, 124, 30, 24, 14],
        ];
    }

    /**
     * @dataProvider rowWidthExceedsMaximum
     * @expectedException LimitException
     */
    public function testExceptionWidthLimit($key, $stringSize, $stringCount, $intCount, $floatCount, $boolCount)
    {
        if (static::getAdapterRowLimit() > 0) {
            $attributes = [];

            // Load the collection up to the limit
            // Strings
            for ($i=0; $i < $stringCount; $i++) {
                $attributes[] = new Document([
                    '$id' => "test_string{$i}",
                    'type' => Database::VAR_STRING,
                    'size' => $stringSize,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            // Integers
            for ($i=0; $i < $intCount; $i++) {
                $attributes[] = new Document([
                    '$id' => "test_int{$i}",
                    'type' => Database::VAR_INTEGER,
                    'size' => 0,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            // Floats
            for ($i=0; $i < $floatCount; $i++) {
                $attributes[] = new Document([
                    '$id' => "test_float{$i}",
                    'type' => Database::VAR_FLOAT,
                    'size' => 0,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            // Booleans
            for ($i=0; $i < $boolCount; $i++) {
                $attributes[] = new Document([
                    '$id' => "test_bool{$i}",
                    'type' => Database::VAR_BOOLEAN,
                    'size' => 0,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            $collection = static::getDatabase()->createCollection("widthLimit{$key}", $attributes);

            $this->expectException(LimitException::class);
            $this->assertEquals(false, static::getDatabase()->createAttribute("widthLimit{$key}", "breaking", Database::VAR_STRING, 100, true));
        } 

        // Default assertion for other adapters
        $this->assertEquals(1,1);
    }

    /**
     * @dataProvider rowWidthExceedsMaximum
     * @depends testExceptionWidthLimit
     */
    public function testCheckAttributeWidthLimit($key, $stringSize, $stringCount, $intCount, $floatCount, $boolCount)
    {
        if (static::getAdapterRowLimit() > 0) {
            $collection = static::getDatabase()->getCollection("widthLimit{$key}");

            // create same attribute in testExceptionWidthLimit
            $attribute = new Document([
                    '$id' => 'breaking',
                    'type' => Database::VAR_STRING,
                    'size' => 100,
                    'required' => true,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
            ]);

            $this->expectException(LimitException::class);
            $this->assertEquals(false, static::getDatabase()->checkAttribute($collection, $attribute));
        }

        // Default assertion for other adapters
        $this->assertEquals(1,1);
    }

    public function testExceptionIndexLimit()
    {
        static::getDatabase()->createCollection('indexLimit');

        // add unique attributes for indexing
        for ($i=0; $i < 64; $i++) {
            $this->assertEquals(true, static::getDatabase()->createAttribute('indexLimit', "test{$i}", Database::VAR_STRING, 16, true));
        }

        // testing for indexLimit = 64
        // MariaDB, MySQL, and MongoDB create 3 indexes per new collection
        // MongoDB create 4 indexes per new collection
        // Add up to the limit, then check if the next index throws IndexLimitException
        for ($i=0; $i < ($this->getDatabase()->getIndexLimit()); $i++) {
            $this->assertEquals(true, static::getDatabase()->createIndex('indexLimit', "index{$i}", Database::INDEX_KEY, ["test{$i}"], [16]));
        }
        $this->expectException(LimitException::class);
        $this->assertEquals(false, static::getDatabase()->createIndex('indexLimit', "index64", Database::INDEX_KEY, ["test64"], [16]));

        static::getDatabase()->deleteCollection('indexLimit');
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionDuplicate(Document $document)
    {
        $document->setAttribute('$id', 'duplicated');
        static::getDatabase()->createDocument($document->getCollection(), $document);

        $this->expectException(DuplicateException::class);
        static::getDatabase()->createDocument($document->getCollection(), $document);
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionCaseInsensitiveDuplicate(Document $document)
    {
        $document->setAttribute('$id', 'caseSensitive');
        static::getDatabase()->createDocument($document->getCollection(), $document);

        $document->setAttribute('$id', 'CaseSensitive');

        $this->expectException(DuplicateException::class);
        static::getDatabase()->createDocument($document->getCollection(), $document);
        
        return $document;
    }

    /**
     * @depends testFind
     */
    public function testUniqueIndexDuplicate()
    {
        $this->expectException(DuplicateException::class);

        $this->assertEquals(true, static::getDatabase()->createIndex('movies', 'uniqueIndex', Database::INDEX_UNIQUE, ['name'], [128], [Database::ORDER_ASC]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
        ]));
    }

    /**
     * @depends testUniqueIndexDuplicate
     */
    public function testUniqueIndexDuplicateUpdate()
    {
        // create document then update to conflict with index
        $document = static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Frozen 5',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
        ]));

        $this->expectException(DuplicateException::class);

        static::getDatabase()->updateDocument('movies', $document->getId(), $document->setAttribute('name',  'Frozen'));
    }

    public function testGetAttributeLimit()
    {
        if (static::getAdapterName() === 'mariadb' || static::getAdapterName() === 'mysql') {
            $this->assertEquals(1012, $this->getDatabase()->getAttributeLimit());
        } else {
            $this->assertEquals(0, $this->getDatabase()->getAttributeLimit());
        }
    }

    public function testGetIndexLimit()
    {
        $this->assertEquals(61, $this->getDatabase()->getIndexLimit());
    }

    public function testGetId()
    {
        $this->assertEquals(20, strlen($this->getDatabase()->getId()));
        $this->assertEquals(13, strlen($this->getDatabase()->getId(0)));
        $this->assertEquals(13, strlen($this->getDatabase()->getId(-1)));
        $this->assertEquals(23, strlen($this->getDatabase()->getId(10)));

        // ensure two sequential calls to getId do not give the same result
        $this->assertNotEquals($this->getDatabase()->getId(10), $this->getDatabase()->getId(10));
    }
}