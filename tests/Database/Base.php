<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as ExceptionAuthorization;
use Utopia\Database\Exception\Duplicate;
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

    public function testCreateExistsDelete()
    {
        $this->assertEquals(false, static::getDatabase()->exists());
        $this->assertEquals(true, static::getDatabase()->create());
        $this->assertEquals(true, static::getDatabase()->exists());
        $this->assertEquals(true, static::getDatabase()->delete());
        $this->assertEquals(false, static::getDatabase()->exists());
        $this->assertEquals(true, static::getDatabase()->create());
    }

    /**
     * @depends testCreateExistsDelete
     */
    public function testCreateListDeleteCollection()
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors'));

        $this->assertCount(1, static::getDatabase()->listCollections());

        $this->assertEquals(false, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors'));
        $this->assertEquals(true, static::getDatabase()->getCollection('actors')->isEmpty());
    }

    public function testCreateDeleteAttribute()
    {
        static::getDatabase()->createCollection('attributes');

        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string1', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string2', Database::VAR_STRING, 16383+1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string3', Database::VAR_STRING, 65535+1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string4', Database::VAR_STRING, 16777215+1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean', Database::VAR_BOOLEAN, 0, true));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(7, $collection->getAttribute('attributes'));

        // Array
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string_list', Database::VAR_STRING, 128, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer_list', Database::VAR_INTEGER, 0, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float_list', Database::VAR_FLOAT, 0, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean_list', Database::VAR_BOOLEAN, 0, true, null, true, true));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(11, $collection->getAttribute('attributes'));

        // Default values
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string_default', Database::VAR_STRING, 256, false, 'test'));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer_default', Database::VAR_INTEGER, 0, false, 1));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float_default', Database::VAR_FLOAT, 0, false, 1.5));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean_default', Database::VAR_BOOLEAN, 0, false, false));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(15, $collection->getAttribute('attributes'));

        // Delete
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string1'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string2'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string3'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string4'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer'));
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
     * Ensure the collection is removed after use
     * 
     * @depends testInvalidDefaultValues
     */
    public function testCleanupInvalidDefaultValues()
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
        $this->assertEquals(false, static::getDatabase()->createAttribute('attributes', 'bad_format', Database::VAR_STRING, 256, true, null, true, false, json_encode(['name'=>'url'])));
    }

    public function testAddRemoveAttribute()
    {
        static::getDatabase()->createCollection('attributesInQueue');

        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'string1', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'string2', Database::VAR_STRING, 16383+1, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'string3', Database::VAR_STRING, 65535+1, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'string4', Database::VAR_STRING, 16777215+1, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'boolean', Database::VAR_BOOLEAN, 0, true));

        $collection = static::getDatabase()->getCollection('attributesInQueue');
        $this->assertCount(7, $collection->getAttribute('attributesInQueue'));

        // Array
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'string_list', Database::VAR_STRING, 128, true, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'integer_list', Database::VAR_INTEGER, 0, true, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'float_list', Database::VAR_FLOAT, 0, true, true));
        $this->assertEquals(true, static::getDatabase()->addAttributeInQueue('attributesInQueue', 'boolean_list', Database::VAR_BOOLEAN, 0, true, true));

        $collection = static::getDatabase()->getCollection('attributesInQueue');
        $this->assertCount(11, $collection->getAttribute('attributesInQueue'));

        // Delete
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'string1'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'string2'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'string3'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'string4'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'integer'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'float'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'boolean'));

        $collection = static::getDatabase()->getCollection('attributesInQueue');
        $this->assertCount(4, $collection->getAttribute('attributesInQueue'));

        // Delete Array
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'string_list'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'integer_list'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'float_list'));
        $this->assertEquals(true, static::getDatabase()->removeAttributeInQueue('attributesInQueue', 'boolean_list'));

        $collection = static::getDatabase()->getCollection('attributesInQueue');
        $this->assertCount(0, $collection->getAttribute('attributesInQueue'));

        static::getDatabase()->deleteCollection('attributesInQueue');
    }

    public function testCreateDeleteIndex()
    {
        static::getDatabase()->createCollection('indexes');

        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'boolean', Database::VAR_BOOLEAN, 0, true));

        // Indexes
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index2', Database::INDEX_KEY, ['float', 'integer'], [], [Database::ORDER_ASC, Database::ORDER_DESC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index3', Database::INDEX_KEY, ['integer', 'boolean'], [], [Database::ORDER_ASC, Database::ORDER_DESC, Database::ORDER_DESC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index4', Database::INDEX_UNIQUE, ['string'], [128], [Database::ORDER_ASC]));
        
        $collection = static::getDatabase()->getCollection('indexes');
        $this->assertCount(4, $collection->getAttribute('indexes'));

        // Delete Indexes
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index2'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index3'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index4'));

        $collection = static::getDatabase()->getCollection('indexes');
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->deleteCollection('indexes');
    }

    public function testAddRemoveIndexInQueue()
    {
        static::getDatabase()->createCollection('indexesInQueue');

        $this->assertEquals(true, static::getDatabase()->createAttribute('indexesInQueue', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexesInQueue', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexesInQueue', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexesInQueue', 'boolean', Database::VAR_BOOLEAN, 0, true));

        // Indexes
        $this->assertEquals(true, static::getDatabase()->addIndexInQueue('indexesInQueue', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, static::getDatabase()->addIndexInQueue('indexesInQueue', 'index2', Database::INDEX_KEY, ['float', 'integer'], [], [Database::ORDER_ASC, Database::ORDER_DESC]));
        $this->assertEquals(true, static::getDatabase()->addIndexInQueue('indexesInQueue', 'index3', Database::INDEX_KEY, ['integer', 'boolean'], [], [Database::ORDER_ASC, Database::ORDER_DESC, Database::ORDER_DESC]));
        
        $collection = static::getDatabase()->getCollection('indexesInQueue');
        $this->assertCount(3, $collection->getAttribute('indexesInQueue'));

        // Delete Indexes
        $this->assertEquals(true, static::getDatabase()->removeIndexInQueue('indexesInQueue', 'index1'));
        $this->assertEquals(true, static::getDatabase()->removeIndexInQueue('indexesInQueue', 'index2'));
        $this->assertEquals(true, static::getDatabase()->removeIndexInQueue('indexesInQueue', 'index3'));

        $collection = static::getDatabase()->getCollection('indexesInQueue');
        $this->assertCount(0, $collection->getAttribute('indexesInQueue'));

        static::getDatabase()->deleteCollection('indexesInQueue');
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

    public function testCreateDocument()
    {
        static::getDatabase()->createCollection('documents');

        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'boolean', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'colors', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'empty', Database::VAR_STRING, 32, false, null, true, true));

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'string' => 'text📝',
            'integer' => 5,
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
        if ($this->getAdapterName() === "mongodb")
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
    }

    /**
     * @depends testFind
     */
    public function testFindFirst()
    {
        $document = static::getDatabase()->findFirst('movies', [], 4, 2, ['name']);
        $this->assertEquals('Frozen', $document['name']);

        $document = static::getDatabase()->findFirst('movies', [], 4, 10);
        $this->assertEquals(false, $document);
    }

    /**
     * @depends testFind
     */
    public function testFindLast()
    {
        $document = static::getDatabase()->findLast('movies', [], 4, 2, ['name']);
        $this->assertEquals('Work in Progress 2', $document['name']);

        $document = static::getDatabase()->findLast('movies', [], 4, 10);
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
    }

    public function testEncodeDecode()
    {
        $collection = new Document([
            '$collection' => Database::COLLECTIONS,
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
            'prefs' => new stdClass,
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
            ['$id' => '1', 'label' => 'x'],
            ['$id' => '2', 'label' => 'y'],
            ['$id' => '3', 'label' => 'z'],
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
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        return $document;
    }

    public function testExceptionAttributeLimit()
    {
        if (static::getAdapterName() === 'mariadb' || static::getAdapterName() === 'mysql') {
            // load the collection up to the limit
            $attributes = [];
            for ($i=0; $i < 1012; $i++) {
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

    public function testExceptionIndexLimit()
    {
        static::getDatabase()->createCollection('indexLimit');

        // add unique attributes for indexing
        for ($i=0; $i < 64; $i++) {
            $this->assertEquals(true, static::getDatabase()->createAttribute('indexLimit', "test{$i}", Database::VAR_STRING, 16, true));
        }

        // testing for indexLimit = 64
        // MariaDB, MySQL, and MongoDB create 3 indexes per new collection
        // Add up to the limit, then check if the next index throws IndexLimitException
        for ($i=0; $i < 61; $i++) {
            $this->assertEquals(true, static::getDatabase()->createIndex('indexLimit', "index{$i}", Database::INDEX_KEY, ["test{$i}"], [16]));
        }
        $this->expectException(LimitException::class);
        $this->assertEquals(false, static::getDatabase()->createIndex('indexLimit', "index62", Database::INDEX_KEY, ["test62"], [16]));

        static::getDatabase()->deleteCollection('indexLimit');
    }

    public function testExceptionIndexLimitInQueue()
    {
        static::getDatabase()->createCollection('indexLimitInQueue');

        // add unique attributes for indexing
        for ($i=0; $i < 64; $i++) {
            $this->assertEquals(true, static::getDatabase()->createAttribute('indexLimitInQueue', "test{$i}", Database::VAR_STRING, 16, true));
        }

        // testing for indexLimit = 64
        // MariaDB, MySQL, and MongoDB create 3 indexes per new collection
        // Add up to the limit, then check if the next index throws IndexLimitException
        for ($i=0; $i < 61; $i++) {
            $this->assertEquals(true, static::getDatabase()->addIndexInQueue('indexLimitInQueue', "index{$i}", Database::INDEX_KEY, ["test{$i}"], [16]));
        }
        $this->expectException(LimitException::class);
        $this->assertEquals(false, static::getDatabase()->addIndexInQueue('indexLimitInQueue', "index62", Database::INDEX_KEY, ["test62"], [16]));
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionDuplicate(Document $document)
    {
        $this->expectException(Duplicate::class);

        $document->setAttribute('$id', 'duplicated');
        
        static::getDatabase()->createDocument($document->getCollection(), $document);
        static::getDatabase()->createDocument($document->getCollection(), $document);
        
        return $document;
    }

    /**
     * @depends testFind
     */
    public function testUniqueIndexDuplicate()
    {
        $this->expectException(Duplicate::class);

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
}