<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

trait CollectionTests
{
    public function testCreateExistsDelete(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForSchemas()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));
        $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->create());
    }

    /**
     * @depends testCreateExistsDelete
     */
    public function testCreateListExistsDeleteCollection(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $this->assertCount(1, static::getDatabase()->listCollections());
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase, 'actors'));

        // Collection names should not be unique
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors2', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $this->assertCount(2, static::getDatabase()->listCollections());
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase, 'actors2'));
        $collection = static::getDatabase()->getCollection('actors2');
        $collection->setAttribute('name', 'actors'); // change name to one that exists
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->updateDocument(
            $collection->getCollection(),
            $collection->getId(),
            $collection
        ));
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors2')); // Delete collection when finished
        $this->assertCount(1, static::getDatabase()->listCollections());

        $this->assertEquals(false, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors'));
        $this->assertEquals(true, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase, 'actors'));
    }

    public function testCreateCollectionWithSchema(): void
    {
        $attributes = [
            new Document([
                '$id' => ID::custom('attribute1'),
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute2'),
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute3'),
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
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index2'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute2'],
                'lengths' => [],
                'orders' => ['DESC'],
            ]),
            new Document([
                '$id' => ID::custom('index3'),
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

        // Test collection with dash (+attribute +index)
        $collection2 = static::getDatabase()->createCollection('with-dash', [
            new Document([
                '$id' => ID::custom('attribute-one'),
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [
            new Document([
                '$id' => ID::custom('index-one'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute-one'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ])
        ]);

        $this->assertEquals(false, $collection2->isEmpty());
        $this->assertEquals('with-dash', $collection2->getId());
        $this->assertIsArray($collection2->getAttribute('attributes'));
        $this->assertCount(1, $collection2->getAttribute('attributes'));
        $this->assertEquals('attribute-one', $collection2->getAttribute('attributes')[0]['$id']);
        $this->assertEquals(Database::VAR_STRING, $collection2->getAttribute('attributes')[0]['type']);
        $this->assertIsArray($collection2->getAttribute('indexes'));
        $this->assertCount(1, $collection2->getAttribute('indexes'));
        $this->assertEquals('index-one', $collection2->getAttribute('indexes')[0]['$id']);
        $this->assertEquals(Database::INDEX_KEY, $collection2->getAttribute('indexes')[0]['type']);
        static::getDatabase()->deleteCollection('with-dash');
    }

    public function testCreateCollectionValidator(): void
    {
        $collections = [
            "validatorTest",
            "validator-test",
            "validator_test",
            "validator.test",
        ];

        $attributes = [
            new Document([
                '$id' => ID::custom('attribute1'),
                'type' => Database::VAR_STRING,
                'size' => 2500, // longer than 768
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute-2'),
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute_3'),
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute.4'),
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute5'),
                'type' => Database::VAR_STRING,
                'size' => 2500,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ])
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index-2'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute-2'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index_3'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute_3'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index.4'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute.4'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index_2_attributes'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1', 'attribute5'],
                'lengths' => [200, 300],
                'orders' => ['DESC'],
            ]),
        ];

        foreach ($collections as $id) {
            $collection = static::getDatabase()->createCollection($id, $attributes, $indexes);

            $this->assertEquals(false, $collection->isEmpty());
            $this->assertEquals($id, $collection->getId());

            $this->assertIsArray($collection->getAttribute('attributes'));
            $this->assertCount(5, $collection->getAttribute('attributes'));
            $this->assertEquals('attribute1', $collection->getAttribute('attributes')[0]['$id']);
            $this->assertEquals(Database::VAR_STRING, $collection->getAttribute('attributes')[0]['type']);
            $this->assertEquals('attribute-2', $collection->getAttribute('attributes')[1]['$id']);
            $this->assertEquals(Database::VAR_INTEGER, $collection->getAttribute('attributes')[1]['type']);
            $this->assertEquals('attribute_3', $collection->getAttribute('attributes')[2]['$id']);
            $this->assertEquals(Database::VAR_BOOLEAN, $collection->getAttribute('attributes')[2]['type']);
            $this->assertEquals('attribute.4', $collection->getAttribute('attributes')[3]['$id']);
            $this->assertEquals(Database::VAR_BOOLEAN, $collection->getAttribute('attributes')[3]['type']);

            $this->assertIsArray($collection->getAttribute('indexes'));
            $this->assertCount(5, $collection->getAttribute('indexes'));
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


    public function testCollectionNotFound(): void
    {
        try {
            static::getDatabase()->find('not_exist', []);
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }
    }

    public function testSizeCollection(): void
    {
        static::getDatabase()->createCollection('sizeTest1');
        static::getDatabase()->createCollection('sizeTest2');

        $size1 = static::getDatabase()->getSizeOfCollection('sizeTest1');
        $size2 = static::getDatabase()->getSizeOfCollection('sizeTest2');
        $sizeDifference = abs($size1 - $size2);
        // Size of an empty collection returns either 172032 or 167936 bytes randomly
        // Therefore asserting with a tolerance of 5000 bytes
        $byteDifference = 5000;

        if (!static::getDatabase()->analyzeCollection('sizeTest2')) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->assertLessThan($byteDifference, $sizeDifference);

        static::getDatabase()->createAttribute('sizeTest2', 'string1', Database::VAR_STRING, 20000, true);
        static::getDatabase()->createAttribute('sizeTest2', 'string2', Database::VAR_STRING, 254 + 1, true);
        static::getDatabase()->createAttribute('sizeTest2', 'string3', Database::VAR_STRING, 254 + 1, true);
        static::getDatabase()->createIndex('sizeTest2', 'index', Database::INDEX_KEY, ['string1', 'string2', 'string3'], [128, 128, 128]);

        $loopCount = 100;

        for ($i = 0; $i < $loopCount; $i++) {
            static::getDatabase()->createDocument('sizeTest2', new Document([
                '$id' => 'doc' . $i,
                'string1' => 'string1' . $i . str_repeat('A', 10000),
                'string2' => 'string2',
                'string3' => 'string3',
            ]));
        }

        static::getDatabase()->analyzeCollection('sizeTest2');

        $size2 = $this->getDatabase()->getSizeOfCollection('sizeTest2');

        $this->assertGreaterThan($size1, $size2);

        Authorization::skip(function () use ($loopCount) {
            for ($i = 0; $i < $loopCount; $i++) {
                $this->getDatabase()->deleteDocument('sizeTest2', 'doc' . $i);
            }
        });

        sleep(5);

        static::getDatabase()->analyzeCollection('sizeTest2');

        $size3 = $this->getDatabase()->getSizeOfCollection('sizeTest2');

        $this->assertLessThan($size2, $size3);
    }

    public function testSizeCollectionOnDisk(): void
    {
        $this->getDatabase()->createCollection('sizeTestDisk1');
        $this->getDatabase()->createCollection('sizeTestDisk2');

        $size1 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk1');
        $size2 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk2');
        $sizeDifference = abs($size1 - $size2);
        // Size of an empty collection returns either 172032 or 167936 bytes randomly
        // Therefore asserting with a tolerance of 5000 bytes
        $byteDifference = 5000;
        $this->assertLessThan($byteDifference, $sizeDifference);

        $this->getDatabase()->createAttribute('sizeTestDisk2', 'string1', Database::VAR_STRING, 20000, true);
        $this->getDatabase()->createAttribute('sizeTestDisk2', 'string2', Database::VAR_STRING, 254 + 1, true);
        $this->getDatabase()->createAttribute('sizeTestDisk2', 'string3', Database::VAR_STRING, 254 + 1, true);
        $this->getDatabase()->createIndex('sizeTestDisk2', 'index', Database::INDEX_KEY, ['string1', 'string2', 'string3'], [128, 128, 128]);

        $loopCount = 40;

        for ($i = 0; $i < $loopCount; $i++) {
            $this->getDatabase()->createDocument('sizeTestDisk2', new Document([
                'string1' => 'string1' . $i,
                'string2' => 'string2' . $i,
                'string3' => 'string3' . $i,
            ]));
        }

        $size2 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk2');

        $this->assertGreaterThan($size1, $size2);
    }

    public function testSizeFullText(): void
    {
        // SQLite does not support fulltext indexes
        if (!static::getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('fullTextSizeTest');

        $size1 = static::getDatabase()->getSizeOfCollection('fullTextSizeTest');

        static::getDatabase()->createAttribute('fullTextSizeTest', 'string1', Database::VAR_STRING, 128, true);
        static::getDatabase()->createAttribute('fullTextSizeTest', 'string2', Database::VAR_STRING, 254, true);
        static::getDatabase()->createAttribute('fullTextSizeTest', 'string3', Database::VAR_STRING, 254, true);
        static::getDatabase()->createIndex('fullTextSizeTest', 'index', Database::INDEX_KEY, ['string1', 'string2', 'string3'], [128, 128, 128]);

        $loopCount = 10;

        for ($i = 0; $i < $loopCount; $i++) {
            static::getDatabase()->createDocument('fullTextSizeTest', new Document([
                'string1' => 'string1' . $i,
                'string2' => 'string2' . $i,
                'string3' => 'string3' . $i,
            ]));
        }

        $size2 = static::getDatabase()->getSizeOfCollectionOnDisk('fullTextSizeTest');

        $this->assertGreaterThan($size1, $size2);

        static::getDatabase()->createIndex('fullTextSizeTest', 'fulltext_index', Database::INDEX_FULLTEXT, ['string1']);

        $size3 = static::getDatabase()->getSizeOfCollectionOnDisk('fullTextSizeTest');

        $this->assertGreaterThan($size2, $size3);
    }

    public function testPurgeCollectionCache(): void
    {
        static::getDatabase()->createCollection('redis');

        $this->assertEquals(true, static::getDatabase()->createAttribute('redis', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('redis', 'age', Database::VAR_INTEGER, 0, true));

        static::getDatabase()->createDocument('redis', new Document([
            '$id' => 'doc1',
            'name' => 'Richard',
            'age' => 15,
            '$permissions' => [
                Permission::read(Role::any()),
            ]
        ]));

        $document = static::getDatabase()->getDocument('redis', 'doc1');

        $this->assertEquals('Richard', $document->getAttribute('name'));
        $this->assertEquals(15, $document->getAttribute('age'));

        $this->assertEquals(true, static::getDatabase()->deleteAttribute('redis', 'age'));

        $document = static::getDatabase()->getDocument('redis', 'doc1');
        $this->assertEquals('Richard', $document->getAttribute('name'));
        $this->assertArrayNotHasKey('age', $document);

        $this->assertEquals(true, static::getDatabase()->createAttribute('redis', 'age', Database::VAR_INTEGER, 0, true));

        $document = static::getDatabase()->getDocument('redis', 'doc1');
        $this->assertEquals('Richard', $document->getAttribute('name'));
        $this->assertArrayHasKey('age', $document);
    }

    public function testSchemaAttributes(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForSchemaAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'schema_attributes';
        $db = static::getDatabase();

        $this->assertEmpty($db->getSchemaAttributes('no_such_collection'));

        $db->createCollection($collection);

        $db->createAttribute($collection, 'username', Database::VAR_STRING, 128, true);
        $db->createAttribute($collection, 'story', Database::VAR_STRING, 20000, true);
        $db->createAttribute($collection, 'string_list', Database::VAR_STRING, 128, true, null, true, true);
        $db->createAttribute($collection, 'dob', Database::VAR_DATETIME, 0, false, '2000-06-12T14:12:55.000+00:00', true, false, null, [], ['datetime']);

        $attributes = [];
        foreach ($db->getSchemaAttributes($collection) as $attribute) {
            /**
             * @var Document $attribute
             */
            $attributes[$attribute->getAttribute('columnName')] = $attribute;
        }

        $attribute = $attributes['username'];
        $this->assertEquals('username', $attribute['columnName']);
        $this->assertEquals('varchar', $attribute['dataType']);
        $this->assertEquals('varchar(128)', $attribute['columnType']);
        $this->assertEquals('128', $attribute['characterMaximumLength']);
        $this->assertEquals('YES', $attribute['isNullable']);

        $attribute = $attributes['story'];
        $this->assertEquals('story', $attribute['columnName']);
        $this->assertEquals('text', $attribute['dataType']);
        $this->assertEquals('text', $attribute['columnType']);
        $this->assertEquals('65535', $attribute['characterMaximumLength']);

        $attribute = $attributes['string_list'];
        $this->assertEquals('string_list', $attribute['columnName']);
        $this->assertTrue(in_array($attribute['dataType'], ['json', 'longtext'])); // mysql vs maria
        $this->assertTrue(in_array($attribute['columnType'], ['json', 'longtext']));
        $this->assertTrue(in_array($attribute['characterMaximumLength'], [null, '4294967295']));
        $this->assertEquals('YES', $attribute['isNullable']);

        $attribute = $attributes['dob'];
        $this->assertEquals('dob', $attribute['columnName']);
        $this->assertEquals('datetime', $attribute['dataType']);
        $this->assertEquals('datetime(3)', $attribute['columnType']);
        $this->assertEquals(null, $attribute['characterMaximumLength']);
        $this->assertEquals('3', $attribute['datetimePrecision']);

        if ($db->getSharedTables()) {
            $attribute = $attributes['_tenant'];
            $this->assertEquals('_tenant', $attribute['columnName']);
            $this->assertEquals('int', $attribute['dataType']);
            $this->assertEquals('10', $attribute['numericPrecision']);
            $this->assertTrue(in_array($attribute['columnType'], ['int unsigned', 'int(11) unsigned']));
        }
    }

    public function testRowSizeToLarge(): void
    {
        if (static::getDatabase()->getAdapter()->getDocumentSizeLimit() === 0) {
            $this->expectNotToPerformAssertions();
            return;
        }
        /**
         * getDocumentSizeLimit = 65535
         * 65535 / 4 = 16383 MB4
         */
        $collection_1 = static::getDatabase()->createCollection('row_size_1');
        $collection_2 = static::getDatabase()->createCollection('row_size_2');

        $this->assertEquals(true, static::getDatabase()->createAttribute($collection_1->getId(), 'attr_1', Database::VAR_STRING, 16000, true));

        try {
            static::getDatabase()->createAttribute($collection_1->getId(), 'attr_2', Database::VAR_STRING, Database::LENGTH_KEY, true);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        }

        /**
         * Relation takes length of Database::LENGTH_KEY so exceeding getDocumentSizeLimit
         */

        try {
            static::getDatabase()->createRelationship(
                collection: $collection_2->getId(),
                relatedCollection: $collection_1->getId(),
                type: Database::RELATION_ONE_TO_ONE,
                twoWay: true,
            );

            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        }

        try {
            static::getDatabase()->createRelationship(
                collection: $collection_1->getId(),
                relatedCollection: $collection_2->getId(),
                type: Database::RELATION_ONE_TO_ONE,
                twoWay: true,
            );

            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        }
    }

    public function testCreateCollectionWithSchemaIndexes(): void
    {
        $database = static::getDatabase();

        $attributes = [
            new Document([
                '$id' => ID::custom('username'),
                'type' => Database::VAR_STRING,
                'size' => 100,
                'required' => false,
                'signed' => true,
                'array' => false,
            ]),
            new Document([
                '$id' => ID::custom('cards'),
                'type' => Database::VAR_STRING,
                'size' => 5000,
                'required' => false,
                'signed' => true,
                'array' => true,
            ]),
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('idx_cards'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['cards'],
                'lengths' => [500], // Will be changed to Database::ARRAY_INDEX_LENGTH (255)
                'orders' => [Database::ORDER_DESC],
            ]),
            new Document([
                '$id' => ID::custom('idx_username'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['username'],
                'lengths' => [100], // Will be removed since equal to attributes size
                'orders' => [],
            ]),
            new Document([
                '$id' => ID::custom('idx_username_created_at'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['username'],
                'lengths' => [99], // Length not equal to attributes length
                'orders' => [Database::ORDER_DESC],
            ]),
        ];

        $collection = $database->createCollection(
            'collection98',
            $attributes,
            $indexes,
            permissions: [
                Permission::create(Role::any()),
            ]
        );

        $this->assertEquals($collection->getAttribute('indexes')[0]['attributes'][0], 'cards');
        $this->assertEquals($collection->getAttribute('indexes')[0]['lengths'][0], Database::ARRAY_INDEX_LENGTH);
        $this->assertEquals($collection->getAttribute('indexes')[0]['orders'][0], null);

        $this->assertEquals($collection->getAttribute('indexes')[1]['attributes'][0], 'username');
        $this->assertEquals($collection->getAttribute('indexes')[1]['lengths'][0], null);

        $this->assertEquals($collection->getAttribute('indexes')[2]['attributes'][0], 'username');
        $this->assertEquals($collection->getAttribute('indexes')[2]['lengths'][0], 99);
        $this->assertEquals($collection->getAttribute('indexes')[2]['orders'][0], Database::ORDER_DESC);
    }

    public function testCollectionUpdate(): Document
    {
        $collection = static::getDatabase()->createCollection('collectionUpdate', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: false);

        $this->assertInstanceOf(Document::class, $collection);

        $collection = static::getDatabase()->getCollection('collectionUpdate');

        $this->assertFalse($collection->getAttribute('documentSecurity'));
        $this->assertIsArray($collection->getPermissions());
        $this->assertCount(4, $collection->getPermissions());

        $collection = static::getDatabase()->updateCollection('collectionUpdate', [], true);

        $this->assertTrue($collection->getAttribute('documentSecurity'));
        $this->assertIsArray($collection->getPermissions());
        $this->assertEmpty($collection->getPermissions());

        $collection = static::getDatabase()->getCollection('collectionUpdate');

        $this->assertTrue($collection->getAttribute('documentSecurity'));
        $this->assertIsArray($collection->getPermissions());
        $this->assertEmpty($collection->getPermissions());

        return $collection;
    }

    public function testUpdateDeleteCollectionNotFound(): void
    {
        try {
            static::getDatabase()->deleteCollection('not_found');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        try {
            static::getDatabase()->updateCollection('not_found', [], true);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }
    }

    public function testGetCollectionId(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForGetConnectionId()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->assertIsString(static::getDatabase()->getConnectionId());
    }

    public function testKeywords(): void
    {
        $database = static::getDatabase();
        $keywords = $database->getKeywords();

        // Collection name tests
        $attributes = [
            new Document([
                '$id' => ID::custom('attribute1'),
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ]),
        ];

        foreach ($keywords as $keyword) {
            $collection = $database->createCollection($keyword, $attributes, $indexes);
            $this->assertEquals($keyword, $collection->getId());

            $document = $database->createDocument($keyword, new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                '$id' => ID::custom('helloWorld'),
                'attribute1' => 'Hello World',
            ]));
            $this->assertEquals('helloWorld', $document->getId());

            $document = $database->getDocument($keyword, 'helloWorld');
            $this->assertEquals('helloWorld', $document->getId());

            $documents = $database->find($keyword);
            $this->assertCount(1, $documents);
            $this->assertEquals('helloWorld', $documents[0]->getId());

            $collection = $database->deleteCollection($keyword);
            $this->assertTrue($collection);
        }

        // TODO: updateCollection name tests

        // Attribute name tests
        foreach ($keywords as $keyword) {
            $collectionName = 'rk' . $keyword; // rk is short-hand for reserved-keyword. We do this sicne there are some limits (64 chars max)

            $collection = $database->createCollection($collectionName);
            $this->assertEquals($collectionName, $collection->getId());

            $attribute = static::getDatabase()->createAttribute($collectionName, $keyword, Database::VAR_STRING, 128, true);
            $this->assertEquals(true, $attribute);

            $document = new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                '$id' => 'reservedKeyDocument'
            ]);
            $document->setAttribute($keyword, 'Reserved:' . $keyword);

            $document = $database->createDocument($collectionName, $document);
            $this->assertEquals('reservedKeyDocument', $document->getId());
            $this->assertEquals('Reserved:' . $keyword, $document->getAttribute($keyword));

            $document = $database->getDocument($collectionName, 'reservedKeyDocument');
            $this->assertEquals('reservedKeyDocument', $document->getId());
            $this->assertEquals('Reserved:' . $keyword, $document->getAttribute($keyword));

            $documents = $database->find($collectionName);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());
            $this->assertEquals('Reserved:' . $keyword, $documents[0]->getAttribute($keyword));

            $documents = $database->find($collectionName, [Query::equal($keyword, ["Reserved:{$keyword}"])]);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());

            $documents = $database->find($collectionName, [
                Query::orderDesc($keyword)
            ]);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());

            $collection = $database->deleteCollection($collectionName);
            $this->assertTrue($collection);
        }
    }

    public function testLabels(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection(
            'labels_test',
        ));
        static::getDatabase()->createAttribute('labels_test', 'attr1', Database::VAR_STRING, 10, false);

        static::getDatabase()->createDocument('labels_test', new Document([
            '$id' => 'doc1',
            'attr1' => 'value1',
            '$permissions' => [
                Permission::read(Role::label('reader')),
            ],
        ]));

        $documents = static::getDatabase()->find('labels_test');

        $this->assertEmpty($documents);

        Authorization::setRole(Role::label('reader')->toString());

        $documents = static::getDatabase()->find('labels_test');

        $this->assertCount(1, $documents);
    }

    public function testMetadata(): void
    {
        static::getDatabase()->setMetadata('key', 'value');

        static::getDatabase()->createCollection('testers');

        $this->assertEquals(['key' => 'value'], static::getDatabase()->getMetadata());

        static::getDatabase()->resetMetadata();

        $this->assertEquals([], static::getDatabase()->getMetadata());
    }

    public function testDeleteCollectionDeletesRelationships(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('devices');

        static::getDatabase()->createRelationship(
            collection: 'testers',
            relatedCollection: 'devices',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'tester'
        );

        $testers = static::getDatabase()->getCollection('testers');
        $devices = static::getDatabase()->getCollection('devices');

        $this->assertEquals(1, \count($testers->getAttribute('attributes')));
        $this->assertEquals(1, \count($devices->getAttribute('attributes')));
        $this->assertEquals(1, \count($devices->getAttribute('indexes')));

        static::getDatabase()->deleteCollection('testers');

        $testers = static::getDatabase()->getCollection('testers');
        $devices = static::getDatabase()->getCollection('devices');

        $this->assertEquals(true, $testers->isEmpty());
        $this->assertEquals(0, \count($devices->getAttribute('attributes')));
        $this->assertEquals(0, \count($devices->getAttribute('indexes')));
    }


    public function testCascadeMultiDelete(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('cascadeMultiDelete1');
        static::getDatabase()->createCollection('cascadeMultiDelete2');
        static::getDatabase()->createCollection('cascadeMultiDelete3');

        static::getDatabase()->createRelationship(
            collection: 'cascadeMultiDelete1',
            relatedCollection: 'cascadeMultiDelete2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        static::getDatabase()->createRelationship(
            collection: 'cascadeMultiDelete2',
            relatedCollection: 'cascadeMultiDelete3',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        $root = static::getDatabase()->createDocument('cascadeMultiDelete1', new Document([
            '$id' => 'cascadeMultiDelete1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any())
            ],
            'cascadeMultiDelete2' => [
                [
                    '$id' => 'cascadeMultiDelete2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::delete(Role::any())
                    ],
                    'cascadeMultiDelete3' => [
                        [
                            '$id' => 'cascadeMultiDelete3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                                Permission::delete(Role::any())
                            ],
                        ],
                    ],
                ],
            ],
        ]));

        $this->assertCount(1, $root->getAttribute('cascadeMultiDelete2'));
        $this->assertCount(1, $root->getAttribute('cascadeMultiDelete2')[0]->getAttribute('cascadeMultiDelete3'));

        $this->assertEquals(true, static::getDatabase()->deleteDocument('cascadeMultiDelete1', $root->getId()));

        $multi2 = static::getDatabase()->getDocument('cascadeMultiDelete2', 'cascadeMultiDelete2');
        $this->assertEquals(true, $multi2->isEmpty());

        $multi3 = static::getDatabase()->getDocument('cascadeMultiDelete3', 'cascadeMultiDelete3');
        $this->assertEquals(true, $multi3->isEmpty());
    }

    /**
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws QueryException
     * @throws StructureException
     * @throws TimeoutException
     */
    public function testSharedTables(): void
    {
        /**
         * Default mode already tested, we'll test 'schema' and 'table' isolation here
         */
        $database = static::getDatabase();
        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();

        if (!$database->getAdapter()->getSupportForSchemas()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if ($database->exists('schema1')) {
            $database->setDatabase('schema1')->delete();
        }
        if ($database->exists('schema2')) {
            $database->setDatabase('schema2')->delete();
        }
        if ($database->exists('sharedTables')) {
            $database->setDatabase('sharedTables')->delete();
        }

        /**
         * Schema
         */
        $database
            ->setDatabase('schema1')
            ->setNamespace('')
            ->create();

        $this->assertEquals(true, $database->exists('schema1'));

        $database
            ->setDatabase('schema2')
            ->setNamespace('')
            ->create();

        $this->assertEquals(true, $database->exists('schema2'));

        /**
         * Table
         */
        $tenant1 = 1;
        $tenant2 = 2;

        $database
            ->setDatabase('sharedTables')
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant($tenant1)
            ->create();

        $this->assertEquals(true, $database->exists('sharedTables'));

        $database->createCollection('people', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 128,
                'required' => true,
            ]),
            new Document([
                '$id' => 'lifeStory',
                'type' => Database::VAR_STRING,
                'size' => 65536,
                'required' => true,
            ])
        ], [
            new Document([
                '$id' => 'idx_name',
                'type' => Database::INDEX_KEY,
                'attributes' => ['name']
            ])
        ], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $this->assertCount(1, $database->listCollections());

        if ($database->getAdapter()->getSupportForFulltextIndex()) {
            $database->createIndex(
                collection: 'people',
                id: 'idx_lifeStory',
                type: Database::INDEX_FULLTEXT,
                attributes: ['lifeStory']
            );
        }

        $docId = ID::unique();

        $database->createDocument('people', new Document([
            '$id' => $docId,
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Spiderman',
            'lifeStory' => 'Spider-Man is a superhero appearing in American comic books published by Marvel Comics.'
        ]));

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals('Spiderman', $doc['name']);
        $this->assertEquals($tenant1, $doc->getTenant());

        /**
         * Remove Permissions
         */
        $doc->setAttribute('$permissions', [
            Permission::read(Role::any())
        ]);

        $database->updateDocument('people', $docId, $doc);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals([Permission::read(Role::any())], $doc['$permissions']);
        $this->assertEquals($tenant1, $doc->getTenant());

        /**
         * Add Permissions
         */
        $doc->setAttribute('$permissions', [
            Permission::read(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->updateDocument('people', $docId, $doc);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals([Permission::read(Role::any()), Permission::delete(Role::any())], $doc['$permissions']);

        $docs = $database->find('people');
        $this->assertCount(1, $docs);

        // Swap to tenant 2, no access
        $database->setTenant($tenant2);

        try {
            $database->getDocument('people', $docId);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        try {
            $database->find('people');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        $this->assertCount(0, $database->listCollections());

        // Swap back to tenant 1, allowed
        $database->setTenant($tenant1);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals('Spiderman', $doc['name']);
        $docs = $database->find('people');
        $this->assertEquals(1, \count($docs));

        // Remove tenant but leave shared tables enabled
        $database->setTenant(null);

        try {
            $database->getDocument('people', $docId);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        // Reset state
        $database
            ->setSharedTables($sharedTables)
            ->setNamespace($namespace)
            ->setDatabase($schema);
    }
    /**
     * @throws LimitException
     * @throws DuplicateException
     * @throws DatabaseException
     */
    public function testCreateDuplicates(): void
    {
        static::getDatabase()->createCollection('duplicates', permissions: [
            Permission::read(Role::any())
        ]);

        try {
            static::getDatabase()->createCollection('duplicates');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $this->assertNotEmpty(static::getDatabase()->listCollections());

        static::getDatabase()->deleteCollection('duplicates');
    }
    public function testSharedTablesDuplicates(): void
    {
        $database = static::getDatabase();
        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();

        if (!$database->getAdapter()->getSupportForSchemas()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if ($database->exists('sharedTables')) {
            $database->setDatabase('sharedTables')->delete();
        }

        $database
            ->setDatabase('sharedTables')
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant(null)
            ->create();

        // Create collection
        $database->createCollection('duplicates', documentSecurity: false);
        $database->createAttribute('duplicates', 'name', Database::VAR_STRING, 10, false);
        $database->createIndex('duplicates', 'nameIndex', Database::INDEX_KEY, ['name']);

        $database->setTenant(2);

        try {
            $database->createCollection('duplicates', documentSecurity: false);
        } catch (DuplicateException) {
            // Ignore
        }

        try {
            $database->createAttribute('duplicates', 'name', Database::VAR_STRING, 10, false);
        } catch (DuplicateException) {
            // Ignore
        }

        try {
            $database->createIndex('duplicates', 'nameIndex', Database::INDEX_KEY, ['name']);
        } catch (DuplicateException) {
            // Ignore
        }

        $collection = $database->getCollection('duplicates');
        $this->assertEquals(1, \count($collection->getAttribute('attributes')));
        $this->assertEquals(1, \count($collection->getAttribute('indexes')));

        $database->setTenant(1);

        $collection = $database->getCollection('duplicates');
        $this->assertEquals(1, \count($collection->getAttribute('attributes')));
        $this->assertEquals(1, \count($collection->getAttribute('indexes')));

        $database
            ->setSharedTables($sharedTables)
            ->setNamespace($namespace)
            ->setDatabase($schema);
    }

    public function testEvents(): void
    {
        Authorization::skip(function () {
            $database = static::getDatabase();

            $events = [
                Database::EVENT_DATABASE_CREATE,
                Database::EVENT_DATABASE_LIST,
                Database::EVENT_COLLECTION_CREATE,
                Database::EVENT_COLLECTION_LIST,
                Database::EVENT_COLLECTION_READ,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_ATTRIBUTE_CREATE,
                Database::EVENT_ATTRIBUTE_UPDATE,
                Database::EVENT_INDEX_CREATE,
                Database::EVENT_DOCUMENT_CREATE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_UPDATE,
                Database::EVENT_DOCUMENT_READ,
                Database::EVENT_DOCUMENT_FIND,
                Database::EVENT_DOCUMENT_FIND,
                Database::EVENT_DOCUMENT_COUNT,
                Database::EVENT_DOCUMENT_SUM,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_INCREASE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_DECREASE,
                Database::EVENT_DOCUMENTS_CREATE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENTS_UPDATE,
                Database::EVENT_INDEX_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENTS_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_ATTRIBUTE_DELETE,
                Database::EVENT_COLLECTION_DELETE,
                Database::EVENT_DATABASE_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENTS_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_ATTRIBUTE_DELETE,
                Database::EVENT_COLLECTION_DELETE,
                Database::EVENT_DATABASE_DELETE
            ];

            $database->on(Database::EVENT_ALL, 'test', function ($event, $data) use (&$events) {
                $shifted = array_shift($events);
                $this->assertEquals($shifted, $event);
            });

            if ($this->getDatabase()->getAdapter()->getSupportForSchemas()) {
                $database->setDatabase('hellodb');
                $database->create();
            } else {
                \array_shift($events);
            }

            $database->list();

            $database->setDatabase($this->testDatabase);

            $collectionId = ID::unique();
            $database->createCollection($collectionId);
            $database->listCollections();
            $database->getCollection($collectionId);
            $database->createAttribute($collectionId, 'attr1', Database::VAR_INTEGER, 2, false);
            $database->updateAttributeRequired($collectionId, 'attr1', true);
            $indexId1 = 'index2_' . uniqid();
            $database->createIndex($collectionId, $indexId1, Database::INDEX_KEY, ['attr1']);

            $document = $database->createDocument($collectionId, new Document([
                '$id' => 'doc1',
                'attr1' => 10,
                '$permissions' => [
                    Permission::delete(Role::any()),
                    Permission::update(Role::any()),
                    Permission::read(Role::any()),
                ],
            ]));

            $executed = false;
            $database->on(Database::EVENT_ALL, 'should-not-execute', function ($event, $data) use (&$executed) {
                $executed = true;
            });

            $database->silent(function () use ($database, $collectionId, $document) {
                $database->updateDocument($collectionId, 'doc1', $document->setAttribute('attr1', 15));
                $database->getDocument($collectionId, 'doc1');
                $database->find($collectionId);
                $database->findOne($collectionId);
                $database->count($collectionId);
                $database->sum($collectionId, 'attr1');
                $database->increaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
                $database->decreaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
            }, ['should-not-execute']);

            $this->assertFalse($executed);

            $database->createDocuments($collectionId, [
                new Document([
                    'attr1' => 10,
                ]),
                new Document([
                    'attr1' => 20,
                ]),
            ]);

            $database->updateDocuments($collectionId, new Document([
                'attr1' => 15,
            ]));

            $database->deleteIndex($collectionId, $indexId1);
            $database->deleteDocument($collectionId, 'doc1');

            $database->deleteDocuments($collectionId);
            $database->deleteAttribute($collectionId, 'attr1');
            $database->deleteCollection($collectionId);
            $database->delete('hellodb');

            // Remove all listeners
            $database->on(Database::EVENT_ALL, 'test', null);
            $database->on(Database::EVENT_ALL, 'should-not-execute', null);
        });
    }

    public function testCreatedAtUpdatedAt(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('created_at'));
        static::getDatabase()->createAttribute('created_at', 'title', Database::VAR_STRING, 100, false);
        $document = static::getDatabase()->createDocument('created_at', new Document([
            '$id' => ID::custom('uid123'),

            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $this->assertNotEmpty($document->getInternalId());
        $this->assertNotNull($document->getInternalId());
    }

    /**
     * @depends testCreatedAtUpdatedAt
     */
    public function testCreatedAtUpdatedAtAssert(): void
    {
        $document = static::getDatabase()->getDocument('created_at', 'uid123');
        $this->assertEquals(true, !$document->isEmpty());
        sleep(1);
        $document->setAttribute('title', 'new title');
        static::getDatabase()->updateDocument('created_at', 'uid123', $document);
        $document = static::getDatabase()->getDocument('created_at', 'uid123');

        $this->assertGreaterThan($document->getCreatedAt(), $document->getUpdatedAt());
        $this->expectException(DuplicateException::class);

        static::getDatabase()->createCollection('created_at');
    }


    public function testTransformations(): void
    {
        static::getDatabase()->createCollection('docs', attributes: [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 767,
                'required' => true,
            ])
        ]);

        static::getDatabase()->createDocument('docs', new Document([
            '$id' => 'doc1',
            'name' => 'value1',
        ]));

        static::getDatabase()->before(Database::EVENT_DOCUMENT_READ, 'test', function (string $query) {
            return "SELECT 1";
        });

        $result = static::getDatabase()->getDocument('docs', 'doc1');

        $this->assertTrue($result->isEmpty());
    }
}
