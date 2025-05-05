<?php

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
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

}
