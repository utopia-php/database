<?php

namespace Utopia\Tests;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as ExceptionAuthorization;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Query;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\DatetimeValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Validator;
use Utopia\Validator\Range;
use Utopia\Database\Exception\Structure as StructureException;


abstract class Base extends TestCase
{
    /**
     * @return Database
     */
    abstract static protected function getDatabase(): Database;

    /**
     * @return string
     */
    abstract static protected function getAdapterName(): string;

    public function setUp(): void
    {
        Authorization::setRole('any');
    }

    public function tearDown(): void
    {
        Authorization::setDefaultStatus(true);
    }

    protected string $testDatabase = 'utopiaTests';

    public function testPing()
    {
        $this->assertEquals(true, static::getDatabase()->ping());
    }

    public function testCreateExistsDelete()
    {
        $schemaSupport = $this->getDatabase()->getAdapter()->getSupportForSchemas();
        if (!$schemaSupport) {
            $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
            $this->assertEquals(true, static::getDatabase()->create());
            return;
        }

        if (!static::getDatabase()->exists($this->testDatabase)) {
            $this->assertEquals(true, static::getDatabase()->create());
        }
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));
        $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->create());
    }

    public function testCreatedAtUpdatedAt()
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('created_at'));

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
     * @depends testCreateExistsDelete
     */
    public function testCreateListExistsDeleteCollection()
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors'));

        $this->assertCount(2, static::getDatabase()->listCollections());
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase, 'actors'));

        // Collection names should not be unique
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors2'));
        $this->assertCount(3, static::getDatabase()->listCollections());
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase, 'actors2'));
        $collection = static::getDatabase()->getCollection('actors2');
        $collection->setAttribute('name', 'actors'); // change name to one that exists
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->updateDocument($collection->getCollection(), $collection->getId(), $collection));
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors2')); // Delete collection when finished
        $this->assertCount(2, static::getDatabase()->listCollections());

        $this->assertEquals(false, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors'));
        $this->assertEquals(true, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase, 'actors'));
    }

    public function testCreateDeleteAttribute()
    {
        static::getDatabase()->createCollection('attributes');

        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string1', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string2', Database::VAR_STRING, 16383 + 1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string3', Database::VAR_STRING, 65535 + 1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string4', Database::VAR_STRING, 16777215 + 1, true));
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
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'datetime_default', Database::VAR_DATETIME, 0, false, '2000-06-12T14:12:55.000+00:00', true, false, null, [], ['datetime']));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(17, $collection->getAttribute('attributes'));

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
        $this->assertCount(9, $collection->getAttribute('attributes'));

        // Delete Array
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean_list'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(5, $collection->getAttribute('attributes'));

        // Delete default
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'datetime_default'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(0, $collection->getAttribute('attributes'));

        // Test for custom chars in ID
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as_5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as5dasdasdas_', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', '.as5dasdasdas', Database::VAR_BOOLEAN, 0, true));
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
     * @depends      testCreateDeleteAttribute
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
        $this->assertEquals(1, 1);
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
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index5', Database::INDEX_UNIQUE, ['$id', 'string'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'order', Database::INDEX_UNIQUE, ['order'], [128], [Database::ORDER_ASC]));

        $collection = static::getDatabase()->getCollection('indexes');
        $this->assertCount(6, $collection->getAttribute('indexes'));

        // Delete Indexes
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index2'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index3'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index4'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index5'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'order'));

        $collection = static::getDatabase()->getCollection('indexes');
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->deleteCollection('indexes');
    }

    public function testCreateCollectionWithSchema()
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
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'with-dash', Database::VAR_STRING, 128, false, null));

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user(ID::custom('1'))),
                Permission::read(Role::user(ID::custom('2'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
            'with-dash' => 'Works',
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
        $this->assertEquals('Works', $document->getAttribute('with-dash'));

        return $document;
    }

    public function testRespectNulls()
    {
        static::getDatabase()->createCollection('documents_nulls');

        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'string', Database::VAR_STRING, 128, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'integer', Database::VAR_INTEGER, 0, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'bigint', Database::VAR_INTEGER, 8, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'float', Database::VAR_FLOAT, 0, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'boolean', Database::VAR_BOOLEAN, 0, false));

        $document = static::getDatabase()->createDocument('documents_nulls', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
        ]));

        $this->assertNotEmpty(true, $document->getId());
        $this->assertNull($document->getAttribute('string'));
        $this->assertNull($document->getAttribute('integer'));
        $this->assertNull($document->getAttribute('bigint'));
        $this->assertNull($document->getAttribute('float'));
        $this->assertNull($document->getAttribute('boolean'));
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
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'datetime', Database::VAR_DATETIME, 0, false, '2000-06-12T14:12:55.000+00:00', true, false, null, [], ['datetime']));

        $document = static::getDatabase()->createDocument('defaults', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $document2 = static::getDatabase()->getDocument('defaults', $document->getId());
        $this->assertCount(4, $document2->getPermissions());
        $this->assertEquals('read("any")', $document2->getPermissions()[0]);
        $this->assertEquals('create("any")', $document2->getPermissions()[1]);
        $this->assertEquals('update("any")', $document2->getPermissions()[2]);
        $this->assertEquals('delete("any")', $document2->getPermissions()[3]);

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
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $document->getAttribute('datetime'));

        // cleanup collection
        static::getDatabase()->deleteCollection('defaults');
    }

    /**
     * @throws ExceptionAuthorization
     * @throws LimitException
     * @throws DuplicateException
     * @throws StructureException
     * @throws Exception
     */
    public function testIncreaseDecrease()
    {
        $collection = 'increase_decrease';
        static::getDatabase()->createCollection($collection);

        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'increase', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'decrease', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'increase_text', Database::VAR_STRING, 255, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'increase_float', Database::VAR_FLOAT, 0, true));

        $document = static::getDatabase()->createDocument($collection, new Document([
            'increase' => 100,
            'decrease' => 100,
            'increase_float' => 100,
            'increase_text' => "some text",
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ]
        ]));

        $this->assertEquals(true, static::getDatabase()->increaseDocumentAttribute($collection, $document->getId(), 'increase', 1, 101));

        $document = static::getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(101, $document->getAttribute('increase'));

        $this->assertEquals(true, static::getDatabase()->decreaseDocumentAttribute($collection, $document->getId(), 'decrease', 1, 98));
        $document = static::getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(99, $document->getAttribute('decrease'));

        $this->assertEquals(true, static::getDatabase()->increaseDocumentAttribute($collection, $document->getId(), 'increase_float', 5.5, 110));
        $document = static::getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(105.5, $document->getAttribute('increase_float'));

        $this->assertEquals(true, static::getDatabase()->decreaseDocumentAttribute($collection, $document->getId(), 'increase_float', 1.1, 100));
        $document = static::getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(104.4, $document->getAttribute('increase_float'));

        return $document;
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseLimitMax(Document $document)
    {
        $this->expectException(Exception::class);
        $this->assertEquals(true, static::getDatabase()->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase', 10.5, 102.4));
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testDecreaseLimitMin(Document $document)
    {
        $this->expectException(Exception::class);
        $this->assertEquals(false, static::getDatabase()->decreaseDocumentAttribute('increase_decrease', $document->getId(), 'decrease', 10, 99));
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseTextAttribute(Document $document)
    {
        $this->expectException(Exception::class);
        $this->assertEquals(false, static::getDatabase()->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase_text'));
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
        $this->assertEquals('Works', $document->getAttribute('with-dash'));

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testGetDocumentSelect(Document $document): Document
    {
        $document = static::getDatabase()->getDocument('documents', $document->getId(), [
            Query::select(['string', 'integer']),
        ]);

        $this->assertNotEmpty(true, $document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer'));
        $this->assertEquals(5, $document->getAttribute('integer'));
        $this->assertArrayNotHasKey('float', $document->getAttributes());
        $this->assertArrayNotHasKey('boolean', $document->getAttributes());
        $this->assertArrayNotHasKey('colors', $document->getAttributes());
        $this->assertArrayNotHasKey('with-dash', $document->getAttributes());

        return $document;
    }


    /**
     * @depends testCreateDocument
     */
    public function testFulltextIndexWithInteger()
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Attribute "integer" cannot be part of a FULLTEXT index');
        static::getDatabase()->createIndex('documents', 'fulltext_integer', Database::INDEX_FULLTEXT, ['string','integer']);
    }


    /**
     * @depends testCreateDocument
     */
    public function testListDocumentSearch(Document $document)
    {
        $fulltextSupport = $this->getDatabase()->getAdapter()->getSupportForFulltextIndex();
        if(!$fulltextSupport) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createIndex('documents', 'string', Database::INDEX_FULLTEXT, ['string']);
        static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
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
            Query::search('string', '*test+alias@email-provider.com'),
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
            ->setAttribute('with-dash', 'Works');

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
        $this->assertEquals('Works', $new->getAttribute('with-dash'));

        $oldPermissions = $document->getPermissions();

        $new
            ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::update(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::delete(Role::guests()), Document::SET_TYPE_APPEND);

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertContains('guests', $new->getRead());
        $this->assertContains('guests', $new->getWrite());
        $this->assertContains('guests', $new->getCreate());
        $this->assertContains('guests', $new->getUpdate());
        $this->assertContains('guests', $new->getDelete());

        $new->setAttribute('$permissions', $oldPermissions);

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertNotContains('guests', $new->getRead());
        $this->assertNotContains('guests', $new->getWrite());
        $this->assertNotContains('guests', $new->getCreate());
        $this->assertNotContains('guests', $new->getUpdate());
        $this->assertNotContains('guests', $new->getDelete());

        return $document;
    }

    /**
     * @depends testGetDocument
     */
    public function testUpdateDocumentDuplicatePermissions(Document $document)
    {
        $new = $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);

        $new
            ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND);

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new, true);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertContains('guests', $new->getRead());
        $this->assertContains('guests', $new->getCreate());

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



    public function testFind(){
        static::getDatabase()->createCollection('movies');

        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'director', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'price', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'active', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'generes', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'with-dash', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'nullable', Database::VAR_STRING, 128, false));

        static::getDatabase()->createDocument('movies', new Document([
            '$id' => ID::custom('frozen'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
            'with-dash' => 'Works'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Frozen II',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2019,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
            'with-dash' => 'Works'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Captain America: The First Avenger',
            'director' => 'Joe Johnston',
            'year' => 2011,
            'price' => 25.94,
            'active' => true,
            'generes' => ['science fiction', 'action', 'comics'],
            'with-dash' => 'Works2'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Captain Marvel',
            'director' => 'Anna Boden & Ryan Fleck',
            'year' => 2019,
            'price' => 25.99,
            'active' => true,
            'generes' => ['science fiction', 'action', 'comics'],
            'with-dash' => 'Works2'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Work in Progress',
            'director' => 'TBD',
            'year' => 2025,
            'price' => 0.0,
            'active' => false,
            'generes' => [],
            'with-dash' => 'Works3'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::user('x')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Work in Progress 2',
            'director' => 'TBD',
            'year' => 2026,
            'price' => 0.0,
            'active' => false,
            'generes' => [],
            'with-dash' => 'Works3',
            'nullable' => 'Not null'
        ]));

    }

    public function testFindBasicChecks()
    {
        $documents = static::getDatabase()->find('movies');
        $movieDocuments = $documents;

        $this->assertEquals(5, count($documents));
        $this->assertNotEmpty($documents[0]->getId());
        $this->assertEquals('movies', $documents[0]->getCollection());
        $this->assertEquals(['any', 'user:1', 'user:2'], $documents[0]->getRead());
        $this->assertEquals(['any', 'user:1x', 'user:2x'], $documents[0]->getWrite());
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
        $this->assertEquals('Works', $documents[0]->getAttribute('with-dash'));

        // Alphabetical order
        $sortedDocuments = $movieDocuments;
        \usort($sortedDocuments, function ($doc1, $doc2) {
            return strcmp($doc1['$id'], $doc2['$id']);
        });

        $firstDocumentId = $sortedDocuments[0]->getId();
        $lastDocumentId = $sortedDocuments[\count($sortedDocuments) - 1]->getId();

        /**
         * Check $id: Notice, this orders ID names alphabetically, not by internal numeric ID
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('$id'),
        ]);
        $this->assertEquals($lastDocumentId, $documents[0]->getId());
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderAsc('$id'),
        ]);
        $this->assertEquals($firstDocumentId, $documents[0]->getId());

        /**
         * Check internal numeric ID sorting
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);
        $this->assertEquals($movieDocuments[\count($movieDocuments) - 1]->getId(), $documents[0]->getId());
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderAsc(''),
        ]);
        $this->assertEquals($movieDocuments[0]->getId(), $documents[0]->getId());

    }

    public function testFindCheckPermissions(){
        /**
         * Check Permissions
         */
        Authorization::setRole('user:x');
        $documents = static::getDatabase()->find('movies');

        $this->assertEquals(6, count($documents));
    }

    public function testFindCheckInteger(){
        /**
         * Query with dash attribute
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('with-dash', ['Works']),
        ]);

        $this->assertEquals(2, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::equal('with-dash', ['Works2', 'Works3']),
        ]);

        $this->assertEquals(4, count($documents));

        /**
         * Check an Integer condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('year', [2019]),
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
    }

    public function testFindBoolean(){
        /**
         * Boolean condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('active', [true]),
        ]);

        $this->assertEquals(4, count($documents));
    }

    public function testFindStringQueryEqual(){
        /**
         * String condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['TBD']),
        ]);

        $this->assertEquals(2, count($documents));
    }

    public function testFindNotEqual()
    {
        /**
         * Not Equal query
         */
        $documents = static::getDatabase()->find('movies', [
            Query::notEqual('director', 'TBD'),
        ]);

        $this->assertGreaterThan(0, count($documents));

        foreach ($documents as $document) {
            $this->assertTrue($document['director'] !== 'TBD');
        }

    }


    public function testFindBetween(){
        $documents = static::getDatabase()->find('movies', [
            Query::between('price', 25.94, 25.99),
        ]);
        $this->assertEquals(2, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::between('price', 30, 35),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::between('$createdAt', '1975-12-06', '2050-12-06'),
        ]);
        $this->assertEquals(6, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::between('$updatedAt', '1975-12-06T07:08:49.733+02:00', '2050-02-05T10:15:21.825+00:00'),
        ]);
        $this->assertEquals(6, count($documents));

    }

    public function testFindFloat(){
        /**
         * Float condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::lessThan('price', 26.00),
            Query::greaterThan('price', 25.98),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindContains(){
        /**
         * Array contains condition
         */
        if ($this->getDatabase()->getAdapter()->getSupportForQueryContains()) {
            $documents = static::getDatabase()->find('movies', [
                Query::contains('generes', ['comics'])
            ]);

            $this->assertEquals(2, count($documents));

            /**
             * Array contains OR condition
             */
            $documents = static::getDatabase()->find('movies', [
                Query::contains('generes', ['comics', 'kids']),
            ]);

            $this->assertEquals(4, count($documents));
        }

        $this->assertEquals(true, true); // Test must do an assertion
    }

    public function testFindFulltext(){
        /**
         * Fulltext search
         */
        if ($this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $success = static::getDatabase()->createIndex('movies', 'name', Database::INDEX_FULLTEXT, ['name']);
            $this->assertEquals(true, $success);

            $documents = static::getDatabase()->find('movies', [
                Query::search('name', 'captain'),
            ]);

            $this->assertEquals(2, count($documents));

            /**
             * Fulltext search (wildcard)
             */

            // TODO: Looks like the MongoDB implementation is a bit more complex, skipping that for now.
            // TODO: I think this needs a changes? how do we distinguish between regular full text and wildcard?

            if ($this->getDatabase()->getAdapter()->getSupportForFulltextWildCardIndex()) {
                $documents = static::getDatabase()->find('movies', [
                    Query::search('name', 'cap'),
                ]);

                $this->assertEquals(2, count($documents));
            }
        }

        $this->assertEquals(true, true); // Test must do an assertion

    }

    public function testFindMultipleConditions(){
        /**
         * Multiple conditions
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['TBD']),
            Query::equal('year', [2026]),
        ]);

        $this->assertEquals(1, count($documents));

        /**
         * Multiple conditions and OR values
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('name', ['Frozen II', 'Captain Marvel']),
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
    }

    public function testFindByID(){
        /**
         * $id condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('$id', ['frozen']),
        ]);

        $this->assertEquals(1, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
    }

    public function testFindOrderBy(){
        /**
         * ORDER BY
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('name')
        ]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
        $this->assertEquals('Frozen II', $documents[1]['name']);
        $this->assertEquals('Captain Marvel', $documents[2]['name']);
        $this->assertEquals('Captain America: The First Avenger', $documents[3]['name']);
        $this->assertEquals('Work in Progress', $documents[4]['name']);
        $this->assertEquals('Work in Progress 2', $documents[5]['name']);
    }

    public function testFindOrderByNatural(){
        /**
         * ORDER BY natural
         */
        $base = array_reverse(static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]));
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals($base[0]['name'], $documents[0]['name']);
        $this->assertEquals($base[1]['name'], $documents[1]['name']);
        $this->assertEquals($base[2]['name'], $documents[2]['name']);
        $this->assertEquals($base[3]['name'], $documents[3]['name']);
        $this->assertEquals($base[4]['name'], $documents[4]['name']);
        $this->assertEquals($base[5]['name'], $documents[5]['name']);
    }

    public function testFindOrderByMultipleAttributes(){
        /**
         * ORDER BY - Multiple attributes
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderDesc('name')
        ]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Frozen', $documents[1]['name']);
        $this->assertEquals('Captain Marvel', $documents[2]['name']);
        $this->assertEquals('Captain America: The First Avenger', $documents[3]['name']);
        $this->assertEquals('Work in Progress 2', $documents[4]['name']);
        $this->assertEquals('Work in Progress', $documents[5]['name']);
    }

    public function testFindOrderByCursorAfter(){
        /**
         * ORDER BY - After
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }


    public function testFindOrderByCursorBefore(){
        /**
         * ORDER BY - Before
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByAfterNaturalOrder(){
        /**
         * ORDER BY - After by natural order
         */
        $movies = array_reverse(static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]));

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByBeforeNaturalOrder(){
        /**
         * ORDER BY - Before by natural order
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderBySingleAttributeAfter(){
        /**
         * ORDER BY - Single Attribute After
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('year')
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[1])
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderBySingleAttributeBefore(){
        /**
         * ORDER BY - Single Attribute Before
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('year')
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByMultipleAttributeAfter(){
        /**
         * ORDER BY - Multiple Attribute After
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year')
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByMultipleAttributeBefore(){
        /**
         * ORDER BY - Multiple Attribute Before
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year')
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[5])
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[4])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByAndCursor(){
        /**
         * ORDER BY + CURSOR
         */
        $documentsTest = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
        ]);
        $documents = static::getDatabase()->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindOrderByIdAndCursor(){
        /**
         * ORDER BY ID + CURSOR
         */
        $documentsTest = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$id'),
        ]);
        $documents = static::getDatabase()->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$id'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindOrderByCreateDateAndCursor(){
        /**
         * ORDER BY CREATE DATE + CURSOR
         */
        $documentsTest = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$createdAt'),
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$createdAt'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindOrderByUpdateDateAndCursor(){
        /**
         * ORDER BY UPDATE DATE + CURSOR
         */
        $documentsTest = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$updatedAt'),
        ]);
        $documents = static::getDatabase()->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$updatedAt'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindLimit(){
        /**
         * Limit
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(4),
            Query::offset(0),
            Query::orderAsc('name')
        ]);

        $this->assertEquals(4, count($documents));
        $this->assertEquals('Captain America: The First Avenger', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
        $this->assertEquals('Frozen', $documents[2]['name']);
        $this->assertEquals('Frozen II', $documents[3]['name']);
    }

    public function testFindLimitAndOffset(){
        /**
         * Limit + Offset
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(4),
            Query::offset(2),
            Query::orderAsc('name')
        ]);

        $this->assertEquals(4, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
        $this->assertEquals('Frozen II', $documents[1]['name']);
        $this->assertEquals('Work in Progress', $documents[2]['name']);
        $this->assertEquals('Work in Progress 2', $documents[3]['name']);
    }

    public function testFindOrQueries(){
        /**
         * Test that OR queries are handled correctly
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['TBD', 'Joe Johnston']),
            Query::equal('year', [2025]),
        ]);
        $this->assertEquals(1, count($documents));
    }

    public function testFindOrderByAfterException(){
        /**
         * ORDER BY - After Exception
         * Must be last assertion in test
         */
        $document = new Document([
            '$collection' => 'other collection'
        ]);

        $this->expectException(Exception::class);
        static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($document)
        ]);
    }

    /**
     * @depends testUpdateDocument
     */
    public function testFindEdgeCases(Document $document)
    {
        $collection = 'edgeCases';

        static::getDatabase()->createCollection($collection);

        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'value', Database::VAR_STRING, 256, true));

        $values = [
            'NormalString',
            '{"type":"json","somekey":"someval"}',
            '{NormalStringInBraces}',
            '"NormalStringInDoubleQuotes"',
            '{"NormalStringInDoubleQuotesAndBraces"}',
            "'NormalStringInSingleQuotes'",
            "{'NormalStringInSingleQuotesAndBraces'}",
            "SingleQuote'InMiddle",
            'DoubleQuote"InMiddle',
            'Slash/InMiddle',
            'Backslash\InMiddle',
            'Colon:InMiddle',
            '"quoted":"colon"'
        ];

        foreach ($values as $value) {
            static::getDatabase()->createDocument($collection, new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any())
                ],
                'value' => $value
            ]));
        }

        /**
         * Check Basic
         */
        $documents = static::getDatabase()->find($collection);

        $this->assertEquals(count($values), count($documents));
        $this->assertNotEmpty($documents[0]->getId());
        $this->assertEquals($collection, $documents[0]->getCollection());
        $this->assertEquals(['any'], $documents[0]->getRead());
        $this->assertEquals(['any'], $documents[0]->getUpdate());
        $this->assertEquals(['any'], $documents[0]->getDelete());
        $this->assertEquals($values[0], $documents[0]->getAttribute('value'));

        /**
         * Check `equals` query 
         */
        foreach ($values as $value) {
            $documents = static::getDatabase()->find($collection, [
                Query::limit(25),
                Query::equal('value', [$value])
            ]);

            $this->assertEquals(1, count($documents));
            $this->assertEquals($value, $documents[0]->getAttribute('value'));
        }
    }

    /**
     * @depends testFind
     */
    public function testFindOne()
    {
        $document = static::getDatabase()->findOne('movies', [
            Query::offset(2),
            Query::orderAsc('name')
        ]);
        $this->assertEquals('Frozen', $document['name']);

        $document = static::getDatabase()->findOne('movies', [
            Query::offset(10)
        ]);
        $this->assertEquals(false, $document);
    }

    public function testFindNull()
    {
        $documents = static::getDatabase()->find('movies', [
            Query::isNull('nullable'),
        ]);

        $this->assertEquals(5, count($documents));
    }

    public function testFindNotNull()
    {
        $documents = static::getDatabase()->find('movies', [
            Query::isNotNull('nullable'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindStartsWith()
    {
        $documents = static::getDatabase()->find('movies', [
            Query::startsWith('name', 'Work'),
        ]);

        $this->assertEquals(2, count($documents));

        if ($this->getDatabase()->getAdapter() instanceof SQL) {
            $documents = static::getDatabase()->find('movies', [
                Query::startsWith('name', '%ork'),
            ]);
        } else {
            $documents = static::getDatabase()->find('movies', [
                Query::startsWith('name', '.*ork'),
            ]);
        }

        $this->assertEquals(0, count($documents));
    }

    public function testFindStartsWithWords()
    {
        $documents = static::getDatabase()->find('movies', [
            Query::startsWith('name', 'Work in Progress'),
        ]);

        $this->assertEquals(2, count($documents));
    }

    public function testFindEndsWith()
    {
        $documents = static::getDatabase()->find('movies', [
            Query::endsWith('name', 'Marvel'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindSelect()
    {
        $documents = static::getDatabase()->find('movies', [
            Query::select(['name', 'year'])
        ]);
        foreach ($documents as $document) {
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayHasKey('$internalId', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);

            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
        }
    }

    /**
     * @depends testFind
     */
    public function testCount()
    {
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        $count = static::getDatabase()->count('movies', [Query::equal('year', [2019])]);
        $this->assertEquals(2, $count);
        $count = static::getDatabase()->count('movies', [Query::equal('with-dash', ['Works'])]);
        $this->assertEquals(2, $count);
        $count = static::getDatabase()->count('movies', [Query::equal('with-dash', ['Works2', 'Works3'])]);
        $this->assertEquals(4, $count);

        Authorization::unsetRole('user:x');
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
            Query::equal('director', ['TBD', 'Joe Johnston']),
            Query::equal('year', [2025]),
        ]);
        $this->assertEquals(1, $count);
        Authorization::reset();
    }

    /**
     * @depends testFind
     */
    public function testSum()
    {
        Authorization::setRole('user:x');
        $sum = static::getDatabase()->sum('movies', 'year', [Query::equal('year', [2019]),]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = static::getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025 + 2026, $sum);
        $sum = static::getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = static::getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));

        $sum = static::getDatabase()->sum('movies', 'year', [Query::equal('year', [2019])], 1);
        $this->assertEquals(2019, $sum);

        Authorization::unsetRole('user:x');
        Authorization::unsetRole('userx');
        $sum = static::getDatabase()->sum('movies', 'year', [Query::equal('year', [2019]),]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = static::getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025, $sum);
        $sum = static::getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = static::getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
    }

    public function testEncodeDecode()
    {
        $collection = new Document([
            '$collection' => ID::custom(Database::METADATA),
            '$id' => ID::custom('users'),
            'name' => 'Users',
            'attributes' => [
                [
                    '$id' => ID::custom('name'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 256,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('email'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 1024,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('status'),
                    'type' => Database::VAR_INTEGER,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('password'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('passwordUpdate'),
                    'type' => Database::VAR_DATETIME,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['datetime'],
                ],
                [
                    '$id' => ID::custom('registration'),
                    'type' => Database::VAR_DATETIME,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['datetime'],
                ],
                [
                    '$id' => ID::custom('emailVerification'),
                    'type' => Database::VAR_BOOLEAN,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('reset'),
                    'type' => Database::VAR_BOOLEAN,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('prefs'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json']
                ],
                [
                    '$id' => ID::custom('sessions'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => ID::custom('tokens'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => ID::custom('memberships'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => ID::custom('roles'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 128,
                    'signed' => true,
                    'required' => false,
                    'array' => true,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('tags'),
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
                    '$id' => ID::custom('_key_email'),
                    'type' => Database::INDEX_UNIQUE,
                    'attributes' => ['email'],
                    'lengths' => [1024],
                    'orders' => [Database::ORDER_ASC],
                ]
            ],
        ]);

        $document = new Document([
            '$id' => ID::custom('608fdbe51361a'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::user('608fdbe51361a')),
                Permission::update(Role::user('608fdbe51361a')),
                Permission::delete(Role::user('608fdbe51361a')),
            ],
            'email' => 'test@example.com',
            'emailVerification' => false,
            'status' => 1,
            'password' => 'randomhash',
            'passwordUpdate' => '2000-06-12 14:12:55',
            'registration' => '1975-06-12 14:12:55+01:00',
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
        $this->assertContains('read("any")', $result->getAttribute('$permissions'));
        $this->assertContains('read("any")', $result->getPermissions());
        $this->assertContains('any', $result->getRead());
        $this->assertContains(Permission::create(Role::user(ID::custom('608fdbe51361a'))), $result->getPermissions());
        $this->assertContains('user:608fdbe51361a', $result->getCreate());
        $this->assertContains('user:608fdbe51361a', $result->getWrite());
        $this->assertEquals('test@example.com', $result->getAttribute('email'));
        $this->assertEquals(false, $result->getAttribute('emailVerification'));
        $this->assertEquals(1, $result->getAttribute('status'));
        $this->assertEquals('randomhash', $result->getAttribute('password'));
        $this->assertEquals('2000-06-12 14:12:55.000', $result->getAttribute('passwordUpdate'));
        $this->assertEquals('1975-06-12 13:12:55.000', $result->getAttribute('registration'));
        $this->assertEquals(false, $result->getAttribute('reset'));
        $this->assertEquals('My Name', $result->getAttribute('name'));
        $this->assertEquals('{}', $result->getAttribute('prefs'));
        $this->assertEquals('[]', $result->getAttribute('sessions'));
        $this->assertEquals('[]', $result->getAttribute('tokens'));
        $this->assertEquals('[]', $result->getAttribute('memberships'));
        $this->assertEquals(['admin', 'developer', 'tester',], $result->getAttribute('roles'));
        $this->assertEquals(['{"$id":"1","label":"x"}', '{"$id":"2","label":"y"}', '{"$id":"3","label":"z"}',], $result->getAttribute('tags'));

        $result = static::getDatabase()->decode($collection, $document);

        $this->assertEquals('608fdbe51361a', $result->getAttribute('$id'));
        $this->assertContains('read("any")', $result->getAttribute('$permissions'));
        $this->assertContains('read("any")', $result->getPermissions());
        $this->assertContains('any', $result->getRead());
        $this->assertContains(Permission::create(Role::user('608fdbe51361a')), $result->getPermissions());
        $this->assertContains('user:608fdbe51361a', $result->getCreate());
        $this->assertContains('user:608fdbe51361a', $result->getWrite());
        $this->assertEquals('test@example.com', $result->getAttribute('email'));
        $this->assertEquals(false, $result->getAttribute('emailVerification'));
        $this->assertEquals(1, $result->getAttribute('status'));
        $this->assertEquals('randomhash', $result->getAttribute('password'));
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $result->getAttribute('passwordUpdate'));
        $this->assertEquals('1975-06-12T13:12:55.000+00:00', $result->getAttribute('registration'));
        $this->assertEquals(false, $result->getAttribute('reset'));
        $this->assertEquals('My Name', $result->getAttribute('name'));
        $this->assertEquals([], $result->getAttribute('prefs'));
        $this->assertEquals([], $result->getAttribute('sessions'));
        $this->assertEquals([], $result->getAttribute('tokens'));
        $this->assertEquals([], $result->getAttribute('memberships'));
        $this->assertEquals(['admin', 'developer', 'tester',], $result->getAttribute('roles'));
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
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
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

        Authorization::setRole(Role::any()->toString());

        return $document;
    }

    public function testReadPermissionsFailure()
    {
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::user('1')),
                Permission::create(Role::user('1')),
                Permission::update(Role::user('1')),
                Permission::delete(Role::user('1')),
            ],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        Authorization::cleanRoles();

        $document = static::getDatabase()->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $document->isEmpty());

        Authorization::setRole(Role::any()->toString());

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testWritePermissionsSuccess(Document $document)
    {
        Authorization::cleanRoles();

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $this->assertEquals(false, $document->isEmpty());

        Authorization::setRole(Role::any()->toString());

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testWritePermissionsUpdateFailure(Document $document)
    {
        $this->expectException(ExceptionAuthorization::class);

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer' => 5,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        Authorization::cleanRoles();

        $document = static::getDatabase()->updateDocument('documents', $document->getId(), new Document([
            '$id' => ID::custom($document->getId()),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
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
        if ($this->getDatabase()->getLimitForAttributes() > 0) {
            // load the collection up to the limit
            $attributes = [];
            for ($i = 0; $i < $this->getDatabase()->getLimitForAttributes(); $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test{$i}"),
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
        $this->assertEquals(1, 1);
    }

    /**
     * @depends testExceptionAttributeLimit
     */
    public function testCheckAttributeCountLimit()
    {
        if ($this->getDatabase()->getLimitForAttributes() > 0) {
            $collection = static::getDatabase()->getCollection('attributeLimit');

            // create same attribute in testExceptionAttributeLimit
            $attribute = new Document([
                '$id' => ID::custom('breaking'),
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
        $this->assertEquals(1, 1);
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
            [0, 1024, 15, 0, 304, 3],
            [1, 512, 31, 0, 0, 333],
            [2, 256, 62, 103, 0, 5],
            [3, 128, 124, 30, 12, 14],
        ];
    }

    /**
     * @dataProvider rowWidthExceedsMaximum
     */
    public function testExceptionWidthLimit($key, $stringSize, $stringCount, $intCount, $floatCount, $boolCount)
    {
        if (static::getDatabase()->getAdapter()::getDocumentSizeLimit() > 0) {
            $attributes = [];

            // Load the collection up to the limit
            // Strings
            for ($i = 0; $i < $stringCount; $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test_string{$i}"),
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
            for ($i = 0; $i < $intCount; $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test_int{$i}"),
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
            for ($i = 0; $i < $floatCount; $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test_float{$i}"),
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
            for ($i = 0; $i < $boolCount; $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test_bool{$i}"),
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
        $this->assertEquals(1, 1);
    }

    /**
     * @dataProvider rowWidthExceedsMaximum
     * @depends      testExceptionWidthLimit
     */
    public function testCheckAttributeWidthLimit($key, $stringSize, $stringCount, $intCount, $floatCount, $boolCount)
    {
        if (static::getDatabase()->getAdapter()::getDocumentSizeLimit()> 0) {
            $collection = static::getDatabase()->getCollection("widthLimit{$key}");

            // create same attribute in testExceptionWidthLimit
            $attribute = new Document([
                '$id' => ID::custom('breaking'),
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
        $this->assertEquals(1, 1);
    }

    public function testExceptionIndexLimit()
    {
        static::getDatabase()->createCollection('indexLimit');

        // add unique attributes for indexing
        for ($i = 0; $i < 64; $i++) {
            $this->assertEquals(true, static::getDatabase()->createAttribute('indexLimit', "test{$i}", Database::VAR_STRING, 16, true));
        }

        // Testing for indexLimit
        // Add up to the limit, then check if the next index throws IndexLimitException
        for ($i = 0; $i < ($this->getDatabase()->getLimitForIndexes()); $i++) {
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
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
            'with-dash' => 'Works4'
        ]));
    }

    /**
     * @depends testUniqueIndexDuplicate
     */
    public function testUniqueIndexDuplicateUpdate()
    {
        // create document then update to conflict with index
        $document = static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Frozen 5',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
            'with-dash' => 'Works4'
        ]));

        $this->expectException(DuplicateException::class);

        static::getDatabase()->updateDocument('movies', $document->getId(), $document->setAttribute('name', 'Frozen'));
    }

    public function testGetAttributeLimit()
    {
        $this->assertIsInt($this->getDatabase()->getLimitForAttributes());
    }

    public function testGetIndexLimit()
    {
        $this->assertEquals(59, $this->getDatabase()->getLimitForIndexes());
    }

    public function testGetId()
    {
        $this->assertEquals(20, strlen(ID::unique()));
        $this->assertEquals(13, strlen(ID::unique(0)));
        $this->assertEquals(13, strlen(ID::unique(-1)));
        $this->assertEquals(23, strlen(ID::unique(10)));

        // ensure two sequential calls to getId do not give the same result
        $this->assertNotEquals(ID::unique(10), ID::unique(10));
    }

    public function testRenameIndex()
    {
        $database = static::getDatabase();

        $numbers = $database->createCollection('numbers');
        $database->createAttribute('numbers', 'verbose', Database::VAR_STRING, 128, true);
        $database->createAttribute('numbers', 'symbol', Database::VAR_INTEGER, 0, true);

        $database->createIndex('numbers', 'index1', Database::INDEX_KEY, ['verbose'], [128], [Database::ORDER_ASC]);
        $database->createIndex('numbers', 'index2', Database::INDEX_KEY, ['symbol'], [0], [Database::ORDER_ASC]);

        $index = $database->renameIndex('numbers', 'index1', 'index3');

        $this->assertTrue($index);

        $numbers = $database->getCollection('numbers');

        $this->assertEquals('index2', $numbers->getAttribute('indexes')[1]['$id']);
        $this->assertEquals('index3', $numbers->getAttribute('indexes')[0]['$id']);
        $this->assertCount(2, $numbers->getAttribute('indexes'));
    }

    /**
     * @depends testRenameIndex
     * @expectedException Exception
     */
    public function testRenameIndexMissing()
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Index not found');
        $index = $database->renameIndex('numbers', 'index1', 'index4');
    }

    /**
     * @depends testRenameIndex
     * @expectedException Exception
     */
    public function testRenameIndexExisting()
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Index name already used');
        $index = $database->renameIndex('numbers', 'index3', 'index2');
    }

    public function testRenameAttribute()
    {
        $database = static::getDatabase();

        $colors = $database->createCollection('colors');
        $database->createAttribute('colors', 'name', Database::VAR_STRING, 128, true);
        $database->createAttribute('colors', 'hex', Database::VAR_STRING, 128, true);

        $database->createIndex('colors', 'index1', Database::INDEX_KEY, ['name'], [128], [Database::ORDER_ASC]);

        $database->createDocument('colors', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'black',
            'hex' => '#000000'
        ]));

        $attribute = $database->renameAttribute('colors', 'name', 'verbose');

        $this->assertTrue($attribute);

        $colors = $database->getCollection('colors');
        $this->assertEquals('hex', $colors->getAttribute('attributes')[1]['$id']);
        $this->assertEquals('verbose', $colors->getAttribute('attributes')[0]['$id']);
        $this->assertCount(2, $colors->getAttribute('attributes'));

        // Attribute in index is renamed automatically on adapter-level. What we need to check is if metadata is properly updated
        $this->assertEquals('verbose', $colors->getAttribute('indexes')[0]->getAttribute("attributes")[0]);
        $this->assertCount(1, $colors->getAttribute('indexes'));

        // Document should be there if adapter migrated properly
        $document = $database->findOne('colors', []);
        $this->assertEquals('black', $document->getAttribute('verbose'));
        $this->assertEquals('#000000', $document->getAttribute('hex'));
        $this->assertEquals(null, $document->getAttribute('name'));
    }

    /**
     * @depends testRenameAttribute
     * @expectedException Exception
     */
    public function textRenameAttributeMissing()
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Attribute not found');
        $database->renameAttribute('colors', 'name2', 'name3');
    }

    /**
     * @depends testRenameAttribute
     * @expectedException Exception
     */
    public function testRenameAttributeExisting()
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Attribute name already used');
        $database->renameAttribute('colors', 'verbose', 'hex');
    }

    public function testUpdateAttributeDefault()
    {
        $database = static::getDatabase();

        $flowers = $database->createCollection('flowers');
        $database->createAttribute('flowers', 'name', Database::VAR_STRING, 128, true);
        $database->createAttribute('flowers', 'inStock', Database::VAR_INTEGER, 0, false);
        $database->createAttribute('flowers', 'date', Database::VAR_STRING, 128, false);

        $database->createDocument('flowers', new Document([
            '$id' => 'flowerWithDate',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Violet',
            'inStock' => 51,
            'date' => '2000-06-12 14:12:55.000'
        ]));

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily'
        ]));

        $this->assertNull($doc->getAttribute('inStock'));

        $database->updateAttributeDefault('flowers', 'inStock', 100);

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Iris'
        ]));

        $this->assertIsNumeric($doc->getAttribute('inStock'));
        $this->assertEquals(100, $doc->getAttribute('inStock'));

        $database->updateAttributeDefault('flowers', 'inStock', null);
    }

    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeRequired()
    {
        $database = static::getDatabase();

        $database->updateAttributeRequired('flowers', 'inStock', true);

        $this->expectExceptionMessage('Invalid document structure: Missing required attribute "inStock"');

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily With Missing Stocks'
        ]));
    }

    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeFilter()
    {
        $database = static::getDatabase();

        $database->createAttribute('flowers', 'cartModel', Database::VAR_STRING, 2000, false);

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily With CartData',
            'inStock' => 50,
            'cartModel' => '{"color":"string","size":"number"}'
        ]));

        $this->assertIsString($doc->getAttribute('cartModel'));
        $this->assertEquals('{"color":"string","size":"number"}', $doc->getAttribute('cartModel'));

        $database->updateAttributeFilters('flowers', 'cartModel', ['json']);

        $doc = $database->getDocument('flowers', $doc->getId());
        $this->assertIsArray($doc->getAttribute('cartModel'));
        $this->assertCount(2, $doc->getAttribute('cartModel'));
        $this->assertEquals('string', $doc->getAttribute('cartModel')['color']);
        $this->assertEquals('number', $doc->getAttribute('cartModel')['size']);
    }

    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeFormat()
    {
        $database = static::getDatabase();

        $database->createAttribute('flowers', 'price', Database::VAR_INTEGER, 0, false);

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            '$id' => ID::custom('LiliPriced'),
            'name' => 'Lily Priced',
            'inStock' => 50,
            'cartModel' => '{}',
            'price' => 500
        ]));

        $this->assertIsNumeric($doc->getAttribute('price'));
        $this->assertEquals(500, $doc->getAttribute('price'));

        Structure::addFormat('priceRange', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];

            return new Range($min, $max);
        }, Database::VAR_INTEGER);

        $database->updateAttributeFormat('flowers', 'price', 'priceRange');
        $database->updateAttributeFormatOptions('flowers', 'price', ['min' => 1, 'max' => 10000]);

        $this->expectExceptionMessage('Invalid document structure: Attribute "price" has invalid format. Value must be a valid range between 1 and 10,000');

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily Overpriced',
            'inStock' => 50,
            'cartModel' => '{}',
            'price' => 15000
        ]));
    }

    /**
     * @depends testUpdateAttributeDefault
     * @depends testUpdateAttributeFormat
     */
    public function testUpdateAttributeStructure()
    {
        // TODO: When this becomes relevant, add many more tests (from all types to all types, chaging size up&down, switchign between array/non-array...

        $database = static::getDatabase();

        $doc = $database->getDocument('flowers', 'LiliPriced');
        $this->assertIsNumeric($doc->getAttribute('price'));
        $this->assertEquals(500, $doc->getAttribute('price'));

        $database->updateAttribute('flowers', 'price', Database::VAR_STRING, 255, false, false);
        $database->updateAttribute('flowers', 'date', Database::VAR_DATETIME, 0, false, filters:['datetime']);

        // Delete cache to force read from database with new schema
        $database->deleteCachedDocument('flowers', 'LiliPriced');

        $doc = $database->getDocument('flowers', 'LiliPriced');

        $this->assertIsString($doc->getAttribute('price'));
        $this->assertEquals('500', $doc->getAttribute('price'));

        // String to Datetime
        $database->deleteCachedDocument('flowers', 'flowerWithDate');
        $doc = $database->getDocument('flowers', 'flowerWithDate');
        //TODO: Timestamps with trailing microsecond zeroes are truncated by postgres
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $doc->getAttribute('date'));
    }

    /**
     * @depends testCreatedAtUpdatedAt
     */
    public function testCreatedAtUpdatedAtAssert()
    {
        $document = static::getDatabase()->getDocument('created_at', 'uid123');
        $this->assertEquals(true, !$document->isEmpty());
        sleep(1);
        static::getDatabase()->updateDocument('created_at', 'uid123', $document);
        $document = static::getDatabase()->getDocument('created_at', 'uid123');

        $this->assertGreaterThan($document->getCreatedAt(), $document->getUpdatedAt());
        $this->expectException(DuplicateException::class);

        static::getDatabase()->createCollection('created_at');
    }

    public function testCreateDatetime()
    {
        static::getDatabase()->createCollection('datetime');

        $this->assertEquals(true, static::getDatabase()->createAttribute('datetime', 'date', Database::VAR_DATETIME, 0, true, null, true, false, null, [], ['datetime']));
        $this->assertEquals(true, static::getDatabase()->createAttribute('datetime', 'date2', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']));

        $doc = static::getDatabase()->createDocument('datetime', new Document([
            '$id' => ID::custom('id1234'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'date' => DateTime::now(),
        ]));

        $this->assertEquals(29, strlen($doc->getCreatedAt()));
        $this->assertEquals(29, strlen($doc->getUpdatedAt()));
        $this->assertEquals('+00:00', substr($doc->getCreatedAt(), -6));
        $this->assertEquals('+00:00', substr($doc->getUpdatedAt(), -6));
        $this->assertGreaterThan('2020-08-16T19:30:08.363+00:00', $doc->getCreatedAt());
        $this->assertGreaterThan('2020-08-16T19:30:08.363+00:00', $doc->getUpdatedAt());

        $document = static::getDatabase()->getDocument('datetime', 'id1234');
        $dateValidator = new DatetimeValidator();
        $this->assertEquals(NULL, $document->getAttribute('date2'));
        $this->assertEquals(true, $dateValidator->isValid($document->getAttribute('date')));
        $this->assertEquals(false, $dateValidator->isValid($document->getAttribute('date2')));

        $documents = static::getDatabase()->find('datetime', [
            Query::greaterThan('date', '1975-12-06 10:00:00+01:00'),
            Query::lessThan('date', '2030-12-06 10:00:00-01:00'),
        ]);

        $this->assertEquals(1, count($documents));

        $this->expectException(StructureException::class);
        static::getDatabase()->createDocument('datetime', new Document([
            '$permissions' => [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'date' => "1975-12-06 00:00:61"
        ]));
    }

    public function testCreateDateTimeAttributeFailure()
    {
        static::getDatabase()->createCollection('datetime_fail');

        /** Test for FAILURE */
        $this->expectException(Exception::class);
        static::getDatabase()->createAttribute('datetime_fail', 'date_fail', Database::VAR_DATETIME, 0, false);
    }

    public function testKeywords()
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

            $documents = $database->find($collectionName, [Query::equal($keyword, ["Reserved:${keyword}"])]);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());

            $documents = $database->find($collectionName, [
                Query::orderDesc($keyword)
            ]);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());


            $collection = $database->deleteCollection($collectionName);
            $this->assertTrue($collection);

            // TODO: updateAttribute name tests
        }

        // TODO: Index name tests
    }

    public function testWritePermissions()
    {
        $database = static::getDatabase();

        $database->createCollection('animals');

        $database->createAttribute('animals', 'type', Database::VAR_STRING, 128, true);

        $dog = $database->createDocument('animals', new Document([
            '$id' => 'dog',
            '$permissions' => [
                Permission::delete(Role::any()),
            ],
            'type' => 'Dog'
        ]));

        $cat = $database->createDocument('animals', new Document([
            '$id' => 'cat',
            '$permissions' => [
                Permission::update(Role::any()),
            ],
            'type' => 'Cat'
        ]));

        // No read permissions:

        $docs = $database->find('animals');
        $this->assertCount(0, $docs);

        $doc = $database->getDocument('animals', 'dog');
        $this->assertTrue($doc->isEmpty());

        $doc = $database->getDocument('animals', 'cat');
        $this->assertTrue($doc->isEmpty());

        // Cannot delete with update permission:
        $didFail = false;

        try {
            $database->deleteDocument('animals', 'cat');
        } catch (ExceptionAuthorization) {
            $didFail = true;
        }

        $this->assertTrue($didFail);

        // Cannot update with delete permission:
        $didFail = false;

        try {
            $newDog = $dog->setAttribute('type', 'newDog');
            $database->updateDocument('animals', 'dog', $newDog);
        } catch (ExceptionAuthorization) {
            $didFail = true;
        }

        $this->assertTrue($didFail);

        // Can delete:
        $database->deleteDocument('animals', 'dog');

        // Can update:
        $newCat = $cat->setAttribute('type', 'newCat');
        $database->updateDocument('animals', 'cat', $newCat);

        $docs = Authorization::skip(fn () => $database->find('animals'));
        $this->assertCount(1, $docs);
        $this->assertEquals('cat', $docs[0]['$id']);
        $this->assertEquals('newCat', $docs[0]['type']);
    }

    public function testEvents()
    {
        Authorization::skip(function () {
            $database = static::getDatabase();

            $events = [
                Database::EVENT_DATABASE_CREATE,
                Database::EVENT_DATABASE_LIST,
                Database::EVENT_COLLECTION_CREATE,
                Database::EVENT_COLLECTION_LIST,
                Database::EVENT_COLLECTION_READ,
                Database::EVENT_ATTRIBUTE_CREATE,
                Database::EVENT_ATTRIBUTE_UPDATE,
                Database::EVENT_INDEX_CREATE,
                Database::EVENT_DOCUMENT_CREATE,
                Database::EVENT_DOCUMENT_UPDATE,
                Database::EVENT_DOCUMENT_READ,
                Database::EVENT_DOCUMENT_FIND,
                Database::EVENT_DOCUMENT_FIND,
                Database::EVENT_DOCUMENT_COUNT,
                Database::EVENT_DOCUMENT_SUM,
                Database::EVENT_DOCUMENT_INCREASE,
                Database::EVENT_DOCUMENT_DECREASE,
                Database::EVENT_INDEX_DELETE,
                Database::EVENT_DOCUMENT_DELETE,
                Database::EVENT_ATTRIBUTE_DELETE,
                Database::EVENT_COLLECTION_DELETE,
                Database::EVENT_DATABASE_DELETE,
            ];

            $database->on(Database::EVENT_ALL, function ($event, $data) use (&$events) {
                $shifted = array_shift($events);

                $this->assertEquals($shifted, $event);
            });

            if ($this->getDatabase()->getAdapter()->getSupportForSchemas()) {
                $database->setDefaultDatabase('hellodb');
                $database->create();
            } else {
                array_shift($events);
            }

            $database->list();

            $database->setDefaultDatabase($this->testDatabase);

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

            $database->updateDocument($collectionId, 'doc1', $document->setAttribute('attr1', 15));
            $database->getDocument($collectionId, 'doc1');
            $database->find($collectionId);
            $database->findOne($collectionId);
            $database->count($collectionId);
            $database->sum($collectionId, 'attr1');
            $database->increaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
            $database->decreaseDocumentAttribute($collectionId, $document->getId(), 'attr1');

            $database->deleteIndex($collectionId, $indexId1);
            $database->deleteDocument($collectionId, 'doc1');
            $database->deleteAttribute($collectionId, 'attr1');
            $database->deleteCollection($collectionId);
            $database->delete('hellodb');
        });
    }

    // Relationships
    public function testOneToOneOneWayRelationship(): void
    {
        static::getDatabase()->createCollection('person');
        static::getDatabase()->createCollection('library');

        static::getDatabase()->createRelationship(
            collection: 'person',
            relatedCollection: 'library',
            type: Database::RELATION_ONE_TO_ONE
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('person');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'library') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('library', $attribute['$id']);
                $this->assertEquals('library', $attribute['key']);
                $this->assertEquals('library', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('person', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'library' => [
                '$id' => 'library1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library2',
        ]));
        static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person2',
            'library' => 'library2'
        ]));

        // Get document with relationship
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('library');
        $this->assertEquals('library1', $library['$id']);

        // Get related document
        $library = static::getDatabase()->getDocument('library', 'library1');
        $person = $library->getAttribute('person');
        $this->assertEquals(null, $person);
        
        // Rename relationship key
        static::getDatabase()->updateRelationship(
            'person',
            'library',
            'newLibrary'
        );

        // Get document with again
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary');
        $this->assertEquals('library1', $library['$id']);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'person',
            'newLibrary'
        );

        // Try to get document again
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary');
        $this->assertEquals(null, $library);
    }

    public function testOneToOneTwoWayRelationship(): void
    {
        static::getDatabase()->createCollection('country');
        static::getDatabase()->createCollection('city');

        static::getDatabase()->createRelationship(
            collection: 'country',
            relatedCollection: 'city',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $collection = static::getDatabase()->getCollection('country');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'city') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('city', $attribute['$id']);
                $this->assertEquals('city', $attribute['key']);
                $this->assertEquals('city', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('country', $attribute['options']['twoWayKey']);
            }
        }

        $collection = static::getDatabase()->getCollection('city');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'country') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('country', $attribute['$id']);
                $this->assertEquals('country', $attribute['key']);
                $this->assertEquals('country', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('city', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'city' => [
                '$id' => 'city1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ]
        ]));
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country2',
            'city' => 'city2'
        ]));

        // Get document with relationship
        $city = static::getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('country');
        $this->assertEquals('country1', $country['$id']);

        // Get inverse document with relationship
        $country = static::getDatabase()->getDocument('country', 'country1');
        $city = $country->getAttribute('city');
        $this->assertEquals('city1', $city['$id']);

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'country',
            'city',
            'newCity',
            'newCountry'
        );

        // Get document with new relationship key
        $city = static::getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals('country1', $country['$id']);

        // Get inverse document with new relationship key
        $country = static::getDatabase()->getDocument('country', 'country1');
        $city = $country->getAttribute('newCity');
        $this->assertEquals('city1', $city['$id']);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'country',
            'newCity'
        );

        // Try to get document again
        $country = static::getDatabase()->getDocument('country', 'country1');
        $city = $country->getAttribute('newCity');
        $this->assertEquals(null, $city);

        // Try to get inverse document again
        $city = static::getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals(null, $country);
    }

    public function testOneToManyOneWayRelationship(): void
    {
        static::getDatabase()->createCollection('artist');
        static::getDatabase()->createCollection('album');

        static::getDatabase()->createRelationship(
            collection: 'artist',
            relatedCollection: 'album',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'albums'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('artist');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'albums') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('albums', $attribute['$id']);
                $this->assertEquals('albums', $attribute['key']);
                $this->assertEquals('album', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('artist', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        static::getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist1',
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'albums' => [
                [
                    '$id' => 'album1',
                    '$permissions' => [
                        Permission::read(Role::any())
                    ],
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('album', new Document([
            '$id' => 'album2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ]
        ]));
        static::getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist2',
            'albums' => [
                'album2'
            ]
        ]));

        // Get document with relationship
        $artist = static::getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album1', $albums[0]['$id']);

        // Get related document
        $album = static::getDatabase()->getDocument('album', 'album1');
        $artist = $album->getAttribute('artist');
        $this->assertEquals(null, $artist);

        // Rename relationship key
        static::getDatabase()->updateRelationship(
            'artist',
            'albums',
            'newAlbums'
        );

        // Get document with new relationship key
        $artist = static::getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('newAlbums');
        $this->assertEquals('album1', $albums[0]['$id']);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'artist',
            'newAlbums'
        );

        // Try to get document again
        $artist = static::getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('newAlbums');
        $this->assertEquals(null, $albums);
    }

    public function testOneToManyTwoWayRelationship(): void
    {
        static::getDatabase()->createCollection('customer');
        static::getDatabase()->createCollection('account');

        static::getDatabase()->createRelationship(
            collection: 'customer',
            relatedCollection: 'account',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'accounts'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('customer');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'accounts') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('accounts', $attribute['$id']);
                $this->assertEquals('accounts', $attribute['key']);
                $this->assertEquals('account', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('customer', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = static::getDatabase()->getCollection('account');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'customer') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('customer', $attribute['$id']);
                $this->assertEquals('customer', $attribute['key']);
                $this->assertEquals('customer', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('accounts', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'accounts' => [
                [
                    '$id' => 'account1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('account', new Document([
            '$id' => 'account2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));
        static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer2',
            'accounts' => [
                'account2'
            ]
        ]));

        // Get document with relationship
        $customer = static::getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account1', $accounts[0]['$id']);

        // Get related document
        $account = static::getDatabase()->getDocument('account', 'account1');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer1', $customer['$id']);

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'customer',
            'accounts',
            'newAccounts',
            'newCustomer'
        );

        // Get document with new relationship key
        $customer = static::getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('newAccounts');
        $this->assertEquals('account1', $accounts[0]['$id']);

        // Get inverse document with new relationship key
        $account = static::getDatabase()->getDocument('account', 'account1');
        $customer = $account->getAttribute('newCustomer');
        $this->assertEquals('customer1', $customer['$id']);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'customer',
            'newAccounts'
        );

        // Try to get document again
        $customer = static::getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('newAccounts');
        $this->assertEquals(null, $accounts);

        // Try to get inverse document again
        $accounts = static::getDatabase()->getDocument('account', 'account1');
        $customer = $accounts->getAttribute('newCustomer');
        $this->assertEquals(null, $customer);
    }

    public function testManyToOneOneWayRelationship(): void
    {
        static::getDatabase()->createCollection('review');
        static::getDatabase()->createCollection('movie');

        static::getDatabase()->createRelationship(
            collection: 'review',
            relatedCollection: 'movie',
            type: Database::RELATION_MANY_TO_ONE,
            twoWayKey: 'reviews'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('review');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'movies') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('movies', $attribute['$id']);
                $this->assertEquals('movies', $attribute['key']);
                $this->assertEquals('movie', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('review', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = static::getDatabase()->getCollection('movie');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'review') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('review', $attribute['$id']);
                $this->assertEquals('review', $attribute['key']);
                $this->assertEquals('review', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('movies', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        static::getDatabase()->createDocument('review', new Document([
            '$id' => 'review1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'movie' => [
                '$id' => 'movie1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('movie', new Document([
            '$id' => 'movie2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));
        static::getDatabase()->createDocument('review', new Document([
            '$id' => 'review2',
            'movie' => 'movie2',
        ]));

        // Get document with relationship
        $review = static::getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('movie', []);
        $this->assertEquals('movie1', $movie['$id']);

        // Get related document
        $movie = static::getDatabase()->getDocument('movie', 'movie1');
        $reviews = $movie->getAttribute('reviews');
        $this->assertEquals(null, $reviews);

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'review',
            'movie',
            'newMovie',
        );

        // Get document with new relationship key
        $review = static::getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('newMovie');
        $this->assertEquals('movie1', $movie['$id']);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'review',
            'newMovie'
        );

        // Try to get document again
        $review = static::getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('newMovie');
        $this->assertEquals(null, $movie);
    }

    public function testManyToOneTwoWayRelationship(): void
    {
        static::getDatabase()->createCollection('product');
        static::getDatabase()->createCollection('store');

        static::getDatabase()->createRelationship(
            collection: 'product',
            relatedCollection: 'store',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            twoWayKey: 'products'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('product');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'stores') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('stores', $attribute['$id']);
                $this->assertEquals('stores', $attribute['key']);
                $this->assertEquals('store', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('product', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = static::getDatabase()->getCollection('store');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'product') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('product', $attribute['$id']);
                $this->assertEquals('product', $attribute['key']);
                $this->assertEquals('product', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('stores', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        static::getDatabase()->createDocument('product', new Document([
            '$id' => 'product1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'store' => [
                '$id' => 'store1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('store', new Document([
            '$id' => 'store2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));
        static::getDatabase()->createDocument('product', new Document([
            '$id' => 'product2',
            'store' => 'store2',
        ]));

        // Get document with relationship
        $product = static::getDatabase()->getDocument('product', 'product1');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store1', $store['$id']);

        // Get related document
        $store = static::getDatabase()->getDocument('store', 'store1');
        $products = $store->getAttribute('products');
        $this->assertEquals('product1', $products[0]['$id']);

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'product',
            'store',
            'newStore',
            'newProducts'
        );

        // Get document with new relationship key
        $store = static::getDatabase()->getDocument('store', 'store1');
        $products = $store->getAttribute('newProducts');
        $this->assertEquals('product1', $products[0]['$id']);

        // Get inverse document with new relationship key
        $product = static::getDatabase()->getDocument('product', 'product1');
        $store = $product->getAttribute('newStore');
        $this->assertEquals('store1', $store['$id']);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'product',
            'newStore'
        );

        // Try to get document again
        $products = static::getDatabase()->getDocument('product', 'product1');
        $store = $products->getAttribute('newStore');
        $this->assertEquals(null, $store);

        // Try to get inverse document again
        $store = static::getDatabase()->getDocument('store', 'store1');
        $products = $store->getAttribute('newProducts');
        $this->assertEquals(null, $products);
    }

    public function testManyToManyOneWayRelationship(): void
    {
        static::getDatabase()->createCollection('playlist');
        static::getDatabase()->createCollection('song');

        static::getDatabase()->createRelationship(
            collection: 'playlist',
            relatedCollection: 'song',
            type: Database::RELATION_MANY_TO_MANY,
            id: 'songs'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('playlist');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'songs') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('songs', $attribute['$id']);
                $this->assertEquals('songs', $attribute['key']);
                $this->assertEquals('song', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('playlist', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'songs' => [
                [
                    '$id' => 'song1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('song', new Document([
            '$id' => 'song2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
        ]));
        static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist2',
            'songs' => [
                'song2'
            ]
        ]));

        // Get document with relationship
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song1', $songs[0]['$id']);

        // Get related document
        $library = static::getDatabase()->getDocument('song', 'song1');
        $songs = $library->getAttribute('playlist');
        $this->assertEquals(null, $songs);

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'playlist',
            'songs',
            'newSongs'
        );

        // Get document with new relationship key
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals('song1', $songs[0]['$id']);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'playlist',
            'newSongs'
        );

        // Try to get document again
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals(null, $songs);
    }

    public function testManyToManyTwoWayRelationship(): void
    {
        static::getDatabase()->createCollection('students');
        static::getDatabase()->createCollection('classes');

        static::getDatabase()->createRelationship(
            collection: 'students',
            relatedCollection: 'classes',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('students');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'students') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('students', $attribute['$id']);
                $this->assertEquals('students', $attribute['key']);
                $this->assertEquals('students', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('classes', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = static::getDatabase()->getCollection('classes');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'customer') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('classes', $attribute['$id']);
                $this->assertEquals('classes', $attribute['key']);
                $this->assertEquals('classes', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('students', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'classes' => [
                [
                    '$id' => 'class1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('classes', new Document([
            '$id' => 'class2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
        ]));
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student2',
            'classes' => [
                'class2'
            ]
        ]));

        // Get document with relationship
        $student = static::getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class1', $classes[0]['$id']);

        // Get related document
        $class = static::getDatabase()->getDocument('classes', 'class1');
        $student = $class->getAttribute('students');
        $this->assertEquals('student1', $student[0]['$id']);

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'students',
            'classes',
            'newClasses',
            'newStudents'
        );

        // Get document with new relationship key
        $students = static::getDatabase()->getDocument('students', 'student1');
        $classes = $students->getAttribute('newClasses');
        $this->assertEquals('class1', $classes[0]['$id']);

        // Get inverse document with new relationship key
        $class = static::getDatabase()->getDocument('classes', 'class1');
        $students = $class->getAttribute('newStudents');
        $this->assertEquals('student1', $students[0]['$id']);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'students',
            'newClasses'
        );

        // Try to get document again
        $student = static::getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('newClasses');
        $this->assertEquals(null, $classes);

        $classes = static::getDatabase()->getDocument('classes', 'class1');
        $students = $classes->getAttribute('newStudents');
        $this->assertEquals(null, $students);
    }
}
