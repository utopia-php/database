<?php

namespace Utopia\Tests;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Exception\Timeout;
use Utopia\Database\Query;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Validator\Range;
use Utopia\Database\Exception\Structure as StructureException;

abstract class Base extends TestCase
{
    /**
     * @return Database
     */
    abstract protected static function getDatabase(): Database;

    /**
     * @return string
     */
    abstract protected static function getAdapterName(): string;

    public function setUp(): void
    {
        Authorization::setRole('any');
    }

    public function tearDown(): void
    {
        Authorization::setDefaultStatus(true);
    }

    protected string $testDatabase = 'utopiaTests';

    public function testPing(): void
    {
        $this->assertEquals(true, static::getDatabase()->ping());
    }

    public function testCreateExistsDelete(): void
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

    public function testCreatedAtUpdatedAt(): void
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
    public function testCreateListExistsDeleteCollection(): void
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

    public function testCreateDeleteAttribute(): void
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
     *
     * @return array<array<bool|float|int|string>>
     */
    public function invalidDefaultValues(): array
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
     */
    public function testInvalidDefaultValues(string $type, mixed $default): void
    {
        $this->expectException(\Exception::class);
        $this->assertEquals(false, static::getDatabase()->createAttribute('attributes', 'bad_default', $type, 256, true, $default));
    }

    /**
     * @depends testInvalidDefaultValues
     */
    public function testAttributeCaseInsensitivity(): void
    {
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'caseSensitive', Database::VAR_STRING, 128, true));
        $this->expectException(DuplicateException::class);
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'CaseSensitive', Database::VAR_STRING, 128, true));
    }

    /**
     * @depends testAttributeCaseInsensitivity
     */
    public function testIndexCaseInsensitivity(): void
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
    public function testCleanupAttributeTests(): void
    {
        static::getDatabase()->deleteCollection('attributes');
        $this->assertEquals(1, 1);
    }

    /**
     * @depends testCreateDeleteAttribute
     * @expectedException Exception
     */
    public function testUnknownFormat(): void
    {
        $this->expectException(\Exception::class);
        $this->assertEquals(false, static::getDatabase()->createAttribute('attributes', 'bad_format', Database::VAR_STRING, 256, true, null, true, false, 'url'));
    }

    public function testCreateDeleteIndex(): void
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

    public function testCreateDocument(): Document
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

    public function testRespectNulls(): Document
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

    public function testCreateDocumentDefaults(): void
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
     * @throws AuthorizationException|LimitException|DuplicateException|StructureException|Exception|\Throwable
     */
    public function testIncreaseDecrease(): Document
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
    public function testIncreaseLimitMax(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(true, static::getDatabase()->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase', 10.5, 102.4));
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testDecreaseLimitMin(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(false, static::getDatabase()->decreaseDocumentAttribute('increase_decrease', $document->getId(), 'decrease', 10, 99));
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseTextAttribute(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(false, static::getDatabase()->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase_text'));
    }

    /**
     * @depends testCreateDocument
     */
    public function testGetDocument(Document $document): Document
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
    public function testFulltextIndexWithInteger(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Attribute "integer" cannot be part of a FULLTEXT index');
        static::getDatabase()->createIndex('documents', 'fulltext_integer', Database::INDEX_FULLTEXT, ['string','integer']);
    }


    /**
     * @depends testCreateDocument
     */
    public function testListDocumentSearch(Document $document): void
    {
        $fulltextSupport = $this->getDatabase()->getAdapter()->getSupportForFulltextIndex();
        if (!$fulltextSupport) {
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
    }

    /**
     * @depends testGetDocument
     */
    public function testUpdateDocument(Document $document): Document
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
     * @depends testUpdateDocument
     */
    public function testUpdateDocumentConflict(Document $document): void
    {
        $document->setAttribute('integer', 7);
        $result = $this->getDatabase()->withRequestTimestamp(new \DateTime(), function () use ($document) {
            return $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);
        });
        $this->assertEquals(7, $result->getAttribute('integer'));

        $oneHourAgo = (new \DateTime())->sub(new \DateInterval('PT1H'));
        $document->setAttribute('integer', 8);
        $this->expectException(ConflictException::class);
        $this->getDatabase()->withRequestTimestamp($oneHourAgo, function () use ($document) {
            return $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);
        });
    }

    /**
     * @depends testUpdateDocument
     */
    public function testDeleteDocumentConflict(Document $document): void
    {
        $oneHourAgo = (new \DateTime())->sub(new \DateInterval('PT1H'));
        $this->expectException(ConflictException::class);
        $this->getDatabase()->withRequestTimestamp($oneHourAgo, function () use ($document) {
            return $this->getDatabase()->deleteDocument($document->getCollection(), $document->getId());
        });
    }

    /**
     * @depends testGetDocument
     */
    public function testUpdateDocumentDuplicatePermissions(Document $document): Document
    {
        $new = $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);

        $new
            ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND);

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertContains('guests', $new->getRead());
        $this->assertContains('guests', $new->getCreate());

        return $document;
    }

    /**
     * @depends testUpdateDocument
     */
    public function testDeleteDocument(Document $document): void
    {
        $result = $this->getDatabase()->deleteDocument($document->getCollection(), $document->getId());
        $document = $this->getDatabase()->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $result);
        $this->assertEquals(true, $document->isEmpty());
    }



    public function testFind(): void
    {
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

    public function testFindBasicChecks(): void
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

    public function testFindCheckPermissions(): void
    {
        /**
         * Check Permissions
         */
        Authorization::setRole('user:x');
        $documents = static::getDatabase()->find('movies');

        $this->assertEquals(6, count($documents));
    }

    public function testFindCheckInteger(): void
    {
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

    public function testFindBoolean(): void
    {
        /**
         * Boolean condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('active', [true]),
        ]);

        $this->assertEquals(4, count($documents));
    }

    public function testFindStringQueryEqual(): void
    {
        /**
         * String condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['TBD']),
        ]);

        $this->assertEquals(2, count($documents));
    }

    public function testFindNotEqual(): void
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


    public function testFindBetween(): void
    {
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

    public function testFindFloat(): void
    {
        /**
         * Float condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::lessThan('price', 26.00),
            Query::greaterThan('price', 25.98),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindContains(): void
    {
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

    public function testFindFulltext(): void
    {
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

    public function testFindMultipleConditions(): void
    {
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

    public function testFindByID(): void
    {
        /**
         * $id condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('$id', ['frozen']),
        ]);

        $this->assertEquals(1, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
    }

    public function testFindOrderBy(): void
    {
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

    public function testFindOrderByNatural(): void
    {
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

    public function testFindOrderByMultipleAttributes(): void
    {
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

    public function testFindOrderByCursorAfter(): void
    {
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


    public function testFindOrderByCursorBefore(): void
    {
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

    public function testFindOrderByAfterNaturalOrder(): void
    {
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

    public function testFindOrderByBeforeNaturalOrder(): void
    {
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

    public function testFindOrderBySingleAttributeAfter(): void
    {
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

    public function testFindOrderBySingleAttributeBefore(): void
    {
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

    public function testFindOrderByMultipleAttributeAfter(): void
    {
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

    public function testFindOrderByMultipleAttributeBefore(): void
    {
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

    public function testFindOrderByAndCursor(): void
    {
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

    public function testFindOrderByIdAndCursor(): void
    {
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

    public function testFindOrderByCreateDateAndCursor(): void
    {
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

    public function testFindOrderByUpdateDateAndCursor(): void
    {
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

    public function testFindLimit(): void
    {
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

    public function testFindLimitAndOffset(): void
    {
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

    public function testFindOrQueries(): void
    {
        /**
         * Test that OR queries are handled correctly
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['TBD', 'Joe Johnston']),
            Query::equal('year', [2025]),
        ]);
        $this->assertEquals(1, count($documents));
    }

    public function testFindOrderByAfterException(): void
    {
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

    public function testTimeout(): void
    {
        if ($this->getDatabase()->getAdapter()->getSupportForTimeouts()) {
            static::getDatabase()->createCollection('timeouts');
            $this->assertEquals(true, static::getDatabase()->createAttribute('timeouts', 'longtext', Database::VAR_STRING, 100000000, true));

            for ($i = 0 ; $i <= 5 ; $i++) {
                static::getDatabase()->createDocument('timeouts', new Document([
                    'longtext' => file_get_contents(__DIR__ . '/../resources/longtext.txt'),
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any())
                    ]
                ]));
            }

            $this->expectException(Timeout::class);

            static::getDatabase()->find('timeouts', [
                Query::notEqual('longtext', 'appwrite'),
            ], 1);
        }

        $this->expectNotToPerformAssertions();
    }

    /**
     * @depends testUpdateDocument
     */
    public function testFindEdgeCases(Document $document): void
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
    public function testFindOne(): void
    {
        $document = static::getDatabase()->findOne('movies', [
            Query::offset(2),
            Query::orderAsc('name')
        ]);

        $this->assertTrue($document instanceof Document);
        $this->assertEquals('Frozen', $document->getAttribute('name'));

        $document = static::getDatabase()->findOne('movies', [
            Query::offset(10)
        ]);
        $this->assertEquals(false, $document);
    }

    public function testFindNull(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::isNull('nullable'),
        ]);

        $this->assertEquals(5, count($documents));
    }

    public function testFindNotNull(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::isNotNull('nullable'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindStartsWith(): void
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

    public function testFindStartsWithWords(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::startsWith('name', 'Work in Progress'),
        ]);

        $this->assertEquals(2, count($documents));
    }

    public function testFindEndsWith(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::endsWith('name', 'Marvel'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindSelect(): void
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
    public function testCount(): void
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
    public function testSum(): void
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

    public function testEncodeDecode(): void
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
            'prefs' => new \stdClass(),
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
    public function testReadPermissionsSuccess(Document $document): Document
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

    public function testReadPermissionsFailure(): Document
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
    public function testWritePermissionsSuccess(Document $document): Document
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
    public function testWritePermissionsUpdateFailure(Document $document): Document
    {
        $this->expectException(AuthorizationException::class);

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

    public function testExceptionAttributeLimit(): void
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
    public function testCheckAttributeCountLimit(): void
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
     *
     * @return array<array<int>>
     */
    public function rowWidthExceedsMaximum(): array
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
    public function testExceptionWidthLimit(int $key, int $stringSize, int $stringCount, int $intCount, int $floatCount, int $boolCount): void
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
    public function testCheckAttributeWidthLimit(int $key, int $stringSize, int $stringCount, int $intCount, int $floatCount, int $boolCount): void
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

    public function testExceptionIndexLimit(): void
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
    public function testExceptionDuplicate(Document $document): void
    {
        $document->setAttribute('$id', 'duplicated');
        static::getDatabase()->createDocument($document->getCollection(), $document);

        $this->expectException(DuplicateException::class);
        static::getDatabase()->createDocument($document->getCollection(), $document);
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionCaseInsensitiveDuplicate(Document $document): Document
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
    public function testUniqueIndexDuplicate(): void
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
    public function testUniqueIndexDuplicateUpdate(): void
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

    public function testGetAttributeLimit(): void
    {
        $this->assertIsInt($this->getDatabase()->getLimitForAttributes());
    }

    public function testGetIndexLimit(): void
    {
        $this->assertEquals(59, $this->getDatabase()->getLimitForIndexes());
    }

    public function testGetId(): void
    {
        $this->assertEquals(20, strlen(ID::unique()));
        $this->assertEquals(13, strlen(ID::unique(0)));
        $this->assertEquals(13, strlen(ID::unique(-1)));
        $this->assertEquals(23, strlen(ID::unique(10)));

        // ensure two sequential calls to getId do not give the same result
        $this->assertNotEquals(ID::unique(10), ID::unique(10));
    }

    public function testRenameIndex(): void
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
    public function testRenameIndexMissing(): void
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Index not found');
        $index = $database->renameIndex('numbers', 'index1', 'index4');
    }

    /**
     * @depends testRenameIndex
     * @expectedException Exception
     */
    public function testRenameIndexExisting(): void
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Index name already used');
        $index = $database->renameIndex('numbers', 'index3', 'index2');
    }

    public function testRenameAttribute(): void
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
        $document = $database->findOne('colors');
        $this->assertTrue($document instanceof Document);
        $this->assertEquals('black', $document->getAttribute('verbose'));
        $this->assertEquals('#000000', $document->getAttribute('hex'));
        $this->assertEquals(null, $document->getAttribute('name'));
    }

    /**
     * @depends testRenameAttribute
     * @expectedException Exception
     */
    public function textRenameAttributeMissing(): void
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Attribute not found');
        $database->renameAttribute('colors', 'name2', 'name3');
    }

    /**
     * @depends testRenameAttribute
     * @expectedException Exception
     */
    public function testRenameAttributeExisting(): void
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Attribute name already used');
        $database->renameAttribute('colors', 'verbose', 'hex');
    }

    public function testUpdateAttributeDefault(): void
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
    public function testUpdateAttributeRequired(): void
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
    public function testUpdateAttributeFilter(): void
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
    public function testUpdateAttributeFormat(): void
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
    public function testUpdateAttributeStructure(): void
    {
        // TODO: When this becomes relevant, add many more tests (from all types to all types, chaging size up&down, switchign between array/non-array...

        Structure::addFormat('priceRangeNew', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];
            return new Range($min, $max);
        }, Database::VAR_INTEGER);

        $database = static::getDatabase();

        // price attribute
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRange', $attribute['format']);
        $this->assertEquals(['min'=>1, 'max'=>10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', default: 100);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRange', $attribute['format']);
        $this->assertEquals(['min'=>1, 'max'=>10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', format: 'priceRangeNew');
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRangeNew', $attribute['format']);
        $this->assertEquals(['min'=>1, 'max'=>10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', format: '');
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals(['min'=>1, 'max'=>10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', formatOptions: ['min' => 1, 'max' => 999]);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals(['min'=>1, 'max'=>999], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', formatOptions: []);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', signed: false);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', required: true);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(true, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', type: Database::VAR_STRING, size: Database::LENGTH_KEY, format: '');
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('string', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(255, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(true, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $collection->getAttribute('attributes')[4]['formatOptions']);

        // Date attribute
        $attribute = $collection->getAttribute('attributes')[2];
        $this->assertEquals('date', $attribute['key']);
        $this->assertEquals('string', $attribute['type']);
        $this->assertEquals(null, $attribute['default']);

        $database->updateAttribute('flowers', 'date', type: Database::VAR_DATETIME, size: 0, filters: ['datetime']);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[2];
        $this->assertEquals('datetime', $attribute['type']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $doc = $database->getDocument('flowers', 'LiliPriced');
        $this->assertIsString($doc->getAttribute('price'));
        $this->assertEquals('500', $doc->getAttribute('price'));

        $doc = $database->getDocument('flowers', 'flowerWithDate');
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $doc->getAttribute('date'));
    }

    /**
     * @depends testCreatedAtUpdatedAt
     */
    public function testCreatedAtUpdatedAtAssert(): void
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

    public function testCreateDatetime(): void
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
        $this->assertEquals(null, $document->getAttribute('date2'));
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

    public function testCreateDateTimeAttributeFailure(): void
    {
        static::getDatabase()->createCollection('datetime_fail');

        /** Test for FAILURE */
        $this->expectException(Exception::class);
        static::getDatabase()->createAttribute('datetime_fail', 'date_fail', Database::VAR_DATETIME, 0, false);
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

    public function testWritePermissions(): void
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
        } catch (AuthorizationException) {
            $didFail = true;
        }

        $this->assertTrue($didFail);

        // Cannot update with delete permission:
        $didFail = false;

        try {
            $newDog = $dog->setAttribute('type', 'newDog');
            $database->updateDocument('animals', 'dog', $newDog);
        } catch (AuthorizationException) {
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

    // Relationships
    public function testOneToOneOneWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('person');
        static::getDatabase()->createCollection('library');

        static::getDatabase()->createAttribute('person', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('library', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('library', 'area', Database::VAR_STRING, 255, true);

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
        $person1 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'library' => [
                '$id' => 'library1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 1',
                'area' => 'Area 1',
            ],
        ]));

        // Update through create
        $library10 = static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Library 10',
            'area' => 'Area 10',
        ]));
        $person10 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 10',
            'library' => [
                '$id' => $library10->getId(),
                'name' => 'Library 10 Updated',
                'area' => 'Area 10 Updated',
            ],
        ]));
        $this->assertEquals('Library 10 Updated', $person10->getAttribute('library')->getAttribute('name'));
        $library10 = static::getDatabase()->getDocument('library', $library10->getId());
        $this->assertEquals('Library 10 Updated', $library10->getAttribute('name'));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Library 2',
            'area' => 'Area 2',
        ]));
        static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 2',
            'library' => 'library2',
        ]));

        // Get documents with relationship
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $library = $person1->getAttribute('library');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('person', $library);

        $person = static::getDatabase()->getDocument('person', 'person2');
        $library = $person->getAttribute('library');
        $this->assertEquals('library2', $library['$id']);
        $this->assertArrayNotHasKey('person', $library);

        // Get related documents
        $library = static::getDatabase()->getDocument('library', 'library1');
        $this->assertArrayNotHasKey('person', $library);

        $library = static::getDatabase()->getDocument('library', 'library2');
        $this->assertArrayNotHasKey('person', $library);

        // Query related document
        $people = static::getDatabase()->find('person', [
            Query::equal('library.name', ['Library 2'])
        ]);

        $this->assertEquals(1, \count($people));
        $this->assertEquals(
            'Library 2',
            $people[0]
            ->getAttribute('library')
            ->getAttribute('name')
        );

        $people = static::getDatabase()->find('person', [
            Query::select(['name'])
        ]);

        $this->assertArrayNotHasKey('library', $people[0]);

        $people = static::getDatabase()->find('person');
        $this->assertEquals(3, \count($people));

        // Select related document attributes
        $person = static::getDatabase()->findOne('person', [
            Query::select(['*', 'library.name'])
        ]);

        if (!$person instanceof Document) {
            throw new Exception('Person not found');
        }

        $this->assertEquals('Library 1', $person->getAttribute('library')->getAttribute('name'));
        $this->assertArrayNotHasKey('area', $person->getAttribute('library'));

        $person = static::getDatabase()->getDocument('person', 'person1', [
            Query::select(['*', 'library.name'])
        ]);

        $this->assertEquals('Library 1', $person->getAttribute('library')->getAttribute('name'));
        $this->assertArrayNotHasKey('area', $person->getAttribute('library'));

        // Update root document attribute without altering relationship
        $person1 = static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('name', 'Person 1 Updated')
        );

        $this->assertEquals('Person 1 Updated', $person1->getAttribute('name'));
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $this->assertEquals('Person 1 Updated', $person1->getAttribute('name'));

        // Update nested document attribute
        $person1 = static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute(
                'library',
                $person1
                ->getAttribute('library')
                ->setAttribute('name', 'Library 1 Updated')
            )
        );

        $this->assertEquals('Library 1 Updated', $person1->getAttribute('library')->getAttribute('name'));
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $this->assertEquals('Library 1 Updated', $person1->getAttribute('library')->getAttribute('name'));

        // Create new document with no relationship
        $person3 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 3',
        ]));

        // Update to relate to created document
        $person3 = static::getDatabase()->updateDocument(
            'person',
            $person3->getId(),
            $person3->setAttribute('library', new Document([
                '$id' => 'library3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Library 3',
                'area' => 'Area 3',
            ]))
        );

        $this->assertEquals('library3', $person3->getAttribute('library')['$id']);
        $person3 = static::getDatabase()->getDocument('person', 'person3');
        $this->assertEquals('library3', $person3->getAttribute('library')['$id']);

        // One to one can't relate to multiple documents, unique index throws duplicate
        try {
            static::getDatabase()->updateDocument(
                'person',
                $person1->getId(),
                $person1->setAttribute('library', 'library2')
            );
            $this->fail('Failed to throw duplicate exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Create new document
        $library4 = static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Library 4',
            'area' => 'Area 4',
        ]));

        // Relate existing document to new document
        static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('library', 'library4')
        );

        // Relate existing document to new document as nested data
        static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('library', $library4)
        );

        // Query new related document
        $people = static::getDatabase()->find('person', [
            Query::equal('library.name', ['Library 4'])
        ]);

        $this->assertEquals(1, \count($people));

        // Rename relationship key
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'library',
            newKey: 'newLibrary'
        );

        // Get document with again
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary');
        $this->assertEquals('library4', $library['$id']);

        // Create person with no relationship
        static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 4',
        ]));

        // Can delete parent document with no relation with on delete set to restrict
        $deleted = static::getDatabase()->deleteDocument('person', 'person4');
        $this->assertEquals(true, $deleted);

        $person4 = static::getDatabase()->getDocument('person', 'person4');
        $this->assertEquals(true, $person4->isEmpty());

        // Can not delete document while still related to another with on delete set to restrict
        try {
            static::getDatabase()->deleteDocument('person', 'person1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Can not delete document because it has at least one related document.', $e->getMessage());
        }

        // Can delete child document while still related to another with on delete set to restrict
        $person5 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 5',
            'newLibrary' => [
                '$id' => 'library5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 5',
                'area' => 'Area 5',
            ],
        ]));
        $deleted = static::getDatabase()->deleteDocument('library', 'library5');
        $this->assertEquals(true, $deleted);
        $person5 = static::getDatabase()->getDocument('person', 'person5');
        $this->assertEquals(null, $person5->getAttribute('newLibrary'));

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'newLibrary',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, no effect on children for one-way
        static::getDatabase()->deleteDocument('person', 'person1');

        // Delete child, set parent relating attribute to null for one-way
        static::getDatabase()->deleteDocument('library', 'library2');

        // Check relation was set to null
        $person2 = static::getDatabase()->getDocument('person', 'person2');
        $this->assertEquals(null, $person2->getAttribute('newLibrary', ''));

        // Relate to another document
        static::getDatabase()->updateDocument(
            'person',
            $person2->getId(),
            $person2->setAttribute('newLibrary', 'library4')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'newLibrary',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('person', 'person2');

        // Check parent and child were deleted
        $person = static::getDatabase()->getDocument('person', 'person2');
        $this->assertEquals(true, $person->isEmpty());

        $library = static::getDatabase()->getDocument('library', 'library4');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'person',
            'newLibrary'
        );

        // Check parent doesn't have relationship anymore
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary', '');
        $this->assertEquals(null, $library);
    }

    /**
     * @throws AuthorizationException
     * @throws LimitException
     * @throws DuplicateException
     * @throws StructureException
     * @throws \Throwable
     */
    public function testOneToOneTwoWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('country');
        static::getDatabase()->createCollection('city');

        static::getDatabase()->createAttribute('country', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('city', 'code', Database::VAR_STRING, 3, true);
        static::getDatabase()->createAttribute('city', 'name', Database::VAR_STRING, 255, true);

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
            'name' => 'England',
            'city' => [
                '$id' => 'city1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'London',
                'code' => 'LON',
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Paris',
            'code' => 'PAR',
        ]));
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'France',
            'city' => 'city2',
        ]));

        // Create from child side
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Christchurch',
            'code' => 'CHC',
            'country' => [
                '$id' => 'country3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'New Zealand',
            ],
        ]));
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Australia',
        ]));
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Sydney',
            'code' => 'SYD',
            'country' => 'country4',
        ]));

        // Get document with relationship
        $city = static::getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('country');
        $this->assertEquals('country1', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city2');
        $country = $city->getAttribute('country');
        $this->assertEquals('country2', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city3');
        $country = $city->getAttribute('country');
        $this->assertEquals('country3', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city4');
        $country = $city->getAttribute('country');
        $this->assertEquals('country4', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        // Get inverse document with relationship
        $country = static::getDatabase()->getDocument('country', 'country1');
        $city = $country->getAttribute('city');
        $this->assertEquals('city1', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country2');
        $city = $country->getAttribute('city');
        $this->assertEquals('city2', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country3');
        $city = $country->getAttribute('city');
        $this->assertEquals('city3', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country4');
        $city = $country->getAttribute('city');
        $this->assertEquals('city4', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        // Query related document
        $countries = static::getDatabase()->find('country', [
            Query::equal('city.name', ['Paris'])
        ]);

        $this->assertEquals(1, \count($countries));
        $this->assertEquals(
            'Paris',
            $countries[0]
            ->getAttribute('city')
            ->getAttribute('name')
        );

        $countries = static::getDatabase()->find('country');

        $this->assertEquals(4, \count($countries));

        // Select related document attributes
        $country = static::getDatabase()->findOne('country', [
            Query::select(['*', 'city.name'])
        ]);

        if (!$country instanceof Document) {
            throw new Exception('Country not found');
        }

        $this->assertEquals('London', $country->getAttribute('city')->getAttribute('name'));
        $this->assertArrayNotHasKey('code', $country->getAttribute('city'));

        $country = static::getDatabase()->getDocument('country', 'country1', [
            Query::select(['*', 'city.name'])
        ]);

        $this->assertEquals('London', $country->getAttribute('city')->getAttribute('name'));
        $this->assertArrayNotHasKey('code', $country->getAttribute('city'));

        $country1 = static::getDatabase()->getDocument('country', 'country1');

        // Update root document attribute without altering relationship
        $country1 = static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('name', 'Country 1 Updated')
        );

        $this->assertEquals('Country 1 Updated', $country1->getAttribute('name'));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('Country 1 Updated', $country1->getAttribute('name'));

        $city2 = static::getDatabase()->getDocument('city', 'city2');

        // Update inverse root document attribute without altering relationship
        $city2 = static::getDatabase()->updateDocument(
            'city',
            $city2->getId(),
            $city2->setAttribute('name', 'City 2 Updated')
        );

        $this->assertEquals('City 2 Updated', $city2->getAttribute('name'));
        $city2 = static::getDatabase()->getDocument('city', 'city2');
        $this->assertEquals('City 2 Updated', $city2->getAttribute('name'));

        // Update nested document attribute
        $country1 = static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute(
                'city',
                $country1
                ->getAttribute('city')
                ->setAttribute('name', 'City 1 Updated')
            )
        );

        $this->assertEquals('City 1 Updated', $country1->getAttribute('city')->getAttribute('name'));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('City 1 Updated', $country1->getAttribute('city')->getAttribute('name'));

        // Update inverse nested document attribute
        $city2 = static::getDatabase()->updateDocument(
            'city',
            $city2->getId(),
            $city2->setAttribute(
                'country',
                $city2
                ->getAttribute('country')
                ->setAttribute('name', 'Country 2 Updated')
            )
        );

        $this->assertEquals('Country 2 Updated', $city2->getAttribute('country')->getAttribute('name'));
        $city2 = static::getDatabase()->getDocument('city', 'city2');
        $this->assertEquals('Country 2 Updated', $city2->getAttribute('country')->getAttribute('name'));

        // Create new document with no relationship
        $country5 = static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Country 5',
        ]));

        // Update to relate to created document
        $country5 = static::getDatabase()->updateDocument(
            'country',
            $country5->getId(),
            $country5->setAttribute('city', new Document([
                '$id' => 'city5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => 'City 5',
                'code' => 'C5',
            ]))
        );

        $this->assertEquals('city5', $country5->getAttribute('city')['$id']);
        $country5 = static::getDatabase()->getDocument('country', 'country5');
        $this->assertEquals('city5', $country5->getAttribute('city')['$id']);

        // Create new document with no relationship
        $city6 = static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'City6',
            'code' => 'C6',
        ]));

        // Update to relate to created document
        $city6 = static::getDatabase()->updateDocument(
            'city',
            $city6->getId(),
            $city6->setAttribute('country', new Document([
                '$id' => 'country6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => 'Country 6',
            ]))
        );

        $this->assertEquals('country6', $city6->getAttribute('country')['$id']);
        $city6 = static::getDatabase()->getDocument('city', 'city6');
        $this->assertEquals('country6', $city6->getAttribute('country')['$id']);

        // One to one can't relate to multiple documents, unique index throws duplicate
        try {
            static::getDatabase()->updateDocument(
                'country',
                $country1->getId(),
                $country1->setAttribute('city', 'city2')
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $city1 = static::getDatabase()->getDocument('city', 'city1');

        // Set relationship to null
        $city1 = static::getDatabase()->updateDocument(
            'city',
            $city1->getId(),
            $city1->setAttribute('country', null)
        );

        $this->assertEquals(null, $city1->getAttribute('country'));
        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertEquals(null, $city1->getAttribute('country'));

        // Create a new city with no relation
        $city7 = static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Copenhagen',
            'code' => 'CPH',
        ]));

        // Update document with relation to new document
        static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('city', 'city7')
        );

        // Relate existing document to new document as nested data
        static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('city', $city7)
        );

        // Query new relationship
        $countries = static::getDatabase()->find('country', [
            Query::equal('city.name', ['Copenhagen'])
        ]);

        $this->assertEquals(1, \count($countries));

        // Create a new country with no relation
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Denmark'
        ]));

        // Update inverse document with new related document
        static::getDatabase()->updateDocument(
            'city',
            $city1->getId(),
            $city1->setAttribute('country', 'country7')
        );

        // Query inverse related document again
        $people = static::getDatabase()->find('city', [
            Query::equal('country.name', ['Denmark'])
        ]);

        $this->assertEquals(1, \count($people));

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
        $this->assertEquals('country7', $country['$id']);

        // Get inverse document with new relationship key
        $country = static::getDatabase()->getDocument('country', 'country7');
        $city = $country->getAttribute('newCity');
        $this->assertEquals('city1', $city['$id']);

        // Create a new country with no relation
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country8',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Denmark'
        ]));

        // Can delete parent document with no relation with on delete set to restrict
        $deleted = static::getDatabase()->deleteDocument('country', 'country8');
        $this->assertEquals(1, $deleted);

        $country8 = static::getDatabase()->getDocument('country', 'country8');
        $this->assertEquals(true, $country8->isEmpty());


        // Can not delete document while still related to another with on delete set to restrict
        try {
            static::getDatabase()->deleteDocument('country', 'country1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Can not delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'country',
            id: 'newCity',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, will set child relationship to null for two-way
        static::getDatabase()->deleteDocument('country', 'country1');

        // Check relation was set to null
        $city7 = static::getDatabase()->getDocument('city', 'city7');
        $this->assertEquals(null, $city7->getAttribute('country', ''));

        // Delete child, set parent relationship to null for two-way
        static::getDatabase()->deleteDocument('city', 'city2');

        // Check relation was set to null
        $country2 = static::getDatabase()->getDocument('country', 'country2');
        $this->assertEquals(null, $country2->getAttribute('city', ''));

        // Relate again
        static::getDatabase()->updateDocument(
            'city',
            $city7->getId(),
            $city7->setAttribute('newCountry', 'country2')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'country',
            id: 'newCity',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('country', 'country7');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('country', 'country7');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('city', 'city1');
        $this->assertEquals(true, $library->isEmpty());

        // Delete child, will delete parent for two-way
        static::getDatabase()->deleteDocument('city', 'city7');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('city', 'city7');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('country', 'country2');
        $this->assertEquals(true, $library->isEmpty());

        // Create new document to check after deleting relationship
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Munich',
            'code' => 'MUC',
            'newCountry' => [
                '$id' => 'country7',
                'name' => 'Germany'
            ]
        ]));

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'country',
            'newCity'
        );

        // Try to get document again
        $country = static::getDatabase()->getDocument('country', 'country4');
        $city = $country->getAttribute('newCity');
        $this->assertEquals(null, $city);

        // Try to get inverse document again
        $city = static::getDatabase()->getDocument('city', 'city7');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals(null, $country);
    }

    public function testOneToManyOneWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('artist');
        static::getDatabase()->createCollection('album');

        static::getDatabase()->createAttribute('artist', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('album', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('album', 'price', Database::VAR_FLOAT, 0, true);

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
        $artist1 = static::getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 1',
            'albums' => [
                [
                    '$id' => 'album1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any())
                    ],
                    'name' => 'Album 1',
                    'price' => 9.99,
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('album', new Document([
            '$id' => 'album2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Album 2',
            'price' => 19.99,
        ]));
        static::getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 2',
            'albums' => [
                'album2',
                [
                    '$id' => 'album33',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Album 3',
                    'price' => 33.33,
                ]
            ]
        ]));

        $documents = static::getDatabase()->find('artist', [
            Query::select(['name']),
            Query::limit(1)
        ]);
        $this->assertArrayNotHasKey('albums', $documents[0]);

        // Get document with relationship
        $artist = static::getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album1', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);

        $artist = static::getDatabase()->getDocument('artist', 'artist2');
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album2', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);
        $this->assertEquals('album33', $albums[1]['$id']);
        $this->assertCount(2, $albums);

        // Get related document
        $album = static::getDatabase()->getDocument('album', 'album1');
        $this->assertArrayNotHasKey('artist', $album);

        $album = static::getDatabase()->getDocument('album', 'album2');
        $this->assertArrayNotHasKey('artist', $album);

        // Query related document
        $artists = static::getDatabase()->find('artist', [
            Query::equal('albums.name', ['Album 2'])
        ]);

        $this->assertCount(1, $artists);
        $this->assertCount(2, $artists[0]['albums']);

        $this->assertEquals(
            'Album 2',
            $artists[0]
            ->getAttribute('albums')[0]
            ->getAttribute('name')
        );

        $artists = static::getDatabase()->find('artist');

        $this->assertEquals(2, \count($artists));

        // Select related document attributes
        $artist = static::getDatabase()->findOne('artist', [
            Query::select(['*', 'albums.name'])
        ]);

        if (!$artist instanceof Document) {
            $this->fail('Artist not found');
        }

        $this->assertEquals('Album 1', $artist->getAttribute('albums')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('price', $artist->getAttribute('albums')[0]);

        $artist = static::getDatabase()->getDocument('artist', 'artist1', [
            Query::select(['*', 'albums.name'])
        ]);

        $this->assertEquals('Album 1', $artist->getAttribute('albums')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('price', $artist->getAttribute('albums')[0]);

        // Update root document attribute without altering relationship
        $artist1 = static::getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('name', 'Artist 1 Updated')
        );

        $this->assertEquals('Artist 1 Updated', $artist1->getAttribute('name'));
        $artist1 = static::getDatabase()->getDocument('artist', 'artist1');
        $this->assertEquals('Artist 1 Updated', $artist1->getAttribute('name'));

        // Update nested document attribute
        $albums = $artist1->getAttribute('albums', []);
        $albums[0]->setAttribute('name', 'Album 1 Updated');

        $artist1 = static::getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', $albums)
        );

        $this->assertEquals('Album 1 Updated', $artist1->getAttribute('albums')[0]->getAttribute('name'));
        $artist1 = static::getDatabase()->getDocument('artist', 'artist1');
        $this->assertEquals('Album 1 Updated', $artist1->getAttribute('albums')[0]->getAttribute('name'));

        // Create new document with no relationship
        $artist3 = static::getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 3',
        ]));

        // Update to relate to created document
        $artist3 = static::getDatabase()->updateDocument(
            'artist',
            $artist3->getId(),
            $artist3->setAttribute('albums', [new Document([
                '$id' => 'album3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Album 3',
                'price' => 29.99,
            ])])
        );

        $this->assertEquals('Album 3', $artist3->getAttribute('albums')[0]->getAttribute('name'));
        $artist3 = static::getDatabase()->getDocument('artist', 'artist3');
        $this->assertEquals('Album 3', $artist3->getAttribute('albums')[0]->getAttribute('name'));

        // Update document with new related documents, will remove existing relations
        static::getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', ['album2'])
        );

        // Query related document again
        $artists = static::getDatabase()->find('artist', [
            Query::equal('albums.name', ['Album 2'])
        ]);

        $this->assertEquals(1, \count($artists));
        $this->assertEquals(1, \count($artists[0]['albums']));

        // Update document with new related documents, will remove existing relations
        static::getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', ['album1', 'album2'])
        );

        // Query related document again
        $artists = static::getDatabase()->find('artist', [
            Query::equal('albums.name', ['Album 2'])
        ]);

        $this->assertEquals(1, \count($artists));
        $this->assertEquals(2, \count($artists[0]['albums']));

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

        // Create new document with no relationship
        static::getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 4',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('artist', 'artist4');
        $this->assertEquals(true, $deleted);

        $artist4 = static::getDatabase()->getDocument('artist', 'artist4');
        $this->assertEquals(true, $artist4->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('artist', 'artist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Can not delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'artist',
            id: 'newAlbums',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, set child relationship to null
        static::getDatabase()->deleteDocument('artist', 'artist1');

        // Check relation was set to null
        $album2 = static::getDatabase()->getDocument('album', 'album2');
        $this->assertEquals(null, $album2->getAttribute('artist', ''));

        // Relate again
        static::getDatabase()->updateDocument(
            'album',
            $album2->getId(),
            $album2->setAttribute('artist', 'artist2')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'artist',
            id: 'newAlbums',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('artist', 'artist2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('artist', 'artist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('album', 'album2');
        $this->assertEquals(true, $library->isEmpty());

        $albums = [];
        for ($i = 1 ; $i <= 50 ; $i++) {
            $albums[] = [
                '$id' => 'album_' . $i,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'album ' . $i . ' ' . 'Artist 100',
                'price' => 100,
            ];
        }

        $artist = static::getDatabase()->createDocument('artist', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 100',
            'newAlbums' => $albums
        ]));

        $artist = static::getDatabase()->getDocument('artist', $artist->getId());
        $this->assertCount(50, $artist->getAttribute('newAlbums'));

        $albums = static::getDatabase()->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999)
        ]);

        $this->assertCount(50, $albums);

        $count = static::getDatabase()->count('album', [
            Query::equal('artist', [$artist->getId()]),
        ]);

        $this->assertEquals(50, $count);

        static::getDatabase()->deleteDocument('artist', $artist->getId());

        $albums = static::getDatabase()->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999)
        ]);

        $this->assertCount(0, $albums);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'artist',
            'newAlbums'
        );

        // Try to get document again
        $artist = static::getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('newAlbums', '');
        $this->assertEquals(null, $albums);
    }

    public function testOneToManyTwoWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('customer');
        static::getDatabase()->createCollection('account');

        static::getDatabase()->createAttribute('customer', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('account', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('account', 'number', Database::VAR_STRING, 255, true);

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
        $customer1 = static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 1',
            'accounts' => [
                [
                    '$id' => 'account1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Account 1',
                    'number' => '123456789',
                ],
            ],
        ]));
        // Create document with relationship with related ID
        $account2 = static::getDatabase()->createDocument('account', new Document([
            '$id' => 'account2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 2',
            'number' => '987654321',
        ]));
        static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 2',
            'accounts' => [
                'account2'
            ]
        ]));

        // Create from child side
        static::getDatabase()->createDocument('account', new Document([
            '$id' => 'account3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 3',
            'number' => '123456789',
            'customer' => [
                '$id' => 'customer3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Customer 3'
            ]
        ]));
        static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 4',
        ]));
        static::getDatabase()->createDocument('account', new Document([
            '$id' => 'account4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 4',
            'number' => '123456789',
            'customer' => 'customer4'
        ]));

        // Get documents with relationship
        $customer = static::getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account1', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = static::getDatabase()->getDocument('customer', 'customer2');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account2', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = static::getDatabase()->getDocument('customer', 'customer3');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account3', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = static::getDatabase()->getDocument('customer', 'customer4');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account4', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        // Get related documents
        $account = static::getDatabase()->getDocument('account', 'account1');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer1', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = static::getDatabase()->getDocument('account', 'account2');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer2', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = static::getDatabase()->getDocument('account', 'account3');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer3', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = static::getDatabase()->getDocument('account', 'account4');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer4', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        // Query related document
        $customers = static::getDatabase()->find('customer', [
            Query::equal('accounts.name', ['Account 2'])
        ]);

        $this->assertEquals(1, \count($customers));

        $this->assertEquals(
            'Account 2',
            $customers[0]
            ->getAttribute('accounts')[0]
            ->getAttribute('name')
        );

        $customers = static::getDatabase()->find('customer');

        $this->assertEquals(4, \count($customers));

        // Select related document attributes
        $customer = static::getDatabase()->findOne('customer', [
            Query::select(['*', 'accounts.name'])
        ]);

        if (!$customer instanceof Document) {
            throw new Exception('Customer not found');
        }

        $this->assertEquals('Account 1', $customer->getAttribute('accounts')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $customer->getAttribute('accounts')[0]);

        $customer = static::getDatabase()->getDocument('customer', 'customer1', [
            Query::select(['*', 'accounts.name'])
        ]);

        $this->assertEquals('Account 1', $customer->getAttribute('accounts')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $customer->getAttribute('accounts')[0]);

        // Update root document attribute without altering relationship
        $customer1 = static::getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('name', 'Customer 1 Updated')
        );

        $this->assertEquals('Customer 1 Updated', $customer1->getAttribute('name'));
        $customer1 = static::getDatabase()->getDocument('customer', 'customer1');
        $this->assertEquals('Customer 1 Updated', $customer1->getAttribute('name'));

        $account2 = static::getDatabase()->getDocument('account', 'account2');

        // Update inverse root document attribute without altering relationship
        $account2 = static::getDatabase()->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute('name', 'Account 2 Updated')
        );

        $this->assertEquals('Account 2 Updated', $account2->getAttribute('name'));
        $account2 = static::getDatabase()->getDocument('account', 'account2');
        $this->assertEquals('Account 2 Updated', $account2->getAttribute('name'));

        // Update nested document attribute
        $accounts = $customer1->getAttribute('accounts', []);
        $accounts[0]->setAttribute('name', 'Account 1 Updated');

        $customer1 = static::getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', $accounts)
        );

        $this->assertEquals('Account 1 Updated', $customer1->getAttribute('accounts')[0]->getAttribute('name'));
        $customer1 = static::getDatabase()->getDocument('customer', 'customer1');
        $this->assertEquals('Account 1 Updated', $customer1->getAttribute('accounts')[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $account2 = static::getDatabase()->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute(
                'customer',
                $account2
                ->getAttribute('customer')
                ->setAttribute('name', 'Customer 2 Updated')
            )
        );

        $this->assertEquals('Customer 2 Updated', $account2->getAttribute('customer')->getAttribute('name'));
        $account2 = static::getDatabase()->getDocument('account', 'account2');
        $this->assertEquals('Customer 2 Updated', $account2->getAttribute('customer')->getAttribute('name'));

        // Create new document with no relationship
        $customer5 = static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 5',
        ]));

        // Update to relate to created document
        $customer5 = static::getDatabase()->updateDocument(
            'customer',
            $customer5->getId(),
            $customer5->setAttribute('accounts', [new Document([
                '$id' => 'account5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Account 5',
                'number' => '123456789',
            ])])
        );

        $this->assertEquals('Account 5', $customer5->getAttribute('accounts')[0]->getAttribute('name'));
        $customer5 = static::getDatabase()->getDocument('customer', 'customer5');
        $this->assertEquals('Account 5', $customer5->getAttribute('accounts')[0]->getAttribute('name'));

        // Create new child document with no relationship
        $account6 = static::getDatabase()->createDocument('account', new Document([
            '$id' => 'account6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 6',
            'number' => '123456789',
        ]));

        // Update inverse to relate to created document
        $account6 = static::getDatabase()->updateDocument(
            'account',
            $account6->getId(),
            $account6->setAttribute('customer', new Document([
                '$id' => 'customer6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Customer 6',
            ]))
        );

        $this->assertEquals('Customer 6', $account6->getAttribute('customer')->getAttribute('name'));
        $account6 = static::getDatabase()->getDocument('account', 'account6');
        $this->assertEquals('Customer 6', $account6->getAttribute('customer')->getAttribute('name'));

        // Update document with new related document, will remove existing relations
        static::getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', ['account2'])
        );

        // Query related document again
        $customers = static::getDatabase()->find('customer', [
            Query::equal('accounts.name', ['Account 2 Updated'])
        ]);

        $this->assertEquals(1, \count($customers));
        $this->assertEquals(1, \count($customers[0]['accounts']));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', ['account1', 'account2'])
        );

        // Query related document again
        $customers = static::getDatabase()->find('customer', [
            Query::equal('accounts.name', ['Account 2 Updated'])
        ]);

        $this->assertEquals(1, \count($customers));
        $this->assertEquals(2, \count($customers[0]['accounts']));

        // Update inverse document
        static::getDatabase()->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute('customer', 'customer2')
        );

        // Query related document again
        $customers = static::getDatabase()->find('customer', [
            Query::equal('accounts.name', ['Account 2 Updated'])
        ]);

        $this->assertEquals(1, \count($customers));
        $this->assertEquals(1, \count($customers[0]['accounts']));

        // Query inverse document again
        $customers = static::getDatabase()->find('account', [
            Query::equal('customer.name', ['Customer 2 Updated'])
        ]);

        $this->assertEquals(1, \count($customers));

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

        // Create new document with no relationship
        static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('customer', 'customer7');
        $this->assertEquals(true, $deleted);

        $customer7 = static::getDatabase()->getDocument('customer', 'customer7');
        $this->assertEquals(true, $customer7->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('customer', 'customer1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Can not delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'customer',
            id: 'newAccounts',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, set child relationship to null
        static::getDatabase()->deleteDocument('customer', 'customer1');

        // Check relation was set to null
        $account1 = static::getDatabase()->getDocument('account', 'account1');
        $this->assertEquals(null, $account2->getAttribute('newCustomer', ''));

        // Relate again
        static::getDatabase()->updateDocument(
            'account',
            $account1->getId(),
            $account1->setAttribute('newCustomer', 'customer2')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'customer',
            id: 'newAccounts',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('customer', 'customer2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('customer', 'customer2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('account', 'account2');
        $this->assertEquals(true, $library->isEmpty());

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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('review');
        static::getDatabase()->createCollection('movie');

        static::getDatabase()->createAttribute('review', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('movie', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('movie', 'length', Database::VAR_INTEGER, 0, true, formatOptions: ['min' => 0, 'max' => 999]);
        static::getDatabase()->createAttribute('movie', 'date', Database::VAR_DATETIME, 0, false, filters: ['datetime']);
        static::getDatabase()->createAttribute('review', 'date', Database::VAR_DATETIME, 0, false, filters: ['datetime']);
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
            if ($attribute['key'] === 'movie') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('movie', $attribute['$id']);
                $this->assertEquals('movie', $attribute['key']);
                $this->assertEquals('movie', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('reviews', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = static::getDatabase()->getCollection('movie');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'reviews') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('reviews', $attribute['$id']);
                $this->assertEquals('reviews', $attribute['key']);
                $this->assertEquals('review', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('movie', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $review1 = static::getDatabase()->createDocument('review', new Document([
            '$id' => 'review1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 1',
            'date' => '2023-04-03 10:35:27.390',
            'movie' => [
                '$id' => 'movie1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Movie 1',
                'date' => '2023-04-03 10:35:27.390',
                'length' => 120,
            ],
        ]));

        // Create document with relationship to existing document by ID
        $review10 = static::getDatabase()->createDocument('review', new Document([
            '$id' => 'review10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 10',
            'movie' => 'movie1',
            'date' => '2023-04-03 10:35:27.390',
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('movie', new Document([
            '$id' => 'movie2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Movie 2',
            'length' => 90,
            'date' => '2023-04-03 10:35:27.390',
        ]));
        static::getDatabase()->createDocument('review', new Document([
            '$id' => 'review2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 2',
            'movie' => 'movie2',
            'date' => '2023-04-03 10:35:27.390',
        ]));

        // Get document with relationship
        $review = static::getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('movie', []);
        $this->assertEquals('movie1', $movie['$id']);
        $this->assertArrayNotHasKey('reviews', $movie);

        $documents = static::getDatabase()->find('review', [
            Query::select(['date', 'movie.date'])
        ]);

        $this->assertCount(3, $documents);

        $document = $documents[0];
        $this->assertArrayHasKey('date', $document);
        $this->assertArrayHasKey('movie', $document);
        $this->assertArrayHasKey('date', $document->getAttribute('movie'));
        $this->assertArrayNotHasKey('name', $document);
        $this->assertEquals(29, strlen($document['date'])); // checks filter
        $this->assertEquals(29, strlen($document['movie']['date']));

        $review = static::getDatabase()->getDocument('review', 'review2');
        $movie = $review->getAttribute('movie', []);
        $this->assertEquals('movie2', $movie['$id']);
        $this->assertArrayNotHasKey('reviews', $movie);

        // Get related document
        $movie = static::getDatabase()->getDocument('movie', 'movie1');
        $this->assertArrayNotHasKey('reviews', $movie);

        $movie = static::getDatabase()->getDocument('movie', 'movie2');
        $this->assertArrayNotHasKey('reviews', $movie);

        // Query related document
        $reviews = static::getDatabase()->find('review', [
            Query::equal('movie.name', ['Movie 2'])
        ]);

        $this->assertEquals(1, \count($reviews));

        $this->assertEquals(
            'Movie 2',
            $reviews[0]
            ->getAttribute('movie')
            ->getAttribute('name')
        );

        $reviews = static::getDatabase()->find('review');

        $this->assertEquals(3, \count($reviews));

        // Select related document attributes
        $review = static::getDatabase()->findOne('review', [
            Query::select(['*', 'movie.name'])
        ]);

        if (!$review instanceof Document) {
            throw new Exception('Review not found');
        }

        $this->assertEquals('Movie 1', $review->getAttribute('movie')->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $review->getAttribute('movie'));

        $review = static::getDatabase()->getDocument('review', 'review1', [
            Query::select(['*', 'movie.name'])
        ]);

        $this->assertEquals('Movie 1', $review->getAttribute('movie')->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $review->getAttribute('movie'));

        // Update root document attribute without altering relationship
        $review1 = static::getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('name', 'Review 1 Updated')
        );

        $this->assertEquals('Review 1 Updated', $review1->getAttribute('name'));
        $review1 = static::getDatabase()->getDocument('review', 'review1');
        $this->assertEquals('Review 1 Updated', $review1->getAttribute('name'));

        // Update nested document attribute
        $movie = $review1->getAttribute('movie');
        $movie->setAttribute('name', 'Movie 1 Updated');

        $review1 = static::getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('movie', $movie)
        );

        $this->assertEquals('Movie 1 Updated', $review1->getAttribute('movie')->getAttribute('name'));
        $review1 = static::getDatabase()->getDocument('review', 'review1');
        $this->assertEquals('Movie 1 Updated', $review1->getAttribute('movie')->getAttribute('name'));

        // Create new document with no relationship
        $review5 = static::getDatabase()->createDocument('review', new Document([
            '$id' => 'review5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 5',
        ]));

        // Update to relate to created document
        $review5 = static::getDatabase()->updateDocument(
            'review',
            $review5->getId(),
            $review5->setAttribute('movie', new Document([
                '$id' => 'movie5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Movie 5',
                'length' => 90,
            ]))
        );

        $this->assertEquals('Movie 5', $review5->getAttribute('movie')->getAttribute('name'));
        $review5 = static::getDatabase()->getDocument('review', 'review5');
        $this->assertEquals('Movie 5', $review5->getAttribute('movie')->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('movie', 'movie2')
        );

        // Query related document again
        $reviews = static::getDatabase()->find('review', [
            Query::equal('movie.name', ['Movie 2'])
        ]);

        $this->assertEquals(2, \count($reviews));

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'review',
            'movie',
            'newMovie',
        );

        // Get document with new relationship key
        $review = static::getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('newMovie');
        $this->assertEquals('movie2', $movie['$id']);

        // Reset values
        $review1 = static::getDatabase()->getDocument('review', 'review1');

        static::getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('newMovie', 'movie1')
        );

        // Create new document with no relationship
        static::getDatabase()->createDocument('movie', new Document([
            '$id' => 'movie3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Movie 3',
            'length' => 90,
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('movie', 'movie3');
        $this->assertEquals(true, $deleted);

        $movie3 = static::getDatabase()->getDocument('movie', 'movie3');
        $this->assertEquals(true, $movie3->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('movie', 'movie1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Can not delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'review',
            id: 'newMovie',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete child, set parent relationship to null
        static::getDatabase()->deleteDocument('movie', 'movie1');

        // Check relation was set to null
        $review1 = static::getDatabase()->getDocument('review', 'review1');
        $this->assertEquals(null, $review1->getAttribute('newMovie'));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'review',
            id: 'newMovie',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete child, will delete parent
        static::getDatabase()->deleteDocument('movie', 'movie2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('movie', 'movie2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('review', 'review2');
        $this->assertEquals(true, $library->isEmpty());


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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('product');
        static::getDatabase()->createCollection('store');

        static::getDatabase()->createAttribute('store', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('store', 'opensAt', Database::VAR_STRING, 5, true);

        static::getDatabase()->createAttribute(
            collection: 'product',
            id: 'name',
            type: Database::VAR_STRING,
            size: 255,
            required: true
        );

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
            if ($attribute['key'] === 'store') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('store', $attribute['$id']);
                $this->assertEquals('store', $attribute['key']);
                $this->assertEquals('store', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('products', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = static::getDatabase()->getCollection('store');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'products') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('products', $attribute['$id']);
                $this->assertEquals('products', $attribute['key']);
                $this->assertEquals('product', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('store', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $product1 = static::getDatabase()->createDocument('product', new Document([
            '$id' => 'product1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 1',
            'store' => [
                '$id' => 'store1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Store 1',
                'opensAt' => '09:00',
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('store', new Document([
            '$id' => 'store2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 2',
            'opensAt' => '09:30',
        ]));
        static::getDatabase()->createDocument('product', new Document([
            '$id' => 'product2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 2',
            'store' => 'store2',
        ]));

        // Create from child side
        static::getDatabase()->createDocument('store', new Document([
            '$id' => 'store3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 3',
            'opensAt' => '11:30',
            'products' => [
                [
                    '$id' => 'product3',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Product 3',
                ],
            ],
        ]));

        static::getDatabase()->createDocument('product', new Document([
            '$id' => 'product4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 4',
        ]));
        static::getDatabase()->createDocument('store', new Document([
            '$id' => 'store4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 4',
            'opensAt' => '11:30',
            'products' => [
                'product4',
            ],
        ]));

        // Get document with relationship
        $product = static::getDatabase()->getDocument('product', 'product1');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store1', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = static::getDatabase()->getDocument('product', 'product2');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store2', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = static::getDatabase()->getDocument('product', 'product3');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store3', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = static::getDatabase()->getDocument('product', 'product4');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store4', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        // Get related document
        $store = static::getDatabase()->getDocument('store', 'store1');
        $products = $store->getAttribute('products');
        $this->assertEquals('product1', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = static::getDatabase()->getDocument('store', 'store2');
        $products = $store->getAttribute('products');
        $this->assertEquals('product2', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = static::getDatabase()->getDocument('store', 'store3');
        $products = $store->getAttribute('products');
        $this->assertEquals('product3', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = static::getDatabase()->getDocument('store', 'store4');
        $products = $store->getAttribute('products');
        $this->assertEquals('product4', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        // Query related document
        $products = static::getDatabase()->find('product', [
            Query::equal('store.name', ['Store 2'])
        ]);

        $this->assertEquals(1, \count($products));

        $this->assertEquals(
            'Store 2',
            $products[0]
            ->getAttribute('store')
            ->getAttribute('name')
        );

        $products = static::getDatabase()->find('product');

        $this->assertEquals(4, \count($products));

        // Select related document attributes
        $product = static::getDatabase()->findOne('product', [
            Query::select(['*', 'store.name'])
        ]);

        if (!$product instanceof Document) {
            throw new Exception('Product not found');
        }

        $this->assertEquals('Store 1', $product->getAttribute('store')->getAttribute('name'));
        $this->assertArrayNotHasKey('opensAt', $product->getAttribute('store'));

        $product = static::getDatabase()->getDocument('product', 'product1', [
            Query::select(['*', 'store.name'])
        ]);

        $this->assertEquals('Store 1', $product->getAttribute('store')->getAttribute('name'));
        $this->assertArrayNotHasKey('opensAt', $product->getAttribute('store'));

        // Update root document attribute without altering relationship
        $product1 = static::getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('name', 'Product 1 Updated')
        );

        $this->assertEquals('Product 1 Updated', $product1->getAttribute('name'));
        $product1 = static::getDatabase()->getDocument('product', 'product1');
        $this->assertEquals('Product 1 Updated', $product1->getAttribute('name'));

        // Update inverse document attribute without altering relationship
        $store1 = static::getDatabase()->getDocument('store', 'store1');
        $store1 = static::getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('name', 'Store 1 Updated')
        );

        $this->assertEquals('Store 1 Updated', $store1->getAttribute('name'));
        $store1 = static::getDatabase()->getDocument('store', 'store1');
        $this->assertEquals('Store 1 Updated', $store1->getAttribute('name'));

        // Update nested document attribute
        $store = $product1->getAttribute('store');
        $store->setAttribute('name', 'Store 1 Updated');

        $product1 = static::getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('store', $store)
        );

        $this->assertEquals('Store 1 Updated', $product1->getAttribute('store')->getAttribute('name'));
        $product1 = static::getDatabase()->getDocument('product', 'product1');
        $this->assertEquals('Store 1 Updated', $product1->getAttribute('store')->getAttribute('name'));

        // Update inverse nested document attribute
        $product = $store1->getAttribute('products')[0];
        $product->setAttribute('name', 'Product 1 Updated');

        $store1 = static::getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('products', [$product])
        );

        $this->assertEquals('Product 1 Updated', $store1->getAttribute('products')[0]->getAttribute('name'));
        $store1 = static::getDatabase()->getDocument('store', 'store1');
        $this->assertEquals('Product 1 Updated', $store1->getAttribute('products')[0]->getAttribute('name'));

        // Create new document with no relationship
        $product5 = static::getDatabase()->createDocument('product', new Document([
            '$id' => 'product5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 5',
        ]));

        // Update to relate to created document
        $product5 = static::getDatabase()->updateDocument(
            'product',
            $product5->getId(),
            $product5->setAttribute('store', new Document([
                '$id' => 'store5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Store 5',
                'opensAt' => '09:00',
            ]))
        );

        $this->assertEquals('Store 5', $product5->getAttribute('store')->getAttribute('name'));
        $product5 = static::getDatabase()->getDocument('product', 'product5');
        $this->assertEquals('Store 5', $product5->getAttribute('store')->getAttribute('name'));

        // Create new child document with no relationship
        $store6 = static::getDatabase()->createDocument('store', new Document([
            '$id' => 'store6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 6',
            'opensAt' => '09:00',
        ]));

        // Update inverse to related to newly created document
        $store6 = static::getDatabase()->updateDocument(
            'store',
            $store6->getId(),
            $store6->setAttribute('products', [new Document([
                '$id' => 'product6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Product 6',
            ])])
        );

        $this->assertEquals('Product 6', $store6->getAttribute('products')[0]->getAttribute('name'));
        $store6 = static::getDatabase()->getDocument('store', 'store6');
        $this->assertEquals('Product 6', $store6->getAttribute('products')[0]->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('store', 'store2')
        );

        // Query related document again
        $products = static::getDatabase()->find('product', [
            Query::equal('store.name', ['Store 2'])
        ]);

        $this->assertEquals(2, \count($products));

        $store1 = static::getDatabase()->getDocument('store', 'store1');

        // Update inverse document
        static::getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('products', ['product1'])
        );

        // Query related document again
        $stores = static::getDatabase()->find('store', [
            Query::equal('products.name', ['Product 1 Updated'])
        ]);

        $this->assertEquals(1, \count($stores));
        $this->assertEquals(1, \count($stores[0]['products']));

        // Query inverse document again
        $products = static::getDatabase()->find('product', [
            Query::equal('store.name', ['Store 2'])
        ]);

        $this->assertEquals(1, \count($products));

        $store2 = static::getDatabase()->getDocument('store', 'store2');

        // Update inverse document
        static::getDatabase()->updateDocument(
            'store',
            $store2->getId(),
            $store2->setAttribute('products', ['product1', 'product2'])
        );

        // Query related document again
        $stores = static::getDatabase()->find('store', [
            Query::equal('products.name', ['Product 1 Updated'])
        ]);

        $this->assertEquals(1, \count($stores));
        $this->assertEquals(2, \count($stores[0]['products']));

        // Query inverse document again
        $products = static::getDatabase()->find('product', [
            Query::equal('store.name', ['Store 2'])
        ]);

        $this->assertEquals(2, \count($products));

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'product',
            'store',
            'newStore',
            'newProducts'
        );

        // Get document with new relationship key
        $store = static::getDatabase()->getDocument('store', 'store2');
        $products = $store->getAttribute('newProducts');
        $this->assertEquals('product1', $products[0]['$id']);

        // Get inverse document with new relationship key
        $product = static::getDatabase()->getDocument('product', 'product1');
        $store = $product->getAttribute('newStore');
        $this->assertEquals('store2', $store['$id']);

        // Reset relationships
        $store1 = static::getDatabase()->getDocument('store', 'store1');
        static::getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('newProducts', ['product1'])
        );

        // Create new document with no relationship
        static::getDatabase()->createDocument('store', new Document([
            '$id' => 'store7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 7',
            'opensAt' => '09:00',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('store', 'store7');
        $this->assertEquals(true, $deleted);

        $store7 = static::getDatabase()->getDocument('store', 'store7');
        $this->assertEquals(true, $store7->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('store', 'store1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Can not delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'product',
            id: 'newStore',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete child, set parent relationship to null
        static::getDatabase()->deleteDocument('store', 'store1');

        // Check relation was set to null
        $review1 = static::getDatabase()->getDocument('product', 'product1');
        $this->assertEquals(null, $product1->getAttribute('newStore'));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'product',
            id: 'newStore',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete child, will delete parent
        static::getDatabase()->deleteDocument('store', 'store2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('store', 'store2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('product', 'product2');
        $this->assertEquals(true, $library->isEmpty());

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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('playlist');
        static::getDatabase()->createCollection('song');

        static::getDatabase()->createAttribute('playlist', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('song', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('song', 'length', Database::VAR_INTEGER, 0, true);

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
        $playlist1 = static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 1',
            'songs' => [
                [
                    '$id' => 'song1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Song 1',
                    'length' => 180,
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('song', new Document([
            '$id' => 'song2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Song 2',
            'length' => 140,
        ]));
        static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 2',
            'songs' => [
                'song2'
            ]
        ]));

        $documents = static::getDatabase()->find('playlist', [
            Query::select(['name']),
            Query::limit(1)
        ]);

        $this->assertArrayNotHasKey('songs', $documents[0]);

        // Get document with relationship
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song1', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        $playlist = static::getDatabase()->getDocument('playlist', 'playlist2');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song2', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        // Get related document
        $library = static::getDatabase()->getDocument('song', 'song1');
        $this->assertArrayNotHasKey('songs', $library);

        $library = static::getDatabase()->getDocument('song', 'song2');
        $this->assertArrayNotHasKey('songs', $library);

        // Query related document
        $playlists = static::getDatabase()->find('playlist', [
            Query::equal('songs.name', ['Song 2'])
        ]);

        $this->assertEquals(1, \count($playlists));

        $this->assertEquals(
            'Song 2',
            $playlists[0]
            ->getAttribute('songs')[0]
            ->getAttribute('name')
        );

        $playlists = static::getDatabase()->find('playlist');

        $this->assertEquals(2, \count($playlists));

        // Select related document attributes
        $playlist = static::getDatabase()->findOne('playlist', [
            Query::select(['*', 'songs.name'])
        ]);

        if (!$playlist instanceof Document) {
            throw new Exception('Playlist not found');
        }

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1', [
            Query::select(['*', 'songs.name'])
        ]);

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        // Update root document attribute without altering relationship
        $playlist1 = static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('name', 'Playlist 1 Updated')
        );

        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));

        // Update nested document attribute
        $songs = $playlist1->getAttribute('songs', []);
        $songs[0]->setAttribute('name', 'Song 1 Updated');

        $playlist1 = static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', $songs)
        );

        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));

        // Create new document with no relationship
        $playlist5 = static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 5',
        ]));

        // Update to relate to created document
        $playlist5 = static::getDatabase()->updateDocument(
            'playlist',
            $playlist5->getId(),
            $playlist5->setAttribute('songs', [new Document([
                '$id' => 'song5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Song 5',
                'length' => 180,
            ])])
        );

        // Playlist relating to existing songs that belong to other playlists
        static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 6',
            'songs' => [
                'song1',
                'song2',
                'song5'
            ]
        ]));

        $this->assertEquals('Song 5', $playlist5->getAttribute('songs')[0]->getAttribute('name'));
        $playlist5 = static::getDatabase()->getDocument('playlist', 'playlist5');
        $this->assertEquals('Song 5', $playlist5->getAttribute('songs')[0]->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', ['song2'])
        );

        // Query related document again
        $playlists = static::getDatabase()->find('playlist', [
            Query::equal('songs.name', ['Song 2'])
        ]);

        $this->assertEquals(3, \count($playlists));
        $this->assertEquals(1, \count($playlists[0]['songs']));

        // Rename relationship key
        static::getDatabase()->updateRelationship(
            'playlist',
            'songs',
            'newSongs'
        );

        // Get document with new relationship key
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals('song2', $songs[0]['$id']);

        // Create new document with no relationship
        static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 3',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('playlist', 'playlist3');
        $this->assertEquals(true, $deleted);

        $playlist3 = static::getDatabase()->getDocument('playlist', 'playlist3');
        $this->assertEquals(true, $playlist3->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('playlist', 'playlist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Can not delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');

        // Reset relationships
        static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('newSongs', ['song1'])
        );

        // Delete child, will delete junction
        static::getDatabase()->deleteDocument('song', 'song1');

        // Check relation was set to null
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals([], $playlist1->getAttribute('newSongs'));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('playlist', 'playlist2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('playlist', 'playlist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('song', 'song2');
        $this->assertEquals(true, $library->isEmpty());

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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('students');
        static::getDatabase()->createCollection('classes');

        static::getDatabase()->createAttribute('students', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classes', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classes', 'number', Database::VAR_INTEGER, 0, true);

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
            if ($attribute['key'] === 'classes') {
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
        $student1 = static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 1',
            'classes' => [
                [
                    '$id' => 'class1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Class 1',
                    'number' => 1,
                ],
            ],
        ]));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('classes', new Document([
            '$id' => 'class2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),

            ],
            'name' => 'Class 2',
            'number' => 2,
        ]));
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 2',
            'classes' => [
                'class2'
            ],
        ]));

        // Create from child side
        static::getDatabase()->createDocument('classes', new Document([
            '$id' => 'class3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Class 3',
            'number' => 3,
            'students' => [
                [
                    '$id' => 'student3',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Student 3',
                ]
            ],
        ]));
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 4'
        ]));
        static::getDatabase()->createDocument('classes', new Document([
            '$id' => 'class4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),

            ],
            'name' => 'Class 4',
            'number' => 4,
            'students' => [
                'student4'
            ],
        ]));

        // Get document with relationship
        $student = static::getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class1', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student2');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class2', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student3');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class3', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student4');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class4', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        // Get related document
        $class = static::getDatabase()->getDocument('classes', 'class1');
        $student = $class->getAttribute('students');
        $this->assertEquals('student1', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class2');
        $student = $class->getAttribute('students');
        $this->assertEquals('student2', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class3');
        $student = $class->getAttribute('students');
        $this->assertEquals('student3', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class4');
        $student = $class->getAttribute('students');
        $this->assertEquals('student4', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        // Query related document
        $students = static::getDatabase()->find('students', [
            Query::equal('classes.name', ['Class 2'])
        ]);

        $this->assertEquals(1, \count($students));

        $this->assertEquals(
            'Class 2',
            $students[0]
            ->getAttribute('classes')[0]
            ->getAttribute('name')
        );

        $students = static::getDatabase()->find('students');

        $this->assertEquals(4, \count($students));

        // Select related document attributes
        $student = static::getDatabase()->findOne('students', [
            Query::select(['*', 'classes.name'])
        ]);

        if (!$student instanceof Document) {
            throw new Exception('Student not found');
        }

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        $student = static::getDatabase()->getDocument('students', 'student1', [
            Query::select(['*', 'classes.name'])
        ]);

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        // Update root document attribute without altering relationship
        $student1 = static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('name', 'Student 1 Updated')
        );

        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));

        // Update inverse root document attribute without altering relationship
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $class2 = static::getDatabase()->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('name', 'Class 2 Updated')
        );

        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));

        // Update nested document attribute
        $classes = $student1->getAttribute('classes', []);
        $classes[0]->setAttribute('name', 'Class 1 Updated');

        $student1 = static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', $classes)
        );

        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $students = $class2->getAttribute('students', []);
        $students[0]->setAttribute('name', 'Student 2 Updated');

        $class2 = static::getDatabase()->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('students', $students)
        );

        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));

        // Create new document with no relationship
        $student5 = static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 5',
        ]));

        // Update to relate to created document
        $student5 = static::getDatabase()->updateDocument(
            'students',
            $student5->getId(),
            $student5->setAttribute('classes', [new Document([
                '$id' => 'class5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Class 5',
                'number' => 5,
            ])])
        );

        $this->assertEquals('Class 5', $student5->getAttribute('classes')[0]->getAttribute('name'));
        $student5 = static::getDatabase()->getDocument('students', 'student5');
        $this->assertEquals('Class 5', $student5->getAttribute('classes')[0]->getAttribute('name'));

        // Create child document with no relationship
        $class6 = static::getDatabase()->createDocument('classes', new Document([
            '$id' => 'class6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Class 6',
            'number' => 6,
        ]));

        // Update to relate to created document
        $class6 = static::getDatabase()->updateDocument(
            'classes',
            $class6->getId(),
            $class6->setAttribute('students', [new Document([
                '$id' => 'student6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Student 6',
            ])])
        );

        $this->assertEquals('Student 6', $class6->getAttribute('students')[0]->getAttribute('name'));
        $class6 = static::getDatabase()->getDocument('classes', 'class6');
        $this->assertEquals('Student 6', $class6->getAttribute('students')[0]->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', ['class2'])
        );

        // Query related document again
        $students = static::getDatabase()->find('students', [
            Query::equal('classes.name', ['Class 2 Updated'])
        ]);

        $this->assertEquals(2, \count($students));
        $this->assertEquals(1, \count($students[0]['classes']));

        $class1 = static::getDatabase()->getDocument('classes', 'class1');

        // Update inverse document
        static::getDatabase()->updateDocument(
            'classes',
            $class1->getId(),
            $class1->setAttribute('students', ['student1'])
        );

        // Query related document again
        $students = static::getDatabase()->find('students', [
            Query::equal('classes.name', ['Class 2 Updated'])
        ]);

        $this->assertEquals(2, \count($students));
        $this->assertEquals(2, \count($students[0]['classes']));
        $this->assertEquals(1, \count($students[1]['classes']));

        // Query inverse document again
        $customers = static::getDatabase()->find('classes', [
            Query::equal('students.name', ['Student 2 Updated'])
        ]);

        $this->assertEquals(1, \count($customers));

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
        $this->assertEquals('class2', $classes[0]['$id']);

        // Get inverse document with new relationship key
        $class = static::getDatabase()->getDocument('classes', 'class1');
        $students = $class->getAttribute('newStudents');
        $this->assertEquals('student1', $students[0]['$id']);

        // Create new document with no relationship
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('students', 'student7');
        $this->assertEquals(true, $deleted);

        $student6 = static::getDatabase()->getDocument('students', 'student7');
        $this->assertEquals(true, $student6->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('students', 'student1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Can not delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $student1 = static::getDatabase()->getDocument('students', 'student1');

        // Reset relationships
        static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('newClasses', ['class1'])
        );

        // Delete child, will delete junction
        static::getDatabase()->deleteDocument('classes', 'class1');

        // Check relation was set to null
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals([], $student1->getAttribute('newClasses'));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('students', 'student2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('students', 'student2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'students',
            'newClasses'
        );

        // Try to get documents again
        $student = static::getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('newClasses');
        $this->assertEquals(null, $classes);

        // Try to get inverse documents again
        $classes = static::getDatabase()->getDocument('classes', 'class1');
        $students = $classes->getAttribute('newStudents');
        $this->assertEquals(null, $students);
    }

    public function testSelectRelationshipAttributes(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('make');
        static::getDatabase()->createCollection('model');

        static::getDatabase()->createAttribute('make', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('model', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('model', 'year', Database::VAR_INTEGER, 0, true);

        static::getDatabase()->createRelationship(
            collection: 'make',
            relatedCollection: 'model',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'models'
        );

        static::getDatabase()->createDocument('make', new Document([
            '$id' => 'ford',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Ford',
            'models' => [
                [
                    '$id' => 'fiesta',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Fiesta',
                    'year' => 2010,
                ],
                [
                    '$id' => 'focus',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Focus',
                    'year' => 2011,
                ],
            ],
        ]));

        // Select some parent attributes, some child attributes
        $make = static::getDatabase()->findOne('make', [
            Query::select(['name', 'models.name']),
        ]);

        if (!$make instanceof Document) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));
        $this->assertEquals('Fiesta', $make['models'][0]['name']);
        $this->assertEquals('Focus', $make['models'][1]['name']);
        $this->assertArrayNotHasKey('year', $make['models'][0]);
        $this->assertArrayNotHasKey('year', $make['models'][1]);

        // Select all parent attributes, some child attributes
        $make = static::getDatabase()->findOne('make', [
            Query::select(['*', 'models.year']),
        ]);

        if (!$make instanceof Document) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));
        $this->assertArrayNotHasKey('name', $make['models'][0]);
        $this->assertArrayNotHasKey('name', $make['models'][1]);
        $this->assertEquals(2010, $make['models'][0]['year']);
        $this->assertEquals(2011, $make['models'][1]['year']);

        // Select all parent attributes, all child attributes
        $make = static::getDatabase()->findOne('make', [
            Query::select(['*', 'models.*']),
        ]);

        if (!$make instanceof Document) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));
        $this->assertEquals('Fiesta', $make['models'][0]['name']);
        $this->assertEquals('Focus', $make['models'][1]['name']);
        $this->assertEquals(2010, $make['models'][0]['year']);
        $this->assertEquals(2011, $make['models'][1]['year']);

        // Select all parent attributes, all child attributes
        // Must select parent if selecting children
        $make = static::getDatabase()->findOne('make', [
            Query::select(['models.*']),
        ]);

        if (!$make instanceof Document) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));
        $this->assertEquals('Fiesta', $make['models'][0]['name']);
        $this->assertEquals('Focus', $make['models'][1]['name']);
        $this->assertEquals(2010, $make['models'][0]['year']);
        $this->assertEquals(2011, $make['models'][1]['year']);

        // Select all parent attributes, no child attributes
        $make = static::getDatabase()->findOne('make', [
            Query::select(['name']),
        ]);

        if (!$make instanceof Document) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertArrayNotHasKey('models', $make);
    }

    public function testNestedOneToOne_OneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('pattern');
        static::getDatabase()->createCollection('shirt');
        static::getDatabase()->createCollection('team');

        static::getDatabase()->createAttribute('pattern', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('shirt', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('team', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'pattern',
            relatedCollection: 'shirt',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'shirt',
            twoWayKey: 'pattern'
        );
        static::getDatabase()->createRelationship(
            collection: 'shirt',
            relatedCollection: 'team',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'team',
            twoWayKey: 'shirt'
        );

        static::getDatabase()->createDocument('pattern', new Document([
            '$id' => 'stripes',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Stripes',
            'shirt' => [
                '$id' => 'red',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Red',
                'team' => [
                    '$id' => 'reds',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Reds',
                ],
            ],
        ]));

        $pattern = static::getDatabase()->getDocument('pattern', 'stripes');
        $this->assertEquals('red', $pattern['shirt']['$id']);
        $this->assertArrayNotHasKey('pattern', $pattern['shirt']);
        $this->assertEquals('reds', $pattern['shirt']['team']['$id']);
        $this->assertArrayNotHasKey('shirt', $pattern['shirt']['team']);

        static::getDatabase()->createDocument('team', new Document([
            '$id' => 'blues',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Blues',
            'shirt' => [
                '$id' => 'blue',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Blue',
                'pattern' => [
                    '$id' => 'plain',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Plain',
                ],
            ],
        ]));

        $team = static::getDatabase()->getDocument('team', 'blues');
        $this->assertEquals('blue', $team['shirt']['$id']);
        $this->assertArrayNotHasKey('team', $team['shirt']);
        $this->assertEquals('plain', $team['shirt']['pattern']['$id']);
        $this->assertArrayNotHasKey('shirt', $team['shirt']['pattern']);
    }

    public function testNestedOneToOne_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('teachers');
        static::getDatabase()->createCollection('classrooms');
        static::getDatabase()->createCollection('children');

        static::getDatabase()->createAttribute('children', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('teachers', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classrooms', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'teachers',
            relatedCollection: 'classrooms',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'classroom',
            twoWayKey: 'teacher'
        );
        static::getDatabase()->createRelationship(
            collection: 'classrooms',
            relatedCollection: 'children',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'classroom'
        );

        static::getDatabase()->createDocument('teachers', new Document([
            '$id' => 'teacher1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Teacher 1',
            'classroom' => [
                '$id' => 'classroom1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Classroom 1',
                'children' => [
                    [
                        '$id' => 'child1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Child 1',
                    ],
                    [
                        '$id' => 'child2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Child 2',
                    ],
                ],
            ],
        ]));

        $teacher1 = static::getDatabase()->getDocument('teachers', 'teacher1');
        $this->assertEquals('classroom1', $teacher1['classroom']['$id']);
        $this->assertArrayNotHasKey('teacher', $teacher1['classroom']);
        $this->assertEquals(2, \count($teacher1['classroom']['children']));
        $this->assertEquals('Child 1', $teacher1['classroom']['children'][0]['name']);
        $this->assertEquals('Child 2', $teacher1['classroom']['children'][1]['name']);

        static::getDatabase()->createDocument('children', new Document([
            '$id' => 'child3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Child 3',
            'classroom' => [
                '$id' => 'classroom2',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Classroom 2',
                'teacher' => [
                    '$id' => 'teacher2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Teacher 2',
                ],
            ],
        ]));

        $child3 = static::getDatabase()->getDocument('children', 'child3');
        $this->assertEquals('classroom2', $child3['classroom']['$id']);
        $this->assertArrayNotHasKey('children', $child3['classroom']);
        $this->assertEquals('teacher2', $child3['classroom']['teacher']['$id']);
        $this->assertArrayNotHasKey('classroom', $child3['classroom']['teacher']);
    }

    public function testNestedOneToOne_ManyToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('users');
        static::getDatabase()->createCollection('profiles');
        static::getDatabase()->createCollection('avatars');

        static::getDatabase()->createAttribute('users', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('profiles', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('avatars', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'users',
            relatedCollection: 'profiles',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'profile',
            twoWayKey: 'user'
        );
        static::getDatabase()->createRelationship(
            collection: 'profiles',
            relatedCollection: 'avatars',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'avatar',
        );

        static::getDatabase()->createDocument('users', new Document([
            '$id' => 'user1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'User 1',
            'profile' => [
                '$id' => 'profile1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Profile 1',
                'avatar' => [
                    '$id' => 'avatar1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Avatar 1',
                ],
            ],
        ]));

        $user1 = static::getDatabase()->getDocument('users', 'user1');
        $this->assertEquals('profile1', $user1['profile']['$id']);
        $this->assertArrayNotHasKey('user', $user1['profile']);
        $this->assertEquals('avatar1', $user1['profile']['avatar']['$id']);
        $this->assertArrayNotHasKey('profile', $user1['profile']['avatar']);

        static::getDatabase()->createDocument('avatars', new Document([
            '$id' => 'avatar2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Avatar 2',
            'profiles' => [
                [
                    '$id' => 'profile2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Profile 2',
                    'user' => [
                        '$id' => 'user2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'User 2',
                    ],
                ]
            ],
        ]));

        $avatar2 = static::getDatabase()->getDocument('avatars', 'avatar2');
        $this->assertEquals('profile2', $avatar2['profiles'][0]['$id']);
        $this->assertArrayNotHasKey('avatars', $avatar2['profiles'][0]);
        $this->assertEquals('user2', $avatar2['profiles'][0]['user']['$id']);
        $this->assertArrayNotHasKey('profiles', $avatar2['profiles'][0]['user']);
    }

    public function testNestedOneToOne_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('addresses');
        static::getDatabase()->createCollection('houses');
        static::getDatabase()->createCollection('buildings');

        static::getDatabase()->createAttribute('addresses', 'street', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('houses', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('buildings', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'addresses',
            relatedCollection: 'houses',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'house',
            twoWayKey: 'address'
        );
        static::getDatabase()->createRelationship(
            collection: 'houses',
            relatedCollection: 'buildings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->createDocument('addresses', new Document([
            '$id' => 'address1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'street' => 'Street 1',
            'house' => [
                '$id' => 'house1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'House 1',
                'buildings' => [
                    [
                        '$id' => 'building1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Building 1',
                    ],
                    [
                        '$id' => 'building2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Building 2',
                    ],
                ],
            ],
        ]));

        $address1 = static::getDatabase()->getDocument('addresses', 'address1');
        $this->assertEquals('house1', $address1['house']['$id']);
        $this->assertArrayNotHasKey('address', $address1['house']);
        $this->assertEquals('building1', $address1['house']['buildings'][0]['$id']);
        $this->assertEquals('building2', $address1['house']['buildings'][1]['$id']);
        $this->assertArrayNotHasKey('houses', $address1['house']['buildings'][0]);
        $this->assertArrayNotHasKey('houses', $address1['house']['buildings'][1]);

        static::getDatabase()->createDocument('buildings', new Document([
            '$id' => 'building3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Building 3',
            'houses' => [
                [
                    '$id' => 'house2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                    'address' => [
                        '$id' => 'address2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'street' => 'Street 2',
                    ],
                ],
            ],
        ]));
    }

    public function testNestedOneToMany_OneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('countries');
        static::getDatabase()->createCollection('cities');
        static::getDatabase()->createCollection('mayors');

        static::getDatabase()->createAttribute('cities', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('countries', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('mayors', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'countries',
            relatedCollection: 'cities',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'country'
        );
        static::getDatabase()->createRelationship(
            collection: 'cities',
            relatedCollection: 'mayors',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'mayor',
            twoWayKey: 'city'
        );

        static::getDatabase()->createDocument('countries', new Document([
            '$id' => 'country1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Country 1',
            'cities' => [
                [
                    '$id' => 'city1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'City 1',
                    'mayor' => [
                        '$id' => 'mayor1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Mayor 1',
                    ],
                ],
                [
                    '$id' => 'city2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'City 2',
                    'mayor' => [
                        '$id' => 'mayor2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Mayor 2',
                    ],
                ],
            ],
        ]));

        $documents = static::getDatabase()->find('countries', [
            Query::limit(1)
        ]);
        $this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        $documents = static::getDatabase()->find('countries', [
            Query::select(['name']),
            Query::limit(1)
        ]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = static::getDatabase()->find('countries', [
            Query::select(['*']),
            Query::limit(1)
        ]);
        // todo: double check please:
        //$this->assertArrayHasKey('cities', $documents[0]); // todo: double check why no attributes are resolved

        $documents = static::getDatabase()->find('countries', [
            Query::select(['*','cities.*']),
            Query::limit(1)
        ]);
        var_dump($documents[0]['cities'][0]);
        // todo: double check why mayor is not resolved
        //$this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        $country1 = static::getDatabase()->getDocument('countries', 'country1');
        $this->assertEquals('city1', $country1['cities'][0]['$id']);
        $this->assertEquals('city2', $country1['cities'][1]['$id']);
        $this->assertEquals('mayor1', $country1['cities'][0]['mayor']['$id']);
        $this->assertEquals('mayor2', $country1['cities'][1]['mayor']['$id']);
        $this->assertArrayNotHasKey('city', $country1['cities'][0]['mayor']);
        $this->assertArrayNotHasKey('city', $country1['cities'][1]['mayor']);

        static::getDatabase()->createDocument('mayors', new Document([
            '$id' => 'mayor3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Mayor 3',
            'city' => [
                '$id' => 'city3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'City 3',
                'country' => [
                    '$id' => 'country2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Country 2',
                ],
            ],
        ]));

        $country2 = static::getDatabase()->getDocument('countries', 'country2');
        $this->assertEquals('city3', $country2['cities'][0]['$id']);
        $this->assertEquals('mayor3', $country2['cities'][0]['mayor']['$id']);
        $this->assertArrayNotHasKey('country', $country2['cities'][0]);
        $this->assertArrayNotHasKey('city', $country2['cities'][0]['mayor']);
    }

    public function testNestedOneToMany_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('dormitories');
        static::getDatabase()->createCollection('occupants');
        static::getDatabase()->createCollection('pets');

        static::getDatabase()->createAttribute('dormitories', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('occupants', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('pets', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'dormitories',
            relatedCollection: 'occupants',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'dormitory'
        );
        static::getDatabase()->createRelationship(
            collection: 'occupants',
            relatedCollection: 'pets',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'occupant'
        );

        static::getDatabase()->createDocument('dormitories', new Document([
            '$id' => 'dormitory1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'House 1',
            'occupants' => [
                [
                    '$id' => 'occupant1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 1',
                    'pets' => [
                        [
                            '$id' => 'pet1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 1',
                        ],
                        [
                            '$id' => 'pet2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 2',
                        ],
                    ],
                ],
                [
                    '$id' => 'occupant2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 2',
                    'pets' => [
                        [
                            '$id' => 'pet3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 3',
                        ],
                        [
                            '$id' => 'pet4',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 4',
                        ],
                    ],
                ],
            ],
        ]));

        $dormitory1 = static::getDatabase()->getDocument('dormitories', 'dormitory1');
        $this->assertEquals('occupant1', $dormitory1['occupants'][0]['$id']);
        $this->assertEquals('occupant2', $dormitory1['occupants'][1]['$id']);
        $this->assertEquals('pet1', $dormitory1['occupants'][0]['pets'][0]['$id']);
        $this->assertEquals('pet2', $dormitory1['occupants'][0]['pets'][1]['$id']);
        $this->assertEquals('pet3', $dormitory1['occupants'][1]['pets'][0]['$id']);
        $this->assertEquals('pet4', $dormitory1['occupants'][1]['pets'][1]['$id']);
        $this->assertArrayNotHasKey('dormitory', $dormitory1['occupants'][0]);
        $this->assertArrayNotHasKey('dormitory', $dormitory1['occupants'][1]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][0]['pets'][0]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][0]['pets'][1]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][1]['pets'][0]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][1]['pets'][1]);

        static::getDatabase()->createDocument('pets', new Document([
            '$id' => 'pet5',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Pet 5',
            'occupant' => [
                '$id' => 'occupant3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Occupant 3',
                'dormitory' => [
                    '$id' => 'dormitory2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                ],
            ],
        ]));

        $pet5 = static::getDatabase()->getDocument('pets', 'pet5');
        $this->assertEquals('occupant3', $pet5['occupant']['$id']);
        $this->assertEquals('dormitory2', $pet5['occupant']['dormitory']['$id']);
        $this->assertArrayNotHasKey('pets', $pet5['occupant']);
        $this->assertArrayNotHasKey('occupant', $pet5['occupant']['dormitory']);
    }

    public function testNestedOneToMany_ManyToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('home');
        static::getDatabase()->createCollection('renters');
        static::getDatabase()->createCollection('floors');

        static::getDatabase()->createAttribute('home', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('renters', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('floors', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'home',
            relatedCollection: 'renters',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );
        static::getDatabase()->createRelationship(
            collection: 'renters',
            relatedCollection: 'floors',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'floor'
        );

        static::getDatabase()->createDocument('home', new Document([
            '$id' => 'home1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'House 1',
            'renters' => [
                [
                    '$id' => 'renter1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 1',
                    'floor' => [
                        '$id' => 'floor1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Floor 1',
                    ],
                ],
            ],
        ]));

        $home1 = static::getDatabase()->getDocument('home', 'home1');
        $this->assertEquals('renter1', $home1['renters'][0]['$id']);
        $this->assertEquals('floor1', $home1['renters'][0]['floor']['$id']);
        $this->assertArrayNotHasKey('home', $home1['renters'][0]);
        $this->assertArrayNotHasKey('renters', $home1['renters'][0]['floor']);

        static::getDatabase()->createDocument('floors', new Document([
            '$id' => 'floor2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Floor 2',
            'renters' => [
                    [
                    '$id' => 'renter2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 2',
                    'home' => [
                        '$id' => 'home2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'House 2',
                    ],
                ],
            ],
        ]));

        $floor2 = static::getDatabase()->getDocument('floors', 'floor2');
        $this->assertEquals('renter2', $floor2['renters'][0]['$id']);
        $this->assertArrayNotHasKey('floor', $floor2['renters'][0]);
        $this->assertEquals('home2', $floor2['renters'][0]['home']['$id']);
        $this->assertArrayNotHasKey('renter', $floor2['renters'][0]['home']);
    }

    public function testNestedOneToMany_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('owners');
        static::getDatabase()->createCollection('cats');
        static::getDatabase()->createCollection('toys');

        static::getDatabase()->createAttribute('owners', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('cats', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('toys', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'owners',
            relatedCollection: 'cats',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'owner'
        );
        static::getDatabase()->createRelationship(
            collection: 'cats',
            relatedCollection: 'toys',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true
        );

        static::getDatabase()->createDocument('owners', new Document([
            '$id' => 'owner1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Owner 1',
            'cats' => [
                [
                    '$id' => 'cat1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pet 1',
                    'toys' => [
                        [
                            '$id' => 'toy1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Toy 1',
                        ],
                    ],
                ],
            ],
        ]));

        $owner1 = static::getDatabase()->getDocument('owners', 'owner1');
        $this->assertEquals('cat1', $owner1['cats'][0]['$id']);
        $this->assertArrayNotHasKey('owner', $owner1['cats'][0]);
        $this->assertEquals('toy1', $owner1['cats'][0]['toys'][0]['$id']);
        $this->assertArrayNotHasKey('cats', $owner1['cats'][0]['toys'][0]);

        static::getDatabase()->createDocument('toys', new Document([
            '$id' => 'toy2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Toy 2',
            'cats' => [
                [
                    '$id' => 'cat2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pet 2',
                    'owner' => [
                        '$id' => 'owner2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Owner 2',
                    ],
                ],
            ],
        ]));

        $toy2 = static::getDatabase()->getDocument('toys', 'toy2');
        $this->assertEquals('cat2', $toy2['cats'][0]['$id']);
        $this->assertArrayNotHasKey('toys', $toy2['cats'][0]);
        $this->assertEquals('owner2', $toy2['cats'][0]['owner']['$id']);
        $this->assertArrayNotHasKey('cats', $toy2['cats'][0]['owner']);
    }

    public function testNestedManyToOne_OneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('towns');
        static::getDatabase()->createCollection('homelands');
        static::getDatabase()->createCollection('capitals');

        static::getDatabase()->createAttribute('towns', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('homelands', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('capitals', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'towns',
            relatedCollection: 'homelands',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'homeland'
        );
        static::getDatabase()->createRelationship(
            collection: 'homelands',
            relatedCollection: 'capitals',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'capital',
            twoWayKey: 'homeland'
        );

        static::getDatabase()->createDocument('towns', new Document([
            '$id' => 'town1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'City 1',
            'homeland' => [
                '$id' => 'homeland1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Country 1',
                'capital' => [
                    '$id' => 'capital1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Flag 1',
                ],
            ],
        ]));

        $town1 = static::getDatabase()->getDocument('towns', 'town1');
        $this->assertEquals('homeland1', $town1['homeland']['$id']);
        $this->assertArrayNotHasKey('towns', $town1['homeland']);
        $this->assertEquals('capital1', $town1['homeland']['capital']['$id']);
        $this->assertArrayNotHasKey('homeland', $town1['homeland']['capital']);

        static::getDatabase()->createDocument('capitals', new Document([
            '$id' => 'capital2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Flag 2',
            'homeland' => [
                '$id' => 'homeland2',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Country 2',
                'towns' => [
                    [
                        '$id' => 'town2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Town 2',
                    ],
                    [
                        '$id' => 'town3',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Town 3',
                    ],
                ],
            ],
        ]));

        $capital2 = static::getDatabase()->getDocument('capitals', 'capital2');
        $this->assertEquals('homeland2', $capital2['homeland']['$id']);
        $this->assertArrayNotHasKey('capital', $capital2['homeland']);
        $this->assertEquals(2, \count($capital2['homeland']['towns']));
        $this->assertEquals('town2', $capital2['homeland']['towns'][0]['$id']);
        $this->assertEquals('town3', $capital2['homeland']['towns'][1]['$id']);
    }

    public function testNestedManyToOne_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('players');
        static::getDatabase()->createCollection('teams');
        static::getDatabase()->createCollection('supporters');

        static::getDatabase()->createAttribute('players', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('teams', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('supporters', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'players',
            relatedCollection: 'teams',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'team'
        );
        static::getDatabase()->createRelationship(
            collection: 'teams',
            relatedCollection: 'supporters',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'supporters',
            twoWayKey: 'team'
        );

        static::getDatabase()->createDocument('players', new Document([
            '$id' => 'player1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Player 1',
            'team' => [
                '$id' => 'team1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Team 1',
                'supporters' => [
                    [
                        '$id' => 'supporter1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Supporter 1',
                    ],
                    [
                        '$id' => 'supporter2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Supporter 2',
                    ],
                ],
            ],
        ]));

        $player1 = static::getDatabase()->getDocument('players', 'player1');
        $this->assertEquals('team1', $player1['team']['$id']);
        $this->assertArrayNotHasKey('players', $player1['team']);
        $this->assertEquals(2, \count($player1['team']['supporters']));
        $this->assertEquals('supporter1', $player1['team']['supporters'][0]['$id']);
        $this->assertEquals('supporter2', $player1['team']['supporters'][1]['$id']);

        static::getDatabase()->createDocument('supporters', new Document([
            '$id' => 'supporter3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Supporter 3',
            'team' => [
                '$id' => 'team2',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Team 2',
                'players' => [
                    [
                        '$id' => 'player2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Player 2',
                    ],
                    [
                        '$id' => 'player3',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Player 3',
                    ],
                ],
            ],
        ]));

        $supporter3 = static::getDatabase()->getDocument('supporters', 'supporter3');
        $this->assertEquals('team2', $supporter3['team']['$id']);
        $this->assertArrayNotHasKey('supporters', $supporter3['team']);
        $this->assertEquals(2, \count($supporter3['team']['players']));
        $this->assertEquals('player2', $supporter3['team']['players'][0]['$id']);
        $this->assertEquals('player3', $supporter3['team']['players'][1]['$id']);
    }

    public function testNestedManyToOne_ManyToOne(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('cows');
        static::getDatabase()->createCollection('farms');
        static::getDatabase()->createCollection('farmer');

        static::getDatabase()->createAttribute('cows', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('farms', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('farmer', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'cows',
            relatedCollection: 'farms',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'farm'
        );
        static::getDatabase()->createRelationship(
            collection: 'farms',
            relatedCollection: 'farmer',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'farmer'
        );

        static::getDatabase()->createDocument('cows', new Document([
            '$id' => 'cow1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Cow 1',
            'farm' => [
                '$id' => 'farm1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Farm 1',
                'farmer' => [
                    '$id' => 'farmer1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Farmer 1',
                ],
            ],
        ]));

        $cow1 = static::getDatabase()->getDocument('cows', 'cow1');
        $this->assertEquals('farm1', $cow1['farm']['$id']);
        $this->assertArrayNotHasKey('cows', $cow1['farm']);
        $this->assertEquals('farmer1', $cow1['farm']['farmer']['$id']);
        $this->assertArrayNotHasKey('farms', $cow1['farm']['farmer']);

        static::getDatabase()->createDocument('farmer', new Document([
            '$id' => 'farmer2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Farmer 2',
            'farms' => [
                [
                    '$id' => 'farm2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Farm 2',
                    'cows' => [
                        [
                            '$id' => 'cow2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Cow 2',
                        ],
                        [
                            '$id' => 'cow3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Cow 3',
                        ],
                    ],
                ],
            ],
        ]));

        $farmer2 = static::getDatabase()->getDocument('farmer', 'farmer2');
        $this->assertEquals('farm2', $farmer2['farms'][0]['$id']);
        $this->assertArrayNotHasKey('farmer', $farmer2['farms'][0]);
        $this->assertEquals(2, \count($farmer2['farms'][0]['cows']));
        $this->assertEquals('cow2', $farmer2['farms'][0]['cows'][0]['$id']);
        $this->assertEquals('cow3', $farmer2['farms'][0]['cows'][1]['$id']);
    }

    public function testNestedManyToOne_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('books');
        static::getDatabase()->createCollection('entrants');
        static::getDatabase()->createCollection('rooms');

        static::getDatabase()->createAttribute('books', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('entrants', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('rooms', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'books',
            relatedCollection: 'entrants',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'entrant'
        );
        static::getDatabase()->createRelationship(
            collection: 'entrants',
            relatedCollection: 'rooms',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->createDocument('books', new Document([
            '$id' => 'book1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Book 1',
            'entrant' => [
                '$id' => 'entrant1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Entrant 1',
                'rooms' => [
                    [
                        '$id' => 'class1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Class 1',
                    ],
                    [
                        '$id' => 'class2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Class 2',
                    ],
                ],
            ],
        ]));

        $book1 = static::getDatabase()->getDocument('books', 'book1');
        $this->assertEquals('entrant1', $book1['entrant']['$id']);
        $this->assertArrayNotHasKey('books', $book1['entrant']);
        $this->assertEquals(2, \count($book1['entrant']['rooms']));
        $this->assertEquals('class1', $book1['entrant']['rooms'][0]['$id']);
        $this->assertEquals('class2', $book1['entrant']['rooms'][1]['$id']);
    }

    public function testNestedManyToMany_OneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('stones');
        static::getDatabase()->createCollection('hearths');
        static::getDatabase()->createCollection('plots');

        static::getDatabase()->createAttribute('stones', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('hearths', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('plots', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'stones',
            relatedCollection: 'hearths',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'hearths',
            relatedCollection: 'plots',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'plot',
            twoWayKey: 'hearth'
        );

        static::getDatabase()->createDocument('stones', new Document([
            '$id' => 'stone1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Building 1',
            'hearths' => [
                [
                    '$id' => 'hearth1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 1',
                    'plot' => [
                        '$id' => 'plot1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Address 1',
                    ],
                ],
                [
                    '$id' => 'hearth2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                    'plot' => [
                        '$id' => 'plot2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Address 2',
                    ],
                ],
            ],
        ]));

        $stone1 = static::getDatabase()->getDocument('stones', 'stone1');
        $this->assertEquals(2, \count($stone1['hearths']));
        $this->assertEquals('hearth1', $stone1['hearths'][0]['$id']);
        $this->assertEquals('hearth2', $stone1['hearths'][1]['$id']);
        $this->assertArrayNotHasKey('stone', $stone1['hearths'][0]);
        $this->assertEquals('plot1', $stone1['hearths'][0]['plot']['$id']);
        $this->assertEquals('plot2', $stone1['hearths'][1]['plot']['$id']);
        $this->assertArrayNotHasKey('hearth', $stone1['hearths'][0]['plot']);

        static::getDatabase()->createDocument('plots', new Document([
            '$id' => 'plot3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Address 3',
            'hearth' => [
                '$id' => 'hearth3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Hearth 3',
                'stones' => [
                    [
                        '$id' => 'stone2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Stone 2',
                    ],
                ],
            ],
        ]));

        $plot3 = static::getDatabase()->getDocument('plots', 'plot3');
        $this->assertEquals('hearth3', $plot3['hearth']['$id']);
        $this->assertArrayNotHasKey('plot', $plot3['hearth']);
        $this->assertEquals('stone2', $plot3['hearth']['stones'][0]['$id']);
        $this->assertArrayNotHasKey('hearths', $plot3['hearth']['stones'][0]);
    }

    public function testNestedManyToMany_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('groups');
        static::getDatabase()->createCollection('tounaments');
        static::getDatabase()->createCollection('prizes');

        static::getDatabase()->createAttribute('groups', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('tounaments', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('prizes', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'groups',
            relatedCollection: 'tounaments',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'tounaments',
            relatedCollection: 'prizes',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'prizes',
            twoWayKey: 'tounament'
        );

        static::getDatabase()->createDocument('groups', new Document([
                    '$id' => 'group1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Group 1',
                    'tounaments' => [
                        [
                            '$id' => 'tounament1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Tounament 1',
                            'prizes' => [
                                [
                                    '$id' => 'prize1',
                                    '$permissions' => [
                                        Permission::read(Role::any()),
                                    ],
                                    'name' => 'Prize 1',
                                ],
                                [
                                    '$id' => 'prize2',
                                    '$permissions' => [
                                        Permission::read(Role::any()),
                                    ],
                                    'name' => 'Prize 2',
                                ],
                            ],
                        ],
                        [
                            '$id' => 'tounament2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Tounament 2',
                            'prizes' => [
                                [
                                    '$id' => 'prize3',
                                    '$permissions' => [
                                        Permission::read(Role::any()),
                                    ],
                                    'name' => 'Prize 3',
                                ],
                                [
                                    '$id' => 'prize4',
                                    '$permissions' => [
                                        Permission::read(Role::any()),
                                    ],
                                    'name' => 'Prize 4',
                                ],
                            ],
                        ],
                    ],
                ]));

        $group1 = static::getDatabase()->getDocument('groups', 'group1');
        $this->assertEquals(2, \count($group1['tounaments']));
        $this->assertEquals('tounament1', $group1['tounaments'][0]['$id']);
        $this->assertEquals('tounament2', $group1['tounaments'][1]['$id']);
        $this->assertArrayNotHasKey('group', $group1['tounaments'][0]);
        $this->assertEquals(2, \count($group1['tounaments'][0]['prizes']));
        $this->assertEquals('prize1', $group1['tounaments'][0]['prizes'][0]['$id']);
        $this->assertEquals('prize2', $group1['tounaments'][0]['prizes'][1]['$id']);
        $this->assertArrayNotHasKey('tounament', $group1['tounaments'][0]['prizes'][0]);
    }

    public function testNestedManyToMany_ManyToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('platforms');
        static::getDatabase()->createCollection('games');
        static::getDatabase()->createCollection('publishers');

        static::getDatabase()->createAttribute('platforms', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('games', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('publishers', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'platforms',
            relatedCollection: 'games',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'games',
            relatedCollection: 'publishers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'publisher',
            twoWayKey: 'games'
        );

        static::getDatabase()->createDocument('platforms', new Document([
            '$id' => 'platform1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Platform 1',
            'games' => [
                [
                    '$id' => 'game1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 1',
                    'publisher' => [
                        '$id' => 'publisher1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Publisher 1',
                    ],
                ],
                [
                    '$id' => 'game2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 2',
                    'publisher' => [
                        '$id' => 'publisher2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Publisher 2',
                    ],
                ],
            ]
        ]));

        $platform1 = static::getDatabase()->getDocument('platforms', 'platform1');
        $this->assertEquals(2, \count($platform1['games']));
        $this->assertEquals('game1', $platform1['games'][0]['$id']);
        $this->assertEquals('game2', $platform1['games'][1]['$id']);
        $this->assertArrayNotHasKey('platforms', $platform1['games'][0]);
        $this->assertEquals('publisher1', $platform1['games'][0]['publisher']['$id']);
        $this->assertEquals('publisher2', $platform1['games'][1]['publisher']['$id']);
        $this->assertArrayNotHasKey('games', $platform1['games'][0]['publisher']);

        static::getDatabase()->createDocument('publishers', new Document([
            '$id' => 'publisher3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Publisher 3',
            'games' => [
                [
                    '$id' => 'game3',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 3',
                    'platforms' => [
                        [
                            '$id' => 'platform2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Platform 2',
                        ]
                    ],
                ],
            ],
        ]));

        $publisher3 = static::getDatabase()->getDocument('publishers', 'publisher3');
        $this->assertEquals(1, \count($publisher3['games']));
        $this->assertEquals('game3', $publisher3['games'][0]['$id']);
        $this->assertArrayNotHasKey('publisher', $publisher3['games'][0]);
        $this->assertEquals('platform2', $publisher3['games'][0]['platforms'][0]['$id']);
        $this->assertArrayNotHasKey('games', $publisher3['games'][0]['platforms'][0]);
    }

    public function testNestedManyToMany_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('sauces');
        static::getDatabase()->createCollection('pizzas');
        static::getDatabase()->createCollection('toppings');

        static::getDatabase()->createAttribute('sauces', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('pizzas', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('toppings', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'sauces',
            relatedCollection: 'pizzas',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'pizzas',
            relatedCollection: 'toppings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'toppings',
            twoWayKey: 'pizzas'
        );

        static::getDatabase()->createDocument('sauces', new Document([
            '$id' => 'sauce1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Sauce 1',
            'pizzas' => [
                [
                    '$id' => 'pizza1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pizza 1',
                    'toppings' => [
                        [
                            '$id' => 'topping1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 1',
                        ],
                        [
                            '$id' => 'topping2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 2',
                        ],
                    ],
                ],
                [
                    '$id' => 'pizza2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pizza 2',
                    'toppings' => [
                        [
                            '$id' => 'topping3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 3',
                        ],
                        [
                            '$id' => 'topping4',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 4',
                        ],
                    ],
                ],
            ]
        ]));

        $sauce1 = static::getDatabase()->getDocument('sauces', 'sauce1');
        $this->assertEquals(2, \count($sauce1['pizzas']));
        $this->assertEquals('pizza1', $sauce1['pizzas'][0]['$id']);
        $this->assertEquals('pizza2', $sauce1['pizzas'][1]['$id']);
        $this->assertArrayNotHasKey('sauces', $sauce1['pizzas'][0]);
        $this->assertEquals(2, \count($sauce1['pizzas'][0]['toppings']));
        $this->assertEquals('topping1', $sauce1['pizzas'][0]['toppings'][0]['$id']);
        $this->assertEquals('topping2', $sauce1['pizzas'][0]['toppings'][1]['$id']);
        $this->assertArrayNotHasKey('pizzas', $sauce1['pizzas'][0]['toppings'][0]);
        $this->assertEquals(2, \count($sauce1['pizzas'][1]['toppings']));
        $this->assertEquals('topping3', $sauce1['pizzas'][1]['toppings'][0]['$id']);
        $this->assertEquals('topping4', $sauce1['pizzas'][1]['toppings'][1]['$id']);
        $this->assertArrayNotHasKey('pizzas', $sauce1['pizzas'][1]['toppings'][0]);
    }

    public function testInheritRelationshipPermissions(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('lawns');
        static::getDatabase()->createCollection('trees');
        static::getDatabase()->createCollection('birds');

        static::getDatabase()->createAttribute('lawns', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('trees', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('birds', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'lawns',
            relatedCollection: 'trees',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'lawn',
            onDelete: Database::RELATION_MUTATE_CASCADE,
        );
        static::getDatabase()->createRelationship(
            collection: 'trees',
            relatedCollection: 'birds',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            onDelete: Database::RELATION_MUTATE_SET_NULL,
        );

        $permissions = [
            Permission::read(Role::any()),
            Permission::read(Role::user('user1')),
            Permission::update(Role::user('user1')),
            Permission::delete(Role::user('user2')),
        ];

        static::getDatabase()->createDocument('lawns', new Document([
            '$id' => 'lawn1',
            '$permissions' => $permissions,
            'name' => 'Lawn 1',
            'trees' => [
                [
                    '$id' => 'tree1',
                    'name' => 'Tree 1',
                    'birds' => [
                        [
                            '$id' => 'bird1',
                            'name' => 'Bird 1',
                        ],
                        [
                            '$id' => 'bird2',
                            'name' => 'Bird 2',
                        ],
                    ],
                ],
            ],
        ]));

        $lawn1 = static::getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals($permissions, $lawn1->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]['birds'][0]->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]['birds'][1]->getPermissions());

        $tree1 = static::getDatabase()->getDocument('trees', 'tree1');
        $this->assertEquals($permissions, $tree1->getPermissions());
        $this->assertEquals($permissions, $tree1['lawn']->getPermissions());
        $this->assertEquals($permissions, $tree1['birds'][0]->getPermissions());
        $this->assertEquals($permissions, $tree1['birds'][1]->getPermissions());
    }

    /**
     * @depends testInheritRelationshipPermissions
     */
    public function testEnforceRelationshipPermissions(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $lawn1 = static::getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals('Lawn 1', $lawn1['name']);

        // Try update root document
        try {
            static::getDatabase()->updateDocument(
                'lawns',
                $lawn1->getId(),
                $lawn1->setAttribute('name', 'Lawn 1 Updated')
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "update" permission for role "user:user1". Only "["any"]" scopes are allowed and "["user:user1"]" was given.', $e->getMessage());
        }

        // Try delete root document
        try {
            static::getDatabase()->deleteDocument(
                'lawns',
                $lawn1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        $tree1 = static::getDatabase()->getDocument('trees', 'tree1');

        // Try update nested document
        try {
            static::getDatabase()->updateDocument(
                'trees',
                $tree1->getId(),
                $tree1->setAttribute('name', 'Tree 1 Updated')
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "update" permission for role "user:user1". Only "["any"]" scopes are allowed and "["user:user1"]" was given.', $e->getMessage());
        }

        // Try delete nested document
        try {
            static::getDatabase()->deleteDocument(
                'trees',
                $tree1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        $bird1 = static::getDatabase()->getDocument('birds', 'bird1');

        // Try update multi-level nested document
        try {
            static::getDatabase()->updateDocument(
                'birds',
                $bird1->getId(),
                $bird1->setAttribute('name', 'Bird 1 Updated')
            );
            $this->fail('Failed to throw exception when updating document with missing permissions');
        } catch (Exception $e) {
            $this->assertEquals('Missing "update" permission for role "user:user1". Only "["any"]" scopes are allowed and "["user:user1"]" was given.', $e->getMessage());
        }

        // Try delete multi-level nested document
        try {
            static::getDatabase()->deleteDocument(
                'birds',
                $bird1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        Authorization::setRole(Role::user('user1')->toString());

        $bird1 = static::getDatabase()->getDocument('birds', 'bird1');

        // Try update multi-level nested document
        $bird1 = static::getDatabase()->updateDocument(
            'birds',
            $bird1->getId(),
            $bird1->setAttribute('name', 'Bird 1 Updated')
        );

        $this->assertEquals('Bird 1 Updated', $bird1['name']);

        Authorization::setRole(Role::user('user2')->toString());

        // Try delete multi-level nested document
        $deleted = static::getDatabase()->deleteDocument(
            'birds',
            $bird1->getId(),
        );

        $this->assertEquals(true, $deleted);
        $tree1 = static::getDatabase()->getDocument('trees', 'tree1');
        $this->assertEquals(1, count($tree1['birds']));

        // Try update nested document
        $tree1 = static::getDatabase()->updateDocument(
            'trees',
            $tree1->getId(),
            $tree1->setAttribute('name', 'Tree 1 Updated')
        );

        $this->assertEquals('Tree 1 Updated', $tree1['name']);

        // Try delete nested document
        $deleted = static::getDatabase()->deleteDocument(
            'trees',
            $tree1->getId(),
        );

        $this->assertEquals(true, $deleted);
        $lawn1 = static::getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals(0, count($lawn1['trees']));

        // Create document with no permissions
        static::getDatabase()->createDocument('lawns', new Document([
            '$id' => 'lawn2',
            'name' => 'Lawn 2',
            'trees' => [
                [
                    '$id' => 'tree2',
                    'name' => 'Tree 2',
                    'birds' => [
                        [
                            '$id' => 'bird3',
                            'name' => 'Bird 3',
                        ],
                    ],
                ],
            ],
        ]));

        $lawn2 = static::getDatabase()->getDocument('lawns', 'lawn2');
        $this->assertEquals(true, $lawn2->isEmpty());

        $tree2 = static::getDatabase()->getDocument('trees', 'tree2');
        $this->assertEquals(true, $tree2->isEmpty());

        $bird3 = static::getDatabase()->getDocument('birds', 'bird3');
        $this->assertEquals(true, $bird3->isEmpty());
    }

    public function testExceedMaxDepth(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('level1');
        static::getDatabase()->createCollection('level2');
        static::getDatabase()->createCollection('level3');
        static::getDatabase()->createCollection('level4');

        static::getDatabase()->createRelationship(
            collection: 'level1',
            relatedCollection: 'level2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'level2',
            relatedCollection: 'level3',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'level3',
            relatedCollection: 'level4',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        // Exceed create depth
        $level1 = static::getDatabase()->createDocument('level1', new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'level2' => [
                [
                    '$id' => 'level2',
                    'level3' => [
                        [
                            '$id' => 'level3',
                            'level4' => [
                                [
                                    '$id' => 'level4',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ]));
        $this->assertEquals(1, count($level1['level2']));
        $this->assertEquals('level2', $level1['level2'][0]->getId());
        $this->assertEquals(1, count($level1['level2'][0]['level3']));
        $this->assertEquals('level3', $level1['level2'][0]['level3'][0]->getId());
        $this->assertArrayNotHasKey('level4', $level1['level2'][0]['level3'][0]);

        // Exceed fetch depth
        $level1 = static::getDatabase()->getDocument('level1', 'level1');
        $this->assertEquals(1, count($level1['level2']));
        $this->assertEquals('level2', $level1['level2'][0]->getId());
        $this->assertEquals(1, count($level1['level2'][0]['level3']));
        $this->assertEquals('level3', $level1['level2'][0]['level3'][0]->getId());
        $this->assertArrayNotHasKey('level4', $level1['level2'][0]['level3'][0]);


        // Exceed update depth
        $level1 = static::getDatabase()->updateDocument(
            'level1',
            'level1',
            $level1
            ->setAttribute('level2', [new Document([
                '$id' => 'level2new',
                'level3' => [
                    [
                        '$id' => 'level3new',
                        'level4' => [
                            [
                                '$id' => 'level4new',
                            ],
                        ],
                    ],
                ],
            ])])
        );
        $this->assertEquals(1, count($level1['level2']));
        $this->assertEquals('level2new', $level1['level2'][0]->getId());
        $this->assertEquals(1, count($level1['level2'][0]['level3']));
        $this->assertEquals('level3new', $level1['level2'][0]['level3'][0]->getId());
        $this->assertArrayNotHasKey('level4', $level1['level2'][0]['level3'][0]);
    }

    public function testCreateRelationshipMissingCollection(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Collection not found');

        static::getDatabase()->createRelationship(
            collection: 'missing',
            relatedCollection: 'missing',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateRelationshipMissingRelatedCollection(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('test');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Related collection not found');

        static::getDatabase()->createRelationship(
            collection: 'test',
            relatedCollection: 'missing',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateDuplicateRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('test1');
        static::getDatabase()->createCollection('test2');

        static::getDatabase()->createRelationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Attribute already exists');

        static::getDatabase()->createRelationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateInvalidRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('test3');
        static::getDatabase()->createCollection('test4');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid relationship type');

        static::getDatabase()->createRelationship(
            collection: 'test3',
            relatedCollection: 'test4',
            type: 'invalid',
            twoWay: true,
        );
    }

    public function testDeleteMissingRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Attribute not found');

        static::getDatabase()->deleteRelationship('test', 'test2');
    }

    public function testCreateInvalidIntValueRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('invalid1');
        static::getDatabase()->createCollection('invalid2');

        static::getDatabase()->createRelationship(
            collection: 'invalid1',
            relatedCollection: 'invalid2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        static::getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => 10,
        ]));
    }

    /**
     * @depends testCreateInvalidIntValueRelationship
     */
    public function testCreateInvalidObjectValueRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        static::getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => new \stdClass(),
        ]));
    }

    /**
     * @depends testCreateInvalidIntValueRelationship
     */
    public function testCreateInvalidArrayIntValueRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createRelationship(
            collection: 'invalid1',
            relatedCollection: 'invalid2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'invalid3',
            twoWayKey: 'invalid4',
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        static::getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid3' => [10],
        ]));
    }

    public function testCreateEmptyValueRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('null1');
        static::getDatabase()->createCollection('null2');

        static::getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'null3',
            twoWayKey: 'null4',
        );
        static::getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'null4',
            twoWayKey: 'null5',
        );
        static::getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'null6',
            twoWayKey: 'null7',
        );

        $document = static::getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null2' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null2'));

        $document = static::getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null1' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null1'));

        $document = static::getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null3' => null,
        ]));

        // One to many will be empty array instead of null
        $this->assertEquals([], $document->getAttribute('null3'));

        $document = static::getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null4' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null4'));

        $document = static::getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null4' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null4'));

        $document = static::getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null5' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null5'));

        $document = static::getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null6' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null6'));

        $document = static::getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null7' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null7'));
    }

    public function testDeleteCollectionDeletesRelationships(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('testers');
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

    public function testDeleteTwoWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('drivers');
        static::getDatabase()->createCollection('licenses');

        static::getDatabase()->createRelationship(
            collection: 'drivers',
            relatedCollection: 'licenses',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'license',
            twoWayKey: 'driver'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(1, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('licenses', 'driver');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'drivers',
            relatedCollection: 'licenses',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'licenses',
            twoWayKey: 'driver'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('licenses', 'driver');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'licenses',
            relatedCollection: 'drivers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'driver',
            twoWayKey: 'licenses'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('drivers', 'licenses');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'licenses',
            relatedCollection: 'drivers',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'drivers',
            twoWayKey: 'licenses'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');
        $junction = static::getDatabase()->getCollection('_licenses_drivers');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));
        $this->assertEquals(2, \count($junction->getAttribute('attributes')));
        $this->assertEquals(2, \count($junction->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('drivers', 'licenses');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');
        $junction = static::getDatabase()->getCollection('_licenses_drivers');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        $this->assertEquals(true, $junction->isEmpty());
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
}
