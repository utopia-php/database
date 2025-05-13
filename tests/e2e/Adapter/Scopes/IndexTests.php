<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Throwable;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Index;

trait IndexTests
{
    public function testCreateIndex(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('indexes');

        /**
         * Check ticks sounding cast index for reserved words
         */
        $database->createAttribute('indexes', 'int', Database::VAR_INTEGER, 8, false, array:true);
        $database->createIndex('indexes', 'indx8711', Database::INDEX_KEY, ['int'], [255]);

        $database->createAttribute('indexes', 'name', Database::VAR_STRING, 10, false);

        $database->createIndex('indexes', 'index_1', Database::INDEX_KEY, ['name']);

        try {
            $database->createIndex('indexes', 'index3', Database::INDEX_KEY, ['$id', '$id']);
        } catch (Throwable $e) {
            self::assertTrue($e instanceof DatabaseException);
            self::assertEquals($e->getMessage(), 'Duplicate attributes provided');
        }

        try {
            $database->createIndex('indexes', 'index4', Database::INDEX_KEY, ['name', 'Name']);
        } catch (Throwable $e) {
            self::assertTrue($e instanceof DatabaseException);
            self::assertEquals($e->getMessage(), 'Duplicate attributes provided');
        }

        $database->deleteCollection('indexes');
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

        // Test non-shared tables duplicates throw duplicate
        static::getDatabase()->createIndex('indexes', 'duplicate', Database::INDEX_KEY, ['string', 'boolean'], [128], [Database::ORDER_ASC]);
        try {
            static::getDatabase()->createIndex('indexes', 'duplicate', Database::INDEX_KEY, ['string', 'boolean'], [128], [Database::ORDER_ASC]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Test delete index when index does not exist
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, static::deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index1'));

        // Test delete index when attribute does not exist
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('indexes', 'string'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index1'));

        static::getDatabase()->deleteCollection('indexes');
    }



    /**
     * @throws Exception|Throwable
     */
    public function testIndexValidation(): void
    {
        $attributes = [
            new Document([
                '$id' => ID::custom('title1'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('title2'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 500,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['title1', 'title2'],
                'lengths' => [701,50],
                'orders' => [],
            ]),
        ];

        $collection = new Document([
            '$id' => ID::custom('index_length'),
            'name' => 'test',
            'attributes' => $attributes,
            'indexes' => $indexes
        ]);

        $validator = new Index(
            $attributes,
            static::getDatabase()->getAdapter()->getMaxIndexLength(),
            static::getDatabase()->getAdapter()->getInternalIndexesKeys()
        );

        $errorMessage = 'Index length 701 is larger than the size for title1: 700"';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            static::getDatabase()->createCollection($collection->getId(), $attributes, $indexes, [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals($errorMessage, $e->getMessage());
        }

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['title1', 'title2'],
                'lengths' => [700], // 700, 500 (length(title2))
                'orders' => [],
            ]),
        ];

        $collection->setAttribute('indexes', $indexes);

        if (static::getDatabase()->getAdapter()->getMaxIndexLength() > 0) {
            $errorMessage = 'Index length is longer than the maximum: ' . static::getDatabase()->getAdapter()->getMaxIndexLength();
            $this->assertFalse($validator->isValid($indexes[0]));
            $this->assertEquals($errorMessage, $validator->getDescription());

            try {
                static::getDatabase()->createCollection($collection->getId(), $attributes, $indexes);
                $this->fail('Failed to throw exception');
            } catch (Exception $e) {
                $this->assertEquals($errorMessage, $e->getMessage());
            }
        }

        $attributes[] = new Document([
            '$id' => ID::custom('integer'),
            'type' => Database::VAR_INTEGER,
            'format' => '',
            'size' => 10000,
            'signed' => true,
            'required' => false,
            'default' => null,
            'array' => false,
            'filters' => [],
        ]);

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_FULLTEXT,
                'attributes' => ['title1', 'integer'],
                'lengths' => [],
                'orders' => [],
            ]),
        ];

        $collection = new Document([
            '$id' => ID::custom('index_length'),
            'name' => 'test',
            'attributes' => $attributes,
            'indexes' => $indexes
        ]);

        $validator = new Index(
            $attributes,
            static::getDatabase()->getAdapter()->getMaxIndexLength(),
            static::getDatabase()->getAdapter()->getInternalIndexesKeys()
        );
        $errorMessage = 'Attribute "integer" cannot be part of a FULLTEXT index, must be of type string';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            static::getDatabase()->createCollection($collection->getId(), $attributes, $indexes);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals($errorMessage, $e->getMessage());
        }


        $indexes = [
            new Document([
                '$id' => ID::custom('index_negative_length'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['title1'],
                'lengths' => [-1],
                'orders' => [],
            ]),
        ];

        $errorMessage = 'Negative index provided for title1';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            static::getDatabase()->createCollection(ID::unique(), $attributes, $indexes);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals($errorMessage, $e->getMessage());
        }

        $indexes = [
            new Document([
                '$id' => ID::custom('index_extra_lengths'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['title1', 'title2'],
                'lengths' => [100, 100, 100],
                'orders' => [],
            ]),
        ];

        $errorMessage = 'Invalid index lengths. Count of lengths must be equal or less than the number of attributes.';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            static::getDatabase()->createCollection(ID::unique(), $attributes, $indexes);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals($errorMessage, $e->getMessage());
        }
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

    public function testListDocumentSearch(): void
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
            'integer_signed' => 0,
            'integer_unsigned' => 0,
            'bigint_signed' => 0,
            'bigint_unsigned' => 0,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
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

    public function testMaxQueriesValues(): void
    {
        $max = static::getDatabase()->getMaxQueryValues();

        static::getDatabase()->setMaxQueryValues(5);

        try {
            static::getDatabase()->find(
                'documents',
                [Query::equal('$id', [1, 2, 3, 4, 5, 6])]
            );
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid query: Query on attribute has greater than 5 values: $id', $e->getMessage());
        }

        static::getDatabase()->setMaxQueryValues($max);
    }

    public function testEmptySearch(): void
    {
        $fulltextSupport = $this->getDatabase()->getAdapter()->getSupportForFulltextIndex();
        if (!$fulltextSupport) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $documents = static::getDatabase()->find('documents', [
            Query::search('string', ''),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = static::getDatabase()->find('documents', [
            Query::search('string', '*'),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = static::getDatabase()->find('documents', [
            Query::search('string', '<>'),
        ]);
        $this->assertEquals(0, count($documents));
    }
}
