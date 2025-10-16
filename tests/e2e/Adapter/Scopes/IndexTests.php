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
        if ($database->getAdapter()->getSupportForIndexArray()) {
            $database->createIndex('indexes', 'indx8711', Database::INDEX_KEY, ['int'], [255]);
        }

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
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('indexes');

        $this->assertEquals(true, $database->createAttribute('indexes', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $database->createAttribute('indexes', 'order', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $database->createAttribute('indexes', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $database->createAttribute('indexes', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, $database->createAttribute('indexes', 'boolean', Database::VAR_BOOLEAN, 0, true));

        // Indexes
        $this->assertEquals(true, $database->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, $database->createIndex('indexes', 'index2', Database::INDEX_KEY, ['float', 'integer'], [], [Database::ORDER_ASC, Database::ORDER_DESC]));
        $this->assertEquals(true, $database->createIndex('indexes', 'index3', Database::INDEX_KEY, ['integer', 'boolean'], [], [Database::ORDER_ASC, Database::ORDER_DESC, Database::ORDER_DESC]));
        $this->assertEquals(true, $database->createIndex('indexes', 'index4', Database::INDEX_UNIQUE, ['string'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, $database->createIndex('indexes', 'index5', Database::INDEX_UNIQUE, ['$id', 'string'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, $database->createIndex('indexes', 'order', Database::INDEX_UNIQUE, ['order'], [128], [Database::ORDER_ASC]));

        $collection = $database->getCollection('indexes');
        $this->assertCount(6, $collection->getAttribute('indexes'));

        // Delete Indexes
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index2'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index3'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index4'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index5'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'order'));

        $collection = $database->getCollection('indexes');
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // Test non-shared tables duplicates throw duplicate
        $database->createIndex('indexes', 'duplicate', Database::INDEX_KEY, ['string', 'boolean'], [128], [Database::ORDER_ASC]);
        try {
            $database->createIndex('indexes', 'duplicate', Database::INDEX_KEY, ['string', 'boolean'], [128], [Database::ORDER_ASC]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Test delete index when index does not exist
        $this->assertEquals(true, $database->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, $this->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index1'));

        // Test delete index when attribute does not exist
        $this->assertEquals(true, $database->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, $database->deleteAttribute('indexes', 'string'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index1'));

        $database->deleteCollection('indexes');
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

        /** @var Database $database */
        $database = $this->getDatabase();

        $validator = new Index(
            $attributes,
            $database->getAdapter()->getMaxIndexLength(),
            $database->getAdapter()->getInternalIndexesKeys(),
            $database->getAdapter()->getSupportForIndexArray()
        );

        $errorMessage = 'Index length 701 is larger than the size for title1: 700"';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            $database->createCollection($collection->getId(), $attributes, $indexes, [
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

        if ($database->getAdapter()->getMaxIndexLength() > 0) {
            $errorMessage = 'Index length is longer than the maximum: ' . $database->getAdapter()->getMaxIndexLength();
            $this->assertFalse($validator->isValid($indexes[0]));
            $this->assertEquals($errorMessage, $validator->getDescription());

            try {
                $database->createCollection($collection->getId(), $attributes, $indexes);
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
            $database->getAdapter()->getMaxIndexLength(),
            $database->getAdapter()->getInternalIndexesKeys(),
            $database->getAdapter()->getSupportForIndexArray()
        );
        $errorMessage = 'Attribute "integer" cannot be part of a FULLTEXT index, must be of type string';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            $database->createCollection($collection->getId(), $attributes, $indexes);
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

        $errorMessage = 'Negative index length provided for title1';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            $database->createCollection(ID::unique(), $attributes, $indexes);
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
            $database->createCollection(ID::unique(), $attributes, $indexes);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals($errorMessage, $e->getMessage());
        }
    }

    public function testIndexLengthZero(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(__FUNCTION__);

        $database->createAttribute(__FUNCTION__, 'title1', Database::VAR_STRING, 1000, true);

        try {
            $database->createIndex(__FUNCTION__, 'index_title1', Database::INDEX_KEY, ['title1'], [0]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
        }


        $database->createAttribute(__FUNCTION__, 'title2', Database::VAR_STRING, 100, true);
        $database->createIndex(__FUNCTION__, 'index_title2', Database::INDEX_KEY, ['title2'], [0]);

        try {
            $database->updateAttribute(__FUNCTION__, 'title2', Database::VAR_STRING, 1000, true);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
        }
    }

    public function testRenameIndex(): void
    {
        $database = $this->getDatabase();

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
        $database = $this->getDatabase();
        $this->expectExceptionMessage('Index not found');
        $index = $database->renameIndex('numbers', 'index1', 'index4');
    }

    /**
    * @depends testRenameIndex
    * @expectedException Exception
    */
    public function testRenameIndexExisting(): void
    {
        $database = $this->getDatabase();
        $this->expectExceptionMessage('Index name already used');
        $index = $database->renameIndex('numbers', 'index3', 'index2');
    }


    public function testExceptionIndexLimit(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('indexLimit');

        // add unique attributes for indexing
        for ($i = 0; $i < 64; $i++) {
            $this->assertEquals(true, $database->createAttribute('indexLimit', "test{$i}", Database::VAR_STRING, 16, true));
        }

        // Testing for indexLimit
        // Add up to the limit, then check if the next index throws IndexLimitException
        for ($i = 0; $i < ($this->getDatabase()->getLimitForIndexes()); $i++) {
            $this->assertEquals(true, $database->createIndex('indexLimit', "index{$i}", Database::INDEX_KEY, ["test{$i}"], [16]));
        }
        $this->expectException(LimitException::class);
        $this->assertEquals(false, $database->createIndex('indexLimit', "index64", Database::INDEX_KEY, ["test64"], [16]));

        $database->deleteCollection('indexLimit');
    }

    public function testListDocumentSearch(): void
    {
        $fulltextSupport = $this->getDatabase()->getAdapter()->getSupportForFulltextIndex();
        if (!$fulltextSupport) {
            $this->expectNotToPerformAssertions();
            return;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createIndex('documents', 'string', Database::INDEX_FULLTEXT, ['string']);
        $database->createDocument('documents', new Document([
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
        $documents = $database->find('documents', [
            Query::search('string', '*test+alias@email-provider.com'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testMaxQueriesValues(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $max = $database->getMaxQueryValues();

        $database->setMaxQueryValues(5);

        try {
            $database->find(
                'documents',
                [Query::equal('$id', [1, 2, 3, 4, 5, 6])]
            );
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid query: Query on attribute has greater than 5 values: $id', $e->getMessage());
        }

        $database->setMaxQueryValues($max);
    }

    public function testEmptySearch(): void
    {
        $fulltextSupport = $this->getDatabase()->getAdapter()->getSupportForFulltextIndex();
        if (!$fulltextSupport) {
            $this->expectNotToPerformAssertions();
            return;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        $documents = $database->find('documents', [
            Query::search('string', ''),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = $database->find('documents', [
            Query::search('string', '*'),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = $database->find('documents', [
            Query::search('string', '<>'),
        ]);
        $this->assertEquals(0, count($documents));
    }
}
