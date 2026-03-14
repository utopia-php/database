<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Throwable;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

trait IndexTests
{
    public function testCreateIndex(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('indexes');

        /**
         * Check ticks sounding cast index for reserved words
         */
        $database->createAttribute('indexes', new Attribute(key: 'int', type: ColumnType::Integer, size: 8, required: false, array: true));
        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            $database->createIndex('indexes', new Index(key: 'indx8711', type: IndexType::Key, attributes: ['int'], lengths: [255]));
        }

        $database->createAttribute('indexes', new Attribute(key: 'name', type: ColumnType::String, size: 10, required: false));

        $database->createIndex('indexes', new Index(key: 'index_1', type: IndexType::Key, attributes: ['name']));

        try {
            $database->createIndex('indexes', new Index(key: 'index3', type: IndexType::Key, attributes: ['$id', '$id']));
        } catch (Throwable $e) {
            self::assertTrue($e instanceof DatabaseException);
            self::assertEquals($e->getMessage(), 'Duplicate attributes provided');
        }

        try {
            $database->createIndex('indexes', new Index(key: 'index4', type: IndexType::Key, attributes: ['name', 'Name']));
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

        $this->assertEquals(true, $database->createAttribute('indexes', new Attribute(key: 'string', type: ColumnType::String, size: 128, required: true)));
        $this->assertEquals(true, $database->createAttribute('indexes', new Attribute(key: 'order', type: ColumnType::String, size: 128, required: true)));
        $this->assertEquals(true, $database->createAttribute('indexes', new Attribute(key: 'integer', type: ColumnType::Integer, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute('indexes', new Attribute(key: 'float', type: ColumnType::Double, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute('indexes', new Attribute(key: 'boolean', type: ColumnType::Boolean, size: 0, required: true)));

        // Indexes
        $this->assertEquals(true, $database->createIndex('indexes', new Index(key: 'index1', type: IndexType::Key, attributes: ['string', 'integer'], lengths: [128], orders: [OrderDirection::Asc->value])));
        $this->assertEquals(true, $database->createIndex('indexes', new Index(key: 'index2', type: IndexType::Key, attributes: ['float', 'integer'], lengths: [], orders: [OrderDirection::Asc->value, OrderDirection::Desc->value])));
        $this->assertEquals(true, $database->createIndex('indexes', new Index(key: 'index3', type: IndexType::Key, attributes: ['integer', 'boolean'], lengths: [], orders: [OrderDirection::Asc->value, OrderDirection::Desc->value, OrderDirection::Desc->value])));
        $this->assertEquals(true, $database->createIndex('indexes', new Index(key: 'index4', type: IndexType::Unique, attributes: ['string'], lengths: [128], orders: [OrderDirection::Asc->value])));
        $this->assertEquals(true, $database->createIndex('indexes', new Index(key: 'index5', type: IndexType::Unique, attributes: ['$id', 'string'], lengths: [128], orders: [OrderDirection::Asc->value])));
        $this->assertEquals(true, $database->createIndex('indexes', new Index(key: 'order', type: IndexType::Unique, attributes: ['order'], lengths: [128], orders: [OrderDirection::Asc->value])));

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
        $database->createIndex('indexes', new Index(key: 'duplicate', type: IndexType::Key, attributes: ['string', 'boolean'], lengths: [128], orders: [OrderDirection::Asc->value]));
        try {
            $database->createIndex('indexes', new Index(key: 'duplicate', type: IndexType::Key, attributes: ['string', 'boolean'], lengths: [128], orders: [OrderDirection::Asc->value]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Test delete index when index does not exist
        $this->assertEquals(true, $database->createIndex('indexes', new Index(key: 'index1', type: IndexType::Key, attributes: ['string', 'integer'], lengths: [128], orders: [OrderDirection::Asc->value])));
        $this->assertEquals(true, $this->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index1'));

        // Test delete index when attribute does not exist
        $this->assertEquals(true, $database->createIndex('indexes', new Index(key: 'index1', type: IndexType::Key, attributes: ['string', 'integer'], lengths: [128], orders: [OrderDirection::Asc->value])));
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
                'type' => ColumnType::String->value,
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
                'type' => ColumnType::String->value,
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
                'type' => IndexType::Key->value,
                'attributes' => ['title1', 'title2'],
                'lengths' => [701, 50],
                'orders' => [],
            ]),
        ];

        $collection = new Document([
            '$id' => ID::custom('index_length'),
            'name' => 'test',
            'attributes' => $attributes,
            'indexes' => $indexes,
        ]);

        /** @var Database $database */
        $database = $this->getDatabase();

        $validator = new IndexValidator(
            $attributes,
            $indexes,
            $database->getAdapter()->getMaxIndexLength(),
            $database->getAdapter()->getInternalIndexesKeys(),
            $database->getAdapter()->supports(Capability::IndexArray),
            $database->getAdapter()->supports(Capability::SpatialIndexNull),
            $database->getAdapter()->supports(Capability::SpatialIndexOrder),
            $database->getAdapter()->supports(Capability::Vectors),
            $database->getAdapter()->supports(Capability::DefinedAttributes),
            $database->getAdapter()->supports(Capability::MultipleFulltextIndexes),
            $database->getAdapter()->supports(Capability::IdenticalIndexes),
            $database->getAdapter()->supports(Capability::Objects),
            $database->getAdapter()->supports(Capability::TrigramIndex),
            $database->getAdapter()->supports(Capability::Spatial),
            $database->getAdapter()->supports(Capability::Index),
            $database->getAdapter()->supports(Capability::UniqueIndex),
            $database->getAdapter()->supports(Capability::Fulltext)
        );
        if ($database->getAdapter()->supports(Capability::IdenticalIndexes)) {
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
        }

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => IndexType::Key->value,
                'attributes' => ['title1', 'title2'],
                'lengths' => [700], // 700, 500 (length(title2))
                'orders' => [],
            ]),
        ];

        $collection->setAttribute('indexes', $indexes);

        if ($database->getAdapter()->supports(Capability::DefinedAttributes) && $database->getAdapter()->getMaxIndexLength() > 0) {
            $errorMessage = 'Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength();
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
            'type' => ColumnType::Integer->value,
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
                'type' => IndexType::Fulltext->value,
                'attributes' => ['title1', 'integer'],
                'lengths' => [],
                'orders' => [],
            ]),
        ];

        $collection = new Document([
            '$id' => ID::custom('index_length'),
            'name' => 'test',
            'attributes' => $attributes,
            'indexes' => $indexes,
        ]);

        // not using $indexes[0] as the index validator skips indexes with same id
        $newIndex = new Document([
            '$id' => ID::custom('newIndex1'),
            'type' => IndexType::Fulltext->value,
            'attributes' => ['title1', 'integer'],
            'lengths' => [],
            'orders' => [],
        ]);

        $validator = new IndexValidator(
            $attributes,
            $indexes,
            $database->getAdapter()->getMaxIndexLength(),
            $database->getAdapter()->getInternalIndexesKeys(),
            $database->getAdapter()->supports(Capability::IndexArray),
            $database->getAdapter()->supports(Capability::SpatialIndexNull),
            $database->getAdapter()->supports(Capability::SpatialIndexOrder),
            $database->getAdapter()->supports(Capability::Vectors),
            $database->getAdapter()->supports(Capability::DefinedAttributes),
            $database->getAdapter()->supports(Capability::MultipleFulltextIndexes),
            $database->getAdapter()->supports(Capability::IdenticalIndexes),
            $database->getAdapter()->supports(Capability::Objects),
            $database->getAdapter()->supports(Capability::TrigramIndex),
            $database->getAdapter()->supports(Capability::Spatial),
            $database->getAdapter()->supports(Capability::Index),
            $database->getAdapter()->supports(Capability::UniqueIndex),
            $database->getAdapter()->supports(Capability::Fulltext)
        );

        $this->assertFalse($validator->isValid($newIndex));

        if (! $database->getAdapter()->supports(Capability::Fulltext)) {
            $this->assertEquals('Fulltext index is not supported', $validator->getDescription());
        } elseif (! $database->getAdapter()->supports(Capability::MultipleFulltextIndexes)) {
            $this->assertEquals('There is already a fulltext index in the collection', $validator->getDescription());
        } elseif ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->assertEquals('Attribute "integer" cannot be part of a fulltext index, must be of type string', $validator->getDescription());
        }

        try {
            $database->createCollection($collection->getId(), $attributes, $indexes);
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->fail('Failed to throw exception');
            }
        } catch (Exception $e) {
            if (! $database->getAdapter()->supports(Capability::Fulltext)) {
                $this->assertEquals('Fulltext index is not supported', $e->getMessage());
            } else {
                $this->assertEquals('Attribute "integer" cannot be part of a fulltext index, must be of type string', $e->getMessage());
            }
        }

        $indexes = [
            new Document([
                '$id' => ID::custom('index_negative_length'),
                'type' => IndexType::Key->value,
                'attributes' => ['title1'],
                'lengths' => [-1],
                'orders' => [],
            ]),
        ];
        if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
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
                    'type' => IndexType::Key->value,
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
    }

    public function testIndexLengthZero(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(__FUNCTION__);

        $database->createAttribute(__FUNCTION__, new Attribute(key: 'title1', type: ColumnType::String, size: $database->getAdapter()->getMaxIndexLength() + 300, required: true));

        try {
            $database->createIndex(__FUNCTION__, new Index(key: 'index_title1', type: IndexType::Key, attributes: ['title1'], lengths: [0]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
        }

        $database->createAttribute(__FUNCTION__, new Attribute(key: 'title2', type: ColumnType::String, size: 100, required: true));
        $database->createIndex(__FUNCTION__, new Index(key: 'index_title2', type: IndexType::Key, attributes: ['title2'], lengths: [0]));

        try {
            $database->updateAttribute(__FUNCTION__, 'title2', ColumnType::String->value, $database->getAdapter()->getMaxIndexLength() + 300, true);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
        }
    }

    public function testRenameIndex(): void
    {
        $database = $this->getDatabase();

        $numbers = $database->createCollection('numbers');
        $database->createAttribute('numbers', new Attribute(key: 'verbose', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute('numbers', new Attribute(key: 'symbol', type: ColumnType::Integer, size: 0, required: true));

        $database->createIndex('numbers', new Index(key: 'index1', type: IndexType::Key, attributes: ['verbose'], lengths: [128], orders: [OrderDirection::Asc->value]));
        $database->createIndex('numbers', new Index(key: 'index2', type: IndexType::Key, attributes: ['symbol'], lengths: [0], orders: [OrderDirection::Asc->value]));

        $index = $database->renameIndex('numbers', 'index1', 'index3');

        $this->assertTrue($index);

        $numbers = $database->getCollection('numbers');

        $this->assertEquals('index2', $numbers->getAttribute('indexes')[1]['$id']);
        $this->assertEquals('index3', $numbers->getAttribute('indexes')[0]['$id']);
        $this->assertCount(2, $numbers->getAttribute('indexes'));
    }

    /**
     * Sets up the 'numbers' collection with renamed indexes as testRenameIndex would.
     */
    private static bool $renameIndexFixtureInit = false;

    protected function initRenameIndexFixture(): void
    {
        if (self::$renameIndexFixtureInit) {
            return;
        }

        $database = $this->getDatabase();

        if (! $database->exists($this->testDatabase, 'numbers')) {
            $database->createCollection('numbers');
            $database->createAttribute('numbers', new Attribute(key: 'verbose', type: ColumnType::String, size: 128, required: true));
            $database->createAttribute('numbers', new Attribute(key: 'symbol', type: ColumnType::Integer, size: 0, required: true));
            $database->createIndex('numbers', new Index(key: 'index1', type: IndexType::Key, attributes: ['verbose'], lengths: [128], orders: [OrderDirection::Asc->value]));
            $database->createIndex('numbers', new Index(key: 'index2', type: IndexType::Key, attributes: ['symbol'], lengths: [0], orders: [OrderDirection::Asc->value]));
            $database->renameIndex('numbers', 'index1', 'index3');
        }

        self::$renameIndexFixtureInit = true;
    }

    /**
     * @expectedException Exception
     */
    public function testRenameIndexMissing(): void
    {
        $this->initRenameIndexFixture();
        $database = $this->getDatabase();
        $this->expectExceptionMessage('Index not found');
        $index = $database->renameIndex('numbers', 'index1', 'index4');
    }

    /**
     * @expectedException Exception
     */
    public function testRenameIndexExisting(): void
    {
        $this->initRenameIndexFixture();
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
            $this->assertEquals(true, $database->createAttribute('indexLimit', new Attribute(key: "test{$i}", type: ColumnType::String, size: 16, required: true)));
        }

        // Testing for indexLimit
        // Add up to the limit, then check if the next index throws IndexLimitException
        for ($i = 0; $i < ($this->getDatabase()->getLimitForIndexes()); $i++) {
            $this->assertEquals(true, $database->createIndex('indexLimit', new Index(key: "index{$i}", type: IndexType::Key, attributes: ["test{$i}"], lengths: [16])));
        }
        $this->expectException(LimitException::class);
        $this->assertEquals(false, $database->createIndex('indexLimit', new Index(key: 'index64', type: IndexType::Key, attributes: ['test64'], lengths: [16])));

        $database->deleteCollection('indexLimit');
    }

    public function testListDocumentSearch(): void
    {
        $fulltextSupport = $this->getDatabase()->getAdapter()->supports(Capability::Fulltext);
        if (! $fulltextSupport) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createIndex('documents', new Index(key: 'string', type: IndexType::Fulltext, attributes: ['string']));
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
        $this->initDocumentsFixture();
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
        $fulltextSupport = $this->getDatabase()->getAdapter()->supports(Capability::Fulltext);
        if (! $fulltextSupport) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        // Create fulltext index if it doesn't exist (was created by testListDocumentSearch in sequential mode)
        try {
            $database->createIndex('documents', new Index(key: 'string', type: IndexType::Fulltext, attributes: ['string']));
        } catch (\Exception $e) {
            // Already exists
        }

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

    public function testMultipleFulltextIndexValidation(): void
    {

        $fulltextSupport = $this->getDatabase()->getAdapter()->supports(Capability::Fulltext);
        if (! $fulltextSupport) {
            $this->expectNotToPerformAssertions();

            return;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        $collectionId = 'multiple_fulltext_test';
        try {
            $database->createCollection($collectionId);

            $database->createAttribute($collectionId, new Attribute(key: 'title', type: ColumnType::String, size: 256, required: false));
            $database->createAttribute($collectionId, new Attribute(key: 'content', type: ColumnType::String, size: 256, required: false));
            $database->createIndex($collectionId, new Index(key: 'fulltext_title', type: IndexType::Fulltext, attributes: ['title']));

            $supportsMultipleFulltext = $database->getAdapter()->supports(Capability::MultipleFulltextIndexes);

            // Try to add second fulltext index
            try {
                $database->createIndex($collectionId, new Index(key: 'fulltext_content', type: IndexType::Fulltext, attributes: ['content']));

                if ($supportsMultipleFulltext) {
                    $this->assertTrue(true, 'Multiple fulltext indexes are supported and second index was created successfully');
                } else {
                    $this->fail('Expected exception when creating second fulltext index, but none was thrown');
                }
            } catch (Throwable $e) {
                if (! $supportsMultipleFulltext) {
                    $this->assertTrue(true, 'Multiple fulltext indexes are not supported and exception was thrown as expected');
                } else {
                    $this->fail('Unexpected exception when creating second fulltext index: '.$e->getMessage());
                }
            }

        } finally {
            // Clean up
            $database->deleteCollection($collectionId);
        }
    }

    public function testIdenticalIndexValidation(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collectionId = 'identical_index_test';

        try {
            $database->createCollection($collectionId);

            $database->createAttribute($collectionId, new Attribute(key: 'name', type: ColumnType::String, size: 256, required: false));
            $database->createAttribute($collectionId, new Attribute(key: 'age', type: ColumnType::Integer, size: 8, required: false));

            $database->createIndex($collectionId, new Index(key: 'index1', type: IndexType::Key, attributes: ['name', 'age'], lengths: [], orders: [OrderDirection::Asc->value, OrderDirection::Desc->value]));

            $supportsIdenticalIndexes = $database->getAdapter()->supports(Capability::IdenticalIndexes);

            // Try to add identical index (failure)
            try {
                $database->createIndex($collectionId, new Index(key: 'index2', type: IndexType::Key, attributes: ['name', 'age'], lengths: [], orders: [OrderDirection::Asc->value, OrderDirection::Desc->value]));
                if ($supportsIdenticalIndexes) {
                    $this->assertTrue(true, 'Identical indexes are supported and second index was created successfully');
                } else {
                    $this->fail('Expected exception but got none');
                }

            } catch (Throwable $e) {
                if (! $supportsIdenticalIndexes) {
                    $this->assertTrue(true, 'Identical indexes are not supported and exception was thrown as expected');
                } else {
                    $this->fail('Unexpected exception when creating identical index: '.$e->getMessage());
                }

            }

            // Test with different attributes order - faliure
            try {
                $database->createIndex($collectionId, new Index(key: 'index3', type: IndexType::Key, attributes: ['age', 'name'], lengths: [], orders: [OrderDirection::Asc->value, OrderDirection::Desc->value]));
                $this->assertTrue(true, 'Index with different attributes was created successfully');
            } catch (Throwable $e) {
                if (! $supportsIdenticalIndexes) {
                    $this->assertTrue(true, 'Identical indexes are not supported and exception was thrown as expected');
                } else {
                    $this->fail('Unexpected exception when creating identical index: '.$e->getMessage());
                }
            }

            // Test with different orders  order - faliure
            try {
                $database->createIndex($collectionId, new Index(key: 'index4', type: IndexType::Key, attributes: ['age', 'name'], lengths: [], orders: [OrderDirection::Desc->value, OrderDirection::Asc->value]));
                $this->assertTrue(true, 'Index with different attributes was created successfully');
            } catch (Throwable $e) {
                if (! $supportsIdenticalIndexes) {
                    $this->assertTrue(true, 'Identical indexes are not supported and exception was thrown as expected');
                } else {
                    $this->fail('Unexpected exception when creating identical index: '.$e->getMessage());
                }
            }

            // Test with different attributes - success
            try {
                $database->createIndex($collectionId, new Index(key: 'index5', type: IndexType::Key, attributes: ['name'], lengths: [], orders: [OrderDirection::Asc->value]));
                $this->assertTrue(true, 'Index with different attributes was created successfully');
            } catch (Throwable $e) {
                $this->fail('Unexpected exception when creating index with different attributes: '.$e->getMessage());
            }

            // Test with different orders - success
            try {
                $database->createIndex($collectionId, new Index(key: 'index6', type: IndexType::Key, attributes: ['name', 'age'], lengths: [], orders: [OrderDirection::Asc->value]));
                $this->assertTrue(true, 'Index with different orders was created successfully');
            } catch (Throwable $e) {
                $this->fail('Unexpected exception when creating index with different orders: '.$e->getMessage());
            }
        } finally {
            // Clean up
            $database->deleteCollection($collectionId);
        }
    }

    public function testTrigramIndex(): void
    {
        $trigramSupport = $this->getDatabase()->getAdapter()->supports(Capability::TrigramIndex);
        if (! $trigramSupport) {
            $this->expectNotToPerformAssertions();

            return;
        }

        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'trigram_test';
        try {
            $database->createCollection($collectionId);

            $database->createAttribute($collectionId, new Attribute(key: 'name', type: ColumnType::String, size: 256, required: false));
            $database->createAttribute($collectionId, new Attribute(key: 'description', type: ColumnType::String, size: 512, required: false));

            // Create trigram index on name attribute
            $this->assertEquals(true, $database->createIndex($collectionId, new Index(key: 'trigram_name', type: IndexType::Trigram, attributes: ['name'])));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->getAttribute('indexes');
            $this->assertCount(1, $indexes);
            $this->assertEquals('trigram_name', $indexes[0]['$id']);
            $this->assertEquals(IndexType::Trigram->value, $indexes[0]['type']);
            $this->assertEquals(['name'], $indexes[0]['attributes']);

            // Create another trigram index on description
            $this->assertEquals(true, $database->createIndex($collectionId, new Index(key: 'trigram_description', type: IndexType::Trigram, attributes: ['description'])));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->getAttribute('indexes');
            $this->assertCount(2, $indexes);

            // Test that trigram index can be deleted
            $this->assertEquals(true, $database->deleteIndex($collectionId, 'trigram_name'));
            $this->assertEquals(true, $database->deleteIndex($collectionId, 'trigram_description'));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->getAttribute('indexes');
            $this->assertCount(0, $indexes);

        } finally {
            // Clean up
            $database->deleteCollection($collectionId);
        }
    }

    public function testTrigramIndexValidation(): void
    {
        $trigramSupport = $this->getDatabase()->getAdapter()->supports(Capability::TrigramIndex);
        if (! $trigramSupport) {
            $this->expectNotToPerformAssertions();

            return;
        }

        /** @var Database $database */
        $database = static::getDatabase();

        $collectionId = 'trigram_validation_test';
        try {
            $database->createCollection($collectionId);

            $database->createAttribute($collectionId, new Attribute(key: 'name', type: ColumnType::String, size: 256, required: false));
            $database->createAttribute($collectionId, new Attribute(key: 'description', type: ColumnType::String, size: 412, required: false));
            $database->createAttribute($collectionId, new Attribute(key: 'age', type: ColumnType::Integer, size: 8, required: false));

            // Test: Trigram index on non-string attribute should fail
            try {
                $database->createIndex($collectionId, new Index(key: 'trigram_invalid', type: IndexType::Trigram, attributes: ['age']));
                $this->fail('Expected exception when creating trigram index on non-string attribute');
            } catch (Exception $e) {
                $this->assertStringContainsString('Trigram index can only be created on string type attributes', $e->getMessage());
            }

            // Test: Trigram index with multiple string attributes should succeed
            $this->assertEquals(true, $database->createIndex($collectionId, new Index(key: 'trigram_multi', type: IndexType::Trigram, attributes: ['name', 'description'])));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->getAttribute('indexes');
            $trigramMultiIndex = null;
            foreach ($indexes as $idx) {
                if ($idx['$id'] === 'trigram_multi') {
                    $trigramMultiIndex = $idx;
                    break;
                }
            }
            $this->assertNotNull($trigramMultiIndex);
            $this->assertEquals(IndexType::Trigram->value, $trigramMultiIndex['type']);
            $this->assertEquals(['name', 'description'], $trigramMultiIndex['attributes']);

            // Test: Trigram index with mixed string and non-string attributes should fail
            try {
                $database->createIndex($collectionId, new Index(key: 'trigram_mixed', type: IndexType::Trigram, attributes: ['name', 'age']));
                $this->fail('Expected exception when creating trigram index with mixed attribute types');
            } catch (Exception $e) {
                $this->assertStringContainsString('Trigram index can only be created on string type attributes', $e->getMessage());
            }

            // Test: Trigram index with orders should fail
            try {
                $database->createIndex($collectionId, new Index(key: 'trigram_order', type: IndexType::Trigram, attributes: ['name'], lengths: [], orders: [OrderDirection::Asc->value]));
                $this->fail('Expected exception when creating trigram index with orders');
            } catch (Exception $e) {
                $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $e->getMessage());
            }

            // Test: Trigram index with lengths should fail
            try {
                $database->createIndex($collectionId, new Index(key: 'trigram_length', type: IndexType::Trigram, attributes: ['name'], lengths: [128]));
                $this->fail('Expected exception when creating trigram index with lengths');
            } catch (Exception $e) {
                $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $e->getMessage());
            }

        } finally {
            // Clean up
            $database->deleteCollection($collectionId);
        }
    }

    public function testTTLIndexes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::TTLIndexes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $col = uniqid('sl_ttl');
        $database->createCollection($col);

        $database->createAttribute($col, new Attribute(key: 'expiresAt', type: ColumnType::Datetime, size: 0, required: false, filters: ['datetime']));

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $this->assertTrue(
            $database->createIndex($col, new Index(key: 'idx_ttl_valid', type: IndexType::Ttl, attributes: ['expiresAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 3600))
        );

        $collection = $database->getCollection($col);
        $indexes = $collection->getAttribute('indexes');
        $this->assertCount(1, $indexes);
        $ttlIndex = $indexes[0];
        $this->assertEquals('idx_ttl_valid', $ttlIndex->getId());
        $this->assertEquals(IndexType::Ttl->value, $ttlIndex->getAttribute('type'));
        $this->assertEquals(3600, $ttlIndex->getAttribute('ttl'));

        $now = new \DateTime();
        $future1 = (clone $now)->modify('+2 hours');
        $future2 = (clone $now)->modify('+1 hour');
        $past = (clone $now)->modify('-1 hour');

        $database->createDocuments($col, [
            new Document([
                '$id' => 'doc1',
                '$permissions' => $permissions,
                'expiresAt' => $future1->format(\DateTime::ATOM),
            ]),
            new Document([
                '$id' => 'doc2',
                '$permissions' => $permissions,
                'expiresAt' => $future2->format(\DateTime::ATOM),
            ]),
            new Document([
                '$id' => 'doc3',
                '$permissions' => $permissions,
                'expiresAt' => $past->format(\DateTime::ATOM),
            ]),
        ]);

        $this->assertTrue($database->deleteIndex($col, 'idx_ttl_valid'));

        $this->assertTrue(
            $database->createIndex($col, new Index(key: 'idx_ttl_min', type: IndexType::Ttl, attributes: ['expiresAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 1))
        );

        $col2 = uniqid('sl_ttl_collection');

        $expiresAtAttr = new Document([
            '$id' => ID::custom('expiresAt'),
            'type' => ColumnType::Datetime->value,
            'size' => 0,
            'signed' => false,
            'required' => false,
            'default' => null,
            'array' => false,
            'filters' => ['datetime'],
        ]);

        $ttlIndexDoc = new Document([
            '$id' => ID::custom('idx_ttl_collection'),
            'type' => IndexType::Ttl->value,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [OrderDirection::Asc->value],
            'ttl' => 7200, // 2 hours
        ]);

        $database->createCollection($col2, [$expiresAtAttr], [$ttlIndexDoc]);

        $collection2 = $database->getCollection($col2);
        $indexes2 = $collection2->getAttribute('indexes');
        $this->assertCount(1, $indexes2);
        $ttlIndex2 = $indexes2[0];
        $this->assertEquals('idx_ttl_collection', $ttlIndex2->getId());
        $this->assertEquals(7200, $ttlIndex2->getAttribute('ttl'));

        $database->deleteCollection($col);
        $database->deleteCollection($col2);
    }

    public function testTTLIndexDuplicatePrevention(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::TTLIndexes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $col = uniqid('sl_ttl_dup');
        $database->createCollection($col);

        $database->createAttribute($col, new Attribute(key: 'expiresAt', type: ColumnType::Datetime, size: 0, required: false, filters: ['datetime']));
        $database->createAttribute($col, new Attribute(key: 'deletedAt', type: ColumnType::Datetime, size: 0, required: false, filters: ['datetime']));

        $this->assertTrue(
            $database->createIndex($col, new Index(key: 'idx_ttl_expires', type: IndexType::Ttl, attributes: ['expiresAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 3600))
        );

        try {
            $database->createIndex($col, new Index(key: 'idx_ttl_expires_duplicate', type: IndexType::Ttl, attributes: ['expiresAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 7200));
            $this->fail('Expected exception for creating a second TTL index in a collection');
        } catch (Exception $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
        }

        try {
            $database->createIndex($col, new Index(key: 'idx_ttl_deleted', type: IndexType::Ttl, attributes: ['deletedAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 86400));
            $this->fail('Expected exception for creating a second TTL index in a collection');
        } catch (Exception $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
        }

        $collection = $database->getCollection($col);
        $indexes = $collection->getAttribute('indexes');
        $this->assertCount(1, $indexes);

        $indexIds = array_map(fn ($idx) => $idx->getId(), $indexes);
        $this->assertContains('idx_ttl_expires', $indexIds);
        $this->assertNotContains('idx_ttl_deleted', $indexIds);

        try {
            $database->createIndex($col, new Index(key: 'idx_ttl_deleted_duplicate', type: IndexType::Ttl, attributes: ['deletedAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 172800));
            $this->fail('Expected exception for creating a second TTL index in a collection');
        } catch (Exception $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
        }

        $this->assertTrue($database->deleteIndex($col, 'idx_ttl_expires'));

        $this->assertTrue(
            $database->createIndex($col, new Index(key: 'idx_ttl_deleted', type: IndexType::Ttl, attributes: ['deletedAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 1800))
        );

        $collection = $database->getCollection($col);
        $indexes = $collection->getAttribute('indexes');
        $this->assertCount(1, $indexes);

        $indexIds = array_map(fn ($idx) => $idx->getId(), $indexes);
        $this->assertNotContains('idx_ttl_expires', $indexIds);
        $this->assertContains('idx_ttl_deleted', $indexIds);

        $col3 = uniqid('sl_ttl_dup_collection');

        $expiresAtAttr = new Document([
            '$id' => ID::custom('expiresAt'),
            'type' => ColumnType::Datetime->value,
            'size' => 0,
            'signed' => false,
            'required' => false,
            'default' => null,
            'array' => false,
            'filters' => ['datetime'],
        ]);

        $ttlIndex1 = new Document([
            '$id' => ID::custom('idx_ttl_1'),
            'type' => IndexType::Ttl->value,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [OrderDirection::Asc->value],
            'ttl' => 3600,
        ]);

        $ttlIndex2 = new Document([
            '$id' => ID::custom('idx_ttl_2'),
            'type' => IndexType::Ttl->value,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [OrderDirection::Asc->value],
            'ttl' => 7200,
        ]);

        try {
            $database->createCollection($col3, [$expiresAtAttr], [$ttlIndex1, $ttlIndex2]);
            $this->fail('Expected exception for duplicate TTL indexes in createCollection');
        } catch (Exception $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
        }

        // Cleanup
        $database->deleteCollection($col);
    }
}
