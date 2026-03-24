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
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
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

}
