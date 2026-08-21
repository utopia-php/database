<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Throwable;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

trait IndexTests
{
    public function testCreateIndex(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'indexes'));

        /**
         * Check ticks sounding cast index for reserved words
         */
        $database->createAttribute('indexes', Attribute::integer(key: 'int', size: 8, array: true));
        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            $database->createIndex('indexes', Index::key(key: 'indx8711', attributes: ['int'], lengths: [255]));
        }

        $database->createAttribute('indexes', Attribute::string(key: 'name', size: 10));

        $database->createIndex('indexes', Index::key(key: 'index_1', attributes: ['name']));

        try {
            $database->createIndex('indexes', Index::key(key: 'index3', attributes: ['$id', '$id']));
        } catch (Throwable $e) {
            self::assertTrue($e instanceof DatabaseException);
            self::assertEquals($e->getMessage(), 'Duplicate attributes provided');
        }

        try {
            $database->createIndex('indexes', Index::key(key: 'index4', attributes: ['name', 'Name']));
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

        $database->createCollection(new Collection(id: 'indexes'));

        $this->assertEquals(true, $database->createAttribute('indexes', Attribute::string(key: 'string', size: 128, required: true)));
        $this->assertEquals(true, $database->createAttribute('indexes', Attribute::string(key: 'order', size: 128, required: true)));
        $this->assertEquals(true, $database->createAttribute('indexes', Attribute::integer(key: 'integer', required: true)));
        $this->assertEquals(true, $database->createAttribute('indexes', Attribute::double(key: 'float', required: true)));
        $this->assertEquals(true, $database->createAttribute('indexes', Attribute::boolean(key: 'boolean', required: true)));

        // Indexes
        $this->assertEquals(true, $database->createIndex('indexes', Index::key(key: 'index1', attributes: ['string', 'integer'], lengths: [128], orders: [Order::Asc])));
        $this->assertEquals(true, $database->createIndex('indexes', Index::key(key: 'index2', attributes: ['float', 'integer'], orders: [Order::Asc, Order::Desc])));
        $this->assertEquals(true, $database->createIndex('indexes', Index::key(key: 'index3', attributes: ['integer', 'boolean'], orders: [Order::Asc, Order::Desc, Order::Desc])));
        $this->assertEquals(true, $database->createIndex('indexes', Index::unique(key: 'index4', attributes: ['string'], lengths: [128], orders: [Order::Asc])));
        $this->assertEquals(true, $database->createIndex('indexes', Index::unique(key: 'index5', attributes: ['$id', 'string'], lengths: [128], orders: [Order::Asc])));
        $this->assertEquals(true, $database->createIndex('indexes', Index::unique(key: 'order', attributes: ['order'], lengths: [128], orders: [Order::Asc])));

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
        $database->createIndex('indexes', Index::key(key: 'duplicate', attributes: ['string', 'boolean'], lengths: [128], orders: [Order::Asc]));
        try {
            $database->createIndex('indexes', Index::key(key: 'duplicate', attributes: ['string', 'boolean'], lengths: [128], orders: [Order::Asc]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Test delete index when index does not exist
        $this->assertEquals(true, $database->createIndex('indexes', Index::key(key: 'index1', attributes: ['string', 'integer'], lengths: [128], orders: [Order::Asc])));
        $this->assertEquals(true, $this->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index1'));

        // Test delete index when attribute does not exist
        $this->assertEquals(true, $database->createIndex('indexes', Index::key(key: 'index1', attributes: ['string', 'integer'], lengths: [128], orders: [Order::Asc])));
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

        $database->createCollection(new Collection(id: __FUNCTION__));

        $database->createAttribute(__FUNCTION__, Attribute::string(key: 'title1', size: $database->getAdapter()->getMaxIndexLength() + 300, required: true));

        try {
            $database->createIndex(__FUNCTION__, Index::key(key: 'index_title1', attributes: ['title1'], lengths: [0]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
        }

        $database->createAttribute(__FUNCTION__, Attribute::string(key: 'title2', size: 100, required: true));
        $database->createIndex(__FUNCTION__, Index::key(key: 'index_title2', attributes: ['title2'], lengths: [0]));

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
        $collection = $this->getNumbersCollection();

        $numbers = $database->createCollection(new Collection(id: $collection));
        $database->createAttribute($collection, Attribute::string(key: 'verbose', size: 128, required: true));
        $database->createAttribute($collection, Attribute::integer(key: 'symbol', required: true));

        $database->createIndex($collection, Index::key(key: 'index1', attributes: ['verbose'], lengths: [128], orders: [Order::Asc]));
        $database->createIndex($collection, Index::key(key: 'index2', attributes: ['symbol'], lengths: [0], orders: [Order::Asc]));

        $index = $database->renameIndex($collection, 'index1', 'index3');

        $this->assertTrue($index);

        $numbers = $database->getCollection($collection);

        $this->assertEquals('index2', $numbers->getAttribute('indexes')[1]['$id']);
        $this->assertEquals('index3', $numbers->getAttribute('indexes')[0]['$id']);
        $this->assertCount(2, $numbers->getAttribute('indexes'));
    }

    private static string $numbersCollection = '';

    protected function getNumbersCollection(): string
    {
        if (self::$numbersCollection === '') {
            self::$numbersCollection = 'numbers_' . uniqid();
        }
        return self::$numbersCollection;
    }

    private static bool $renameIndexFixtureInit = false;

    protected function initRenameIndexFixture(): void
    {
        if (self::$renameIndexFixtureInit) {
            return;
        }

        $database = $this->getDatabase();
        $collection = $this->getNumbersCollection();

        $database->createCollection(new Collection(id: $collection));
        $database->createAttribute($collection, Attribute::string(key: 'verbose', size: 128, required: true));
        $database->createAttribute($collection, Attribute::integer(key: 'symbol', required: true));
        $database->createIndex($collection, Index::key(key: 'index1', attributes: ['verbose'], lengths: [128], orders: [Order::Asc]));
        $database->createIndex($collection, Index::key(key: 'index2', attributes: ['symbol'], lengths: [0], orders: [Order::Asc]));
        $database->renameIndex($collection, 'index1', 'index3');

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

        $database->createIndex($this->getDocumentsCollection(), Index::fullText(key: 'string', attributes: ['string']));
        $database->createDocument($this->getDocumentsCollection(), new Document([
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
        $documents = $database->find($this->getDocumentsCollection(), [
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
            $database->createIndex($this->getDocumentsCollection(), Index::fullText(key: 'string', attributes: ['string']));
        } catch (\Exception $e) {
            // Already exists
        }

        $documents = $database->find($this->getDocumentsCollection(), [
            Query::search('string', ''),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = $database->find($this->getDocumentsCollection(), [
            Query::search('string', '*'),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = $database->find($this->getDocumentsCollection(), [
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
            $database->createCollection(new Collection(id: $collectionId));

            $database->createAttribute($collectionId, Attribute::string(key: 'name', size: 256));
            $database->createAttribute($collectionId, Attribute::string(key: 'description', size: 512));

            // Create trigram index on name attribute
            $this->assertEquals(true, $database->createIndex($collectionId, Index::trigram(key: 'trigram_name', attributes: ['name'])));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->getAttribute('indexes');
            $this->assertCount(1, $indexes);
            $this->assertEquals('trigram_name', $indexes[0]['$id']);
            $this->assertEquals(IndexType::Trigram->value, $indexes[0]['type']);
            $this->assertEquals(['name'], $indexes[0]['attributes']);

            // Create another trigram index on description
            $this->assertEquals(true, $database->createIndex($collectionId, Index::trigram(key: 'trigram_description', attributes: ['description'])));

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
        $database->createCollection(new Collection(id: $col));

        $database->createAttribute($col, Attribute::datetime(key: 'expiresAt', filters: ['datetime']));

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $this->assertTrue(
            $database->createIndex($col, Index::ttl(key: 'idx_ttl_valid', attributes: ['expiresAt'], orders: [Order::Asc], ttl: 3600))
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
            $database->createIndex($col, Index::ttl(key: 'idx_ttl_min', attributes: ['expiresAt'], orders: [Order::Asc]))
        );

        $col2 = uniqid('sl_ttl_collection');

        $expiresAtAttr = Attribute::datetime(key: 'expiresAt', signed: false, filters: ['datetime']);

        $ttlIndexDoc = Index::ttl(key: 'idx_ttl_collection', attributes: ['expiresAt'], orders: [Order::Asc], ttl: 7200);

        $database->createCollection(new Collection(id: $col2, attributes: [$expiresAtAttr], indexes: [$ttlIndexDoc]));

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
