<?php

namespace Tests\E2E\Adapter\Scopes;

use DateTime as NativeDateTime;
use Exception;
use MongoDB\BSON\UTCDateTime;
use stdClass;
use Throwable;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\IntegerWidth;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\IndexDefinition;
use Utopia\Mongo\Client;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

trait IndexTests
{
    public function testCreateIndex(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: 'indexes'));

        /**
         * Check ticks sounding cast index for reserved words
         */
        $database->createAttribute('indexes', Attribute::integer(key: 'int', width: IntegerWidth::Bits64, array: true));
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

        $database->createCollection(Collection::create(id: 'indexes'));

        $database->createAttribute('indexes', Attribute::string(key: 'string', size: 128, required: true));
        $database->createAttribute('indexes', Attribute::string(key: 'order', size: 128, required: true));
        $database->createAttribute('indexes', Attribute::integer(key: 'integer', required: true));
        $database->createAttribute('indexes', Attribute::double(key: 'float', required: true));
        $database->createAttribute('indexes', Attribute::boolean(key: 'boolean', required: true));

        // Indexes
        $database->createIndex('indexes', Index::key(key: 'index1', attributes: ['string', 'integer'], lengths: [128], orders: [OrderDirection::Asc]));
        $database->createIndex('indexes', Index::key(key: 'index2', attributes: ['float', 'integer'], orders: [OrderDirection::Asc, OrderDirection::Desc]));
        $database->createIndex('indexes', Index::key(key: 'index3', attributes: ['integer', 'boolean'], orders: [OrderDirection::Asc, OrderDirection::Desc, OrderDirection::Desc]));
        $database->createIndex('indexes', Index::unique(key: 'index4', attributes: ['string'], lengths: [128], orders: [OrderDirection::Asc]));
        $database->createIndex('indexes', Index::unique(key: 'index5', attributes: ['$id', 'string'], lengths: [128], orders: [OrderDirection::Asc]));
        $database->createIndex('indexes', Index::unique(key: 'order', attributes: ['order'], lengths: [128], orders: [OrderDirection::Asc]));

        $collection = $database->getCollection('indexes');
        $this->assertCount(6, $collection->indexes());

        // Delete Indexes
        $database->deleteIndex('indexes', 'index1');
        $database->deleteIndex('indexes', 'index2');
        $database->deleteIndex('indexes', 'index3');
        $database->deleteIndex('indexes', 'index4');
        $database->deleteIndex('indexes', 'index5');
        $database->deleteIndex('indexes', 'order');

        $collection = $database->getCollection('indexes');
        $this->assertCount(0, $collection->indexes());

        // Test non-shared tables duplicates throw duplicate
        $database->createIndex('indexes', Index::key(key: 'duplicate', attributes: ['string', 'boolean'], lengths: [128], orders: [OrderDirection::Asc]));
        try {
            $database->createIndex('indexes', Index::key(key: 'duplicate', attributes: ['string', 'boolean'], lengths: [128], orders: [OrderDirection::Asc]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Test delete index when index does not exist
        $database->createIndex('indexes', Index::key(key: 'index1', attributes: ['string', 'integer'], lengths: [128], orders: [OrderDirection::Asc]));
        $this->assertEquals(true, $this->deleteIndex('indexes', 'index1'));
        $database->deleteIndex('indexes', 'index1');

        // Test delete index when attribute does not exist
        $database->createIndex('indexes', Index::key(key: 'index1', attributes: ['string', 'integer'], lengths: [128], orders: [OrderDirection::Asc]));
        $database->deleteAttribute('indexes', 'string');
        $database->deleteIndex('indexes', 'index1');

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

        $database->createCollection(Collection::create(id: __FUNCTION__));

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
            $database->updateAttribute(__FUNCTION__, 'title2', new AttributeUpdate(type: ColumnType::String, size: $database->getAdapter()->getMaxIndexLength() + 300, required: true));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
        }
    }

    /**
     * An index length may not exceed the size of the attribute it covers. This is
     * a different bound from the adapter's maximum index length that
     * {@see self::testIndexLengthZero} covers: 701 is well under the maximum, and
     * only oversized relative to title1's own 700.
     *
     * Ported from main's testIndexValidation, which drove the index validator
     * directly. Going through createIndex() proves the validator is actually
     * consulted on the path a caller takes, which a direct construction cannot.
     */
    public function testIndexLengthExceedsAttributeSize(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)
            || ! $database->getAdapter()->supports(Capability::IdenticalIndexes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(Collection::create(id: __FUNCTION__));
        $database->createAttribute(__FUNCTION__, Attribute::string(key: 'title1', size: 700, required: false));
        $database->createAttribute(__FUNCTION__, Attribute::string(key: 'title2', size: 500, required: false));

        try {
            $database->createIndex(__FUNCTION__, Index::key(key: 'index1', attributes: ['title1', 'title2'], lengths: [701, 50]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Index length 701 is larger than the size for title1: 700"', $e->getMessage());
        }

        $database->deleteCollection(__FUNCTION__);
    }

    public function testRenameIndex(): void
    {
        $database = $this->getDatabase();
        $collection = $this->getNumbersCollection();
        $this->initRenameIndexFixture();

        $numbers = $database->getCollection($collection);

        $this->assertCount(2, $numbers->indexes());
        $this->assertSame('index3', $numbers->indexes()[0]->key);
        $this->assertSame('index2', $numbers->indexes()[1]->key);

        $database->renameIndex($collection, 'index2', 'index4');
        $this->assertSame('index4', $database->getCollection($collection)->indexes()[1]->key);

        $database->renameIndex($collection, 'index4', 'index2');
        $this->assertSame('index2', $database->getCollection($collection)->indexes()[1]->key);
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

        $database->createCollection(Collection::create(id: $collection));
        $database->createAttribute($collection, Attribute::string(key: 'verbose', size: 128, required: true));
        $database->createAttribute($collection, Attribute::integer(key: 'symbol', required: true));
        $database->createIndex($collection, Index::key(key: 'index1', attributes: ['verbose'], lengths: [128], orders: [OrderDirection::Asc]));
        $database->createIndex($collection, Index::key(key: 'index2', attributes: ['symbol'], lengths: [0], orders: [OrderDirection::Asc]));
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

        $database->createIndex($this->getDocumentsCollection(), Index::fulltext(key: 'string', attributes: ['string']));
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
            $database->createIndex($this->getDocumentsCollection(), Index::fulltext(key: 'string', attributes: ['string']));
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
            $database->createCollection(Collection::create(id: $collectionId));

            $database->createAttribute($collectionId, Attribute::string(key: 'name', size: 256));
            $database->createAttribute($collectionId, Attribute::string(key: 'description', size: 512));

            // Create trigram index on name attribute
            $database->createIndex($collectionId, Index::trigram(key: 'trigram_name', attributes: ['name']));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->indexes();
            $this->assertCount(1, $indexes);
            $this->assertEquals('trigram_name', $indexes[0]->key);
            $this->assertEquals(IndexType::Trigram, $indexes[0]->type);
            $this->assertEquals(['name'], $indexes[0]->attributes);

            // Create another trigram index on description
            $database->createIndex($collectionId, Index::trigram(key: 'trigram_description', attributes: ['description']));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->indexes();
            $this->assertCount(2, $indexes);

            // Test that trigram index can be deleted
            $database->deleteIndex($collectionId, 'trigram_name');
            $database->deleteIndex($collectionId, 'trigram_description');

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->indexes();
            $this->assertCount(0, $indexes);

        } finally {
            // Clean up
            $database->deleteCollection($collectionId);
        }
    }

    public function testTrigramIndexValidation(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::TrigramIndex)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collectionId = 'trigram_validation_test';

        try {
            $database->createCollection(Collection::create(id: $collectionId));

            $database->createAttribute($collectionId, Attribute::string(key: 'name', size: 256));
            $database->createAttribute($collectionId, Attribute::string(key: 'description', size: 412));
            $database->createAttribute($collectionId, Attribute::integer(key: 'age', width: IntegerWidth::Bits64));

            try {
                $database->createIndex($collectionId, Index::trigram(key: 'trigram_invalid', attributes: ['age']));
                $this->fail('Expected exception when creating trigram index on non-string attribute');
            } catch (DatabaseException $e) {
                $this->assertStringContainsString('Trigram index can only be created on string type attributes', $e->getMessage());
            }

            $database->createIndex($collectionId, Index::trigram(key: 'trigram_multi', attributes: ['name', 'description']));

            $indexes = \array_values(\array_filter(
                $database->getCollection($collectionId)->indexes(),
                fn (Index $index) => $index->key === 'trigram_multi'
            ));
            $this->assertCount(1, $indexes);
            $this->assertSame(IndexType::Trigram, $indexes[0]->type);
            $this->assertSame(['name', 'description'], $indexes[0]->attributes);

            try {
                $database->createIndex($collectionId, Index::trigram(key: 'trigram_mixed', attributes: ['name', 'age']));
                $this->fail('Expected exception when creating trigram index with mixed attribute types');
            } catch (DatabaseException $e) {
                $this->assertStringContainsString('Trigram index can only be created on string type attributes', $e->getMessage());
            }

            try {
                $database->createIndex($collectionId, Index::fromArray(['key' => 'trigram_order', 'type' => IndexType::Trigram, 'attributes' => ['name'], 'orders' => [OrderDirection::Asc]]));
                $this->fail('Expected exception when creating trigram index with orders');
            } catch (DatabaseException $e) {
                $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $e->getMessage());
            }

            try {
                $database->createIndex($collectionId, Index::fromArray(['key' => 'trigram_length', 'type' => IndexType::Trigram, 'attributes' => ['name'], 'lengths' => [128]]));
                $this->fail('Expected exception when creating trigram index with lengths');
            } catch (DatabaseException $e) {
                $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $e->getMessage());
            }

            $this->assertSame(
                ['trigram_multi'],
                \array_map(fn (Index $index) => $index->key, $database->getCollection($collectionId)->indexes())
            );
        } finally {
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
        $database->createCollection(Collection::create(id: $col));

        $database->createAttribute($col, Attribute::datetime(key: 'expiresAt'));

        $permissions = [
            Permission::read(Role::any()),
            Permission::write(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $database->createIndex($col, Index::ttl(key: 'idx_ttl_valid', attribute: 'expiresAt', ttl: 3600));

        $collection = $database->getCollection($col);
        $indexes = $collection->indexes();
        $this->assertCount(1, $indexes);
        $ttlIndex = $indexes[0];
        $this->assertEquals('idx_ttl_valid', $ttlIndex->key);
        $this->assertEquals(IndexType::Ttl, $ttlIndex->type);
        $this->assertEquals(3600, $ttlIndex->ttl);

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

        $database->deleteIndex($col, 'idx_ttl_valid');

        $database->createIndex($col, Index::ttl(key: 'idx_ttl_min', attribute: 'expiresAt', ttl: 1));

        $col2 = uniqid('sl_ttl_collection');

        $expiresAtAttr = Attribute::datetime(key: 'expiresAt');

        $ttlIndexDoc = Index::ttl(key: 'idx_ttl_collection', attribute: 'expiresAt', ttl: 7200);

        $database->createCollection(Collection::create(id: $col2, attributes: [$expiresAtAttr], indexes: [$ttlIndexDoc]));

        $collection2 = $database->getCollection($col2);
        $indexes2 = $collection2->indexes();
        $this->assertCount(1, $indexes2);
        $ttlIndex2 = $indexes2[0];
        $this->assertEquals('idx_ttl_collection', $ttlIndex2->key);
        $this->assertEquals(7200, $ttlIndex2->ttl);

        $database->deleteCollection($col);
        $database->deleteCollection($col2);
    }

    public function testRenameIndexMissing(): void
    {
        $database = $this->getDatabase();
        $this->initRenameIndexFixture();

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Index not found');
        $database->renameIndex($this->getNumbersCollection(), 'index1', 'index4');
    }

    public function testRenameIndexExisting(): void
    {
        $database = $this->getDatabase();
        $this->initRenameIndexFixture();

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Index name already used');
        $database->renameIndex($this->getNumbersCollection(), 'index3', 'index2');
    }

    /**
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     */
    private function indexValidator(array $attributes, array $indexes): IndexDefinition
    {
        $adapter = $this->getDatabase()->getAdapter();

        return new IndexDefinition(
            $attributes,
            $indexes,
            $adapter->getMaxIndexLength(),
            $adapter->getInternalIndexesKeys(),
            $adapter->supports(Capability::IndexArray),
            $adapter->supports(Capability::SpatialIndexNull),
            $adapter->supports(Capability::SpatialIndexOrder),
            $adapter->supports(Capability::Vectors),
            $adapter->supports(Capability::DefinedAttributes),
            $adapter->supports(Capability::MultipleFulltextIndexes),
            $adapter->supports(Capability::IdenticalIndexes),
            $adapter->supports(Capability::ObjectIndexes),
            $adapter->supports(Capability::TrigramIndex),
            $adapter->hasFeature(Feature\Spatial::class),
            $adapter->supports(Capability::Index),
            $adapter->supports(Capability::UniqueIndex),
            $adapter->supports(Capability::Fulltext),
            $adapter->supports(Capability::TTLIndexes),
            $adapter->supports(Capability::Objects),
        );
    }

    public function testIndexValidation(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        $attributes = [
            Attribute::string(key: 'title1', size: 700),
            Attribute::string(key: 'title2', size: 500),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['title1', 'title2'], lengths: [701, 50]),
        ];

        $validator = $this->indexValidator($attributes, $indexes);

        if ($adapter->supports(Capability::IdenticalIndexes)) {
            $errorMessage = 'Index length 701 is larger than the size for title1: 700"';
            $this->assertFalse($validator->isValid($indexes[0]));
            $this->assertSame($errorMessage, $validator->getDescription());

            try {
                $database->createCollection(Collection::create(id: 'index_length', attributes: $attributes, indexes: $indexes, permissions: [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                ]));
                $this->fail('Failed to throw exception');
            } catch (Exception $e) {
                $this->assertSame($errorMessage, $e->getMessage());
            }
        }

        $indexes = [
            Index::key(key: 'index1', attributes: ['title1', 'title2'], lengths: [700]),
        ];

        if ($adapter->supports(Capability::DefinedAttributes) && $adapter->getMaxIndexLength() > 0) {
            $errorMessage = 'Index length is longer than the maximum: '.$adapter->getMaxIndexLength();
            $this->assertFalse($validator->isValid($indexes[0]));
            $this->assertSame($errorMessage, $validator->getDescription());

            try {
                $database->createCollection(Collection::create(id: 'index_length', attributes: $attributes, indexes: $indexes));
                $this->fail('Failed to throw exception');
            } catch (Exception $e) {
                $this->assertSame($errorMessage, $e->getMessage());
            }
        }

        $attributes[] = Attribute::integer(key: 'integer', width: IntegerWidth::Bits64);

        $indexes = [
            Index::fulltext(key: 'index1', attributes: ['title1', 'integer']),
        ];

        $newIndex = Index::fulltext(key: 'newIndex1', attributes: ['title1', 'integer']);

        $validator = $this->indexValidator($attributes, $indexes);

        $this->assertFalse($validator->isValid($newIndex));

        if (! $adapter->supports(Capability::Fulltext)) {
            $this->assertSame('Fulltext index is not supported', $validator->getDescription());
        } elseif (! $adapter->supports(Capability::MultipleFulltextIndexes)) {
            $this->assertSame('There is already a fulltext index in the collection', $validator->getDescription());
        } elseif ($adapter->supports(Capability::DefinedAttributes)) {
            $this->assertSame('Attribute "integer" cannot be part of a fulltext index, must be of type string', $validator->getDescription());
        }

        try {
            $database->createCollection(Collection::create(id: 'index_length', attributes: $attributes, indexes: $indexes));
            if ($adapter->supports(Capability::DefinedAttributes)) {
                $this->fail('Failed to throw exception');
            }
            $database->deleteCollection('index_length');
        } catch (Exception $e) {
            if (! $adapter->supports(Capability::Fulltext)) {
                $this->assertSame('Fulltext index is not supported', $e->getMessage());
            } else {
                $this->assertSame('Attribute "integer" cannot be part of a fulltext index, must be of type string', $e->getMessage());
            }
        }

        if (! $adapter->supports(Capability::DefinedAttributes)) {
            return;
        }

        $indexes = [
            Index::key(key: 'index_negative_length', attributes: ['title1'], lengths: [-1]),
        ];

        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertSame('Negative index length provided for title1', $validator->getDescription());

        try {
            $database->createCollection(Collection::create(id: ID::unique(), attributes: $attributes, indexes: $indexes));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertSame('Negative index length provided for title1', $e->getMessage());
        }

        $indexes = [
            Index::key(key: 'index_extra_lengths', attributes: ['title1', 'title2'], lengths: [100, 100, 100]),
        ];

        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertSame('Invalid index lengths. Count of lengths must be equal or less than the number of attributes.', $validator->getDescription());

        try {
            $database->createCollection(Collection::create(id: ID::unique(), attributes: $attributes, indexes: $indexes));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertSame('Invalid index lengths. Count of lengths must be equal or less than the number of attributes.', $e->getMessage());
        }
    }

    public function testCreateCollectionWithIndexOnSequence(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Index)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $database->createCollection(Collection::create(id: 'sequenceIndexes', attributes: [
            Attribute::string(key: 'username', size: 128),
            Attribute::string(key: 'email', size: 128),
        ], indexes: [
            Index::key(key: '_index 123', attributes: ['username', '$sequence'], orders: [OrderDirection::Asc, OrderDirection::Desc]),
            Index::unique(key: '_index 456', attributes: ['email', '$sequence'], orders: [OrderDirection::Asc, OrderDirection::Desc]),
        ]));

        $indexes = $collection->indexes();
        $this->assertCount(2, $indexes);
        $this->assertSame('_index 123', $indexes[0]->key);
        $this->assertSame(['username', '$sequence'], $indexes[0]->attributes);
        $this->assertSame('_index 456', $indexes[1]->key);
        $this->assertSame(['email', '$sequence'], $indexes[1]->attributes);

        $this->assertSequenceIndexesAnswerQueries('sequenceIndexes');

        $database->deleteCollection('sequenceIndexes');
    }

    public function testCreateIndexOnSequence(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Index)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(Collection::create(id: __FUNCTION__));

        $database->createAttribute(__FUNCTION__, Attribute::string(key: 'username', size: 128));
        $database->createAttribute(__FUNCTION__, Attribute::string(key: 'email', size: 128));

        $database->createIndex(__FUNCTION__, Index::key(key: '_index 123', attributes: ['username', '$sequence'], orders: [OrderDirection::Asc, OrderDirection::Desc]));
        $database->createIndex(__FUNCTION__, Index::unique(key: '_index 456', attributes: ['email', '$sequence'], orders: [OrderDirection::Asc, OrderDirection::Desc]));

        $indexes = $database->getCollection(__FUNCTION__)->indexes();
        $this->assertCount(2, $indexes);
        $this->assertSame('_index 123', $indexes[0]->key);
        $this->assertSame(['username', '$sequence'], $indexes[0]->attributes);
        $this->assertSame('_index 456', $indexes[1]->key);
        $this->assertSame(['email', '$sequence'], $indexes[1]->attributes);

        $this->assertSequenceIndexesAnswerQueries(__FUNCTION__);

        $database->deleteCollection(__FUNCTION__);
    }

    private function assertSequenceIndexesAnswerQueries(string $collection): void
    {
        $database = $this->getDatabase();

        $database->createDocument($collection, new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'username' => 'chester',
            'email' => 'chester@example.com',
        ]));

        $documents = $database->find($collection, [
            Query::equal('username', ['chester']),
            Query::orderDesc('$sequence'),
        ]);

        $this->assertCount(1, $documents);
        $this->assertSame('chester', $documents[0]->getAttribute('username'));

        $database->createDocument($collection, new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'username' => 'chester',
            'email' => 'chester@example.com',
        ]));

        $this->assertCount(2, $database->find($collection, [
            Query::equal('email', ['chester@example.com']),
        ]), '$sequence is unique on its own, so a unique index containing it never conflicts. A duplicate here means the index was built without the $sequence column');
    }

    public function testCompositeIndexKeepsArrayAttributePosition(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! ($adapter instanceof MariaDB || $adapter instanceof Postgres) || ! $adapter->supports(Capability::IndexArray)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $attributes = [
            Attribute::string(key: 'tags', size: 64, array: true),
            Attribute::string(key: 'status', size: 32),
            Attribute::string(key: 'name', size: 128),
        ];
        $index = Index::key(key: 'tagsfirst', attributes: ['tags', 'status', 'name'], lengths: [255, null, 16], orders: [null, null, OrderDirection::Desc]);

        $tenant = $database->getSharedTables() ? ['_tenant'] : [];
        $expected = match (true) {
            $adapter instanceof Postgres => [...$tenant, 'tags', 'status', 'name DESC'],
            $adapter->supports(Capability::CastIndexArray) => [...$tenant, '', 'status', 'name(16)'],
            default => [...$tenant, 'tags(255)', 'status', 'name(16)'],
        };

        $database->createCollection(Collection::create(id: 'index_array_position_created', attributes: $attributes, indexes: [$index]));
        try {
            $this->assertSame($expected, $this->getIndexKeyParts($database, 'index_array_position_created', 'tagsfirst'));
        } finally {
            $database->deleteCollection('index_array_position_created');
        }

        $database->createCollection(Collection::create(id: 'index_array_position_added'));
        try {
            $database->createAttributes('index_array_position_added', $attributes);
            $database->createIndex('index_array_position_added', $index);
            $this->assertSame($expected, $this->getIndexKeyParts($database, 'index_array_position_added', 'tagsfirst'));
        } finally {
            $database->deleteCollection('index_array_position_added');
        }
    }

    public function testCompositeIndexKeepsObjectPathPosition(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter instanceof Postgres || ! $adapter->supports(Capability::Objects)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'index_object_path_position';
        $database->createCollection(Collection::create(id: $collection));

        try {
            $database->createAttribute($collection, Attribute::object(key: 'data'));
            $database->createAttribute($collection, Attribute::string(key: 'status', size: 32));
            $database->createIndex($collection, Index::key(key: 'countryfirst', attributes: ['data.country', 'status'], orders: [OrderDirection::Desc, null]));

            $parts = $this->getIndexKeyParts($database, $collection, 'countryfirst');
            $tenant = $database->getSharedTables() ? ['_tenant'] : [];

            $this->assertSame([...$tenant, "(data ->> 'country'::text) DESC", 'status'], $parts);
        } finally {
            $database->deleteCollection($collection);
        }
    }

    /**
     * Key parts of an index in the order the engine stores them: MariaDB and MySQL prefix lengths as "column(length)",
     * PostgreSQL descending parts as "part DESC".
     *
     * @return list<string>
     */
    private function getIndexKeyParts(Database $database, string $collection, string $index): array
    {
        $adapter = $database->getAdapter();

        if ($adapter instanceof Postgres) {
            $rows = $adapter->rawQuery(
                'SELECT c.relname AS "index", pg_get_indexdef(i.indexrelid, k.position, true) || CASE WHEN i.indoption[k.position - 1] & 1 = 1 THEN \' DESC\' ELSE \'\' END AS "part"
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid
                CROSS JOIN LATERAL generate_series(1, i.indnkeyatts) AS k(position)
                WHERE i.indrelid = to_regclass(?)
                ORDER BY c.relname, k.position',
                ['"'.$database->getDatabase().'"."'.$database->getNamespace().'_'.$collection.'"'],
            );

            $parts = [];
            foreach ($rows as $row) {
                $name = $row->getAttribute('index');
                $part = $row->getAttribute('part');
                $this->assertIsString($name);
                $this->assertIsString($part);
                if (\str_ends_with($name, '_'.$index)) {
                    $parts[] = $part;
                }
            }
            $this->assertNotEmpty($parts, 'Index '.$index.' was not found on '.$collection);

            return $parts;
        }

        foreach ($database->getSchemaIndexes($collection) as $schemaIndex) {
            if ($schemaIndex->getId() !== $index) {
                continue;
            }

            $columns = $schemaIndex->getAttribute('columns');
            $lengths = $schemaIndex->getAttribute('lengths');
            $this->assertIsArray($columns);
            $this->assertIsArray($lengths);

            $parts = [];
            foreach (\array_values($columns) as $position => $column) {
                $this->assertIsString($column);
                $length = $lengths[$position] ?? null;
                $parts[] = \is_int($length) ? $column.'('.$length.')' : $column;
            }

            return $parts;
        }

        $this->fail('Index '.$index.' was not found on '.$collection);
    }

    public function testExceptionIndexLimit(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: 'indexLimit'));

        for ($i = 0; $i < 64; $i++) {
            $database->createAttribute('indexLimit', Attribute::string(key: "test{$i}", size: 16, required: true));
        }

        for ($i = 0; $i < $database->getLimitForIndexes(); $i++) {
            $database->createIndex('indexLimit', Index::key(key: "index{$i}", attributes: ["test{$i}"], lengths: [16]));
        }

        try {
            $database->createIndex('indexLimit', Index::key(key: 'index64', attributes: ['test64'], lengths: [16]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        } finally {
            $database->deleteCollection('indexLimit');
        }
    }

    public function testIdenticalIndexValidation(): void
    {
        $database = $this->getDatabase();

        $collectionId = 'identical_index_test';

        try {
            $database->createCollection(Collection::create(id: $collectionId));

            $database->createAttribute($collectionId, Attribute::string(key: 'name', size: 256));
            $database->createAttribute($collectionId, Attribute::integer(key: 'age', width: IntegerWidth::Bits64));

            $database->createIndex($collectionId, Index::key(key: 'index1', attributes: ['name', 'age'], orders: [OrderDirection::Asc, OrderDirection::Desc]));

            $supportsIdenticalIndexes = $database->getAdapter()->supports(Capability::IdenticalIndexes);

            try {
                $database->createIndex($collectionId, Index::key(key: 'index2', attributes: ['name', 'age'], orders: [OrderDirection::Asc, OrderDirection::Desc]));
                $this->assertTrue($supportsIdenticalIndexes, 'An identical index must be rejected when the adapter does not support identical indexes');
            } catch (Throwable $e) {
                $this->assertFalse($supportsIdenticalIndexes, 'Unexpected exception when creating identical index: '.$e->getMessage());
                $this->assertSame('There is already an index with the same attributes and orders', $e->getMessage());
            }

            try {
                $database->createIndex($collectionId, Index::key(key: 'index3', attributes: ['age', 'name'], orders: [OrderDirection::Asc, OrderDirection::Desc]));
            } catch (Throwable $e) {
                $this->assertFalse($supportsIdenticalIndexes, 'Unexpected exception when creating index with a different attribute order: '.$e->getMessage());
            }

            try {
                $database->createIndex($collectionId, Index::key(key: 'index4', attributes: ['age', 'name'], orders: [OrderDirection::Desc, OrderDirection::Asc]));
            } catch (Throwable $e) {
                $this->assertFalse($supportsIdenticalIndexes, 'Unexpected exception when creating index with different orders: '.$e->getMessage());
            }

            $database->createIndex($collectionId, Index::key(key: 'index5', attributes: ['name'], orders: [OrderDirection::Asc]));
            $database->createIndex($collectionId, Index::key(key: 'index6', attributes: ['name', 'age'], orders: [OrderDirection::Asc]));
        } finally {
            $database->deleteCollection($collectionId);
        }
    }

    public function testMaxQueriesValues(): void
    {
        $database = $this->getDatabase();
        $collection = 'maxQueryValues_'.uniqid();

        $database->createCollection(Collection::create(id: $collection));

        $max = $database->getMaxQueryValues();
        $database->setMaxQueryValues(5);

        try {
            $database->find($collection, [Query::equal('$id', ['1', '2', '3', '4', '5', '6'])]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(QueryException::class, $e);
            $this->assertSame('Invalid query: Query on attribute has greater than 5 values: $id', $e->getMessage());
        } finally {
            $database->setMaxQueryValues($max);
            $database->deleteCollection($collection);
        }
    }

    public function testMultipleFulltextIndexValidation(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Fulltext)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collectionId = 'multiple_fulltext_test';

        try {
            $database->createCollection(Collection::create(id: $collectionId));

            $database->createAttribute($collectionId, Attribute::string(key: 'title', size: 256));
            $database->createAttribute($collectionId, Attribute::string(key: 'content', size: 256));
            $database->createIndex($collectionId, Index::fulltext(key: 'fulltext_title', attributes: ['title']));

            $supportsMultipleFulltext = $database->getAdapter()->supports(Capability::MultipleFulltextIndexes);

            try {
                $database->createIndex($collectionId, Index::fulltext(key: 'fulltext_content', attributes: ['content']));
                $this->assertTrue($supportsMultipleFulltext, 'Expected exception when creating second fulltext index, but none was thrown');
            } catch (Throwable $e) {
                $this->assertFalse($supportsMultipleFulltext, 'Unexpected exception when creating second fulltext index: '.$e->getMessage());
                $this->assertSame('There is already a fulltext index in the collection', $e->getMessage());
            }
        } finally {
            $database->deleteCollection($collectionId);
        }
    }

    public function testTTLIndexDuplicatePrevention(): void
    {
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::TTLIndexes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = uniqid('sl_ttl_dup');
        $database->createCollection(Collection::create(id: $collection));

        $database->createAttribute($collection, Attribute::datetime(key: 'expiresAt'));
        $database->createAttribute($collection, Attribute::datetime(key: 'deletedAt'));

        $database->createIndex($collection, Index::ttl(key: 'idx_ttl_expires', attribute: 'expiresAt', ttl: 3600));

        foreach ([
            Index::ttl(key: 'idx_ttl_expires_duplicate', attribute: 'expiresAt', ttl: 7200),
            Index::ttl(key: 'idx_ttl_deleted', attribute: 'deletedAt', ttl: 86400),
        ] as $duplicate) {
            try {
                $database->createIndex($collection, $duplicate);
                $this->fail('Expected exception for creating a second TTL index in a collection');
            } catch (Exception $e) {
                $this->assertInstanceOf(DatabaseException::class, $e);
                $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
            }
        }

        $indexes = $database->getCollection($collection)->indexes();
        $this->assertCount(1, $indexes);

        $indexIds = array_map(fn (Index $index) => $index->key, $indexes);
        $this->assertContains('idx_ttl_expires', $indexIds);
        $this->assertNotContains('idx_ttl_deleted', $indexIds);

        try {
            $database->createIndex($collection, Index::ttl(key: 'idx_ttl_deleted_duplicate', attribute: 'deletedAt', ttl: 172800));
            $this->fail('Expected exception for creating a second TTL index in a collection');
        } catch (Exception $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
        }

        $database->deleteIndex($collection, 'idx_ttl_expires');

        $database->createIndex($collection, Index::ttl(key: 'idx_ttl_deleted', attribute: 'deletedAt', ttl: 1800));

        $indexes = $database->getCollection($collection)->indexes();
        $this->assertCount(1, $indexes);

        $indexIds = array_map(fn (Index $index) => $index->key, $indexes);
        $this->assertNotContains('idx_ttl_expires', $indexIds);
        $this->assertContains('idx_ttl_deleted', $indexIds);

        try {
            $database->createCollection(Collection::create(id: uniqid('sl_ttl_dup_collection'), attributes: [
                Attribute::datetime(key: 'expiresAt'),
            ], indexes: [
                Index::ttl(key: 'idx_ttl_1', attribute: 'expiresAt', ttl: 3600),
                Index::ttl(key: 'idx_ttl_2', attribute: 'expiresAt', ttl: 7200),
            ]));
            $this->fail('Expected exception for duplicate TTL indexes in createCollection');
        } catch (Exception $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
        }

        $database->deleteCollection($collection);
    }

    public function testSchemaIndexesListFulltextIndexes(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->hasFeature(Feature\SchemaIndexes::class) || ! $adapter->supports(Capability::Fulltext)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'schema_fulltext';
        $database->createCollection(Collection::create(id: $collection, attributes: [
            Attribute::string(key: 'title', size: 128),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));

        try {
            $database->createIndex($collection, Index::fulltext(key: 'title_search', attributes: ['title']));
            $this->assertSame(['title_search' => ['title']], $this->getFulltextSchemaIndexes($database, $collection));

            $database->renameIndex($collection, 'title_search', 'title_lookup');
            $this->assertSame(['title_lookup' => ['title']], $this->getFulltextSchemaIndexes($database, $collection));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testDeleteFulltextIndexDropsItsTables(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->hasFeature(Feature\SchemaIndexes::class) || ! $adapter->supports(Capability::Fulltext)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'delete_fulltext';
        $database->createCollection(Collection::create(id: $collection, attributes: [
            Attribute::string(key: 'title', size: 128),
            Attribute::string(key: 'body', size: 128),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));

        $search = static fn (string $attribute, string $term): array => \array_map(
            static fn (Document $document): string => $document->getId(),
            $database->find($collection, [Query::search($attribute, $term)]),
        );

        try {
            $database->createDocument($collection, new Document([
                '$id' => 'fox',
                'title' => 'quick brown fox',
                'body' => 'lazy dog',
            ]));

            $multiple = $adapter->supports(Capability::MultipleFulltextIndexes);
            $database->createIndex($collection, Index::fulltext(key: 'title_search', attributes: ['title']));
            if ($multiple) {
                $database->createIndex($collection, Index::fulltext(key: 'body_search', attributes: ['body']));
            }
            $remaining = $multiple ? ['body_search' => ['body']] : [];

            $database->deleteIndex($collection, 'title_search');
            $this->assertSame($remaining, $this->getFulltextSchemaIndexes($database, $collection));

            try {
                $search('title', 'quick');
                $this->fail('A search on an attribute whose fulltext index was deleted must be refused');
            } catch (QueryException $error) {
                $this->assertSame('Searching by attribute "title" requires a fulltext index.', $error->getMessage());
            }

            if ($multiple) {
                $this->assertSame(['fox'], $search('body', 'lazy'));
            }

            $database->createIndex($collection, Index::fulltext(key: 'title_search', attributes: ['title']));
            $this->assertSame($remaining + ['title_search' => ['title']], $this->getFulltextSchemaIndexes($database, $collection));
            $this->assertSame(['fox'], $search('title', 'quick'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    /**
     * @return array<string, list<string>> The columns of each fulltext index by id, without the tenant column, sorted by id
     */
    private function getFulltextSchemaIndexes(Database $database, string $collection): array
    {
        $indexes = [];
        foreach ($database->getSchemaIndexes($collection) as $schemaIndex) {
            $type = $schemaIndex->getAttribute('indexType');
            $this->assertIsString($type);
            if (\strtoupper($type) !== 'FULLTEXT') {
                continue;
            }

            $columns = $schemaIndex->getAttribute('columns');
            $this->assertIsArray($columns);
            $columns = \array_values(\array_filter(
                $columns,
                static fn (mixed $column): bool => \is_string($column) && $column !== '_tenant',
            ));
            /** @var list<string> $columns */
            $indexes[$schemaIndex->getId()] = $columns;
        }
        \ksort($indexes);

        return $indexes;
    }

    public function testCreateIndexReplacesMismatchedOrphanIndex(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(Feature\SchemaIndexes::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'orphanIndex';
        $database->createCollection(Collection::create(id: $collection, attributes: [
            Attribute::string(key: 'name', size: 64),
            Attribute::string(key: 'email', size: 64),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));

        try {
            $database->getAdapter()->createIndex($collection, Index::key(key: 'lookup', attributes: ['name']));

            if ($this->getSchemaIndexColumns($database, $collection, 'lookup') === null) {
                $this->markTestSkipped('getSchemaIndexes() does not report indexes under their id on this adapter');
            }

            if ($database->getSharedTables()) {
                try {
                    $database->createIndex($collection, Index::unique(key: 'lookup', attributes: ['email']));
                    $this->fail('An index another tenant may use must not be replaced under shared tables');
                } catch (DuplicateException $error) {
                    $this->assertSame('Index exists in the shared table with another definition', $error->getMessage());
                }

                $this->assertSame(['name'], $this->getSchemaIndexColumns($database, $collection, 'lookup'));
                $this->assertSame([], $database->getCollection($collection)->indexes());

                return;
            }

            $database->createIndex($collection, Index::unique(key: 'lookup', attributes: ['email']));
            $this->assertSame(['email'], $this->getSchemaIndexColumns($database, $collection, 'lookup'));

            $database->createDocument($collection, new Document(['email' => 'user@example.com']));
            try {
                $database->createDocument($collection, new Document(['email' => 'user@example.com']));
                $this->fail('The replaced index must enforce uniqueness on email');
            } catch (DuplicateException) {
                $this->assertSame(1, $database->count($collection));
            }
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testUpdateAttributeCoveredByAKeyIndexSucceeds(): void
    {
        $database = $this->getDatabase();
        $collection = 'indexedResize';
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [Attribute::string(key: 'name', size: 64)],
            indexes: [Index::key(key: 'by_name', attributes: ['name'])],
        ));

        try {
            $updated = $database->updateAttribute($collection, 'name', new AttributeUpdate(size: 128));

            $this->assertSame(128, $updated->size);
            $this->assertSame(128, $database->getCollection($collection)->attributes()[0]->size);
            $this->assertSame(['by_name'], \array_map(
                static fn (Index $index): string => $index->key,
                $database->getCollection($collection)->indexes(),
            ));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    /**
     * @return list<string>|null The index's columns without the tenant column, or null when the schema does not list it
     */
    private function getSchemaIndexColumns(Database $database, string $collection, string $index): ?array
    {
        foreach ($database->getSchemaIndexes($collection) as $schemaIndex) {
            if ($schemaIndex->getId() !== $index) {
                continue;
            }

            $columns = $schemaIndex->getAttribute('columns');
            $this->assertIsArray($columns);

            $names = [];
            foreach ($columns as $column) {
                $this->assertIsString($column);
                if ($column !== Storage::TENANT) {
                    $names[] = $column;
                }
            }

            return $names;
        }

        return null;
    }

    public function testMongoUniqueIndexOnAnIntegerIsEnforced(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof Mongo) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->createMongoUniqueIndexCollection($database, [Attribute::integer(key: 'count', width: IntegerWidth::Bits64)]);

        $this->assertMongoUniqueIndexRejectsDuplicates($database, $collection, 'count', 7);
        $this->assertMongoUniqueIndexRejectsDuplicates($database, $collection, 'count', 5_000_000_000);

        $database->deleteCollection($collection);
    }

    public function testMongoUniqueIndexesOnFloatBooleanAndDatetimeAreEnforced(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof Mongo) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->createMongoUniqueIndexCollection($database, [
            Attribute::double(key: 'price'),
            Attribute::boolean(key: 'active'),
            Attribute::datetime(key: 'seenAt'),
        ]);

        $this->assertMongoUniqueIndexRejectsDuplicates($database, $collection, 'price', 9.5);
        $this->assertMongoUniqueIndexRejectsDuplicates($database, $collection, 'active', true);
        $this->assertMongoUniqueIndexRejectsDuplicates($database, $collection, 'seenAt', '2026-01-01T00:00:00.000+00:00');

        $database->deleteCollection($collection);
    }

    public function testMongoKeyIndexesServeEqualityFilters(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter instanceof Mongo) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $values = [
            'count' => 7,
            'price' => 9.5,
            'active' => true,
            'seenAt' => new UTCDateTime(new NativeDateTime('2026-01-01T00:00:00+00:00')),
            'name' => 'first',
        ];
        $attributes = [
            Attribute::integer(key: 'count'),
            Attribute::double(key: 'price'),
            Attribute::boolean(key: 'active'),
            Attribute::datetime(key: 'seenAt'),
            Attribute::string(key: 'name', size: 16),
            Attribute::string(key: 'group', size: 16),
        ];
        $indexes = \array_map(
            fn (string $attribute): Index => Index::key(key: $attribute.'_key', attributes: [$attribute]),
            \array_keys($values),
        );
        $indexes[] = Index::key(key: 'group_count', attributes: ['group', 'count']);
        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];

        $fromCollection = 'key_scan_collection_'.\uniqid();
        $database->createCollection(Collection::create(id: $fromCollection, attributes: $attributes, indexes: $indexes, permissions: $permissions, documentSecurity: false));

        $fromIndex = 'key_scan_index_'.\uniqid();
        $database->createCollection(Collection::create(id: $fromIndex, attributes: $attributes, permissions: $permissions, documentSecurity: false));
        foreach ($indexes as $index) {
            $database->createIndex($fromIndex, $index);
        }

        foreach ([$fromCollection, $fromIndex] as $collection) {
            $database->createDocument($collection, new Document([
                'count' => 7,
                'price' => 9.5,
                'active' => true,
                'seenAt' => '2026-01-01T00:00:00.000+00:00',
                'name' => 'first',
                'group' => 'a',
            ]));

            foreach ($values as $attribute => $value) {
                $plan = $this->explainMongoFind($adapter, $collection, [$attribute => $value]);

                $this->assertStringContainsString('"stage":"IXSCAN"', $plan, $collection.': an equality on '.$attribute.' must scan its key index');
                $this->assertStringContainsString('"indexName":"'.$attribute.'_key"', $plan, $collection.': an equality on '.$attribute.' must use '.$attribute.'_key');
            }

            $plan = $this->explainMongoFind($adapter, $collection, ['group' => 'a']);
            $this->assertStringContainsString('"stage":"IXSCAN"', $plan, $collection.': an equality on the leading field of a compound key index must scan it');
            $this->assertStringContainsString('"indexName":"group_count"', $plan, $collection.': an equality on group alone must use group_count');

            $database->deleteCollection($collection);
        }
    }

    public function testRenamingAnIndexTheSchemaNoLongerHasFails(): void
    {
        $database = $this->getDatabase();
        $collection = 'renameDroppedIndex';
        $database->createCollection(Collection::create(id: $collection, attributes: [
            Attribute::integer(key: 'age'),
        ], indexes: [
            Index::key(key: 'byAge', attributes: ['age']),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));

        try {
            $database->getAdapter()->deleteIndex($collection, 'byAge');

            if ($database->getAdapter() instanceof SQLite) {
                $database->renameIndex($collection, 'byAge', 'ageIndex');
                $this->assertSame(['ageIndex'], $this->getIndexKeys($database, $collection));

                return;
            }

            try {
                $database->renameIndex($collection, 'byAge', 'ageIndex');
                $this->fail('A rename of an index the schema does not have must fail');
            } catch (DatabaseException $error) {
                $this->assertStringStartsWith("Failed to rename index 'byAge' to 'ageIndex': ", $error->getMessage());
            }

            $this->assertSame(['byAge'], $this->getIndexKeys($database, $collection));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testRenamingAnIndexTheSchemaAlreadyRenamedCompletes(): void
    {
        $database = $this->getDatabase();
        $collection = 'renameRenamedIndex';
        $database->createCollection(Collection::create(id: $collection, attributes: [
            Attribute::integer(key: 'age'),
        ], indexes: [
            Index::key(key: 'byAge', attributes: ['age']),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));

        try {
            $this->assertTrue($database->getAdapter()->renameIndex($collection, 'byAge', 'ageIndex'));

            if ($database->getAdapter() instanceof Mongo) {
                try {
                    $database->renameIndex($collection, 'byAge', 'ageIndex');
                    $this->fail('MongoDB drops the old index before it renames, so a rename the schema already made fails');
                } catch (DatabaseException $error) {
                    $this->assertStringStartsWith("Failed to rename index 'byAge' to 'ageIndex': ", $error->getMessage());
                }

                $this->assertSame(['byAge'], $this->getIndexKeys($database, $collection));

                return;
            }

            $database->renameIndex($collection, 'byAge', 'ageIndex');
            $this->assertSame(['ageIndex'], $this->getIndexKeys($database, $collection));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    /**
     * @return list<string>
     */
    private function getIndexKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Index $index): string => $index->key,
            $database->getCollection($collection)->indexes(),
        );
    }

    /**
     * @param  array<string, mixed>  $filter
     */
    private function explainMongoFind(Mongo $adapter, string $collection, array $filter): string
    {
        if ($adapter->getSharedTables()) {
            $filter = [Storage::TENANT => $adapter->getTenant(), ...$filter];
        }

        $client = $adapter->getDriver();
        $this->assertInstanceOf(Client::class, $client);

        $explain = $client->query([
            'explain' => [
                'find' => $adapter->getNamespace().'_'.$adapter->filter($collection),
                'filter' => $filter,
            ],
            'verbosity' => 'queryPlanner',
        ]);
        $this->assertInstanceOf(stdClass::class, $explain);

        $planner = $explain->queryPlanner ?? null;
        $this->assertInstanceOf(stdClass::class, $planner);

        $plan = \json_encode($planner->winningPlan ?? null);
        $this->assertIsString($plan);

        return $plan;
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private function createMongoUniqueIndexCollection(Database $database, array $attributes): string
    {
        $collection = 'unique_types_'.\uniqid();

        $database->createCollection(Collection::create(
            id: $collection,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));

        foreach ($attributes as $attribute) {
            $database->createIndex($collection, Index::unique(key: $attribute->key.'_unique', attributes: [$attribute->key]));
        }

        return $collection;
    }

    private function assertMongoUniqueIndexRejectsDuplicates(Database $database, string $collection, string $attribute, mixed $value): void
    {
        $database->createDocument($collection, new Document([$attribute => $value]));

        $error = null;
        try {
            $database->createDocument($collection, new Document([$attribute => $value]));
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf(UniqueException::class, $error, 'The unique index on '.$attribute.' must reject a second document with the same value');
    }
}
