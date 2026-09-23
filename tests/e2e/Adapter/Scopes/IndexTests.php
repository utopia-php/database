<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Throwable;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Index as IndexValidator;
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
        $this->assertCount(6, $collection->indexes);

        // Delete Indexes
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index2'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index3'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index4'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'index5'));
        $this->assertEquals(true, $database->deleteIndex('indexes', 'order'));

        $collection = $database->getCollection('indexes');
        $this->assertCount(0, $collection->indexes);

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

    /**
     * An index length may not exceed the size of the attribute it covers. This is
     * a different bound from the adapter's maximum index length that
     * {@see self::testIndexLengthZero} covers: 701 is well under the maximum, and
     * only oversized relative to title1's own 700.
     *
     * Ported from main's testIndexValidation, which drove Validator\Index
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

        $database->createCollection(new Collection(id: __FUNCTION__));
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

        $this->assertCount(2, $numbers->indexes);
        $this->assertSame('index3', $numbers->indexes[0]->getId());
        $this->assertSame('index2', $numbers->indexes[1]->getId());

        $this->assertTrue($database->renameIndex($collection, 'index2', 'index4'));
        $this->assertSame('index4', $database->getCollection($collection)->indexes[1]->getId());

        $this->assertTrue($database->renameIndex($collection, 'index4', 'index2'));
        $this->assertSame('index2', $database->getCollection($collection)->indexes[1]->getId());
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
            $indexes = $collection->indexes;
            $this->assertCount(1, $indexes);
            $this->assertEquals('trigram_name', $indexes[0]['$id']);
            $this->assertEquals(IndexType::Trigram->value, $indexes[0]['type']);
            $this->assertEquals(['name'], $indexes[0]['attributes']);

            // Create another trigram index on description
            $this->assertEquals(true, $database->createIndex($collectionId, Index::trigram(key: 'trigram_description', attributes: ['description'])));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->indexes;
            $this->assertCount(2, $indexes);

            // Test that trigram index can be deleted
            $this->assertEquals(true, $database->deleteIndex($collectionId, 'trigram_name'));
            $this->assertEquals(true, $database->deleteIndex($collectionId, 'trigram_description'));

            $collection = $database->getCollection($collectionId);
            $indexes = $collection->indexes;
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
        $indexes = $collection->indexes;
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
        $indexes2 = $collection2->indexes;
        $this->assertCount(1, $indexes2);
        $ttlIndex2 = $indexes2[0];
        $this->assertEquals('idx_ttl_collection', $ttlIndex2->getId());
        $this->assertEquals(7200, $ttlIndex2->getAttribute('ttl'));

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
    private function indexValidator(array $attributes, array $indexes): IndexValidator
    {
        $adapter = $this->getDatabase()->getAdapter();

        return new IndexValidator(
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
                $database->createCollection(new Collection(id: 'index_length', attributes: $attributes, indexes: $indexes, permissions: [
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
                $database->createCollection(new Collection(id: 'index_length', attributes: $attributes, indexes: $indexes));
                $this->fail('Failed to throw exception');
            } catch (Exception $e) {
                $this->assertSame($errorMessage, $e->getMessage());
            }
        }

        $attributes[] = Attribute::integer(key: 'integer', size: 10000);

        $indexes = [
            Index::fullText(key: 'index1', attributes: ['title1', 'integer']),
        ];

        $newIndex = Index::fullText(key: 'newIndex1', attributes: ['title1', 'integer']);

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
            $database->createCollection(new Collection(id: 'index_length', attributes: $attributes, indexes: $indexes));
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
            $database->createCollection(new Collection(id: ID::unique(), attributes: $attributes, indexes: $indexes));
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
            $database->createCollection(new Collection(id: ID::unique(), attributes: $attributes, indexes: $indexes));
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

        $collection = $database->createCollection(new Collection(id: 'sequenceIndexes', attributes: [
            Attribute::string(key: 'username', size: 128),
            Attribute::string(key: 'email', size: 128),
        ], indexes: [
            Index::key(key: '_index 123', attributes: ['username', '$sequence'], orders: [Order::Asc, Order::Desc]),
            Index::unique(key: '_index 456', attributes: ['email', '$sequence'], orders: [Order::Asc, Order::Desc]),
        ]));

        $indexes = $collection->indexes;
        $this->assertCount(2, $indexes);
        $this->assertSame('_index 123', $indexes[0]->getId());
        $this->assertSame(['username', '$sequence'], $indexes[0]->attributes);
        $this->assertSame('_index 456', $indexes[1]->getId());
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

        $database->createCollection(new Collection(id: __FUNCTION__));

        $this->assertTrue($database->createAttribute(__FUNCTION__, Attribute::string(key: 'username', size: 128)));
        $this->assertTrue($database->createAttribute(__FUNCTION__, Attribute::string(key: 'email', size: 128)));

        $this->assertTrue($database->createIndex(__FUNCTION__, Index::key(key: '_index 123', attributes: ['username', '$sequence'], orders: [Order::Asc, Order::Desc])));
        $this->assertTrue($database->createIndex(__FUNCTION__, Index::unique(key: '_index 456', attributes: ['email', '$sequence'], orders: [Order::Asc, Order::Desc])));

        $indexes = $database->getCollection(__FUNCTION__)->indexes;
        $this->assertCount(2, $indexes);
        $this->assertSame('_index 123', $indexes[0]->getId());
        $this->assertSame(['username', '$sequence'], $indexes[0]->attributes);
        $this->assertSame('_index 456', $indexes[1]->getId());
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
        $index = Index::key(key: 'tagsfirst', attributes: ['tags', 'status', 'name'], lengths: [255, null, 16], orders: [null, null, Order::Desc]);

        $tenant = $database->getSharedTables() ? ['_tenant'] : [];
        $expected = match (true) {
            $adapter instanceof Postgres => [...$tenant, 'tags', 'status', 'name DESC'],
            $adapter->supports(Capability::CastIndexArray) => [...$tenant, '', 'status', 'name(16)'],
            default => [...$tenant, 'tags(255)', 'status', 'name(16)'],
        };

        $database->createCollection(new Collection(id: 'index_array_position_created', attributes: $attributes, indexes: [$index]));
        try {
            $this->assertSame($expected, $this->getIndexKeyParts($database, 'index_array_position_created', 'tagsfirst'));
        } finally {
            $database->deleteCollection('index_array_position_created');
        }

        $database->createCollection(new Collection(id: 'index_array_position_added'));
        try {
            $this->assertTrue($database->createAttributes('index_array_position_added', $attributes));
            $this->assertTrue($database->createIndex('index_array_position_added', $index));
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
        $database->createCollection(new Collection(id: $collection));

        try {
            $this->assertTrue($database->createAttribute($collection, Attribute::object(key: 'data')));
            $this->assertTrue($database->createAttribute($collection, Attribute::string(key: 'status', size: 32)));
            $this->assertTrue($database->createIndex($collection, Index::key(key: 'countryfirst', attributes: ['data.country', 'status'], orders: [Order::Desc, null])));

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

        $database->createCollection(new Collection(id: 'indexLimit'));

        for ($i = 0; $i < 64; $i++) {
            $this->assertTrue($database->createAttribute('indexLimit', Attribute::string(key: "test{$i}", size: 16, required: true)));
        }

        for ($i = 0; $i < $database->getLimitForIndexes(); $i++) {
            $this->assertTrue($database->createIndex('indexLimit', Index::key(key: "index{$i}", attributes: ["test{$i}"], lengths: [16])));
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
            $database->createCollection(new Collection(id: $collectionId));

            $database->createAttribute($collectionId, Attribute::string(key: 'name', size: 256));
            $database->createAttribute($collectionId, Attribute::integer(key: 'age', size: 8));

            $database->createIndex($collectionId, Index::key(key: 'index1', attributes: ['name', 'age'], orders: [Order::Asc, Order::Desc]));

            $supportsIdenticalIndexes = $database->getAdapter()->supports(Capability::IdenticalIndexes);

            try {
                $database->createIndex($collectionId, Index::key(key: 'index2', attributes: ['name', 'age'], orders: [Order::Asc, Order::Desc]));
                $this->assertTrue($supportsIdenticalIndexes, 'An identical index must be rejected when the adapter does not support identical indexes');
            } catch (Throwable $e) {
                $this->assertFalse($supportsIdenticalIndexes, 'Unexpected exception when creating identical index: '.$e->getMessage());
                $this->assertSame('There is already an index with the same attributes and orders', $e->getMessage());
            }

            try {
                $database->createIndex($collectionId, Index::key(key: 'index3', attributes: ['age', 'name'], orders: [Order::Asc, Order::Desc]));
            } catch (Throwable $e) {
                $this->assertFalse($supportsIdenticalIndexes, 'Unexpected exception when creating index with a different attribute order: '.$e->getMessage());
            }

            try {
                $database->createIndex($collectionId, Index::key(key: 'index4', attributes: ['age', 'name'], orders: [Order::Desc, Order::Asc]));
            } catch (Throwable $e) {
                $this->assertFalse($supportsIdenticalIndexes, 'Unexpected exception when creating index with different orders: '.$e->getMessage());
            }

            $this->assertTrue($database->createIndex($collectionId, Index::key(key: 'index5', attributes: ['name'], orders: [Order::Asc])));
            $this->assertTrue($database->createIndex($collectionId, Index::key(key: 'index6', attributes: ['name', 'age'], orders: [Order::Asc])));
        } finally {
            $database->deleteCollection($collectionId);
        }
    }

    public function testMaxQueriesValues(): void
    {
        $database = $this->getDatabase();
        $collection = 'maxQueryValues_'.uniqid();

        $database->createCollection(new Collection(id: $collection));

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
            $database->createCollection(new Collection(id: $collectionId));

            $database->createAttribute($collectionId, Attribute::string(key: 'title', size: 256));
            $database->createAttribute($collectionId, Attribute::string(key: 'content', size: 256));
            $database->createIndex($collectionId, Index::fullText(key: 'fulltext_title', attributes: ['title']));

            $supportsMultipleFulltext = $database->getAdapter()->supports(Capability::MultipleFulltextIndexes);

            try {
                $database->createIndex($collectionId, Index::fullText(key: 'fulltext_content', attributes: ['content']));
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
        $database->createCollection(new Collection(id: $collection));

        $database->createAttribute($collection, Attribute::datetime(key: 'expiresAt'));
        $database->createAttribute($collection, Attribute::datetime(key: 'deletedAt'));

        $this->assertTrue($database->createIndex($collection, Index::ttl(key: 'idx_ttl_expires', attributes: ['expiresAt'], orders: [Order::Asc], ttl: 3600)));

        foreach ([
            Index::ttl(key: 'idx_ttl_expires_duplicate', attributes: ['expiresAt'], orders: [Order::Asc], ttl: 7200),
            Index::ttl(key: 'idx_ttl_deleted', attributes: ['deletedAt'], orders: [Order::Asc], ttl: 86400),
        ] as $duplicate) {
            try {
                $database->createIndex($collection, $duplicate);
                $this->fail('Expected exception for creating a second TTL index in a collection');
            } catch (Exception $e) {
                $this->assertInstanceOf(DatabaseException::class, $e);
                $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
            }
        }

        $indexes = $database->getCollection($collection)->indexes;
        $this->assertCount(1, $indexes);

        $indexIds = array_map(fn (Index $index) => $index->getId(), $indexes);
        $this->assertContains('idx_ttl_expires', $indexIds);
        $this->assertNotContains('idx_ttl_deleted', $indexIds);

        $this->assertTrue($database->deleteIndex($collection, 'idx_ttl_expires'));

        $this->assertTrue($database->createIndex($collection, Index::ttl(key: 'idx_ttl_deleted', attributes: ['deletedAt'], orders: [Order::Asc], ttl: 1800)));

        $indexes = $database->getCollection($collection)->indexes;
        $this->assertCount(1, $indexes);

        $indexIds = array_map(fn (Index $index) => $index->getId(), $indexes);
        $this->assertNotContains('idx_ttl_expires', $indexIds);
        $this->assertContains('idx_ttl_deleted', $indexIds);

        try {
            $database->createCollection(new Collection(id: uniqid('sl_ttl_dup_collection'), attributes: [
                Attribute::datetime(key: 'expiresAt', signed: false),
            ], indexes: [
                Index::ttl(key: 'idx_ttl_1', attributes: ['expiresAt'], orders: [Order::Asc], ttl: 3600),
                Index::ttl(key: 'idx_ttl_2', attributes: ['expiresAt'], orders: [Order::Asc], ttl: 7200),
            ]));
            $this->fail('Expected exception for duplicate TTL indexes in createCollection');
        } catch (Exception $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
        }

        $database->deleteCollection($collection);
    }
}
