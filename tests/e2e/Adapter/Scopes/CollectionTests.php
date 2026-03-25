<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use PHPUnit\Framework\Attributes\Depends;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\QueryTransform;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;

trait CollectionTests
{
    private static string $createdAtCollection = '';

    protected function getCreatedAtCollection(): string
    {
        if (self::$createdAtCollection === '') {
            self::$createdAtCollection = 'created_at_' . uniqid();
        }
        return self::$createdAtCollection;
    }

    public function testCreateExistsDelete(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Schemas)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->assertEquals(true, $database->exists($this->testDatabase));
        $this->assertEquals(true, $database->delete($this->testDatabase));
        $this->assertEquals(false, $database->exists($this->testDatabase));
        $this->assertEquals(true, $database->create());
    }

    public function testCreateListExistsDeleteCollection(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        // Clean up any leftover collections from prior runs
        foreach ($database->listCollections(100) as $col) {
            try {
                $database->deleteCollection($col->getId());
            } catch (\Throwable) {
                // ignore
            }
        }

        $this->assertInstanceOf('Utopia\Database\Document', $database->createCollection('actors', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $this->assertCount(1, $database->listCollections());
        $this->assertEquals(true, $database->exists($this->testDatabase, 'actors'));

        // Collection names should not be unique
        $this->assertInstanceOf('Utopia\Database\Document', $database->createCollection('actors2', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $this->assertCount(2, $database->listCollections());
        $this->assertEquals(true, $database->exists($this->testDatabase, 'actors2'));
        $collection = $database->getCollection('actors2');
        $collection->setAttribute('name', 'actors'); // change name to one that exists
        $this->assertInstanceOf('Utopia\Database\Document', $database->updateDocument(
            $collection->getCollection(),
            $collection->getId(),
            $collection
        ));
        $this->assertEquals(true, $database->deleteCollection('actors2')); // Delete collection when finished
        $this->assertCount(1, $database->listCollections());

        $this->assertEquals(false, $database->getCollection('actors')->isEmpty());
        $this->assertEquals(true, $database->deleteCollection('actors'));
        $this->assertEquals(true, $database->getCollection('actors')->isEmpty());
        $this->assertEquals(false, $database->exists($this->testDatabase, 'actors'));
    }

    public function testCreateCollectionWithSchema(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $attributes = [
            new Attribute(key: 'attribute1', type: ColumnType::String, size: 256, required: false, signed: true, array: false, filters: []),
            new Attribute(key: 'attribute2', type: ColumnType::Integer, size: 0, required: false, signed: true, array: false, filters: []),
            new Attribute(key: 'attribute3', type: ColumnType::Boolean, size: 0, required: false, signed: true, array: false, filters: []),
            new Attribute(key: 'attribute4', type: ColumnType::Id, size: 0, required: false, signed: false, array: false, filters: []),
        ];

        $indexes = [
            new Index(key: 'index1', type: IndexType::Key, attributes: ['attribute1'], lengths: [256], orders: ['ASC']),
            new Index(key: 'index2', type: IndexType::Key, attributes: ['attribute2'], lengths: [], orders: ['DESC']),
            new Index(key: 'index3', type: IndexType::Key, attributes: ['attribute3', 'attribute2'], lengths: [], orders: ['DESC', 'ASC']),
            new Index(key: 'index4', type: IndexType::Key, attributes: ['attribute4'], lengths: [], orders: ['DESC']),
        ];

        $collection = $database->createCollection('withSchema', $attributes, $indexes);

        $this->assertEquals(false, $collection->isEmpty());
        $this->assertEquals('withSchema', $collection->getId());

        $this->assertIsArray($collection->getAttribute('attributes'));
        $this->assertCount(4, $collection->getAttribute('attributes'));
        $this->assertEquals('attribute1', $collection->getAttribute('attributes')[0]['$id']);
        $this->assertEquals(ColumnType::String->value, $collection->getAttribute('attributes')[0]['type']);
        $this->assertEquals('attribute2', $collection->getAttribute('attributes')[1]['$id']);
        $this->assertEquals(ColumnType::Integer->value, $collection->getAttribute('attributes')[1]['type']);
        $this->assertEquals('attribute3', $collection->getAttribute('attributes')[2]['$id']);
        $this->assertEquals(ColumnType::Boolean->value, $collection->getAttribute('attributes')[2]['type']);
        $this->assertEquals('attribute4', $collection->getAttribute('attributes')[3]['$id']);
        $this->assertEquals(ColumnType::Id->value, $collection->getAttribute('attributes')[3]['type']);

        $this->assertIsArray($collection->getAttribute('indexes'));
        $this->assertCount(4, $collection->getAttribute('indexes'));
        $this->assertEquals('index1', $collection->getAttribute('indexes')[0]['$id']);
        $this->assertEquals(IndexType::Key->value, $collection->getAttribute('indexes')[0]['type']);
        $this->assertEquals('index2', $collection->getAttribute('indexes')[1]['$id']);
        $this->assertEquals(IndexType::Key->value, $collection->getAttribute('indexes')[1]['type']);
        $this->assertEquals('index3', $collection->getAttribute('indexes')[2]['$id']);
        $this->assertEquals(IndexType::Key->value, $collection->getAttribute('indexes')[2]['type']);
        $this->assertEquals('index4', $collection->getAttribute('indexes')[3]['$id']);
        $this->assertEquals(IndexType::Key->value, $collection->getAttribute('indexes')[3]['type']);

        $database->deleteCollection('withSchema');

        // Test collection with dash (+attribute +index)
        $collection2 = $database->createCollection('with-dash', [
            new Attribute(key: 'attribute-one', type: ColumnType::String, size: 256, required: false, signed: true, array: false, filters: []),
        ], [
            new Index(key: 'index-one', type: IndexType::Key, attributes: ['attribute-one'], lengths: [256], orders: ['ASC']),
        ]);

        $this->assertEquals(false, $collection2->isEmpty());
        $this->assertEquals('with-dash', $collection2->getId());
        $this->assertIsArray($collection2->getAttribute('attributes'));
        $this->assertCount(1, $collection2->getAttribute('attributes'));
        $this->assertEquals('attribute-one', $collection2->getAttribute('attributes')[0]['$id']);
        $this->assertEquals(ColumnType::String->value, $collection2->getAttribute('attributes')[0]['type']);
        $this->assertIsArray($collection2->getAttribute('indexes'));
        $this->assertCount(1, $collection2->getAttribute('indexes'));
        $this->assertEquals('index-one', $collection2->getAttribute('indexes')[0]['$id']);
        $this->assertEquals(IndexType::Key->value, $collection2->getAttribute('indexes')[0]['type']);
        $database->deleteCollection('with-dash');
    }

    public function testSizeCollection(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('sizeTest1');
        $database->createCollection('sizeTest2');

        $size1 = $database->getSizeOfCollection('sizeTest1');
        $size2 = $database->getSizeOfCollection('sizeTest2');
        $sizeDifference = abs($size1 - $size2);
        // Size of an empty collection returns either 172032 or 167936 bytes randomly
        // Therefore asserting with a tolerance of 5000 bytes
        $byteDifference = 5000;

        if (! $database->analyzeCollection('sizeTest2')) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->assertLessThan($byteDifference, $sizeDifference);

        $database->createAttribute('sizeTest2', new Attribute(key: 'string1', type: ColumnType::String, size: 20000, required: true));
        $database->createAttribute('sizeTest2', new Attribute(key: 'string2', type: ColumnType::String, size: 254 + 1, required: true));
        $database->createAttribute('sizeTest2', new Attribute(key: 'string3', type: ColumnType::String, size: 254 + 1, required: true));
        $database->createIndex('sizeTest2', new Index(key: 'index', type: IndexType::Key, attributes: ['string1', 'string2', 'string3'], lengths: [128, 128, 128]));

        $loopCount = 100;

        for ($i = 0; $i < $loopCount; $i++) {
            $database->createDocument('sizeTest2', new Document([
                '$id' => 'doc'.$i,
                'string1' => 'string1'.$i.str_repeat('A', 10000),
                'string2' => 'string2',
                'string3' => 'string3',
            ]));
        }

        $database->analyzeCollection('sizeTest2');

        $size2 = $this->getDatabase()->getSizeOfCollection('sizeTest2');

        $this->assertGreaterThan($size1, $size2);

        $this->getDatabase()->getAuthorization()->skip(function () use ($loopCount) {
            for ($i = 0; $i < $loopCount; $i++) {
                $this->getDatabase()->deleteDocument('sizeTest2', 'doc'.$i);
            }
        });

        sleep(5);

        $database->analyzeCollection('sizeTest2');

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

        $this->getDatabase()->createAttribute('sizeTestDisk2', new Attribute(key: 'string1', type: ColumnType::String, size: 20000, required: true));
        $this->getDatabase()->createAttribute('sizeTestDisk2', new Attribute(key: 'string2', type: ColumnType::String, size: 254 + 1, required: true));
        $this->getDatabase()->createAttribute('sizeTestDisk2', new Attribute(key: 'string3', type: ColumnType::String, size: 254 + 1, required: true));
        $this->getDatabase()->createIndex('sizeTestDisk2', new Index(key: 'index', type: IndexType::Key, attributes: ['string1', 'string2', 'string3'], lengths: [128, 128, 128]));

        $loopCount = 40;

        for ($i = 0; $i < $loopCount; $i++) {
            $this->getDatabase()->createDocument('sizeTestDisk2', new Document([
                'string1' => 'string1'.$i,
                'string2' => 'string2'.$i,
                'string3' => 'string3'.$i,
            ]));
        }

        $size2 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk2');

        $this->assertGreaterThan($size1, $size2);
    }

    public function testSizeFullText(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        // SQLite does not support fulltext indexes
        if (! $database->getAdapter()->supports(Capability::Fulltext)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('fullTextSizeTest');

        $size1 = $database->getSizeOfCollection('fullTextSizeTest');

        $database->createAttribute('fullTextSizeTest', new Attribute(key: 'string1', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute('fullTextSizeTest', new Attribute(key: 'string2', type: ColumnType::String, size: 254, required: true));
        $database->createAttribute('fullTextSizeTest', new Attribute(key: 'string3', type: ColumnType::String, size: 254, required: true));
        $database->createIndex('fullTextSizeTest', new Index(key: 'index', type: IndexType::Key, attributes: ['string1', 'string2', 'string3'], lengths: [128, 128, 128]));

        $loopCount = 10;

        for ($i = 0; $i < $loopCount; $i++) {
            $database->createDocument('fullTextSizeTest', new Document([
                'string1' => 'string1'.$i,
                'string2' => 'string2'.$i,
                'string3' => 'string3'.$i,
            ]));
        }

        $size2 = $database->getSizeOfCollectionOnDisk('fullTextSizeTest');

        $this->assertGreaterThan($size1, $size2);

        $database->createIndex('fullTextSizeTest', new Index(key: 'fulltext_index', type: IndexType::Fulltext, attributes: ['string1']));

        $size3 = $database->getSizeOfCollectionOnDisk('fullTextSizeTest');

        $this->assertGreaterThan($size2, $size3);
    }

    public function testSchemaAttributes(): void
    {
        if (! $this->getDatabase()->getAdapter()->supports(Capability::SchemaAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'schema_attributes';
        $db = $this->getDatabase();

        $this->assertEmpty($db->getSchemaAttributes('no_such_collection'));

        $db->createCollection($collection);

        $db->createAttribute($collection, new Attribute(key: 'username', type: ColumnType::String, size: 128, required: true));
        $db->createAttribute($collection, new Attribute(key: 'story', type: ColumnType::String, size: 20000, required: true));
        $db->createAttribute($collection, new Attribute(key: 'string_list', type: ColumnType::String, size: 128, required: true, default: null, signed: true, array: true));
        $db->createAttribute($collection, new Attribute(key: 'dob', type: ColumnType::Datetime, size: 0, required: false, default: '2000-06-12T14:12:55.000+00:00', signed: true, array: false, format: null, formatOptions: [], filters: ['datetime']));

        $attributes = [];
        foreach ($db->getSchemaAttributes($collection) as $attribute) {
            /**
             * @var Document $attribute
             */
            $attributes[$attribute->getId()] = $attribute;
        }

        $attribute = $attributes['username'];
        $this->assertEquals('username', $attribute['$id']);
        $this->assertEquals('varchar', $attribute['dataType']);
        $this->assertEquals('varchar(128)', $attribute['columnType']);
        $this->assertEquals('128', $attribute['characterMaximumLength']);
        $this->assertEquals('YES', $attribute['isNullable']);

        $attribute = $attributes['story'];
        $this->assertEquals('story', $attribute['$id']);
        $this->assertEquals('text', $attribute['dataType']);
        $this->assertEquals('text', $attribute['columnType']);
        $this->assertEquals('65535', $attribute['characterMaximumLength']);

        $attribute = $attributes['string_list'];
        $this->assertEquals('string_list', $attribute['$id']);
        $this->assertTrue(in_array($attribute['dataType'], ['json', 'longtext'])); // mysql vs maria
        $this->assertTrue(in_array($attribute['columnType'], ['json', 'longtext']));
        $this->assertTrue(in_array($attribute['characterMaximumLength'], [null, '4294967295']));
        $this->assertEquals('YES', $attribute['isNullable']);

        $attribute = $attributes['dob'];
        $this->assertEquals('dob', $attribute['$id']);
        $this->assertEquals('datetime', $attribute['dataType']);
        $this->assertEquals('datetime(3)', $attribute['columnType']);
        $this->assertEquals(null, $attribute['characterMaximumLength']);
        $this->assertEquals('3', $attribute['datetimePrecision']);

        if ($db->getSharedTables()) {
            $attribute = $attributes['_tenant'];
            $this->assertEquals('_tenant', $attribute['$id']);
            $this->assertEquals('int', $attribute['dataType']);
            $this->assertEquals('10', $attribute['numericPrecision']);
            $this->assertTrue(in_array($attribute['columnType'], ['int unsigned', 'int(11) unsigned']));
        }
    }

    public function testCreateCollectionWithSchemaIndexes(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $attributes = [
            new Attribute(key: 'username', type: ColumnType::String, size: 100, required: false, signed: true, array: false),
            new Attribute(key: 'cards', type: ColumnType::String, size: 5000, required: false, signed: true, array: true),
        ];

        $indexes = [
            new Index(key: 'idx_username', type: IndexType::Key, attributes: ['username'], lengths: [100], orders: []),
            new Index(key: 'idx_username_uid', type: IndexType::Key, attributes: ['username', '$id'], lengths: [99, 200], orders: [OrderDirection::Desc->value]),
        ];

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            $indexes[] = new Index(key: 'idx_cards', type: IndexType::Key, attributes: ['cards'], lengths: [500], orders: [OrderDirection::Desc->value]);
        }

        $collection = $database->createCollection(
            'collection98',
            $attributes,
            $indexes,
            permissions: [
                Permission::create(Role::any()),
            ]
        );

        $this->assertEquals($collection->getAttribute('indexes')[0]['attributes'][0], 'username');
        $this->assertEquals($collection->getAttribute('indexes')[0]['lengths'][0], null);

        $this->assertEquals($collection->getAttribute('indexes')[1]['attributes'][0], 'username');
        $this->assertEquals($collection->getAttribute('indexes')[1]['lengths'][0], 99);
        $this->assertEquals($collection->getAttribute('indexes')[1]['orders'][0], OrderDirection::Desc->value);

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            $this->assertEquals($collection->getAttribute('indexes')[2]['attributes'][0], 'cards');
            $this->assertEquals($collection->getAttribute('indexes')[2]['lengths'][0], Database::MAX_ARRAY_INDEX_LENGTH);
            $this->assertEquals($collection->getAttribute('indexes')[2]['orders'][0], null);
        }
    }

    public function testGetCollectionId(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::ConnectionId)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->assertIsString($database->getConnectionId());
    }

    public function testKeywords(): void
    {
        $database = $this->getDatabase();
        $keywords = $database->getKeywords();

        // Collection name tests
        $attributes = [
            new Attribute(key: 'attribute1', type: ColumnType::String, size: 256, required: false, signed: true, array: false, filters: []),
        ];

        $indexes = [
            new Index(key: 'index1', type: IndexType::Key, attributes: ['attribute1'], lengths: [256], orders: ['ASC']),
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
            $collectionName = 'rk'.$keyword; // rk is shorthand for reserved-keyword. We do this since there are some limits (64 chars max)

            $collection = $database->createCollection($collectionName);
            $this->assertEquals($collectionName, $collection->getId());

            $attribute = $database->createAttribute($collectionName, new Attribute(key: $keyword, type: ColumnType::String, size: 128, required: true));
            $this->assertEquals(true, $attribute);

            $document = new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                '$id' => 'reservedKeyDocument',
            ]);
            $document->setAttribute($keyword, 'Reserved:'.$keyword);

            $document = $database->createDocument($collectionName, $document);
            $this->assertEquals('reservedKeyDocument', $document->getId());
            $this->assertEquals('Reserved:'.$keyword, $document->getAttribute($keyword));

            $document = $database->getDocument($collectionName, 'reservedKeyDocument');
            $this->assertEquals('reservedKeyDocument', $document->getId());
            $this->assertEquals('Reserved:'.$keyword, $document->getAttribute($keyword));

            $documents = $database->find($collectionName);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());
            $this->assertEquals('Reserved:'.$keyword, $documents[0]->getAttribute($keyword));

            $documents = $database->find($collectionName, [Query::equal($keyword, ["Reserved:{$keyword}"])]);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());

            $documents = $database->find($collectionName, [
                Query::orderDesc($keyword),
            ]);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());

            $collection = $database->deleteCollection($collectionName);
            $this->assertTrue($collection);
        }
    }

    public function testDeleteCollectionDeletesRelationships(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create 'testers' collection if not already created (was created by testMetadata in sequential mode)
        if ($database->getCollection('testers')->isEmpty()) {
            $database->createCollection('testers');
        }

        $database->createCollection('devices');

        $database->createRelationship(new Relationship(collection: 'testers', relatedCollection: 'devices', type: RelationType::OneToMany, twoWay: true, twoWayKey: 'tester'));

        $testers = $database->getCollection('testers');
        $devices = $database->getCollection('devices');

        $this->assertEquals(1, \count($testers->getAttribute('attributes')));
        $this->assertEquals(1, \count($devices->getAttribute('attributes')));
        $this->assertEquals(1, \count($devices->getAttribute('indexes')));

        $database->deleteCollection('testers');

        $testers = $database->getCollection('testers');
        $devices = $database->getCollection('devices');

        $this->assertEquals(true, $testers->isEmpty());
        $this->assertEquals(0, \count($devices->getAttribute('attributes')));
        $this->assertEquals(0, \count($devices->getAttribute('indexes')));
    }

    public function testCascadeMultiDelete(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('cascadeMultiDelete1');
        $database->createCollection('cascadeMultiDelete2');
        $database->createCollection('cascadeMultiDelete3');

        $database->createRelationship(new Relationship(collection: 'cascadeMultiDelete1', relatedCollection: 'cascadeMultiDelete2', type: RelationType::OneToMany, twoWay: true, onDelete: ForeignKeyAction::Cascade));

        $database->createRelationship(new Relationship(collection: 'cascadeMultiDelete2', relatedCollection: 'cascadeMultiDelete3', type: RelationType::OneToMany, twoWay: true, onDelete: ForeignKeyAction::Cascade));

        $root = $database->createDocument('cascadeMultiDelete1', new Document([
            '$id' => 'cascadeMultiDelete1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'cascadeMultiDelete2' => [
                [
                    '$id' => 'cascadeMultiDelete2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'cascadeMultiDelete3' => [
                        [
                            '$id' => 'cascadeMultiDelete3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                                Permission::delete(Role::any()),
                            ],
                        ],
                    ],
                ],
            ],
        ]));

        $this->assertCount(1, $root->getAttribute('cascadeMultiDelete2'));
        $this->assertCount(1, $root->getAttribute('cascadeMultiDelete2')[0]->getAttribute('cascadeMultiDelete3'));

        $this->assertEquals(true, $database->deleteDocument('cascadeMultiDelete1', $root->getId()));

        $multi2 = $database->getDocument('cascadeMultiDelete2', 'cascadeMultiDelete2');
        $this->assertEquals(true, $multi2->isEmpty());

        $multi3 = $database->getDocument('cascadeMultiDelete3', 'cascadeMultiDelete3');
        $this->assertEquals(true, $multi3->isEmpty());
    }

    /**
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws QueryException
     * @throws StructureException
     * @throws TimeoutException
     */
    public function testSharedTables(): void
    {
        /**
         * Default mode already tested, we'll test 'schema' and 'table' isolation here
         */
        /** @var Database $database */
        $database = $this->getDatabase();
        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();
        $tenant = $database->getTenant();

        if (! $database->getAdapter()->supports(Capability::Schemas)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $token = static::getTestToken();
        $schema1 = 'schema1_'.$token;
        $schema2 = 'schema2_'.$token;
        $sharedTablesDb = 'sharedTables_'.$token;

        if ($database->exists($schema1)) {
            $database->setDatabase($schema1)->delete();
        }
        if ($database->exists($schema2)) {
            $database->setDatabase($schema2)->delete();
        }
        if ($database->exists($sharedTablesDb)) {
            $database->setDatabase($sharedTablesDb)->delete();
        }

        /**
         * Schema
         */
        $database
            ->setDatabase($schema1)
            ->setNamespace('')
            ->create();

        $this->assertEquals(true, $database->exists($schema1));

        $database
            ->setDatabase($schema2)
            ->setNamespace('')
            ->create();

        $this->assertEquals(true, $database->exists($schema2));

        /**
         * Table
         */
        $tenant1 = 1;
        $tenant2 = 2;

        $database
            ->setDatabase($sharedTablesDb)
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant($tenant1)
            ->create();

        $this->assertEquals(true, $database->exists($sharedTablesDb));

        $database->createCollection('people', [
            new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true),
            new Attribute(key: 'lifeStory', type: ColumnType::String, size: 65536, required: true),
        ], [
            new Index(key: 'idx_name', type: IndexType::Key, attributes: ['name']),
        ], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $this->assertCount(1, $database->listCollections());

        if ($database->getAdapter()->supports(Capability::Fulltext)) {
            $database->createIndex('people', new Index(key: 'idx_lifeStory', type: IndexType::Fulltext, attributes: ['lifeStory']));
        }

        $docId = ID::unique();

        $database->createDocument('people', new Document([
            '$id' => $docId,
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Spiderman',
            'lifeStory' => 'Spider-Man is a superhero appearing in American comic books published by Marvel Comics.',
        ]));

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals('Spiderman', $doc['name']);
        $this->assertEquals($tenant1, $doc->getTenant());

        /**
         * Remove Permissions
         */
        $doc->setAttribute('$permissions', [
            Permission::read(Role::any()),
        ]);

        $database->updateDocument('people', $docId, $doc);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals([Permission::read(Role::any())], $doc['$permissions']);
        $this->assertEquals($tenant1, $doc->getTenant());

        /**
         * Add Permissions
         */
        $doc->setAttribute('$permissions', [
            Permission::read(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->updateDocument('people', $docId, $doc);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals([Permission::read(Role::any()), Permission::delete(Role::any())], $doc['$permissions']);

        $docs = $database->find('people');
        $this->assertCount(1, $docs);

        // Swap to tenant 2, no access
        $database->setTenant($tenant2);

        try {
            $database->getDocument('people', $docId);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        try {
            $database->find('people');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        $this->assertCount(0, $database->listCollections());

        // Swap back to tenant 1, allowed
        $database->setTenant($tenant1);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals('Spiderman', $doc['name']);
        $docs = $database->find('people');
        $this->assertEquals(1, \count($docs));

        // Remove tenant but leave shared tables enabled
        $database->setTenant(null);

        try {
            $database->getDocument('people', $docId);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        // Reset state
        $database
            ->setSharedTables($sharedTables)
            ->setTenant($tenant)
            ->setNamespace($namespace)
            ->setDatabase($schema);
    }

    /**
     * @throws LimitException
     * @throws DuplicateException
     * @throws DatabaseException
     */
    public function testCreateDuplicates(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('duplicates', permissions: [
            Permission::read(Role::any()),
        ]);

        try {
            $database->createCollection('duplicates');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $this->assertNotEmpty($database->listCollections());

        $database->deleteCollection('duplicates');
    }

    public function testSharedTablesDuplicates(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();
        $tenant = $database->getTenant();

        if (! $database->getAdapter()->supports(Capability::Schemas)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $sharedTablesDb = 'sharedTables_'.static::getTestToken();

        if ($database->exists($sharedTablesDb)) {
            $database->setDatabase($sharedTablesDb)->delete();
        }

        $database
            ->setDatabase($sharedTablesDb)
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant(null)
            ->create();

        // Create collection
        $database->createCollection('duplicates', documentSecurity: false);
        $database->createAttribute('duplicates', new Attribute(key: 'name', type: ColumnType::String, size: 10, required: false));
        $database->createIndex('duplicates', new Index(key: 'nameIndex', type: IndexType::Key, attributes: ['name']));

        $database->setTenant(2);

        try {
            $database->createCollection('duplicates', documentSecurity: false);
        } catch (DuplicateException) {
            // Ignore
        }

        try {
            $database->createAttribute('duplicates', new Attribute(key: 'name', type: ColumnType::String, size: 10, required: false));
        } catch (DuplicateException) {
            // Ignore
        }

        try {
            $database->createIndex('duplicates', new Index(key: 'nameIndex', type: IndexType::Key, attributes: ['name']));
        } catch (DuplicateException) {
            // Ignore
        }

        $collection = $database->getCollection('duplicates');
        $this->assertEquals(1, \count($collection->getAttribute('attributes')));
        $this->assertEquals(1, \count($collection->getAttribute('indexes')));

        $database->setTenant(null);
        $database->purgeCachedCollection('duplicates');

        $collection = $database->getCollection('duplicates');
        $this->assertEquals(1, \count($collection->getAttribute('attributes')));
        $this->assertEquals(1, \count($collection->getAttribute('indexes')));

        $database
            ->setSharedTables($sharedTables)
            ->setTenant($tenant)
            ->setNamespace($namespace)
            ->setDatabase($schema);
    }

    public function testSharedTablesMultiTenantCreateCollection(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();
        $originalTenant = $database->getTenant();
        $createdDb = false;

        if ($sharedTables) {
            // Already in shared-tables mode (SharedTables/* test classes)
        } elseif ($database->getAdapter()->supports(Capability::Schemas)) {
            $dbName = 'stMultiTenant';
            if ($database->exists($dbName)) {
                $database->setDatabase($dbName)->delete();
            }
            $database
                ->setDatabase($dbName)
                ->setNamespace('')
                ->setSharedTables(true)
                ->setTenant(10)
                ->create();
            $createdDb = true;
        } else {
            $this->expectNotToPerformAssertions();

            return;
        }

        try {
            $tenant1 = $database->getAdapter()->getIdAttributeType() === ColumnType::Integer->value ? 10 : 'tenant_10';
            $tenant2 = $database->getAdapter()->getIdAttributeType() === ColumnType::Integer->value ? 20 : 'tenant_20';
            $colName = 'multiTenantCol';

            $database->setTenant($tenant1);

            $database->createCollection($colName, [
                new Document([
                    '$id' => 'name',
                    'type' => ColumnType::String->value,
                    'size' => 128,
                    'required' => true,
                ]),
            ]);

            $col1 = $database->getCollection($colName);
            $this->assertFalse($col1->isEmpty());
            $this->assertEquals(1, \count($col1->getAttribute('attributes')));

            $database->setTenant($tenant2);

            $database->createCollection($colName, [
                new Document([
                    '$id' => 'name',
                    'type' => ColumnType::String->value,
                    'size' => 128,
                    'required' => true,
                ]),
            ]);

            $col2 = $database->getCollection($colName);
            $this->assertFalse($col2->isEmpty());
            $this->assertEquals(1, \count($col2->getAttribute('attributes')));

            $database->setTenant($tenant1);
            $col1Again = $database->getCollection($colName);
            $this->assertFalse($col1Again->isEmpty());

            if ($createdDb) {
                $database->delete();
            } else {
                $database->setTenant($tenant1);
                $database->deleteCollection($colName);
                $database->setTenant($tenant2);
                $database->deleteCollection($colName);
            }
        } finally {
            $database
                ->setSharedTables($sharedTables)
                ->setNamespace($namespace)
                ->setDatabase($schema)
                ->setTenant($originalTenant);
        }
    }

    public function testSharedTablesMultiTenantCreate(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();
        $originalTenant = $database->getTenant();

        try {
            $tenant1 = $database->getAdapter()->getIdAttributeType() === ColumnType::Integer->value ? 100 : 'tenant_100';
            $tenant2 = $database->getAdapter()->getIdAttributeType() === ColumnType::Integer->value ? 200 : 'tenant_200';

            if ($sharedTables) {
                // Already in shared-tables mode; create() should be idempotent.
                // No assertion on exists() since SQLite always returns false for
                // database-level exists. The test verifies create() doesn't throw.
                $database->setTenant($tenant1);
                $database->create();
                $database->setTenant($tenant2);
                $database->create();
                $this->assertTrue(true);
            } elseif ($database->getAdapter()->supports(Capability::Schemas)) {
                $dbName = 'stMultiCreate';
                if ($database->exists($dbName)) {
                    $database->setDatabase($dbName)->delete();
                }
                $database
                    ->setDatabase($dbName)
                    ->setNamespace('')
                    ->setSharedTables(true)
                    ->setTenant($tenant1)
                    ->create();
                $this->assertTrue($database->exists($dbName));
                $database->setTenant($tenant2);
                $database->create();
                $this->assertTrue($database->exists($dbName));
                $database->delete();
            } else {
                $this->expectNotToPerformAssertions();

                return;
            }
        } finally {
            $database
                ->setSharedTables($sharedTables)
                ->setNamespace($namespace)
                ->setDatabase($schema)
                ->setTenant($originalTenant);
        }
    }

    public function testEvents(): void
    {
        $this->getDatabase()->getAuthorization()->skip(function () {
            $database = $this->getDatabase();

            $events = [
                Event::DatabaseCreate,
                Event::DatabaseList,
                Event::CollectionCreate,
                Event::CollectionList,
                Event::CollectionRead,
                Event::DocumentPurge,
                Event::AttributeCreate,
                Event::AttributeUpdate,
                Event::IndexCreate,
                Event::DocumentCreate,
                Event::DocumentPurge,
                Event::DocumentUpdate,
                Event::DocumentRead,
                Event::DocumentFind,
                Event::DocumentFind,
                Event::DocumentCount,
                Event::DocumentSum,
                Event::DocumentPurge,
                Event::DocumentIncrease,
                Event::DocumentPurge,
                Event::DocumentDecrease,
                Event::DocumentsCreate,
                Event::DocumentPurge,
                Event::DocumentPurge,
                Event::DocumentPurge,
                Event::DocumentsUpdate,
                Event::IndexDelete,
                Event::DocumentPurge,
                Event::DocumentDelete,
                Event::DocumentPurge,
                Event::DocumentPurge,
                Event::DocumentsDelete,
                Event::DocumentPurge,
                Event::AttributeDelete,
                Event::CollectionDelete,
                Event::DatabaseDelete,
                Event::DocumentPurge,
                Event::DocumentsDelete,
                Event::DocumentPurge,
                Event::AttributeDelete,
                Event::CollectionDelete,
                Event::DatabaseDelete,
            ];

            $test = $this;
            $database->addLifecycleHook(new class ($events, $test) implements Lifecycle {
                /** @param array<Event> $events */
                public function __construct(private array &$events, private $test)
                {
                }

                public function handle(Event $event, mixed $data): void
                {
                    $shifted = array_shift($this->events);
                    $this->test->assertEquals($shifted, $event);
                }
            });

            if ($this->getDatabase()->getAdapter()->supports(Capability::Schemas)) {
                $database->setDatabase('hellodb');
                $database->create();
            } else {
                \array_shift($events);
            }

            $database->list();

            $database->setDatabase($this->testDatabase);

            $collectionId = ID::unique();
            $database->createCollection($collectionId);
            $database->listCollections();
            $database->getCollection($collectionId);
            $database->createAttribute($collectionId, new Attribute(key: 'attr1', type: ColumnType::Integer, size: 2, required: false));
            $database->updateAttributeRequired($collectionId, 'attr1', true);
            $indexId1 = 'index2_'.uniqid();
            $database->createIndex($collectionId, new Index(key: $indexId1, type: IndexType::Key, attributes: ['attr1']));

            $document = $database->createDocument($collectionId, new Document([
                '$id' => 'doc1',
                'attr1' => 10,
                '$permissions' => [
                    Permission::delete(Role::any()),
                    Permission::update(Role::any()),
                    Permission::read(Role::any()),
                ],
            ]));

            $executed = false;

            $database->silent(function () use ($database, $collectionId, $document) {
                $database->updateDocument($collectionId, 'doc1', $document->setAttribute('attr1', 15));
                $database->getDocument($collectionId, 'doc1');
                $database->find($collectionId);
                $database->findOne($collectionId);
                $database->count($collectionId);
                $database->sum($collectionId, 'attr1');
                $database->increaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
                $database->decreaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
            });

            $this->assertFalse($executed);

            $database->createDocuments($collectionId, [
                new Document([
                    'attr1' => 10,
                ]),
                new Document([
                    'attr1' => 20,
                ]),
            ]);

            $database->updateDocuments($collectionId, new Document([
                'attr1' => 15,
            ]));

            $database->deleteIndex($collectionId, $indexId1);
            $database->deleteDocument($collectionId, 'doc1');

            $database->deleteDocuments($collectionId);
            $database->deleteAttribute($collectionId, 'attr1');
            $database->deleteCollection($collectionId);
            $database->delete('hellodb');
        });
    }

    public function testCreatedAtUpdatedAt(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $this->assertInstanceOf('Utopia\Database\Document', $database->createCollection($this->getCreatedAtCollection()));
        $database->createAttribute($this->getCreatedAtCollection(), new Attribute(key: 'title', type: ColumnType::String, size: 100, required: false));
        $document = $database->createDocument($this->getCreatedAtCollection(), new Document([
            '$id' => ID::custom('uid123'),

            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $this->assertNotEmpty($document->getSequence());
        $this->assertNotNull($document->getSequence());
    }

    #[Depends('testCreatedAtUpdatedAt')]
    public function testCreatedAtUpdatedAtAssert(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->getDocument($this->getCreatedAtCollection(), 'uid123');
        $this->assertEquals(true, ! $document->isEmpty());
        sleep(1);
        $document->setAttribute('title', 'new title');
        $database->updateDocument($this->getCreatedAtCollection(), 'uid123', $document);
        $document = $database->getDocument($this->getCreatedAtCollection(), 'uid123');

        $this->assertGreaterThan($document->getCreatedAt(), $document->getUpdatedAt());
        $this->expectException(DuplicateException::class);

        $database->createCollection($this->getCreatedAtCollection());
    }

    public function testTransformations(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('docs', attributes: [
            new Document([
                '$id' => 'name',
                'type' => ColumnType::String->value,
                'size' => 767,
                'required' => true,
            ]),
        ]);

        $database->createDocument('docs', new Document([
            '$id' => 'doc1',
            'name' => 'value1',
        ]));

        $database->addQueryTransform('test', new class () implements QueryTransform {
            public function transform(Event $event, string $query): string
            {
                return 'SELECT 1';
            }
        });

        $result = $database->getDocument('docs', 'doc1');

        $this->assertTrue($result->isEmpty());

        $database->removeQueryTransform('test');
    }

    public function testSetGlobalCollection(): void
    {
        $db = $this->getDatabase();

        $collectionId = 'globalCollection';

        // set collection as global
        $db->setGlobalCollections([$collectionId]);

        // metadata collection should not contain tenant in the cache key
        [$collectionKey, $documentKey, $hashKey] = $db->getCacheKeys(
            Database::METADATA,
            $collectionId,
            []
        );

        $this->assertNotEmpty($collectionKey);
        $this->assertNotEmpty($documentKey);
        $this->assertNotEmpty($hashKey);

        if ($db->getSharedTables()) {
            $this->assertStringNotContainsString((string) $db->getAdapter()->getTenant(), $collectionKey);
        }

        // non global collection should contain tenant in the cache key
        $nonGlobalCollectionId = 'nonGlobalCollection';
        [$collectionKeyRegular] = $db->getCacheKeys(
            Database::METADATA,
            $nonGlobalCollectionId
        );
        if ($db->getSharedTables()) {
            $this->assertStringContainsString((string) $db->getAdapter()->getTenant(), $collectionKeyRegular);
        }

        // Non metadata collection should contain tenant in the cache key
        [$collectionKey, $documentKey, $hashKey] = $db->getCacheKeys(
            $collectionId,
            ID::unique(),
            []
        );

        $this->assertNotEmpty($collectionKey);
        $this->assertNotEmpty($documentKey);
        $this->assertNotEmpty($hashKey);

        if ($db->getSharedTables()) {
            $this->assertStringContainsString((string) $db->getAdapter()->getTenant(), $collectionKey);
        }

        $db->resetGlobalCollections();
        $this->assertEmpty($db->getGlobalCollections());
    }

    public function testCreateCollectionWithLongId(): void
    {
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = '019a91aa-58cd-708d-a55c-5f7725ef937a';

        $attributes = [
            new Attribute(key: 'name', type: ColumnType::String, size: 256, required: true, array: false),
            new Attribute(key: 'age', type: ColumnType::Integer, size: 0, required: false, array: false),
            new Attribute(key: 'isActive', type: ColumnType::Boolean, size: 0, required: false, array: false),
        ];

        $indexes = [
            new Index(key: 'idx_name', type: IndexType::Key, attributes: ['name'], lengths: [128], orders: ['ASC']),
            new Index(key: 'idx_name_age', type: IndexType::Key, attributes: ['name', 'age'], lengths: [128, null], orders: ['ASC', 'DESC']),
        ];

        $collectionDocument = $database->createCollection(
            $collection,
            $attributes,
            $indexes,
            permissions: [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        );

        $this->assertEquals($collection, $collectionDocument->getId());
        $this->assertCount(3, $collectionDocument->getAttribute('attributes'));
        $this->assertCount(2, $collectionDocument->getAttribute('indexes'));

        $document = $database->createDocument($collection, new Document([
            '$id' => 'longIdDoc',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'LongId Test',
            'age' => 42,
            'isActive' => true,
        ]));

        $this->assertEquals('longIdDoc', $document->getId());
        $this->assertEquals('LongId Test', $document->getAttribute('name'));
        $this->assertEquals(42, $document->getAttribute('age'));
        $this->assertTrue($document->getAttribute('isActive'));

        $found = $database->find($collection, [
            Query::equal('name', ['LongId Test']),
        ]);

        $this->assertCount(1, $found);
        $this->assertEquals('longIdDoc', $found[0]->getId());

        $fetched = $database->getDocument($collection, 'longIdDoc');
        $this->assertEquals('LongId Test', $fetched->getAttribute('name'));

        $this->assertTrue($database->deleteCollection($collection));
    }
}
