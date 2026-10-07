<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Tests\E2E\Adapter\Support\EventRecorder;
use Throwable;
use Utopia\Cache\Adapter\None as NoneCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\Redis;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Storage;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
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

        $database->createCollection(Collection::create(id: 'actors', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $this->assertCount(1, $database->listCollections());
        $this->assertEquals(true, $database->collectionExists('actors', $this->testDatabase));

        // Collection names should not be unique
        $database->createCollection(Collection::create(id: 'actors2', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $this->assertCount(2, $database->listCollections(100));
        $this->assertEquals(true, $database->collectionExists('actors2', $this->testDatabase));
        $collection = $database->getCollection('actors2');
        $collection->setAttribute('name', 'actors'); // change name to one that exists
        $updated = $database->updateDocument(
            $collection->getCollection(),
            $collection->getId(),
            $collection
        );
        $this->assertSame('actors', $updated->getAttribute('name'));
        $database->deleteCollection('actors2'); // Delete collection when finished
        $this->assertCount(1, $database->listCollections());

        $this->assertNotNull($database->findCollection('actors'));
        $database->deleteCollection('actors');
        $this->assertNull($database->findCollection('actors'));
        $this->assertEquals(false, $database->collectionExists('actors', $this->testDatabase));
    }

    public function testDatabaseHostname(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $host = $database->getHostname();
        if ($host === null) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $adapter = $database->getAdapter();
        if ($adapter->hasFeature(SQLite::class) || $adapter->hasFeature(Redis::class)) {
            $this->assertSame('', $host, 'An engine reached without a network host names none');
            return;
        }

        $this->assertContains($host, ['mysql', 'mariadb', 'postgres', 'mongo']);
    }

    public function testCreateCollectionWithSchema(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $attributes = [
            Attribute::string(key: 'attribute1', size: 256),
            Attribute::integer(key: 'attribute2'),
            Attribute::boolean(key: 'attribute3'),
            Attribute::id(key: 'attribute4'),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['attribute1'], lengths: [256], orders: [OrderDirection::Asc]),
            Index::key(key: 'index2', attributes: ['attribute2'], orders: [OrderDirection::Desc]),
            Index::key(key: 'index3', attributes: ['attribute3', 'attribute2'], orders: [OrderDirection::Desc, OrderDirection::Asc]),
            Index::key(key: 'index4', attributes: ['attribute4'], orders: [OrderDirection::Desc]),
        ];

        $collection = $database->createCollection(Collection::create(id: 'withSchema', attributes: $attributes, indexes: $indexes));

        $this->assertSame('withSchema', $collection->getId());

        $this->assertCount(4, $collection->attributes());
        $this->assertSame('attribute1', $collection->attributes()[0]->key);
        $this->assertSame(ColumnType::String, $collection->attributes()[0]->type);
        $this->assertSame('attribute2', $collection->attributes()[1]->key);
        $this->assertSame(ColumnType::Integer, $collection->attributes()[1]->type);
        $this->assertSame('attribute3', $collection->attributes()[2]->key);
        $this->assertSame(ColumnType::Boolean, $collection->attributes()[2]->type);
        $this->assertSame('attribute4', $collection->attributes()[3]->key);
        $this->assertSame(ColumnType::Id, $collection->attributes()[3]->type);

        $this->assertCount(4, $collection->indexes());
        $this->assertSame('index1', $collection->indexes()[0]->key);
        $this->assertSame(IndexType::Key, $collection->indexes()[0]->type);
        $this->assertSame('index2', $collection->indexes()[1]->key);
        $this->assertSame(IndexType::Key, $collection->indexes()[1]->type);
        $this->assertSame('index3', $collection->indexes()[2]->key);
        $this->assertSame(IndexType::Key, $collection->indexes()[2]->type);
        $this->assertSame('index4', $collection->indexes()[3]->key);
        $this->assertSame(IndexType::Key, $collection->indexes()[3]->type);

        $fetched = $database->getCollection('withSchema');
        $this->assertSame('attribute1', $fetched->attributes()[0]->key);

        $database->deleteCollection('withSchema');

        $collection2 = $database->createCollection(Collection::create(id: 'with-dash', attributes: [
            Attribute::string(key: 'attribute-one', size: 256),
        ], indexes: [
            Index::key(key: 'index-one', attributes: ['attribute-one'], lengths: [256], orders: [OrderDirection::Asc]),
        ]));

        $this->assertSame('with-dash', $collection2->getId());
        $this->assertCount(1, $collection2->attributes());
        $this->assertSame('attribute-one', $collection2->attributes()[0]->key);
        $this->assertSame(ColumnType::String, $collection2->attributes()[0]->type);
        $this->assertCount(1, $collection2->indexes());
        $this->assertSame('index-one', $collection2->indexes()[0]->key);
        $this->assertSame(IndexType::Key, $collection2->indexes()[0]->type);
        $database->deleteCollection('with-dash');
    }

    public function testSizeCollection(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: 'sizeTest1'));
        $database->createCollection(Collection::create(id: 'sizeTest2'));

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

        $database->createAttribute('sizeTest2', Attribute::string(key: 'string1', size: 20000, required: true));
        $database->createAttribute('sizeTest2', Attribute::string(key: 'string2', size: 254 + 1, required: true));
        $database->createAttribute('sizeTest2', Attribute::string(key: 'string3', size: 254 + 1, required: true));
        $database->createIndex('sizeTest2', Index::key(key: 'index', attributes: ['string1', 'string2', 'string3'], lengths: [128, 128, 128]));

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

        if ($database->getAdapter()->hasFeature(Postgres::class)) {
            $this->assertLessThanOrEqual($size2, $size3);

            return;
        }

        $this->assertLessThan($size2, $size3);
    }

    public function testSizeCollectionOnDisk(): void
    {
        $this->getDatabase()->createCollection(Collection::create(id: 'sizeTestDisk1'));
        $this->getDatabase()->createCollection(Collection::create(id: 'sizeTestDisk2'));

        $size1 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk1');
        $size2 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk2');
        $sizeDifference = abs($size1 - $size2);
        // Size of an empty collection returns either 172032 or 167936 bytes randomly
        // Therefore asserting with a tolerance of 5000 bytes
        $byteDifference = 5000;
        $this->assertLessThan($byteDifference, $sizeDifference);

        $this->getDatabase()->createAttribute('sizeTestDisk2', Attribute::string(key: 'string1', size: 20000, required: true));
        $this->getDatabase()->createAttribute('sizeTestDisk2', Attribute::string(key: 'string2', size: 254 + 1, required: true));
        $this->getDatabase()->createAttribute('sizeTestDisk2', Attribute::string(key: 'string3', size: 254 + 1, required: true));
        $this->getDatabase()->createIndex('sizeTestDisk2', Index::key(key: 'index', attributes: ['string1', 'string2', 'string3'], lengths: [128, 128, 128]));

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
        if (! $database->getAdapter()->supports(Capability::IndexFulltext)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(Collection::create(id: 'fullTextSizeTest'));

        $size1 = $database->getSizeOfCollection('fullTextSizeTest');

        $database->createAttribute('fullTextSizeTest', Attribute::string(key: 'string1', size: 128, required: true));
        $database->createAttribute('fullTextSizeTest', Attribute::string(key: 'string2', size: 254, required: true));
        $database->createAttribute('fullTextSizeTest', Attribute::string(key: 'string3', size: 254, required: true));
        $database->createIndex('fullTextSizeTest', Index::key(key: 'index', attributes: ['string1', 'string2', 'string3'], lengths: [128, 128, 128]));

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

        $database->createIndex('fullTextSizeTest', Index::fulltext(key: 'fulltext_index', attributes: ['string1']));

        $size3 = $database->getSizeOfCollectionOnDisk('fullTextSizeTest');

        $this->assertGreaterThan($size2, $size3);
    }

    public function testSchemaAttributes(): void
    {
        $db = $this->getDatabase();
        $adapter = $db->getAdapter();

        if (! $adapter->supports(Capability::SchemaIntrospection)) {
            $this->assertSame([], $db->getSchemaAttributes('no_such_collection'));

            return;
        }

        $collection = 'schema_attributes';

        $this->assertSame([], $db->getSchemaAttributes('no_such_collection'));

        $db->createCollection(Collection::create(id: $collection));

        $attributes = [
            Attribute::string(key: 'username', size: 128, required: true),
            Attribute::string(key: 'story', size: 20000, required: true),
            Attribute::string(key: 'string_list', size: 128, required: true, array: true),
            Attribute::datetime(key: 'dob', default: '2000-06-12T14:12:55.000+00:00'),
        ];
        foreach ($attributes as $attribute) {
            $db->createAttribute($collection, $attribute);
        }

        $columns = [];
        foreach ($db->getSchemaAttributes($collection) as $column) {
            $columns[$column->name] = $column;
        }

        foreach ($attributes as $attribute) {
            $this->assertArrayHasKey($attribute->key, $columns);
            $this->assertSame($adapter->getColumnType($attribute), $columns[$attribute->key]->type, $attribute->key);
        }

        $this->assertSame(128, $columns['username']->length);
        $this->assertTrue($columns['username']->nullable);
        $this->assertTrue($columns['string_list']->nullable);
        $this->assertNull($columns['dob']->length);

        foreach ([Storage::SEQUENCE, Storage::UID, Storage::CREATED_AT, Storage::UPDATED_AT, Storage::PERMISSIONS] as $internal) {
            $this->assertArrayHasKey($internal, $columns, 'The engine-only column '.$internal.' is read back as a column');
            $this->assertNotSame('', $columns[$internal]->type);
        }

        if ($db->getSharedTables()) {
            $this->assertArrayHasKey(Storage::TENANT, $columns);
            $this->assertNull($columns[Storage::TENANT]->length);
            $this->assertTrue($columns[Storage::TENANT]->nullable);
        }
    }

    public function testCreateCollectionWithSchemaIndexes(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $attributes = [
            Attribute::string(key: 'username', size: 100),
            Attribute::string(key: 'cards', size: 5000, array: true),
        ];

        $indexes = [
            Index::key(key: 'idx_username', attributes: ['username'], lengths: [100]),
            Index::key(key: 'idx_username_uid', attributes: ['username', '$id'], lengths: [99, 200], orders: [OrderDirection::Desc]),
        ];

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            $indexes[] = Index::key(key: 'idx_cards', attributes: ['cards'], lengths: [500], orders: [OrderDirection::Desc]);
        }

        $collection = $database->createCollection(Collection::create(id: 'collection98', attributes: $attributes, indexes: $indexes, permissions: [
            Permission::create(Role::any()),
        ]));

        $this->assertEquals($collection->indexes()[0]->attributes[0], 'username');
        $this->assertEquals($collection->indexes()[0]->lengths[0], null);

        $this->assertEquals($collection->indexes()[1]->attributes[0], 'username');
        $this->assertEquals($collection->indexes()[1]->lengths[0], 99);
        $this->assertEquals($collection->indexes()[1]->orders[0], OrderDirection::Desc);

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            $this->assertEquals($collection->indexes()[2]->attributes[0], 'cards');
            $this->assertEquals($collection->indexes()[2]->lengths[0], Database::MAX_ARRAY_INDEX_LENGTH);
            $this->assertEquals($collection->indexes()[2]->orders[0], null);
        }
    }

    public function testGetCollectionId(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Connection::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->assertNotSame('', $database->getConnectionId());
    }

    public function testKeywords(): void
    {
        $database = $this->getDatabase();
        $keywords = $database->profile()->limits->keywords;

        if ($keywords === []) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Collection name tests
        $attributes = [
            Attribute::string(key: 'attribute1', size: 256),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['attribute1'], lengths: [256], orders: [OrderDirection::Asc]),
        ];

        foreach ($keywords as $keyword) {
            $collection = $database->createCollection(Collection::create(id: $keyword, attributes: $attributes, indexes: $indexes));
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

            $database->deleteCollection($keyword);
            $this->assertNull($database->findCollection($keyword));
        }

        // TODO: updateCollection name tests

        // Attribute name tests
        foreach ($keywords as $keyword) {
            $collectionName = 'rk'.$keyword; // rk is shorthand for reserved-keyword. We do this since there are some limits (64 chars max)

            $collection = $database->createCollection(Collection::create(id: $collectionName));
            $this->assertEquals($collectionName, $collection->getId());

            $database->createAttribute($collectionName, Attribute::string(key: $keyword, size: 128, required: true));

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

            $database->deleteCollection($collectionName);
            $this->assertNull($database->findCollection($collectionName));
        }
    }

    public function testLabels(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();
        $reader = Role::label('reader')->toString();

        $database->createCollection(Collection::create(id: 'labels_test'));
        $database->createAttribute('labels_test', Attribute::string(key: 'attr1', size: 10));

        $database->createDocument('labels_test', new Document([
            '$id' => 'doc1',
            'attr1' => 'value1',
            '$permissions' => [
                Permission::read(Role::label('reader')),
            ],
        ]));

        $withoutLabel = $database->find('labels_test');
        $this->assertSame([], $withoutLabel);
        $this->assertTrue($database->getDocument('labels_test', 'doc1')->isEmpty());

        $authorization->addRole($reader);

        try {
            $withLabel = $database->find('labels_test');
            $this->assertCount(1, $withLabel);
            $this->assertSame('doc1', $withLabel[0]->getId());
            $this->assertSame('value1', $database->getDocument('labels_test', 'doc1')->getAttribute('attr1'));
        } finally {
            $authorization->removeRole($reader);
        }

        $labelRemoved = $database->find('labels_test');
        $this->assertSame([], $labelRemoved);
        $this->assertTrue($database->getDocument('labels_test', 'doc1')->isEmpty());

        $database->deleteCollection('labels_test');
    }

    public function testDeleteCollectionDeletesRelationships(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create 'testers' collection if not already created (was created by testMetadata in sequential mode)
        if ($database->findCollection('testers') === null) {
            $database->createCollection(Collection::create(id: 'testers'));
        }

        $database->createCollection(Collection::create(id: 'devices'));

        $database->createRelationship('testers', Relationship::oneToMany(relatedCollection: 'devices', twoWay: true, twoWayKey: 'tester'));

        $testers = $database->getCollection('testers');
        $devices = $database->getCollection('devices');

        $this->assertEquals(1, \count($testers->attributes()));
        $this->assertEquals(1, \count($devices->attributes()));
        $this->assertEquals(1, \count($devices->indexes()));

        $database->deleteCollection('testers');

        $testers = $database->findCollection('testers');
        $devices = $database->getCollection('devices');

        $this->assertNull($testers);
        $this->assertEquals(0, \count($devices->attributes()));
        $this->assertEquals(0, \count($devices->indexes()));
    }

    public function testCascadeMultiDelete(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(Collection::create(id: 'cascadeMultiDelete1'));
        $database->createCollection(Collection::create(id: 'cascadeMultiDelete2'));
        $database->createCollection(Collection::create(id: 'cascadeMultiDelete3'));

        $database->createRelationship('cascadeMultiDelete1', Relationship::oneToMany(relatedCollection: 'cascadeMultiDelete2', twoWay: true, onDelete: RelationshipDeleteAction::Cascade));

        $database->createRelationship('cascadeMultiDelete2', Relationship::oneToMany(relatedCollection: 'cascadeMultiDelete3', twoWay: true, onDelete: RelationshipDeleteAction::Cascade));

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

        $cascade2 = $root->getDocuments('cascadeMultiDelete2');
        $this->assertCount(1, $cascade2);
        $this->assertCount(1, $cascade2[0]->getDocuments('cascadeMultiDelete3'));

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

        $database->createCollection(Collection::create(id: 'people', attributes: [
            Attribute::string(key: 'name', size: 128, required: true),
            Attribute::string(key: 'lifeStory', size: 65536, required: true),
        ], indexes: [
            Index::key(key: 'idx_name', attributes: ['name']),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $this->assertCount(1, $database->listCollections());

        if ($database->getAdapter()->supports(Capability::IndexFulltext)) {
            $database->createIndex('people', Index::fulltext(key: 'idx_lifeStory', attributes: ['lifeStory']));
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

        $this->assertCount(0, $database->listCollections(100));

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

        $database->createCollection(Collection::create(id: 'duplicates', permissions: [
            Permission::read(Role::any()),
        ]));

        try {
            $database->createCollection(Collection::create(id: 'duplicates'));
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
        $database->createCollection(Collection::create(id: 'duplicates', documentSecurity: false));
        $database->createAttribute('duplicates', Attribute::string(key: 'name', size: 10));
        $database->createIndex('duplicates', Index::key(key: 'nameIndex', attributes: ['name']));

        $database->setTenant(2);

        try {
            $database->createCollection(Collection::create(id: 'duplicates', documentSecurity: false));
        } catch (DuplicateException) {
            // Ignore
        }

        try {
            $database->createAttribute('duplicates', Attribute::string(key: 'name', size: 10));
        } catch (DuplicateException) {
            // Ignore
        }

        try {
            $database->createIndex('duplicates', Index::key(key: 'nameIndex', attributes: ['name']));
        } catch (DuplicateException) {
            // Ignore
        }

        $collection = $database->getCollection('duplicates');
        $this->assertEquals(1, \count($collection->attributes()));
        $this->assertEquals(1, \count($collection->indexes()));

        $database->setTenant(null);
        $database->purgeCachedCollection('duplicates');

        $collection = $database->getCollection('duplicates');
        $this->assertEquals(1, \count($collection->attributes()));
        $this->assertEquals(1, \count($collection->indexes()));

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
            $tenant1 = $database->getIdAttributeType() === ColumnType::Integer ? 10 : 'tenant_10';
            $tenant2 = $database->getIdAttributeType() === ColumnType::Integer ? 20 : 'tenant_20';
            $colName = 'mt_' . uniqid();

            $database->setTenant($tenant1);

            $database->createCollection(Collection::create(id: $colName, attributes: [
                Attribute::string(key: 'name', size: 128, required: true),
            ]));

            $col1 = $database->findCollection($colName);
            $this->assertNotNull($col1);
            $this->assertEquals(1, \count($col1->attributes()));

            $database->setTenant($tenant2);

            $database->createCollection(Collection::create(id: $colName, attributes: [
                Attribute::string(key: 'name', size: 128, required: true),
            ]));

            $col2 = $database->findCollection($colName);
            $this->assertNotNull($col2);
            $this->assertEquals(1, \count($col2->attributes()));

            $database->setTenant($tenant1);
            $col1Again = $database->findCollection($colName);
            $this->assertNotNull($col1Again);

            if ($createdDb) {
                $database->delete();
            } else {
                $database->setTenant($tenant1);
                $database->deleteCollection($colName);
                try {
                    $database->setTenant($tenant2);
                    $database->deleteCollection($colName);
                } catch (\Throwable) {
                }
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
            $tenant1 = $database->getIdAttributeType() === ColumnType::Integer ? 100 : 'tenant_100';
            $tenant2 = $database->getIdAttributeType() === ColumnType::Integer ? 200 : 'tenant_200';

            if ($sharedTables) {
                // Already in shared-tables mode; create() should be idempotent.
                // No assertion on exists() since SQLite always returns false for
                // database-level exists. The test verifies create() doesn't throw.
                $database->setTenant($tenant1);
                $database->create();
                $database->setTenant($tenant2);
                $database->create();
                $this->assertSame($tenant2, $database->getTenant());
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

            $expected = [
                Event::DatabaseCreate,
                Event::DatabaseList,
                Event::CollectionCreate,
                Event::CollectionList,
                Event::CollectionRead,
                Event::DocumentPurge,
                Event::AttributeCreate,
                Event::DocumentPurge,
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
            ];

            $supportsSchemas = $this->getDatabase()->getAdapter()->supports(Capability::Schemas);
            if (! $supportsSchemas) {
                \array_shift($expected);
            }
            $recorder = new EventRecorder('test');
            $database->addHook($recorder);

            if ($supportsSchemas) {
                $database->setDatabase('hellodb');
                $database->create();
            }

            $database->list();

            $database->setDatabase($this->testDatabase);

            $collectionId = ID::unique();
            $database->createCollection(Collection::create(id: $collectionId));
            $database->listCollections();
            $database->getCollection($collectionId);
            $database->createAttribute($collectionId, Attribute::integer(key: 'attr1'));
            $database->updateAttribute($collectionId, 'attr1', new AttributeUpdate(required: true));
            $indexId1 = 'index2_'.uniqid();
            $database->createIndex($collectionId, Index::key(key: $indexId1, attributes: ['attr1']));

            $document = $database->createDocument($collectionId, new Document([
                '$id' => 'doc1',
                'attr1' => 10,
                '$permissions' => [
                    Permission::delete(Role::any()),
                    Permission::update(Role::any()),
                    Permission::read(Role::any()),
                ],
            ]));

            $silenced = new EventRecorder('should-not-execute');
            $database->addHook($silenced);

            $database->silent(function () use ($database, $collectionId, $document) {
                $database->updateDocument($collectionId, 'doc1', $document->setAttribute('attr1', 15));
                $database->getDocument($collectionId, 'doc1');
                $database->find($collectionId);
                $database->findOne($collectionId);
                $database->count($collectionId);
                $database->sum($collectionId, 'attr1');
                $database->increaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
                $database->decreaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
            }, ['should-not-execute']);

            $this->assertSame([], $silenced->stop());

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

            $this->assertSame($expected, $recorder->stop());
        });
    }

    public function testSilentNamedListeners(): void
    {
        $this->getDatabase()->getAuthorization()->skip(function () {
            $database = $this->getDatabase();
            $collectionId = ID::unique();

            $replaced = new EventRecorder('audits');
            $replacement = new EventRecorder('audits');
            $usage = new EventRecorder('usage');
            $database->addHook($replaced)->addHook($usage);

            $database->silent(fn () => $database->createCollection(Collection::create(id: $collectionId)), ['audits']);
            $database->silent(fn () => $database->getCollection($collectionId));
            $database->addHook($replacement);
            $database->deleteCollection($collectionId);

            $this->assertSame([], $replaced->stop());
            $this->assertSame([Event::CollectionDelete], $replacement->stop());
            $this->assertSame([Event::CollectionCreate, Event::CollectionDelete], $usage->stop());
        });
    }

    public function testCreatedAtUpdatedAt(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $created = $database->createCollection(Collection::create(id: $this->getCreatedAtCollection()));
        $this->assertSame($this->getCreatedAtCollection(), $created->getId());
        $database->createAttribute($this->getCreatedAtCollection(), Attribute::string(key: 'title', size: 100));
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

    public function testCreatedAtUpdatedAtAssert(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $collection = $this->initCreatedAtAssertFixture();

        $document = $database->getDocument($collection, 'uid123');
        $this->assertEquals(true, ! $document->isEmpty());
        sleep(1);
        $document->setAttribute('title', 'new title');
        $database->updateDocument($collection, 'uid123', $document);
        $document = $database->getDocument($collection, 'uid123');

        $this->assertGreaterThan($document->getCreatedAt(), $document->getUpdatedAt());
        $this->expectException(DuplicateException::class);

        $database->createCollection(Collection::create(id: $collection));
    }

    private function initCreatedAtAssertFixture(): string
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $collection = ID::unique();

        $database->createCollection(Collection::create(id: $collection));
        $database->createAttribute($collection, Attribute::string(key: 'title', size: 100));
        $database->createDocument($collection, new Document([
            '$id' => ID::custom('uid123'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        return $collection;
    }

    public function testTransformations(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        // Transform hooks rewrite SQL statements, so only SQL adapters have a query to rewrite.
        if (! $database->getAdapter()->hasFeature(Feature\RawQuery::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(Collection::create(id: 'docs', attributes: [
            Attribute::string(key: 'name', size: 767, required: true),
        ]));

        $database->createDocument('docs', new Document([
            '$id' => 'doc1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'value1',
        ]));

        $this->assertCount(1, $database->find('docs'));

        $database->setMetadata('scope', 'api.users');

        $hook = new class ($database->getNamespace().'_docs') implements Transform {
            public string $query = '';

            public function __construct(private readonly string $table)
            {
            }

            public function transform(Event $event, string $query): string
            {
                if ($event !== Event::DocumentRead || ! \str_contains($query, $this->table)) {
                    return $query;
                }

                $this->query = $query;

                return $query.' AND 1 = 0';
            }
        };
        $database->addHook($hook);

        try {
            // getDocument() resolves an uncached collection with a DocumentRead of the metadata table, which the
            // transform must leave alone. Evicting the definition makes that read reach the transform on every run.
            $database->purgeCachedDocument(Database::METADATA, 'docs');

            $this->assertTrue($database->getDocument('docs', 'doc1')->isEmpty());
            $this->assertStringContainsString('/* scope: api.users */', $hook->query);
        } finally {
            $database->removeTransform($hook::class);
            $database->resetMetadata();
        }

        $this->assertCount(1, $database->find('docs'));
    }

    /**
     * The tenant is the segment before the 'collection' marker. Substring
     * matching is unsafe because the namespace is a hex uniqid() that may
     * legitimately contain the tenant digits.
     */
    private function cacheKeyTenantSegment(string $collectionKey): string
    {
        $segments = \explode(':', $collectionKey);
        $marker = \array_search('collection', $segments, true);
        $this->assertIsInt($marker);
        $this->assertGreaterThan(0, $marker);

        return $segments[$marker - 1];
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
            $this->assertSame('', $this->cacheKeyTenantSegment($collectionKey));
        }

        // non global collection should contain tenant in the cache key
        $nonGlobalCollectionId = 'nonGlobalCollection';
        [$collectionKeyRegular] = $db->getCacheKeys(
            Database::METADATA,
            $nonGlobalCollectionId
        );
        if ($db->getSharedTables()) {
            $this->assertSame(
                (string) $db->getAdapter()->getTenant(),
                $this->cacheKeyTenantSegment($collectionKeyRegular)
            );
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
            Attribute::string(key: 'name', size: 256, required: true),
            Attribute::integer(key: 'age'),
            Attribute::boolean(key: 'isActive'),
        ];

        $indexes = [
            Index::key(key: 'idx_name', attributes: ['name'], lengths: [128], orders: [OrderDirection::Asc]),
            Index::key(key: 'idx_name_age', attributes: ['name', 'age'], lengths: [128, null], orders: [OrderDirection::Asc, OrderDirection::Desc]),
        ];

        $collectionDocument = $database->createCollection(Collection::create(id: $collection, attributes: $attributes, indexes: $indexes, permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $this->assertEquals($collection, $collectionDocument->getId());
        $this->assertCount(3, $collectionDocument->attributes());
        $this->assertCount(2, $collectionDocument->indexes());

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

        $database->deleteCollection($collection);
    }

    /**
     * Two processes reconciling the same schema race: one reads the collection
     * as missing, a peer creates it and commits, and only then does the first
     * process try to create it. The loser must not mistake the peer's table for
     * an orphan and drop it.
     */
    public function testCreateCollectionConcurrentlyKeepsPeerData(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = 'concurrentCreate';

        // A peer process: same database, its own cache, so its writes do not
        // purge the negative cache entry this process is about to record.
        $authorization = self::$authorization ?? throw new \RuntimeException('Authorization not initialised');
        $peer = (new Database($database->getAdapter(), new Cache(new NoneCache())))
            ->setAuthorization($authorization);

        $this->assertNull($database->findCollection($collection));

        $name = Attribute::string(key: 'name', size: 128);

        $peer->createCollection(Collection::create(id: $collection, attributes: [$name], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));

        $peer->createDocument($collection, new Document([
            '$id' => ID::custom('written'),
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'peer',
        ]));

        try {
            $database->createCollection(Collection::create(id: $collection, attributes: [$name], permissions: [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
            ]));
            $this->fail('Expected DuplicateException for a collection a peer already created');
        } catch (DuplicateException) {
        }

        $survivor = $peer->getDocument($collection, 'written');
        $this->assertSame('peer', $survivor->getAttribute('name'), 'Peer document was destroyed by the losing creator');

        $this->assertNotNull($peer->findCollection($collection), 'Peer collection metadata was destroyed by the losing creator');

        // The loser's cache still held the collection as missing from the read
        // it took before the peer committed, and the peer's purge cannot reach
        // this instance. Losing the race has to clear it, or the collection
        // stays invisible here until the entry expires.
        $this->assertNotNull($database->findCollection($collection), 'Losing creator kept a stale empty collection cached');
        $this->assertSame('peer', $database->getDocument($collection, 'written')->getAttribute('name'));

        $database->deleteCollection($collection);
    }

    /**
     * A physical collection with no metadata is indistinguishable from a peer
     * that has created the table and not yet committed its metadata row.
     * createCollection must leave that table alone.
     */
    public function testCreateCollectionDoesNotDropUncommittedPeerTable(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if ($database->getAdapter()->getSharedTables()) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'preCommitCreate';
        $name = Attribute::string(key: 'name', size: 128);

        $database->getAdapter()->createCollection($collection, [$name], []);

        $schema = new Document([
            '$id' => $collection,
            '$collection' => Database::METADATA,
            'name' => $collection,
            'attributes' => [$name->toDocument()],
            'indexes' => [],
            'documentSecurity' => true,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]);

        $database->getAdapter()->createDocument($schema, new Document([
            '$id' => ID::custom('written'),
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'peer',
        ]));

        try {
            $database->createCollection(Collection::create(id: $collection, attributes: [$name], permissions: [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
            ]));
        } catch (DuplicateException) {
            // SQL adapters report the existing table as Duplicate. Mongo's
            // createCollection is idempotent, so this process continues and
            // claims metadata. Either way the physical collection must stay.
        }

        $this->assertSame(
            'peer',
            $database->getAdapter()->getDocument($schema, 'written')->getAttribute('name'),
            'Physical collection was dropped while metadata was still uncommitted'
        );

        try {
            $database->deleteCollection($collection);
        } catch (\Throwable) {
            $database->getAdapter()->deleteCollection($collection);
        }
    }

    public function testCollectionNotFound(): void
    {
        $database = $this->getDatabase();

        try {
            $database->find('not_exist', []);
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(NotFoundException::class, $e);
            $this->assertSame('Collection not found', $e->getMessage());
        }

        try {
            $database->count('not_exist');
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(NotFoundException::class, $e);
            $this->assertSame('Collection not found', $e->getMessage());
        }

        try {
            $database->sum('not_exist', 'value');
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(NotFoundException::class, $e);
            $this->assertSame('Collection not found', $e->getMessage());
        }

        try {
            $database->getAuthorization()->skip(fn () => $database->count('not_exist'));
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(NotFoundException::class, $e);
            $this->assertSame('Collection not found', $e->getMessage());
        }
    }

    public function testUpdateDeleteCollectionNotFound(): void
    {
        $database = $this->getDatabase();

        try {
            $database->deleteCollection('not_found');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(NotFoundException::class, $e);
            $this->assertSame('Collection not found', $e->getMessage());
        }

        try {
            $database->updateCollection('not_found', new CollectionUpdate(permissions: [], documentSecurity: true));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(NotFoundException::class, $e);
            $this->assertSame('Collection not found', $e->getMessage());
        }
    }

    public function testCollectionUpdate(): void
    {
        $database = $this->getDatabase();

        $collection = $database->createCollection(Collection::create(id: 'collectionUpdate', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: false));

        $this->assertSame('collectionUpdate', $collection->getId());

        $collection = $database->getCollection('collectionUpdate');

        $this->assertFalse($collection->getAttribute('documentSecurity'));
        $this->assertCount(4, $collection->getPermissions());

        $collection = $database->updateCollection('collectionUpdate', new CollectionUpdate(permissions: [], documentSecurity: true));

        $this->assertTrue($collection->getAttribute('documentSecurity'));
        $this->assertSame([], $collection->getPermissions());

        $collection = $database->getCollection('collectionUpdate');

        $this->assertTrue($collection->getAttribute('documentSecurity'));
        $this->assertSame([], $collection->getPermissions());

        $database->deleteCollection('collectionUpdate');
    }

    public function testCreateCollectionValidator(): void
    {
        $database = $this->getDatabase();

        $collections = [
            'validatorTest',
            'validator-test',
            'validator_test',
            'validator.test',
        ];

        $attributes = [
            Attribute::string(key: 'attribute1', size: 2500),
            Attribute::integer(key: 'attribute-2'),
            Attribute::boolean(key: 'attribute_3'),
            Attribute::boolean(key: 'attribute.4'),
            Attribute::string(key: 'attribute5', size: 2500),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['attribute1'], lengths: [256], orders: [OrderDirection::Asc]),
            Index::key(key: 'index-2', attributes: ['attribute-2'], orders: [OrderDirection::Asc]),
            Index::key(key: 'index_3', attributes: ['attribute_3'], orders: [OrderDirection::Asc]),
            Index::key(key: 'index.4', attributes: ['attribute.4'], orders: [OrderDirection::Asc]),
            Index::key(key: 'index_2_attributes', attributes: ['attribute1', 'attribute5'], lengths: [200, 300], orders: [OrderDirection::Desc]),
        ];

        foreach ($collections as $id) {
            $collection = $database->createCollection(Collection::create(id: $id, attributes: $attributes, indexes: $indexes));

            $this->assertSame($id, $collection->getId());

            $this->assertCount(5, $collection->attributes());
            $this->assertSame('attribute1', $collection->attributes()[0]->key);
            $this->assertSame(ColumnType::String, $collection->attributes()[0]->type);
            $this->assertSame('attribute-2', $collection->attributes()[1]->key);
            $this->assertSame(ColumnType::Integer, $collection->attributes()[1]->type);
            $this->assertSame('attribute_3', $collection->attributes()[2]->key);
            $this->assertSame(ColumnType::Boolean, $collection->attributes()[2]->type);
            $this->assertSame('attribute.4', $collection->attributes()[3]->key);
            $this->assertSame(ColumnType::Boolean, $collection->attributes()[3]->type);

            $this->assertCount(5, $collection->indexes());
            $this->assertSame('index1', $collection->indexes()[0]->key);
            $this->assertSame(IndexType::Key, $collection->indexes()[0]->type);
            $this->assertSame('index-2', $collection->indexes()[1]->key);
            $this->assertSame(IndexType::Key, $collection->indexes()[1]->type);
            $this->assertSame('index_3', $collection->indexes()[2]->key);
            $this->assertSame(IndexType::Key, $collection->indexes()[2]->type);
            $this->assertSame('index.4', $collection->indexes()[3]->key);
            $this->assertSame(IndexType::Key, $collection->indexes()[3]->type);

            $database->deleteCollection($id);
        }
    }

    public function testMetadata(): void
    {
        $database = $this->getDatabase();

        $database->setMetadata('key', 'value');

        $database->createCollection(Collection::create(id: 'testers'));

        $this->assertSame(['key' => 'value'], $database->getMetadata());

        $database->resetMetadata();

        $this->assertSame([], $database->getMetadata());

        $database->deleteCollection('testers');
    }

    public function testPurgeCollectionCache(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: 'purgeCache'));

        $database->createAttribute('purgeCache', Attribute::string(key: 'name', size: 128, required: true));
        $database->createAttribute('purgeCache', Attribute::integer(key: 'age', required: true));

        $database->createDocument('purgeCache', new Document([
            '$id' => 'doc1',
            'name' => 'Richard',
            'age' => 15,
            '$permissions' => [
                Permission::read(Role::any()),
            ],
        ]));

        $document = $database->getDocument('purgeCache', 'doc1');

        $this->assertSame('Richard', $document->getAttribute('name'));
        $this->assertSame(15, $document->getAttribute('age'));

        $database->deleteAttribute('purgeCache', 'age');

        $document = $database->getDocument('purgeCache', 'doc1');
        $this->assertSame('Richard', $document->getAttribute('name'));
        $this->assertArrayNotHasKey('age', $document);

        $database->createAttribute('purgeCache', Attribute::integer(key: 'age', required: true));

        $document = $database->getDocument('purgeCache', 'doc1');
        $this->assertSame('Richard', $document->getAttribute('name'));
        $this->assertArrayHasKey('age', $document);

        $database->deleteCollection('purgeCache');
    }

    public function testRowSizeToLarge(): void
    {
        $database = $this->getDatabase();

        if ($database->getAdapter()->limits()->documentSize === 0) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection1 = $database->createCollection(Collection::create(id: 'row_size_1'));
        $collection2 = $database->createCollection(Collection::create(id: 'row_size_2'));

        $database->createAttribute($collection1->getId(), Attribute::string(key: 'attr_1', size: 16000, required: true));

        try {
            $database->createAttribute($collection1->getId(), Attribute::string(key: 'attr_2', size: Database::LENGTH_KEY, required: true));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        }

        if ($database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            try {
                $database->createRelationship($collection2->getId(), Relationship::oneToOne(
                    relatedCollection: $collection1->getId(),
                    twoWay: true,
                ));
                $this->fail('Failed to throw exception');
            } catch (Exception $e) {
                $this->assertInstanceOf(LimitException::class, $e, 'A relationship column takes the length of a key and must respect the row size limit');
            }

            try {
                $database->createRelationship($collection1->getId(), Relationship::oneToOne(
                    relatedCollection: $collection2->getId(),
                    twoWay: true,
                ));
                $this->fail('Failed to throw exception');
            } catch (Exception $e) {
                $this->assertInstanceOf(LimitException::class, $e);
            }
        }

        $database->deleteCollection('row_size_1');
        $database->deleteCollection('row_size_2');
    }

    public function testCollectionWhoseTableIsGoneIsNotFoundAndCanBeDeleted(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->hasFeature(Feature\RawQuery::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'tableGone';
        $database->createCollection(Collection::create(id: $collection, attributes: [Attribute::string(key: 'name', size: 64)], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));

        $this->dropCollectionTable($database, $collection);

        try {
            $database->find($collection);
            $this->fail('Expected NotFoundException for a collection whose table is gone');
        } catch (NotFoundException $e) {
            $this->assertSame('Collection not found', $e->getMessage());
        }

        $database->deleteCollection($collection);
        $this->assertNull($database->findCollection($collection));

        if ($adapter instanceof Postgres || $adapter instanceof SQLite) {
            $database->createCollection(Collection::create(id: $collection, permissions: [Permission::read(Role::any())]));
            $database->deleteCollection($collection);
        }
    }

    public function testIndexOnAColumnTheTableLacksIsAttributeNotFound(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->hasFeature(Feature\RawQuery::class) || $adapter->hasFeature(SQLite::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'indexDrifted';
        $database->createCollection(Collection::create(id: $collection, attributes: [Attribute::string(key: 'name', size: 64)], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));

        $this->deleteColumn($collection, 'name');

        try {
            $database->createIndex($collection, Index::key(key: 'nameIndex', attributes: ['name']));
            $this->fail('Expected NotFoundException for an index on a column the table lacks');
        } catch (NotFoundException $e) {
            $this->assertSame('Attribute not found', $e->getMessage());
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testPostgresAggregateOverATypeWithoutTheFunctionIsAQueryError(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(Postgres::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'maxOverBoolean';
        $database->createCollection(Collection::create(id: $collection, attributes: [Attribute::boolean(key: 'active')], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));
        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'active' => true,
        ]));

        try {
            $database->skipValidation(fn () => $database->find($collection, [Query::max('active', 'most')]));
            $this->fail('Expected QueryException for max() over a boolean attribute');
        } catch (QueryException $e) {
            $this->assertSame('Query applies a function or operator the attribute type does not support', $e->getMessage());
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testMongoIncrementOfATextValueIsAnInvalidOperation(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->hasFeature(Mongo::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'incrementText';
        $database->createCollection(Collection::create(id: $collection, attributes: [Attribute::string(key: 'name', size: 64)], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));
        $document = $database->createDocument($collection, new Document([
            '$id' => 'text',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'plain',
        ]));

        try {
            $adapter->increaseDocumentAttribute($database->getCollection($collection), 'text', 'name', 1, $document->getUpdatedAt() ?? '');
            $this->fail('Expected TypeException for an increment of a text value');
        } catch (TypeException $e) {
            $this->assertSame('Invalid operation', $e->getMessage());
        } finally {
            $database->deleteCollection($collection);
        }
    }

    private function dropCollectionTable(Database $database, string $collection): void
    {
        $table = $database->getNamespace().'_'.$collection;
        if (! $database->getAdapter() instanceof SQLite) {
            $table = $database->getDatabase().'.'.$table;
        }

        $database->getAuthorization()->skip(fn () => $database->schema()->table($table)->drop()->execute());
    }

    public function testAnalyzeCollectionRecordsStatisticsForTheTableAndItsPermissions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->hasFeature(SQL::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'analyzed';
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [Attribute::string(key: 'name', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: true,
        ));

        try {
            for ($number = 0; $number < 20; $number++) {
                $database->createDocument($collection, new Document([
                    '$permissions' => [Permission::read(Role::user('user'.$number))],
                    'name' => 'name'.($number % 4),
                ]));
            }

            $this->assertTrue($database->analyzeCollection($collection));

            $tables = [$database->getNamespace().'_'.$collection, $database->getNamespace().'_'.$collection.'_perms'];

            if ($adapter instanceof Postgres) {
                $rows = $adapter->rawQuery(
                    'SELECT DISTINCT tablename FROM pg_stats WHERE schemaname = ? AND tablename IN (?, ?) ORDER BY tablename',
                    [$database->getDatabase(), ...$tables],
                );
                $this->assertSame($tables, \array_map(static fn (Document $row): mixed => $row->getAttribute('tablename'), $rows));
            }

            if ($adapter instanceof SQLite) {
                $rows = $adapter->rawQuery('SELECT DISTINCT tbl FROM sqlite_stat1 WHERE tbl IN (?, ?) ORDER BY tbl', $tables);
                $this->assertSame($tables, \array_map(static fn (Document $row): mixed => $row->getAttribute('tbl'), $rows));
            }
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testRewritingADatetimeColumnKeepsItsValues(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = 'datetimeRewrite';
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [Attribute::datetime(key: 'at')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        try {
            $database->createDocument($collection, new Document([
                '$id' => 'moment',
                'at' => '2024-05-06T07:08:09.123+00:00',
            ]));

            $database->updateAttribute($collection, 'at', new AttributeUpdate(key: 'happenedAt'));
            $this->assertSame('2024-05-06T07:08:09.123+00:00', $database->getDocument($collection, 'moment')->getAttribute('happenedAt'));

            $database->updateAttribute($collection, 'happenedAt', new AttributeUpdate(type: ColumnType::Datetime, required: true));
            $this->assertSame('2024-05-06T07:08:09.123+00:00', $database->getDocument($collection, 'moment')->getAttribute('happenedAt'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testIndexOnAnObjectPathAttribute(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter instanceof Postgres || ! $adapter->supports(Capability::Objects)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'objectPathIndex';
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [Attribute::object(key: 'data'), Attribute::string(key: 'status', size: 32)],
            indexes: [Index::key(key: 'countryfirst', attributes: ['data.country', 'status'], orders: [OrderDirection::Desc, null])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        try {
            $rows = $adapter->rawQuery(
                'SELECT indexdef FROM pg_indexes WHERE schemaname = ? AND tablename = ? AND indexname LIKE ?',
                [$database->getDatabase(), $database->getNamespace().'_'.$collection, '%\_countryfirst'],
            );
            $this->assertCount(1, $rows);
            $definition = $rows[0]->getAttribute('indexdef');
            $this->assertIsString($definition);
            $this->assertStringContainsString("((data ->> 'country'::text)) DESC, status)", $definition);

            $database->createDocument($collection, new Document([
                '$id' => 'nz',
                'data' => ['country' => 'NZ'],
                'status' => 'active',
            ]));
            $this->assertSame(['country' => 'NZ'], $database->getDocument($collection, 'nz')->getAttribute('data'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testDeletingACollectionWhoseTableIsGoneDropsItsPermissionsTable(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(MariaDB::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'mainTableGone';
        $database->createCollection(Collection::create(id: $collection, permissions: [Permission::read(Role::any())]));
        $this->assertTrue($database->collectionExists(Storage::permissionsTable($collection)));

        $table = $database->getDatabase().'.'.$database->getNamespace().'_'.$collection;
        $database->getAuthorization()->skip(fn () => $database->schema()->table($table)->drop()->execute());

        $database->deleteCollection($collection);
        $this->assertNull($database->findCollection($collection));
        $this->assertFalse($database->collectionExists(Storage::permissionsTable($collection)), 'The permissions table of a collection whose table was gone was left behind');
    }

    public function testPostgresSharedTablesRefuseAnotherTenantsColumnOfAnotherType(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getSharedTables() || ! $database->getAdapter()->hasFeature(Postgres::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $originalTenant = $database->getTenant();
        $integerTenants = $database->getIdAttributeType() === ColumnType::Integer;
        $first = $integerTenants ? 401 : 'tenant_401';
        $second = $integerTenants ? 402 : 'tenant_402';
        $collection = 'sharedColumnType';
        $definition = Collection::create(id: $collection, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);

        try {
            $database->setTenant($first);
            $database->createCollection($definition);
            $database->createAttribute($collection, Attribute::integer(key: 'age'));
            $database->createDocument($collection, new Document(['$id' => 'first', 'age' => 7]));

            $database->setTenant($second);
            $database->createCollection($definition);

            try {
                $database->createAttribute($collection, Attribute::string(key: 'age', size: 64));
                $this->fail('A column another tenant stores with another type must be refused');
            } catch (DuplicateException $e) {
                $this->assertSame('Attribute exists in the shared table with another type', $e->getMessage());
            }

            try {
                $database->createAttributes($collection, [Attribute::string(key: 'label', size: 16), Attribute::string(key: 'age', size: 64)]);
                $this->fail('A batch holding a column another tenant stores with another type must be refused');
            } catch (DuplicateException $e) {
                $this->assertSame('Attribute exists in the shared table with another type', $e->getMessage());
            }

            $stored = $database->getCollection($collection);
            $this->assertSame([], $stored->attributes());

            $database->createAttribute($collection, Attribute::integer(key: 'age'));
            $stored = $database->getCollection($collection);
            $this->assertSame(['age'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $stored->attributes()));

            $database->setTenant($first);
            $this->assertSame(7, $database->getDocument($collection, 'first')->getAttribute('age'));
        } finally {
            foreach ([$second, $first] as $tenant) {
                try {
                    $database->setTenant($tenant)->deleteCollection($collection);
                } catch (Throwable) {
                }
            }
            $database->setTenant($originalTenant);
        }
    }

    public function testPostgresSharedTablesReuseAnotherTenantsColumnOfTheSameType(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getSharedTables() || ! $database->getAdapter()->hasFeature(Postgres::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $originalTenant = $database->getTenant();
        $integerTenants = $database->getIdAttributeType() === ColumnType::Integer;
        $tenants = $integerTenants ? [411, 412] : ['tenant_411', 'tenant_412'];
        $collection = 'sharedColumnSameType';
        $definition = Collection::create(id: $collection, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);

        try {
            foreach ($tenants as $tenant) {
                $database->setTenant($tenant);
                $database->createCollection($definition);
                $database->createAttribute($collection, Attribute::integer(key: 'age'));
                $database->createAttribute($collection, Attribute::string(key: 'name', size: 64));
                $database->createAttributes($collection, [
                    Attribute::datetime(key: 'seen'),
                    Attribute::string(key: 'bio', size: 20000),
                ]);

                $this->assertSame(['age', 'name', 'seen', 'bio'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $database->getCollection($collection)->attributes()));

                $database->createDocument($collection, new Document([
                    '$id' => 'own',
                    'age' => 7,
                    'name' => 'tenant '.$tenant,
                    'seen' => '2024-05-06T07:08:09.123+00:00',
                    'bio' => 'about '.$tenant,
                ]));
            }

            foreach ($tenants as $tenant) {
                $database->setTenant($tenant);
                $document = $database->getDocument($collection, 'own');
                $this->assertSame(7, $document->getAttribute('age'));
                $this->assertSame('tenant '.$tenant, $document->getAttribute('name'));
                $this->assertSame('2024-05-06T07:08:09.123+00:00', $document->getAttribute('seen'));
                $this->assertSame('about '.$tenant, $document->getAttribute('bio'));
            }
        } finally {
            foreach (\array_reverse($tenants) as $tenant) {
                try {
                    $database->setTenant($tenant)->deleteCollection($collection);
                } catch (Throwable) {
                }
            }
            $database->setTenant($originalTenant);
        }
    }
}
