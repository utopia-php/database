<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Throwable;
use Utopia\CLI\Console;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

trait GeneralTests
{
    public function testPing(): void
    {
        $this->assertEquals(true, static::getDatabase()->ping());
    }

    /**
     * @throws AuthorizationException
     * @throws DuplicateException
     * @throws ConflictException
     * @throws LimitException
     * @throws StructureException
     * @throws DatabaseException
     */
    public function testQueryTimeout(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForTimeouts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('global-timeouts');

        $this->assertEquals(
            true,
            static::getDatabase()->createAttribute(
                collection: 'global-timeouts',
                id: 'longtext',
                type: Database::VAR_STRING,
                size: 100000000,
                required: true
            )
        );

        for ($i = 0; $i < 20; $i++) {
            static::getDatabase()->createDocument('global-timeouts', new Document([
                'longtext' => file_get_contents(__DIR__ . '/../../../resources/longtext.txt'),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any())
                ]
            ]));
        }

        static::getDatabase()->setTimeout(1);

        try {
            static::getDatabase()->find('global-timeouts', [
                Query::notEqual('longtext', 'appwrite'),
            ]);
            $this->fail('Failed to throw exception');
        } catch (\Exception $e) {
            static::getDatabase()->clearTimeout();
            static::getDatabase()->deleteCollection('global-timeouts');
            $this->assertInstanceOf(TimeoutException::class, $e);
        }
    }


    public function testCreatedAtUpdatedAt(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('created_at'));
        static::getDatabase()->createAttribute('created_at', 'title', Database::VAR_STRING, 100, false);
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


    public function testPreserveDatesUpdate(): void
    {
        Authorization::disable();

        static::getDatabase()->setPreserveDates(true);

        static::getDatabase()->createCollection('preserve_update_dates');

        static::getDatabase()->createAttribute('preserve_update_dates', 'attr1', Database::VAR_STRING, 10, false);

        $doc1 = static::getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'attr1' => 'value1',
        ]));

        $doc2 = static::getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc2',
            '$permissions' => [],
            'attr1' => 'value2',
        ]));

        $doc3 = static::getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc3',
            '$permissions' => [],
            'attr1' => 'value3',
        ]));

        $newDate = '2000-01-01T10:00:00.000+00:00';

        $doc1->setAttribute('$updatedAt', $newDate);
        $doc1 = static::getDatabase()->updateDocument('preserve_update_dates', 'doc1', $doc1);
        $this->assertEquals($newDate, $doc1->getAttribute('$updatedAt'));
        $doc1 = static::getDatabase()->getDocument('preserve_update_dates', 'doc1');
        $this->assertEquals($newDate, $doc1->getAttribute('$updatedAt'));

        $this->getDatabase()->updateDocuments(
            'preserve_update_dates',
            new Document([
                '$updatedAt' => $newDate
            ]),
            [
                Query::equal('$id', [
                    $doc2->getId(),
                    $doc3->getId()
                ])
            ]
        );

        $doc2 = static::getDatabase()->getDocument('preserve_update_dates', 'doc2');
        $doc3 = static::getDatabase()->getDocument('preserve_update_dates', 'doc3');
        $this->assertEquals($newDate, $doc2->getAttribute('$updatedAt'));
        $this->assertEquals($newDate, $doc3->getAttribute('$updatedAt'));

        static::getDatabase()->deleteCollection('preserve_update_dates');

        static::getDatabase()->setPreserveDates(false);

        Authorization::reset();
    }

    public function testPreserveDatesCreate(): void
    {
        Authorization::disable();

        static::getDatabase()->setPreserveDates(true);

        static::getDatabase()->createCollection('preserve_create_dates');

        static::getDatabase()->createAttribute('preserve_create_dates', 'attr1', Database::VAR_STRING, 10, false);

        $date = '2000-01-01T10:00:00.000+00:00';

        static::getDatabase()->createDocument('preserve_create_dates', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'attr1' => 'value1',
            '$createdAt' => $date
        ]));

        static::getDatabase()->createDocuments('preserve_create_dates', [
            new Document([
                '$id' => 'doc2',
                '$permissions' => [],
                'attr1' => 'value2',
                '$createdAt' => $date
            ]),
            new Document([
                '$id' => 'doc3',
                '$permissions' => [],
                'attr1' => 'value3',
                '$createdAt' => $date
            ]),
        ], batchSize: 2);

        $doc1 = static::getDatabase()->getDocument('preserve_create_dates', 'doc1');
        $doc2 = static::getDatabase()->getDocument('preserve_create_dates', 'doc2');
        $doc3 = static::getDatabase()->getDocument('preserve_create_dates', 'doc3');
        $this->assertEquals($date, $doc1->getAttribute('$createdAt'));
        $this->assertEquals($date, $doc2->getAttribute('$createdAt'));
        $this->assertEquals($date, $doc3->getAttribute('$createdAt'));

        static::getDatabase()->deleteCollection('preserve_create_dates');

        static::getDatabase()->setPreserveDates(false);

        Authorization::reset();
    }

    public function testGetAttributeLimit(): void
    {
        $this->assertIsInt($this->getDatabase()->getLimitForAttributes());
    }
    public function testGetIndexLimit(): void
    {
        $this->assertEquals(58, $this->getDatabase()->getLimitForIndexes());
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

    public function testSharedTablesUpdateTenant(): void
    {
        $database = static::getDatabase();
        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();

        if (!$database->getAdapter()->getSupportForSchemas()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if ($database->exists('sharedTables')) {
            $database->setDatabase('sharedTables')->delete();
        }

        $database
            ->setDatabase('sharedTables')
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant(null)
            ->create();

        // Create collection
        $database->createCollection(__FUNCTION__, documentSecurity: false);

        $database
            ->setTenant(1)
            ->updateDocument(Database::METADATA, __FUNCTION__, new Document([
                '$id' => __FUNCTION__,
                'name' => 'Scooby Doo',
            ]));

        // Ensure tenant was not swapped
        $doc = $database
            ->setTenant(null)
            ->getDocument(Database::METADATA, __FUNCTION__);

        $this->assertEquals('Scooby Doo', $doc['name']);

        // Reset state
        $database
            ->setSharedTables($sharedTables)
            ->setNamespace($namespace)
            ->setDatabase($schema);
    }

    /**
     * @depends testCreatedAtUpdatedAt
     */
    public function testCreatedAtUpdatedAtAssert(): void
    {
        $document = static::getDatabase()->getDocument('created_at', 'uid123');
        $this->assertEquals(true, !$document->isEmpty());
        sleep(1);
        $document->setAttribute('title', 'new title');
        static::getDatabase()->updateDocument('created_at', 'uid123', $document);
        $document = static::getDatabase()->getDocument('created_at', 'uid123');

        $this->assertGreaterThan($document->getCreatedAt(), $document->getUpdatedAt());
        $this->expectException(DuplicateException::class);

        static::getDatabase()->createCollection('created_at');
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
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_ATTRIBUTE_CREATE,
                Database::EVENT_ATTRIBUTE_UPDATE,
                Database::EVENT_INDEX_CREATE,
                Database::EVENT_DOCUMENT_CREATE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_UPDATE,
                Database::EVENT_DOCUMENT_READ,
                Database::EVENT_DOCUMENT_FIND,
                Database::EVENT_DOCUMENT_FIND,
                Database::EVENT_DOCUMENT_COUNT,
                Database::EVENT_DOCUMENT_SUM,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_INCREASE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_DECREASE,
                Database::EVENT_DOCUMENTS_CREATE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENTS_UPDATE,
                Database::EVENT_INDEX_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENTS_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_ATTRIBUTE_DELETE,
                Database::EVENT_COLLECTION_DELETE,
                Database::EVENT_DATABASE_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_DOCUMENTS_DELETE,
                Database::EVENT_DOCUMENT_PURGE,
                Database::EVENT_ATTRIBUTE_DELETE,
                Database::EVENT_COLLECTION_DELETE,
                Database::EVENT_DATABASE_DELETE
            ];

            $database->on(Database::EVENT_ALL, 'test', function ($event, $data) use (&$events) {
                $shifted = array_shift($events);
                $this->assertEquals($shifted, $event);
            });

            if ($this->getDatabase()->getAdapter()->getSupportForSchemas()) {
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

            $executed = false;
            $database->on(Database::EVENT_ALL, 'should-not-execute', function ($event, $data) use (&$executed) {
                $executed = true;
            });

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

            // Remove all listeners
            $database->on(Database::EVENT_ALL, 'test', null);
            $database->on(Database::EVENT_ALL, 'should-not-execute', null);
        });
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

    public function testTransformations(): void
    {
        static::getDatabase()->createCollection('docs', attributes: [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 767,
                'required' => true,
            ])
        ]);

        static::getDatabase()->createDocument('docs', new Document([
            '$id' => 'doc1',
            'name' => 'value1',
        ]));

        static::getDatabase()->before(Database::EVENT_DOCUMENT_READ, 'test', function (string $query) {
            return "SELECT 1";
        });

        $result = static::getDatabase()->getDocument('docs', 'doc1');

        $this->assertTrue($result->isEmpty());
    }

    public function testEnableDisableValidation(): void
    {
        $database = static::getDatabase();

        $database->createCollection('validation', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $database->createAttribute(
            'validation',
            'name',
            Database::VAR_STRING,
            10,
            false
        );

        $database->createDocument('validation', new Document([
            '$id' => 'docwithmorethan36charsasitsidentifier',
            'name' => 'value1',
        ]));

        try {
            $database->find('validation', queries: [
                Query::equal('$id', ['docwithmorethan36charsasitsidentifier']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
        }

        $database->disableValidation();

        $database->find('validation', queries: [
            Query::equal('$id', ['docwithmorethan36charsasitsidentifier']),
        ]);

        $database->enableValidation();

        try {
            $database->find('validation', queries: [
                Query::equal('$id', ['docwithmorethan36charsasitsidentifier']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
        }

        $database->skipValidation(function () use ($database) {
            $database->find('validation', queries: [
                Query::equal('$id', ['docwithmorethan36charsasitsidentifier']),
            ]);
        });
    }

    public function testNestedQueryValidation(): void
    {
        $this->getDatabase()->createCollection(__FUNCTION__, [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'size' => 255,
                'required' => true,
            ])
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $this->getDatabase()->createDocuments(__FUNCTION__, [
            new Document([
                '$id' => ID::unique(),
                'name' => 'test1',
            ]),
            new Document([
                '$id' => ID::unique(),
                'name' => 'doc2',
            ]),
        ]);

        try {
            $this->getDatabase()->find(__FUNCTION__, [
                Query::or([
                    Query::equal('name', ['test1']),
                    Query::search('name', 'doc'),
                ])
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(QueryException::class, $e);
            $this->assertEquals('Searching by attribute "name" requires a fulltext index.', $e->getMessage());
        }
    }

    public function testEmptyOperatorValues(): void
    {
        try {
            static::getDatabase()->findOne('documents', [
                Query::equal('string', []),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals('Invalid query: Equal queries require at least one value.', $e->getMessage());
        }

        try {
            static::getDatabase()->findOne('documents', [
                Query::contains('string', []),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals('Invalid query: Contains queries require at least one value.', $e->getMessage());
        }
    }

    public function testEmptyTenant(): void
    {
        if (static::getDatabase()->getAdapter()->getSharedTables()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $documents = static::getDatabase()->find(
            'documents',
            [Query::notEqual('$id', '56000')] // Mongo bug with Integer UID
        );

        $document = $documents[0];
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);

        $document = static::getDatabase()->getDocument('documents', $document->getId());
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);

        $document = static::getDatabase()->updateDocument('documents', $document->getId(), $document);
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);
    }

    public function testSharedTablesTenantPerDocument(): void
    {
        $database = static::getDatabase();
        $sharedTables = $database->getSharedTables();
        $tenantPerDocument = $database->getTenantPerDocument();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();

        if (!$database->getAdapter()->getSupportForSchemas()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if ($database->exists(__FUNCTION__)) {
            $database->delete(__FUNCTION__);
        }

        $database
            ->setDatabase(__FUNCTION__)
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant(null)
            ->create();

        // Create collection
        $database->createCollection(__FUNCTION__, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ], documentSecurity: false);

        $database->createAttribute(__FUNCTION__, 'name', Database::VAR_STRING, 100, false);
        $database->createIndex(__FUNCTION__, 'nameIndex', Database::INDEX_KEY, ['name']);

        $doc1Id = ID::unique();

        // Create doc for tenant 1
        $database
            ->setTenant(null)
            ->setTenantPerDocument(true)
            ->createDocument(__FUNCTION__, new Document([
                '$id' => $doc1Id,
                '$tenant' => 1,
                'name' => 'Spiderman',
            ]));

        // Set to tenant 1 and read
        $doc = $database
            ->setTenantPerDocument(false)
            ->setTenant(1)
            ->getDocument(__FUNCTION__, $doc1Id);

        $this->assertEquals('Spiderman', $doc['name']);

        $doc2Id = ID::unique();

        // Create doc for tenant 2
        $database
            ->setTenant(null)
            ->setTenantPerDocument(true)
            ->createDocument(__FUNCTION__, new Document([
                '$id' => $doc2Id,
                '$tenant' => 2,
                'name' => 'Batman',
            ]));

        // Set to tenant 2 and read
        $doc = $database
            ->setTenantPerDocument(false)
            ->setTenant(2)
            ->getDocument(__FUNCTION__, $doc2Id);

        $this->assertEquals('Batman', $doc['name']);
        $this->assertEquals(2, $doc->getTenant());

        // Ensure no read cross-tenant
        $docs = $database
            ->setTenantPerDocument(false)
            ->setTenant(1)
            ->find(__FUNCTION__);

        $this->assertEquals(1, \count($docs));
        $this->assertEquals($doc1Id, $docs[0]->getId());

        if (static::getDatabase()->getAdapter()->getSupportForUpserts()) {
            // Test upsert with tenant per doc
            $doc3Id = ID::unique();
            $database
                ->setTenant(null)
                ->setTenantPerDocument(true)
                ->createOrUpdateDocuments(__FUNCTION__, [new Document([
                    '$id' => $doc3Id,
                    '$tenant' => 3,
                    'name' => 'Superman3',
                ])]);

            // Set to tenant 3 and read
            $doc = $database
                ->setTenantPerDocument(false)
                ->setTenant(3)
                ->getDocument(__FUNCTION__, $doc3Id);

            $this->assertEquals('Superman3', $doc['name']);
            $this->assertEquals(3, $doc->getTenant());
            $this->assertEquals($doc3Id, $doc->getId());

            // Test no read from other tenants
            $docs = $database
                ->setTenantPerDocument(false)
                ->setTenant(1)
                ->find(__FUNCTION__);

            $this->assertEquals(1, \count($docs));

            // Ensure no cross-tenant read from upsert
            $doc = $database
                ->setTenant(1)
                ->setTenantPerDocument(false)
                ->getDocument(__FUNCTION__, $doc3Id);

            $this->assertEquals(true, $doc->isEmpty());

            // Upsert new documents with different tenants
            $doc4Id = ID::unique();
            $doc5Id = ID::unique();
            $database
                ->setTenant(null)
                ->setTenantPerDocument(true)
                ->createOrUpdateDocuments(__FUNCTION__, [new Document([
                    '$id' => $doc4Id,
                    '$tenant' => 4,
                    'name' => 'Superman4',
                ]), new Document([
                    '$id' => $doc5Id,
                    '$tenant' => 5,
                    'name' => 'Superman5',
                ])]);

            // Set to tenant 4 and read
            $doc = $database
                ->setTenantPerDocument(false)
                ->setTenant(4)
                ->getDocument(__FUNCTION__, $doc4Id);

            $this->assertEquals('Superman4', $doc['name']);
            $this->assertEquals(4, $doc->getTenant());

            // Set to tenant 5 and read
            $doc = $database
                ->setTenantPerDocument(false)
                ->setTenant(5)
                ->getDocument(__FUNCTION__, $doc5Id);

            $this->assertEquals('Superman5', $doc['name']);
            $this->assertEquals(5, $doc->getTenant());

            // Update names via upsert
            $database
                ->setTenant(null)
                ->setTenantPerDocument(true)
                ->createOrUpdateDocuments(__FUNCTION__, [new Document([
                    '$id' => $doc4Id,
                    '$tenant' => 4,
                    'name' => 'Superman4 updated',
                ]), new Document([
                    '$id' => $doc5Id,
                    '$tenant' => 5,
                    'name' => 'Superman5 updated',
                ])]);

            // Set to tenant 4 and read
            $doc = $database
                ->setTenantPerDocument(false)
                ->setTenant(4)
                ->getDocument(__FUNCTION__, $doc4Id);

            $this->assertEquals('Superman4 updated', $doc['name']);
            $this->assertEquals(4, $doc->getTenant());

            // Set to tenant 5 and read
            $doc = $database
                ->setTenantPerDocument(false)
                ->setTenant(5)
                ->getDocument(__FUNCTION__, $doc5Id);

            $this->assertEquals('Superman5 updated', $doc['name']);
            $this->assertEquals(5, $doc->getTenant());
        }

        // Reset instance
        $database
            ->setSharedTables($sharedTables)
            ->setTenantPerDocument($tenantPerDocument)
            ->setNamespace($namespace)
            ->setDatabase($schema);
    }


    public function testCacheFallback(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForCacheSkipOnFailure()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());
        $database = static::getDatabase();

        // Write mock data
        $database->createCollection('testRedisFallback', attributes: [
            new Document([
                '$id' => ID::custom('string'),
                'type' => Database::VAR_STRING,
                'size' => 767,
                'required' => true,
            ])
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $database->createDocument('testRedisFallback', new Document([
            '$id' => 'doc1',
            'string' => 'text📝',
        ]));

        $database->createIndex('testRedisFallback', 'index1', Database::INDEX_KEY, ['string']);
        $this->assertCount(1, $database->find('testRedisFallback', [Query::equal('string', ['text📝'])]));

        // Bring down Redis
        $stdout = '';
        $stderr = '';
        Console::execute('docker ps -a --filter "name=utopia-redis" --format "{{.Names}}" | xargs -r docker stop', "", $stdout, $stderr);

        // Check we can read data still
        $this->assertCount(1, $database->find('testRedisFallback', [Query::equal('string', ['text📝'])]));
        $this->assertFalse(($database->getDocument('testRedisFallback', 'doc1'))->isEmpty());

        // Check we cannot modify data
        try {
            $database->updateDocument('testRedisFallback', 'doc1', new Document([
                'string' => 'text📝 updated',
            ]));
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertEquals('Redis server redis:6379 went away', $e->getMessage());
        }

        try {
            $database->deleteDocument('testRedisFallback', 'doc1');
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertEquals('Redis server redis:6379 went away', $e->getMessage());
        }

        // Bring backup Redis
        Console::execute('docker ps -a --filter "name=utopia-redis" --format "{{.Names}}" | xargs -r docker start', "", $stdout, $stderr);
        sleep(5);

        $this->assertCount(1, $database->find('testRedisFallback', [Query::equal('string', ['text📝'])]));
    }



}
