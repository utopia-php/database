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
        /** @var Database $database */
        $database = static::getDatabase();

        $this->assertEquals(true, $database->ping());
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

        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection('global-timeouts');

        $this->assertEquals(
            true,
            $database->createAttribute(
                collection: 'global-timeouts',
                id: 'longtext',
                type: Database::VAR_STRING,
                size: 100000000,
                required: true
            )
        );

        for ($i = 0; $i < 20; $i++) {
            $database->createDocument('global-timeouts', new Document([
                'longtext' => file_get_contents(__DIR__ . '/../../../resources/longtext.txt'),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any())
                ]
            ]));
        }

        $database->setTimeout(1);

        try {
            $database->find('global-timeouts', [
                Query::notEqual('longtext', 'appwrite'),
            ]);
            $this->fail('Failed to throw exception');
        } catch (\Exception $e) {
            $database->clearTimeout();
            $database->deleteCollection('global-timeouts');
            $this->assertInstanceOf(TimeoutException::class, $e);
        }
    }



    public function testPreserveDatesUpdate(): void
    {
        Authorization::disable();

        /** @var Database $database */
        $database = static::getDatabase();

        $database->setPreserveDates(true);

        $database->createCollection('preserve_update_dates');

        $database->createAttribute('preserve_update_dates', 'attr1', Database::VAR_STRING, 10, false);

        $doc1 = $database->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'attr1' => 'value1',
        ]));

        $doc2 = $database->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc2',
            '$permissions' => [],
            'attr1' => 'value2',
        ]));

        $doc3 = $database->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc3',
            '$permissions' => [],
            'attr1' => 'value3',
        ]));

        $newDate = '2000-01-01T10:00:00.000+00:00';

        $doc1->setAttribute('$updatedAt', $newDate);
        $doc1 = $database->updateDocument('preserve_update_dates', 'doc1', $doc1);
        $this->assertEquals($newDate, $doc1->getAttribute('$updatedAt'));
        $doc1 = $database->getDocument('preserve_update_dates', 'doc1');
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

        $doc2 = $database->getDocument('preserve_update_dates', 'doc2');
        $doc3 = $database->getDocument('preserve_update_dates', 'doc3');
        $this->assertEquals($newDate, $doc2->getAttribute('$updatedAt'));
        $this->assertEquals($newDate, $doc3->getAttribute('$updatedAt'));

        $database->deleteCollection('preserve_update_dates');

        $database->setPreserveDates(false);

        Authorization::reset();
    }

    public function testPreserveDatesCreate(): void
    {
        Authorization::disable();

        /** @var Database $database */
        $database = static::getDatabase();

        $database->setPreserveDates(true);

        $database->createCollection('preserve_create_dates');

        $database->createAttribute('preserve_create_dates', 'attr1', Database::VAR_STRING, 10, false);

        $date = '2000-01-01T10:00:00.000+00:00';

        $database->createDocument('preserve_create_dates', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'attr1' => 'value1',
            '$createdAt' => $date
        ]));

        $database->createDocuments('preserve_create_dates', [
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

        $doc1 = $database->getDocument('preserve_create_dates', 'doc1');
        $doc2 = $database->getDocument('preserve_create_dates', 'doc2');
        $doc3 = $database->getDocument('preserve_create_dates', 'doc3');
        $this->assertEquals($date, $doc1->getAttribute('$createdAt'));
        $this->assertEquals($date, $doc2->getAttribute('$createdAt'));
        $this->assertEquals($date, $doc3->getAttribute('$createdAt'));

        $database->deleteCollection('preserve_create_dates');

        $database->setPreserveDates(false);

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

        /** @var Database $database */
        $database = static::getDatabase();

        $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($document)
        ]);
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

        if ($database->getAdapter()->getSupportForUpserts()) {
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
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForCacheSkipOnFailure()) {
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
