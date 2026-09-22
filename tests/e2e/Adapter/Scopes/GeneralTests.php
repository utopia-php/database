<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Redis;
use Throwable;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
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
use Utopia\Database\Index;
use Utopia\Database\Query;

trait GeneralTests
{
    public function testPing(): void
    {
        $this->assertEquals(true, $this->getDatabase()->ping());
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
        if (! ($this->getDatabase()->getAdapter()->hasFeature(Feature\Timeouts::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'global-timeouts'));

        $this->assertEquals(
            true,
            $database->createAttribute('global-timeouts', Attribute::string(key: 'longtext', size: 100000000, required: true))
        );

        for ($i = 0; $i < 20; $i++) {
            $database->createDocument('global-timeouts', new Document([
                'longtext' => file_get_contents(__DIR__.'/../../../resources/longtext.txt'),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
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

    public function testSharedTablesUpdateTenant(): void
    {
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

        try {
            $database->createCollection(new Collection(id: __FUNCTION__, documentSecurity: false));

            $database
                ->setTenant(1)
                ->updateDocument(Database::METADATA, __FUNCTION__, new Document([
                    '$id' => __FUNCTION__,
                    'name' => 'Scooby Doo',
                ]));

            $database->setTenant(null);
            $database->purgeCachedDocument(Database::METADATA, __FUNCTION__);
            $doc = $database->getDocument(Database::METADATA, __FUNCTION__);

            $this->assertFalse($doc->isEmpty());
            $this->assertEquals(__FUNCTION__, $doc->getId());
        } finally {
            $database->setTenant(null)->setSharedTables(false);
            if ($database->exists($sharedTablesDb)) {
                $database->delete($sharedTablesDb);
            }
            $database
                ->setSharedTables($sharedTables)
                ->setTenant($tenant)
                ->setNamespace($namespace)
                ->setDatabase($schema);
        }
    }

    public function testSharedTablesTenantPerDocument(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $sharedTables = $database->getSharedTables();
        $tenantPerDocument = $database->getTenantPerDocument();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();
        $tenant = $database->getTenant();

        if (! $database->getAdapter()->supports(Capability::Schemas)) {
            $this->markTestSkipped('Tenant per document needs a schema to hold the shared table');
        }

        $tenantPerDocDb = 'sharedTablesTenantPerDocument_'.static::getTestToken();

        if ($database->exists($tenantPerDocDb)) {
            $database->delete($tenantPerDocDb);
        }

        $database
            ->setDatabase($tenantPerDocDb)
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant(null)
            ->create();

        try {
            // Create collection
            $database->createCollection(new Collection(id: __FUNCTION__, permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ], documentSecurity: false));

            $database->createAttribute(__FUNCTION__, Attribute::string(key: 'name', size: 100));
            $database->createIndex(__FUNCTION__, Index::key(key: 'nameIndex', attributes: ['name']));

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
            $doc1CreatedAt = $doc->getCreatedAt();

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

            // Selecting no tenant has to scope a read to no tenant rather than to every
            // tenant: this collection's own metadata row is tenantless, so nothing above
            // the document read is left to keep one tenant out of another's rows.
            $database->setTenant(null)->setTenantPerDocument(true);

            $this->assertCount(0, $database->find(__FUNCTION__));
            $this->assertSame(0, $database->count(__FUNCTION__));
            $this->assertTrue($database->getDocument(__FUNCTION__, $doc1Id)->isEmpty());

            if ($database->getAdapter()->hasFeature(Feature\Upserts::class)) {
                // An upsert has to recognise a row that createDocument() wrote, not shadow it
                // with a second one: a duplicate moves $createdAt and is checked against
                // create permission rather than update permission.
                $database
                    ->setTenant(null)
                    ->setTenantPerDocument(true)
                    ->upsertDocuments(__FUNCTION__, [new Document([
                        '$id' => $doc1Id,
                        '$tenant' => 1,
                        'name' => 'Spiderman revised',
                    ])]);

                $documents = $database
                    ->setTenantPerDocument(false)
                    ->setTenant(1)
                    ->find(__FUNCTION__);

                $this->assertCount(1, $documents);
                $this->assertSame('Spiderman revised', $documents[0]->getAttribute('name'));
                $this->assertSame($doc1CreatedAt, $documents[0]->getCreatedAt());

                // Test upsert with tenant per doc
                $doc3Id = ID::unique();
                $database
                    ->setTenant(null)
                    ->setTenantPerDocument(true)
                    ->upsertDocuments(__FUNCTION__, [new Document([
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

                // Upsert new documents with different tenants. The sequence lookup binds one
                // placeholder per distinct tenant, so a cross-tenant batch has to keep each
                // tenant's value at the position its placeholder was named for -- collected here
                // because $onNext is the only way these documents reach the caller.
                $doc4Id = ID::unique();
                $doc5Id = ID::unique();
                $sequences = [];
                $database
                    ->setTenant(null)
                    ->setTenantPerDocument(true)
                    ->upsertDocuments(
                        __FUNCTION__,
                        [new Document([
                            '$id' => $doc4Id,
                            '$tenant' => 4,
                            'name' => 'Superman4',
                        ]), new Document([
                            '$id' => $doc5Id,
                            '$tenant' => 5,
                            'name' => 'Superman5',
                        ])],
                        onNext: function (Document $document) use (&$sequences) {
                            $sequences[$document->getId()] = $document->getSequence();
                        }
                    );

                $this->assertCount(2, $sequences);
                $this->assertNotEmpty($sequences[$doc4Id]);
                $this->assertNotEmpty($sequences[$doc5Id]);

                // Set to tenant 4 and read
                $doc = $database
                    ->setTenantPerDocument(false)
                    ->setTenant(4)
                    ->getDocument(__FUNCTION__, $doc4Id);

                $this->assertEquals('Superman4', $doc['name']);
                $this->assertEquals(4, $doc->getTenant());
                $this->assertEquals($doc->getSequence(), $sequences[$doc4Id]);

                // Set to tenant 5 and read
                $doc = $database
                    ->setTenantPerDocument(false)
                    ->setTenant(5)
                    ->getDocument(__FUNCTION__, $doc5Id);

                $this->assertEquals('Superman5', $doc['name']);
                $this->assertEquals(5, $doc->getTenant());
                $this->assertEquals($doc->getSequence(), $sequences[$doc5Id]);

                // Update names via upsert
                $database
                    ->setTenant(null)
                    ->setTenantPerDocument(true)
                    ->upsertDocuments(__FUNCTION__, [new Document([
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
        } finally {
            $database
                ->setSharedTables($sharedTables)
                ->setTenantPerDocument($tenantPerDocument)
                ->setTenant($tenant)
                ->setNamespace($namespace)
                ->setDatabase($schema);
        }
    }

    public function testSharedTablesReadsScopeToTheSelectedTenant(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getSharedTables()) {
            $this->markTestSkipped('Reads are only tenant scoped when tables are shared');
        }

        $tenant = $database->getTenant();
        $tenantPerDocument = $database->getTenantPerDocument();
        $collection = 'sharedTablesTenantScopedReads';

        try {
            // A collection whose own metadata row is tenantless, the way a shared pool
            // holds one definition for every tenant on it. The collection lookup then has
            // no tenant to refuse on, so the document read is the only thing keeping one
            // tenant out of another's rows.
            $database->setTenant(null)->setTenantPerDocument(true);

            $database->createCollection(new Collection(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 128, required: true)],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                ],
                documentSecurity: false,
            ));

            $database->createDocument($collection, new Document([
                Document::ID => 'one',
                Document::TENANT => 1,
                'name' => 'tenant one',
            ]));
            $database->createDocument($collection, new Document([
                Document::ID => 'two',
                Document::TENANT => 2,
                'name' => 'tenant two',
            ]));

            $database->setTenantPerDocument(false)->setTenant(1);

            $this->assertSame(
                ['one'],
                \array_map(fn (Document $document) => $document->getId(), $database->find($collection))
            );
            $this->assertSame(1, $database->count($collection));
            $this->assertTrue($database->getDocument($collection, 'two')->isEmpty());

            $database->setTenant(null)->setTenantPerDocument(true);

            $this->assertCount(0, $database->find($collection));
            $this->assertSame(0, $database->count($collection));
            $this->assertTrue($database->getDocument($collection, 'one')->isEmpty());
        } finally {
            $database->setTenant($tenant)->setTenantPerDocument($tenantPerDocument);
        }
    }

    public function testCacheFallbackOnFailure(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::CacheSkipOnFailure)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'cacheFallback_'.uniqid();

        $database->createCollection(new Collection(id: $collection, attributes: [
            Attribute::string(key: 'title', size: 128, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));

        $database->createDocument($collection, new Document([
            '$id' => 'doc1',
            'title' => 'hello',
        ]));

        $this->assertCount(1, $database->find($collection));

        $brokenRedis = $this->createStub(\Redis::class);
        $brokenRedis->method('get')->willThrowException(new \RedisException('gone'));
        $brokenRedis->method('set')->willThrowException(new \RedisException('gone'));
        $brokenRedis->method('del')->willThrowException(new \RedisException('gone'));
        $brokenRedis->method('expire')->willThrowException(new \RedisException('gone'));

        $brokenAdapter = new \Utopia\Cache\Adapter\Redis($brokenRedis);
        $brokenAdapter->setMaxRetries(0);
        $originalCache = $database->getCache();
        $database->setCache(new \Utopia\Cache\Cache($brokenAdapter));

        $doc = $database->getDocument($collection, 'doc1');
        $this->assertFalse($doc->isEmpty());
        $this->assertEquals('hello', $doc->getAttribute('title'));

        $results = $database->find($collection);
        $this->assertCount(1, $results);

        $database->setCache($originalCache);
        $database->deleteCollection($collection);
    }

    /**
     * Test that withTransaction properly rolls back on failure.
     * With the Pool adapter, this verifies that the entire transaction
     * (start, callback, commit/rollback) runs on a single pinned connection.
     */
    public function testTransactionAtomicity(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'transactionAtomicity'));
        $database->createAttribute('transactionAtomicity', Attribute::string(key: 'title', size: 128, required: true));

        // Verify a successful transaction commits
        $doc = $database->withTransaction(function () use ($database) {
            return $database->createDocument('transactionAtomicity', new Document([
                '$id' => 'tx_success',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'title' => 'Committed',
            ]));
        });
        $this->assertEquals('tx_success', $doc->getId());
        $found = $database->getDocument('transactionAtomicity', 'tx_success');
        $this->assertFalse($found->isEmpty());
        $this->assertEquals('Committed', $found->getAttribute('title'));

        // Verify a failed transaction rolls back completely
        try {
            $database->withTransaction(function () use ($database) {
                $database->createDocument('transactionAtomicity', new Document([
                    '$id' => 'tx_fail',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'title' => 'Should be rolled back',
                ]));

                throw new \Exception('Intentional failure to trigger rollback');
            });
        } catch (\Exception $e) {
            $this->assertEquals('Intentional failure to trigger rollback', $e->getMessage());
        }

        // Document should NOT exist since the transaction was rolled back
        $notFound = $database->getDocument('transactionAtomicity', 'tx_fail');
        $this->assertTrue($notFound->isEmpty(), 'Document should not exist after transaction rollback');

        $database->deleteCollection('transactionAtomicity');
    }

    public function testDocumentCacheEpochStaysBlockedUntilOuterTransactionCommit(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Caching)) {
            $this->markTestSkipped('Adapter does not use the document cache.');
        }

        $collection = 'txDocumentCacheCommit';
        $database->createCollection(new Collection(id: $collection, attributes: [
            Attribute::string(key: 'name', required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        try {
            $database->createDocument($collection, new Document([
                '$id' => 'user',
                'name' => 'original',
            ]));
            $this->assertSame('original', $database->getDocument($collection, 'user')->getAttribute('name'));
            [$collectionKey] = $database->getCacheKeys($collection, 'user');
            $epochKey = $collectionKey.'#epoch';

            $database->withTransaction(function () use ($database, $collection, $epochKey): void {
                $database->updateDocument($collection, 'user', new Document(['name' => 'updated']));
                $epoch = $database->getCache()->load($epochKey, Database::TTL);
                $this->assertIsString($epoch);
                $this->assertStringStartsWith('blocked:', $epoch);
            });

            $epoch = $database->getCache()->load($epochKey, Database::TTL);
            $this->assertIsString($epoch);
            $this->assertStringNotContainsString('blocked:', $epoch);
            $this->assertSame('updated', $database->getDocument($collection, 'user')->getAttribute('name'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testDocumentCacheEpochIsReleasedAfterOuterTransactionRollback(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Caching)) {
            $this->markTestSkipped('Adapter does not use the document cache.');
        }

        $collection = 'txDocumentCacheRollback';
        $database->createCollection(new Collection(id: $collection, attributes: [
            Attribute::string(key: 'name', required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        try {
            $database->createDocument($collection, new Document([
                '$id' => 'user',
                'name' => 'original',
            ]));
            $this->assertSame('original', $database->getDocument($collection, 'user')->getAttribute('name'));
            [$collectionKey] = $database->getCacheKeys($collection, 'user');
            $epochKey = $collectionKey.'#epoch';

            try {
                $database->withTransaction(function () use ($database, $collection, $epochKey): void {
                    $database->updateDocument($collection, 'user', new Document(['name' => 'rolled-back']));
                    $epoch = $database->getCache()->load($epochKey, Database::TTL);
                    $this->assertIsString($epoch);
                    $this->assertStringStartsWith('blocked:', $epoch);

                    throw new ConflictException('rollback');
                });
            } catch (ConflictException) {
            }

            $epoch = $database->getCache()->load($epochKey, Database::TTL);
            $this->assertIsString($epoch);
            $this->assertStringNotContainsString('blocked:', $epoch);
            $this->assertSame('original', $database->getDocument($collection, 'user')->getAttribute('name'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testNestedTransactionKeepsDocumentCacheEpochBlockedUntilOuterCommit(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Caching)) {
            $this->markTestSkipped('Adapter does not use the document cache.');
        }
        if (! $database->getAdapter()->supports(Capability::NestedTransactions)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'txNestedDocumentCache';
        $database->createCollection(new Collection(id: $collection, attributes: [
            Attribute::string(key: 'name', required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        try {
            $database->createDocument($collection, new Document([
                '$id' => 'user',
                'name' => 'original',
            ]));
            $this->assertSame('original', $database->getDocument($collection, 'user')->getAttribute('name'));
            [$collectionKey] = $database->getCacheKeys($collection, 'user');
            $epochKey = $collectionKey.'#epoch';

            $database->withTransaction(function () use ($database, $collection, $epochKey): void {
                $database->withTransaction(function () use ($database, $collection): void {
                    $database->updateDocument($collection, 'user', new Document(['name' => 'updated']));
                });

                $epoch = $database->getCache()->load($epochKey, Database::TTL);
                $this->assertIsString($epoch);
                $this->assertStringStartsWith('blocked:', $epoch);
            });

            $epoch = $database->getCache()->load($epochKey, Database::TTL);
            $this->assertIsString($epoch);
            $this->assertStringNotContainsString('blocked:', $epoch);
            $this->assertSame('updated', $database->getDocument($collection, 'user')->getAttribute('name'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    /**
     * Test that withTransaction correctly resets inTransaction state
     * when a known exception (DuplicateException) is thrown after successful rollback.
     */
    public function testTransactionStateAfterKnownException(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'txKnownException'));
        $database->createAttribute('txKnownException', Attribute::string(key: 'title', size: 128, required: true));

        $database->createDocument('txKnownException', new Document([
            '$id' => 'existing_doc',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'title' => 'Original',
        ]));

        // Trigger a DuplicateException inside withTransaction by inserting a duplicate ID
        try {
            $database->withTransaction(function () use ($database) {
                $database->createDocument('txKnownException', new Document([
                    '$id' => 'existing_doc',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'title' => 'Duplicate',
                ]));
            });
            $this->fail('Expected DuplicateException was not thrown');
        } catch (DuplicateException $e) {
            // Expected
        }

        // inTransaction must be false after the exception
        $this->assertFalse(
            $database->getAdapter()->inTransaction(),
            'Adapter should not be in transaction after DuplicateException'
        );

        // Database should still be functional
        $doc = $database->getDocument('txKnownException', 'existing_doc');
        $this->assertEquals('Original', $doc->getAttribute('title'));

        $database->deleteCollection('txKnownException');
    }

    /**
     * Test that withTransaction correctly resets inTransaction state
     * when retries are exhausted for a generic exception.
     *
     * MongoDB's withTransaction has no retry logic, so this test
     * only applies to SQL-based adapters.
     */
    public function testTransactionStateAfterRetriesExhausted(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::TransactionRetries)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $attempts = 0;

        try {
            $database->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new \RuntimeException('Persistent failure');
            });
        } catch (\RuntimeException $e) {
            $this->assertEquals('Persistent failure', $e->getMessage());
        }

        // Should have attempted 3 times (initial + 2 retries)
        $this->assertEquals(3, $attempts, 'Should have exhausted all retry attempts');

        // inTransaction must be false after retries exhausted
        $this->assertFalse(
            $database->getAdapter()->inTransaction(),
            'Adapter should not be in transaction after retries exhausted'
        );
    }

    /**
     * Test that nested withTransaction calls maintain correct inTransaction state
     * when the inner transaction throws a known exception.
     *
     * MongoDB does not support nested transactions or savepoints, so a duplicate
     * key error inside an inner transaction aborts the entire transaction.
     */
    public function testNestedTransactionState(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::NestedTransactions)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'txNested'));
        $database->createAttribute('txNested', Attribute::string(key: 'title', size: 128, required: true));

        $database->createDocument('txNested', new Document([
            '$id' => 'nested_existing',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'title' => 'Original',
        ]));

        // Outer transaction should succeed even if inner transaction throws
        $database->withTransaction(function () use ($database) {
            $database->createDocument('txNested', new Document([
                '$id' => 'outer_doc',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'title' => 'Outer',
            ]));

            // Inner transaction throws a DuplicateException
            try {
                $database->withTransaction(function () use ($database) {
                    $database->createDocument('txNested', new Document([
                        '$id' => 'nested_existing',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'title' => 'Duplicate',
                    ]));
                });
            } catch (DuplicateException $e) {
                // Caught and handled — outer transaction should continue
            }

            return true;
        });

        // inTransaction must be false after everything completes
        $this->assertFalse(
            $database->getAdapter()->inTransaction(),
            'Adapter should not be in transaction after nested transactions complete'
        );

        // Outer document should have been committed
        $outerDoc = $database->getDocument('txNested', 'outer_doc');
        $this->assertFalse($outerDoc->isEmpty(), 'Outer transaction document should exist');
        $this->assertEquals('Outer', $outerDoc->getAttribute('title'));

        // Original document should be unchanged
        $existingDoc = $database->getDocument('txNested', 'nested_existing');
        $this->assertEquals('Original', $existingDoc->getAttribute('title'));

        $database->deleteCollection('txNested');
    }

    /**
     * Wait for Redis to be ready with a readiness probe
     */

    public function testCacheReconnect(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::CacheSkipOnFailure)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $original = $database->getCache();
        $database->setCache($cache);

        $collection = 'cacheReconnect_'.uniqid();

        try {
            $database->createCollection(new Collection(id: $collection, attributes: [
                Attribute::string(key: 'title', size: 255, required: true),
            ], permissions: [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
            ]));

            $database->createDocument($collection, new Document([
                '$id' => 'reconnect_doc',
                'title' => 'Test Document',
            ]));

            $this->assertSame('Test Document', $database->getDocument($collection, 'reconnect_doc')->getAttribute('title'));

            $this->dropRedisConnection($redis);

            $this->assertTrue((bool) $cache->save('reconnect_probe', 'alive'), 'The cache must reconnect after the server dropped the connection');
            $this->assertSame('alive', $cache->load('reconnect_probe', 60));

            $this->assertSame('Test Document', $database->getDocument($collection, 'reconnect_doc')->getAttribute('title'));

            $database->updateDocument($collection, 'reconnect_doc', new Document([
                '$id' => 'reconnect_doc',
                'title' => 'Updated Title',
            ]));

            $this->assertSame('Updated Title', $database->getDocument($collection, 'reconnect_doc')->getAttribute('title'));
        } finally {
            $database->setCache($original);
            $database->deleteCollection($collection);
        }
    }

    private function dropRedisConnection(Redis $redis): void
    {
        $id = $redis->rawCommand('CLIENT', 'ID');
        $this->assertIsInt($id);

        $killer = new Redis();
        $killer->connect('redis', 6379);
        $killer->rawCommand('CLIENT', 'KILL', 'ID', (string) $id);
        $killer->close();
    }

    public function testCountTimeout(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(Feature\Timeouts::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'count-timeouts'));

        $this->assertTrue($database->createAttribute('count-timeouts', Attribute::string(key: 'longtext', size: 100000000, required: true)));

        $longtext = file_get_contents(__DIR__.'/../../../resources/longtext.txt');
        $this->assertIsString($longtext);

        for ($i = 0; $i < 20; $i++) {
            $database->createDocument('count-timeouts', new Document([
                'longtext' => $longtext,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]));
        }

        try {
            $database->setTimeout(1);

            $thrown = null;
            try {
                $database->count('count-timeouts', [
                    Query::containsString('longtext', ['needle-that-does-not-exist']),
                ]);
            } catch (Exception $e) {
                $thrown = $e;
            }

            $this->assertInstanceOf(TimeoutException::class, $thrown, 'count() must throw a timeout exception');
        } finally {
            $database->clearTimeout();
            $database->deleteCollection('count-timeouts');
        }
    }

    public function testFindOrderByAfterException(): void
    {
        $database = $this->getDatabase();
        $collection = 'cursorCollection_'.uniqid();

        $database->createCollection(new Collection(id: $collection));

        try {
            $database->find($collection, [
                Query::limit(2),
                Query::offset(0),
                Query::cursorAfter(new Document([
                    '$id' => 'cursor',
                    '$sequence' => '1',
                    '$collection' => 'other collection',
                ])),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertSame('cursor Document must be from the same Collection.', $e->getMessage());
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testGetAttributeLimit(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if ($adapter->getLimitForAttributes() === 0) {
            $this->assertSame(0, $database->getLimitForAttributes(), 'An adapter without a column limit reports no limit');

            return;
        }

        $this->assertSame($adapter->getLimitForAttributes() - $adapter->getCountOfDefaultAttributes(), $database->getLimitForAttributes(), 'The limit must leave room for the internal columns');
    }

    public function testGetIndexLimit(): void
    {
        $this->assertSame(58, $this->getDatabase()->getLimitForIndexes());
    }

    public function testGetId(): void
    {
        $this->assertSame(20, strlen(ID::unique()));
        $this->assertSame(13, strlen(ID::unique(0)));
        $this->assertSame(13, strlen(ID::unique(-1)));
        $this->assertSame(23, strlen(ID::unique(10)));

        $this->assertNotSame(ID::unique(10), ID::unique(10));
    }

    public function testNestedQueryValidation(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: __FUNCTION__, attributes: [
            Attribute::string(key: 'name', size: 255, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $database->createDocuments(__FUNCTION__, [
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
            $database->find(__FUNCTION__, [
                Query::or([
                    Query::equal('name', ['test1']),
                    Query::search('name', 'doc'),
                ]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(QueryException::class, $e);
            $this->assertSame('Searching by attribute "name" requires a fulltext index.', $e->getMessage());
        } finally {
            $database->deleteCollection(__FUNCTION__);
        }
    }

    public function testPreserveDatesCreate(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->getAuthorization()->disable();
        $database->setPreserveDates(true);

        try {
            $database->createCollection(new Collection(id: 'preserve_create_dates', attributes: [
                Attribute::string(key: 'attr1', size: 10),
            ]));

            $date = '';

            try {
                $database->createDocument('preserve_create_dates', new Document([
                    '$id' => 'doc1',
                    '$permissions' => [],
                    'attr1' => 'value1',
                    '$createdAt' => $date,
                ]));
                $this->fail('Failed to throw structure exception');
            } catch (Exception $e) {
                $this->assertInstanceOf(StructureException::class, $e);
                $this->assertSame('Invalid document structure: Missing required attribute "$createdAt"', $e->getMessage());
            }

            try {
                $database->createDocuments('preserve_create_dates', [
                    new Document([
                        '$id' => 'doc2',
                        '$permissions' => [],
                        'attr1' => 'value2',
                        '$createdAt' => $date,
                    ]),
                    new Document([
                        '$id' => 'doc3',
                        '$permissions' => [],
                        'attr1' => 'value3',
                        '$createdAt' => $date,
                    ]),
                ], batchSize: 2);
                $this->fail('Failed to throw structure exception');
            } catch (Exception $e) {
                $this->assertInstanceOf(StructureException::class, $e);
                $this->assertSame('Invalid document structure: Missing required attribute "$createdAt"', $e->getMessage());
            }

            $date = '2000-01-01T10:00:00.000+00:00';

            $database->createDocument('preserve_create_dates', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
                'attr1' => 'value1',
                '$createdAt' => $date,
            ]));

            $database->createDocuments('preserve_create_dates', [
                new Document([
                    '$id' => 'doc2',
                    '$permissions' => [],
                    'attr1' => 'value2',
                    '$createdAt' => $date,
                ]),
                new Document([
                    '$id' => 'doc3',
                    '$permissions' => [],
                    'attr1' => 'value3',
                    '$createdAt' => $date,
                ]),
                new Document([
                    '$id' => 'doc4',
                    '$permissions' => [],
                    'attr1' => 'value3',
                    '$createdAt' => null,
                ]),
                new Document([
                    '$id' => 'doc5',
                    '$permissions' => [],
                    'attr1' => 'value3',
                ]),
            ], batchSize: 2);

            $doc1 = $database->getDocument('preserve_create_dates', 'doc1');
            $doc2 = $database->getDocument('preserve_create_dates', 'doc2');
            $doc3 = $database->getDocument('preserve_create_dates', 'doc3');
            $doc4 = $database->getDocument('preserve_create_dates', 'doc4');
            $doc5 = $database->getDocument('preserve_create_dates', 'doc5');
            $this->assertSame($date, $doc1->getCreatedAt());
            $this->assertSame($date, $doc2->getCreatedAt());
            $this->assertSame($date, $doc3->getCreatedAt());
            $this->assertNotEmpty($doc4->getCreatedAt());
            $this->assertNotSame($date, $doc4->getCreatedAt(), 'A null date is replaced by the current time');
            $this->assertNotEmpty($doc5->getCreatedAt());
            $this->assertNotSame($date, $doc5->getCreatedAt(), 'A missing date is replaced by the current time');
        } finally {
            $database->deleteCollection('preserve_create_dates');
            $database->setPreserveDates(false);
            $database->getAuthorization()->reset();
        }
    }

    public function testPreserveDatesUpdate(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->getAuthorization()->disable();
        $database->setPreserveDates(true);

        try {
            $database->createCollection(new Collection(id: 'preserve_update_dates', attributes: [
                Attribute::string(key: 'attr1', size: 10),
            ]));

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

            try {
                $doc1->setAttribute('$updatedAt', '');
                $database->updateDocument('preserve_update_dates', 'doc1', $doc1);
                $this->fail('Failed to throw structure exception');
            } catch (Exception $e) {
                $this->assertInstanceOf(StructureException::class, $e);
                $this->assertSame('Invalid document structure: Missing required attribute "$updatedAt"', $e->getMessage());
            }

            try {
                $database->updateDocuments(
                    'preserve_update_dates',
                    new Document([
                        '$updatedAt' => '',
                    ]),
                    [
                        Query::equal('$id', [
                            $doc2->getId(),
                            $doc3->getId(),
                        ]),
                    ]
                );
                $this->fail('Failed to throw structure exception');
            } catch (Exception $e) {
                $this->assertInstanceOf(StructureException::class, $e);
                $this->assertSame('Invalid document structure: Missing required attribute "$updatedAt"', $e->getMessage());
            }

            $newDate = '2000-01-01T10:00:00.000+00:00';

            $doc1->setAttribute('$updatedAt', $newDate);
            $doc1 = $database->updateDocument('preserve_update_dates', 'doc1', $doc1);
            $this->assertSame($newDate, $doc1->getUpdatedAt());
            $doc1 = $database->getDocument('preserve_update_dates', 'doc1');
            $this->assertSame($newDate, $doc1->getUpdatedAt());

            $database->updateDocuments(
                'preserve_update_dates',
                new Document([
                    '$updatedAt' => $newDate,
                ]),
                [
                    Query::equal('$id', [
                        $doc2->getId(),
                        $doc3->getId(),
                    ]),
                ]
            );

            $doc2 = $database->getDocument('preserve_update_dates', 'doc2');
            $doc3 = $database->getDocument('preserve_update_dates', 'doc3');
            $this->assertSame($newDate, $doc2->getUpdatedAt());
            $this->assertSame($newDate, $doc3->getUpdatedAt());
        } finally {
            $database->deleteCollection('preserve_update_dates');
            $database->setPreserveDates(false);
            $database->getAuthorization()->reset();
        }
    }
}
