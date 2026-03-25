<?php

namespace Tests\E2E\Adapter\Scopes;

use PHPUnit\Framework\Attributes\Group;
use Utopia\Console;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

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
        if (! $this->getDatabase()->getAdapter()->supports(Capability::Timeouts)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('global-timeouts');

        $this->assertEquals(
            true,
            $database->createAttribute('global-timeouts', new Attribute(key: 'longtext', type: ColumnType::String, size: 100000000, required: true))
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
            $database->createCollection(__FUNCTION__, documentSecurity: false);

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
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->markTestSkipped('tenantPerDocument requires collection-level tenant bypass (not yet implemented)');

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

        // Create collection
        $database->createCollection(__FUNCTION__, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ], documentSecurity: false);

        $database->createAttribute(__FUNCTION__, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false));
        $database->createIndex(__FUNCTION__, new Index(key: 'nameIndex', type: IndexType::Key, attributes: ['name']));

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

        if ($database->getAdapter()->supports(Capability::Upserts)) {
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

            // Upsert new documents with different tenants
            $doc4Id = ID::unique();
            $doc5Id = ID::unique();
            $database
                ->setTenant(null)
                ->setTenantPerDocument(true)
                ->upsertDocuments(__FUNCTION__, [new Document([
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

        // Reset instance
        $database
            ->setSharedTables($sharedTables)
            ->setTenantPerDocument($tenantPerDocument)
            ->setTenant($tenant)
            ->setNamespace($namespace)
            ->setDatabase($schema);
    }

    #[Group('redis-destructive')]
    public function testCacheFallback(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::CacheSkipOnFailure)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        // Write mock data
        $database->createCollection('testRedisFallback', attributes: [
            new Attribute(key: 'string', type: ColumnType::String, size: 767, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createDocument('testRedisFallback', new Document([
            '$id' => 'doc1',
            'string' => 'text📝',
        ]));

        $database->createIndex('testRedisFallback', new Index(key: 'index1', type: IndexType::Key, attributes: ['string']));
        $this->assertCount(1, $database->find('testRedisFallback', [Query::equal('string', ['text📝'])]));

        // Bring down Redis
        $stdout = '';
        $stderr = '';
        Console::execute('docker ps -a --filter "name=utopia-redis" --format "{{.Names}}" | xargs -r docker stop -t 0', '', $stdout, $stderr);

        // Check we can read data still
        $this->assertCount(1, $database->find('testRedisFallback', [Query::equal('string', ['text📝'])]));
        $this->assertFalse(($database->getDocument('testRedisFallback', 'doc1'))->isEmpty());

        // Check we cannot modify data (error message varies: "went away", DNS failure, connection refused)
        try {
            $database->updateDocument('testRedisFallback', 'doc1', new Document([
                'string' => 'text📝 updated',
            ]));
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\RedisException::class, $e);
        }

        try {
            $database->deleteDocument('testRedisFallback', 'doc1');
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\RedisException::class, $e);
        }

        // Restart Redis containers
        Console::execute('docker ps -a --filter "name=utopia-redis" --format "{{.Names}}" | xargs -r docker start', '', $stdout, $stderr);
        sleep(2);
        $this->reconnectCache();

        $this->assertCount(1, $database->find('testRedisFallback', [Query::equal('string', ['text📝'])]));
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

        $database->createCollection('transactionAtomicity');
        $database->createAttribute('transactionAtomicity', new Attribute(key: 'title', type: ColumnType::String, size: 128, required: true));

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

    /**
     * Test that withTransaction correctly resets inTransaction state
     * when a known exception (DuplicateException) is thrown after successful rollback.
     */
    public function testTransactionStateAfterKnownException(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('txKnownException');
        $database->createAttribute('txKnownException', new Attribute(key: 'title', type: ColumnType::String, size: 128, required: true));

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

        $database->createCollection('txNested');
        $database->createAttribute('txNested', new Attribute(key: 'title', type: ColumnType::String, size: 128, required: true));

        $database->createDocument('txNested', new Document([
            '$id' => 'nested_existing',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'title' => 'Original',
        ]));

        // Outer transaction should succeed even if inner transaction throws
        $result = $database->withTransaction(function () use ($database) {
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

        $this->assertTrue($result);

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
    private function reconnectCache(): void
    {
        $redis = new \Redis();
        $redis->connect('redis', 6379, 2.0);
        $redis->setOption(\Redis::OPT_READ_TIMEOUT, 5);
        $redis->select(0);
        $adapter = new \Utopia\Cache\Adapter\Redis($redis);
        $adapter->setMaxRetries(3);
        $this->getDatabase()->setCache(new \Utopia\Cache\Cache($adapter));
    }

}
