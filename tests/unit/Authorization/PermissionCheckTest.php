<?php

namespace Tests\Unit\Authorization;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoneAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;

class PermissionCheckTest extends TestCase
{
    private Database $database;

    private Adapter&Stub $adapter;

    private Authorization $authorization;

    #[\Override]
    protected function setUp(): void
    {
        $this->adapter = self::createStub(Adapter::class);

        $this->adapter->method('hasSharedTables')->willReturn(false);
        $this->adapter->method('getTenant')->willReturn(null);
        $this->adapter->method('isTenantPerDocument')->willReturn(false);
        $this->adapter->method('limits')->willReturn(new Limits(
            string: 0,
            varchar: 0,
            integer: 0,
            bigInteger: 0,
            attributes: 0,
            indexes: 0,
            defaultAttributes: 0,
            defaultIndexes: 0,
            indexLength: 0,
            uidLength: 36,
            documentSize: 0,
            minDateTime: new DateTime('1970-01-01 00:00:00'),
            maxDateTime: new DateTime('2999-12-31 23:59:59'),
            idType: ColumnType::String,
            keywords: [],
            internalIndexKeys: [],
        ));
        $this->adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return match ($cap) {
                Capability::DefinedAttributes => true,
                default => false,
            };
        });
        $this->adapter->method('withTransaction')->willReturnCallback(
            fn (callable $callback) => $callback()
        );
        $this->adapter->method('getSequences')->willReturnCallback(
            fn (string $collection, array $documents) => $documents
        );

        $cache = new Cache(new NoneAdapter());
        $this->database = new Database($this->adapter, $cache);
        $this->database->setValidation(false);
        $this->database->setFiltering(false);

        $this->authorization = $this->database->getAuthorization();
    }

    /**
     * @param  array<string>  $permissions
     */
    private function buildCollectionDoc(
        string $id,
        array $permissions = [],
        bool $documentSecurity = false
    ): Document {
        return new Document([
            '$id' => $id,
            '$collection' => Database::METADATA,
            '$permissions' => $permissions,
            'name' => $id,
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => $documentSecurity,
        ]);
    }

    private function configureAdapterForCollection(Document $collection): void
    {
        $collectionId = $collection->getId();

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }

                return new Document();
            }
        );
    }

    public function testCreateDocumentThrowsWithoutCreatePermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::user('owner')),
            Permission::read(Role::any()),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:other');

        $this->expectException(AuthorizationException::class);

        $this->database->createDocument('test_col', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
        ]));
    }

    public function testCreateDocumentSucceedsWithCreatePermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::user('owner')),
            Permission::read(Role::any()),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('createDocument')->willReturnCallback(
            fn (Document $col, Document $doc) => $doc
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:owner');

        $result = $this->database->createDocument('test_col', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
        ]));

        $this->assertEquals('doc1', $result->getId());
    }

    public function testCreateDocumentSucceedsWithCollectionCreatePermissionForAny(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('createDocument')->willReturnCallback(
            fn (Document $col, Document $doc) => $doc
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('any');

        $result = $this->database->createDocument('test_col', new Document([
            '$id' => 'doc2',
            '$permissions' => [],
        ]));

        $this->assertEquals('doc2', $result->getId());
    }

    public function testUpdateDocumentThrowsWithoutUpdatePermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::user('owner')),
        ]);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            'title' => 'old',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:other');

        $this->expectException(AuthorizationException::class);

        $this->database->updateDocument('test_col', 'doc1', new Document([
            '$id' => 'doc1',
            'title' => 'new',
        ]));
    }

    public function testUpdateDocumentSucceedsWithUpdatePermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::user('owner')),
        ]);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            'title' => 'old',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->adapter->method('updateDocument')->willReturnCallback(
            fn (Document $col, string $id, Document $doc, bool $skipPerms) => $doc
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:owner');

        $result = $this->database->updateDocument('test_col', 'doc1', new Document([
            '$id' => 'doc1',
            'title' => 'new',
        ]));

        $this->assertNotEmpty($result->getId());
    }

    public function testDeleteDocumentThrowsWithoutDeletePermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::delete(Role::user('owner')),
        ]);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:other');

        $this->expectException(AuthorizationException::class);

        $this->database->deleteDocument('test_col', 'doc1');
    }

    public function testDeleteDocumentSucceedsWithDeletePermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::delete(Role::user('owner')),
        ]);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->adapter->method('deleteDocument')->willReturn(true);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:owner');

        $result = $this->database->deleteDocument('test_col', 'doc1');
        $this->assertTrue($result);
    }

    public function testGetDocumentReturnsEmptyWithoutReadPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::user('owner')),
        ]);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:other');

        $result = $this->database->getDocument('test_col', 'doc1');
        $this->assertTrue($result->isEmpty());
    }

    public function testGetDocumentSucceedsWithReadPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::user('owner')),
        ]);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:owner');

        $result = $this->database->getDocument('test_col', 'doc1');
        $this->assertEquals('doc1', $result->getId());
    }

    public function testFindThrowsWithoutReadPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::user('owner')),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:other');

        $this->expectException(AuthorizationException::class);

        $this->database->find('test_col');
    }

    public function testFindSucceedsWithReadPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::user('owner')),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('find')->willReturn([
            new Document([
                '$id' => 'doc1',
                '$permissions' => [],
            ]),
        ]);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:owner');

        $results = $this->database->find('test_col');
        $this->assertCount(1, $results);
    }

    public function testDocumentLevelSecurityAllowsReadWithDocPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
        ], documentSecurity: true);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [
                Permission::read(Role::user('reader')),
            ],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:reader');

        $result = $this->database->getDocument('test_col', 'doc1');
        $this->assertEquals('doc1', $result->getId());
    }

    public function testDocumentLevelSecurityDeniesReadWithoutDocPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
        ], documentSecurity: true);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [
                Permission::read(Role::user('reader')),
            ],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:stranger');

        $result = $this->database->getDocument('test_col', 'doc1');
        $this->assertTrue($result->isEmpty());
    }

    public function testAggregatedWritePermissionGrantsCreate(): void
    {
        $permissions = Permission::aggregate([Permission::write(Role::user('writer'))]);
        $permissions[] = Permission::read(Role::any());

        $collection = $this->buildCollectionDoc('test_col', $permissions);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('createDocument')->willReturnCallback(
            fn (Document $col, Document $doc) => $doc
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:writer');

        $result = $this->database->createDocument('test_col', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
        ]));

        $this->assertEquals('doc1', $result->getId());
    }

    public function testAggregatedWritePermissionGrantsUpdate(): void
    {
        $permissions = Permission::aggregate([Permission::write(Role::user('writer'))]);
        $permissions[] = Permission::read(Role::any());

        $collection = $this->buildCollectionDoc('test_col', $permissions);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            'title' => 'old',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->adapter->method('updateDocument')->willReturnCallback(
            fn (Document $col, string $id, Document $doc, bool $skipPerms) => $doc
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:writer');

        $result = $this->database->updateDocument('test_col', 'doc1', new Document([
            '$id' => 'doc1',
            'title' => 'new',
        ]));

        $this->assertNotEmpty($result->getId());
    }

    public function testAggregatedWritePermissionGrantsDelete(): void
    {
        $permissions = Permission::aggregate([Permission::write(Role::user('writer'))]);
        $permissions[] = Permission::read(Role::any());

        $collection = $this->buildCollectionDoc('test_col', $permissions);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->adapter->method('deleteDocument')->willReturn(true);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:writer');

        $result = $this->database->deleteDocument('test_col', 'doc1');
        $this->assertTrue($result);
    }

    public function testSkipAuthorizationBypassesAllChecks(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::user('nobody')),
            Permission::read(Role::user('nobody')),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('createDocument')->willReturnCallback(
            fn (Document $col, Document $doc) => $doc
        );

        $this->authorization->cleanRoles();

        $result = $this->authorization->skip(function () {
            return $this->database->createDocument('test_col', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
            ]));
        });

        $this->assertEquals('doc1', $result->getId());
    }

    public function testCountThrowsWithoutReadPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::user('owner')),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:other');

        $this->expectException(AuthorizationException::class);

        $this->database->count('test_col');
    }

    public function testCountSucceedsWithReadPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::user('owner')),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('count')->willReturn(5);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:owner');

        $result = $this->database->count('test_col');
        $this->assertEquals(5, $result);
    }

    public function testSumThrowsWithoutReadPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::user('owner')),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:other');

        $this->expectException(AuthorizationException::class);

        $this->database->sum('test_col', 'amount');
    }

    public function testSumSucceedsWithReadPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::user('owner')),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('sum')->willReturn(42.5);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:owner');

        $result = $this->database->sum('test_col', 'amount');
        $this->assertEquals(42.5, $result);
    }

    public function testDocumentSecurityAllowsUpdateWithDocPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::any()),
        ], documentSecurity: true);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [
                Permission::update(Role::user('editor')),
                Permission::read(Role::any()),
            ],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            'title' => 'old',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->adapter->method('updateDocument')->willReturnCallback(
            fn (Document $col, string $id, Document $doc, bool $skipPerms) => $doc
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:editor');

        $result = $this->database->updateDocument('test_col', 'doc1', new Document([
            '$id' => 'doc1',
            'title' => 'new',
        ]));

        $this->assertNotEmpty($result->getId());
    }

    public function testDocumentSecurityAllowsDeleteWithDocPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::read(Role::any()),
        ], documentSecurity: true);

        $existingDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'test_col',
            '$permissions' => [
                Permission::delete(Role::user('deleter')),
            ],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);

        $collectionId = $collection->getId();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id, array $queries = [], bool $forUpdate = false) use ($collection, $collectionId, $existingDoc) {
                if ($col->getId() === Database::METADATA && $id === $collectionId) {
                    return $collection;
                }
                if ($id === 'doc1') {
                    return $existingDoc;
                }

                return new Document();
            }
        );

        $this->adapter->method('deleteDocument')->willReturn(true);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:deleter');

        $result = $this->database->deleteDocument('test_col', 'doc1');
        $this->assertTrue($result);
    }

    public function testFindWithDocumentSecurityAndNoCollectionPermission(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
        ], documentSecurity: true);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('find')->willReturn([
            new Document([
                '$id' => 'doc1',
                '$permissions' => [Permission::read(Role::user('viewer'))],
            ]),
        ]);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:viewer');

        $results = $this->database->find('test_col');
        $this->assertCount(1, $results);
    }

    public function testFindWithDocumentSecurityThrowsWithNoPermissionAtAll(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [], documentSecurity: false);

        $this->configureAdapterForCollection($collection);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:nobody');

        $this->expectException(AuthorizationException::class);

        $this->database->find('test_col');
    }

    public function testCountWithDocumentSecurityDoesNotThrow(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::any()),
        ], documentSecurity: true);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('count')->willReturn(3);

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:viewer');

        $result = $this->database->count('test_col');
        $this->assertEquals(3, $result);
    }

    public function testCreateDocumentWithUsersRole(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::users()),
            Permission::read(Role::any()),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('createDocument')->willReturnCallback(
            fn (Document $col, Document $doc) => $doc
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('users');

        $result = $this->database->createDocument('test_col', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
        ]));

        $this->assertEquals('doc1', $result->getId());
    }

    public function testCreateDocumentWithTeamRole(): void
    {
        $collection = $this->buildCollectionDoc('test_col', [
            Permission::create(Role::team('abc', 'admin')),
            Permission::read(Role::any()),
        ]);

        $this->configureAdapterForCollection($collection);

        $this->adapter->method('createDocument')->willReturnCallback(
            fn (Document $col, Document $doc) => $doc
        );

        $this->authorization->cleanRoles();
        $this->authorization->addRole('team:abc/admin');

        $result = $this->database->createDocument('test_col', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
        ]));

        $this->assertEquals('doc1', $result->getId());
    }
}
