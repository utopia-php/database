<?php

namespace Tests\Unit\Documents;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class UpdateDocumentLogicTest extends TestCase
{
    private function buildDatabase(Adapter&Stub $adapter): Database
    {
        $cache = new Cache(new None());
        $db = new Database($adapter, $cache);
        $db->getAuthorization()->addRole(Role::any()->toString());

        return $db;
    }

    private function makeAdapter(): Adapter&Stub
    {
        $adapter = self::createStub(Adapter::class);
        $adapter->method('getSharedTables')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getTenantPerDocument')->willReturn(false);
        $adapter->method('getNamespace')->willReturn('');
        $adapter->method('getIdAttributeType')->willReturn('string');
        $adapter->method('getMaxUIDLength')->willReturn(36);
        $adapter->method('getMinDateTime')->willReturn(new DateTime('0000-01-01'));
        $adapter->method('getMaxDateTime')->willReturn(new DateTime('9999-12-31'));
        $adapter->method('getLimitForString')->willReturn(16777215);
        $adapter->method('getLimitForInt')->willReturn(2147483647);
        $adapter->method('getLimitForAttributes')->willReturn(0);
        $adapter->method('getLimitForIndexes')->willReturn(64);
        $adapter->method('getMaxIndexLength')->willReturn(768);
        $adapter->method('getMaxVarcharLength')->willReturn(16383);
        $adapter->method('getDocumentSizeLimit')->willReturn(0);
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('getInternalIndexesKeys')->willReturn([]);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::Index,
                Capability::IndexArray,
                Capability::UniqueIndex,
                Capability::DefinedAttributes,
            ]);
        });
        $adapter->method('castingBefore')->willReturnArgument(1);
        $adapter->method('castingAfter')->willReturnArgument(1);
        $adapter->method('startTransaction')->willReturn(true);
        $adapter->method('commitTransaction')->willReturn(true);
        $adapter->method('rollbackTransaction')->willReturn(true);
        $adapter->method('withTransaction')->willReturnCallback(function (callable $callback) {
            return $callback();
        });
        $adapter->method('updateDocument')->willReturnArgument(2);

        return $adapter;
    }

    private function setupCollectionAndDocument(
        Adapter&Stub $adapter,
        string $collectionId,
        Document $existingDoc,
        array $attributes = [],
        array $collectionPermissions = []
    ): void {
        if (empty($collectionPermissions)) {
            $collectionPermissions = [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ];
        }

        $collection = new Document([
            '$id' => $collectionId,
            '$collection' => Database::METADATA,
            '$permissions' => $collectionPermissions,
            'name' => $collectionId,
            'attributes' => $attributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collectionId, $collection, $existingDoc) {
                if ($col->getId() === Database::METADATA && $docId === $collectionId) {
                    return $collection;
                }
                if ($col->getId() === $collectionId && $docId === $existingDoc->getId()) {
                    return $existingDoc;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return new Document(Database::COLLECTION);
                }

                return new Document();
            }
        );
    }

    public function testUpdateDocumentSetsUpdatedAt(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 1,
            'name' => 'old',
        ]);
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollectionAndDocument($adapter, 'testCol', $existing, $attributes);
        $db = $this->buildDatabase($adapter);

        $updated = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'new',
        ]);

        $result = $db->updateDocument('testCol', 'doc1', $updated);
        $this->assertNotSame('2024-01-01T00:00:00.000+00:00', $result->getUpdatedAt());
    }

    public function testUpdateDocumentIncrementsVersion(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 5,
            'name' => 'old',
        ]);
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollectionAndDocument($adapter, 'testCol', $existing, $attributes);
        $db = $this->buildDatabase($adapter);

        $updated = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'new',
        ]);

        $result = $db->updateDocument('testCol', 'doc1', $updated);
        $this->assertSame(6, $result->getVersion());
    }

    public function testUpdateDocumentChecksUpdatePermission(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'restricted',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::update(Role::user('admin'))],
            '$version' => 1,
            'name' => 'old',
        ]);
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollectionAndDocument($adapter, 'restricted', $existing, $attributes, [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::user('admin')),
        ]);

        $db = new Database($adapter, new Cache(new None()));

        $this->expectException(AuthorizationException::class);
        $db->updateDocument('restricted', 'doc1', new Document([
            '$id' => 'doc1',
            '$collection' => 'restricted',
            'name' => 'new',
        ]));
    }

    public function testUpdateDocumentValidatesStructure(): void
    {
        $adapter = $this->makeAdapter();
        $attributes = [
            new Document(['$id' => 'title', 'key' => 'title', 'type' => 'string', 'size' => 5, 'required' => true, 'array' => false, 'signed' => true, 'filters' => []]),
        ];

        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 1,
            'title' => 'ok',
        ]);

        $this->setupCollectionAndDocument($adapter, 'testCol', $existing, $attributes);
        $db = $this->buildDatabase($adapter);
        $db->enableValidation();

        $updated = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'this string is way too long for size 5',
        ]);

        $this->expectException(StructureException::class);
        $db->updateDocument('testCol', 'doc1', $updated);
    }

    public function testUpdateDocumentDetectsNoChangesAndPreservesVersion(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 3,
            'name' => 'same',
        ]);
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollectionAndDocument($adapter, 'testCol', $existing, $attributes);
        $db = $this->buildDatabase($adapter);

        $noChange = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'same',
        ]);

        $result = $db->updateDocument('testCol', 'doc1', $noChange);
        $this->assertSame(3, $result->getVersion());
    }

    public function testUpdateDocumentRequiresId(): void
    {
        $adapter = $this->makeAdapter();
        $adapter->method('getDocument')->willReturn(new Document());
        $db = $this->buildDatabase($adapter);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Must define $id attribute');
        $db->updateDocument('testCol', '', new Document([]));
    }

    public function testUpdateDocumentReturnsEmptyForMissingDocument(): void
    {
        $adapter = $this->makeAdapter();
        $collection = new Document([
            '$id' => 'testCol',
            '$collection' => Database::METADATA,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'testCol',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => true,
        ]);
        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection) {
                if ($col->getId() === Database::METADATA && $docId === 'testCol') {
                    return $collection;
                }

                return new Document();
            }
        );

        $db = $this->buildDatabase($adapter);

        $result = $db->updateDocument('testCol', 'nonexistent', new Document([
            '$id' => 'nonexistent',
        ]));
        $this->assertTrue($result->isEmpty());
    }

    public function testUpdateDocumentPreservesCreatedAt(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2020-06-15T12:00:00.000+00:00',
            '$updatedAt' => '2020-06-15T12:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 1,
            'name' => 'old',
        ]);
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollectionAndDocument($adapter, 'testCol', $existing, $attributes);
        $db = $this->buildDatabase($adapter);

        $updated = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'new',
        ]);

        $result = $db->updateDocument('testCol', 'doc1', $updated);
        $this->assertSame('2020-06-15T12:00:00.000+00:00', $result->getCreatedAt());
    }

    public function testUpdateDocumentVersionNotIncrementedWhenNoChanges(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 7,
            'name' => 'unchanged',
        ]);
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollectionAndDocument($adapter, 'testCol', $existing, $attributes);
        $db = $this->buildDatabase($adapter);

        $noChange = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'unchanged',
        ]);

        $result = $db->updateDocument('testCol', 'doc1', $noChange);
        $this->assertSame(7, $result->getVersion());
    }

    public function testUpdateDocumentPermissionChangeIsHandled(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 1,
            'name' => 'same',
        ]);
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollectionAndDocument($adapter, 'testCol', $existing, $attributes);
        $db = $this->buildDatabase($adapter);

        $updated = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
            'name' => 'same',
        ]);

        $result = $db->updateDocument('testCol', 'doc1', $updated);
        $this->assertNotEmpty($result->getId());
    }
}
