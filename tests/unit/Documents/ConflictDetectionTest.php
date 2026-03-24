<?php

namespace Tests\Unit\Documents;

use DateTime;
use DateTime as NativeDateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class ConflictDetectionTest extends TestCase
{
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
        $adapter->method('deleteDocument')->willReturn(true);

        return $adapter;
    }

    private function buildDatabase(Adapter&Stub $adapter): Database
    {
        $cache = new Cache(new None());
        $db = new Database($adapter, $cache);
        $db->getAuthorization()->addRole(Role::any()->toString());

        return $db;
    }

    private function setupCollectionAndDocument(
        Adapter&Stub $adapter,
        string $collectionId,
        Document $existingDoc,
        array $attributes = [],
    ): void {
        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $collection = new Document([
            '$id' => $collectionId,
            '$collection' => Database::METADATA,
            '$permissions' => $permissions,
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
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return new Document(Database::COLLECTION);
                }
                if ($col->getId() === $collectionId && $docId === $existingDoc->getId()) {
                    return $existingDoc;
                }

                return new Document();
            }
        );
    }

    public function testUpdateDocumentConflictThrows(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-06-15T12:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 1,
            'name' => 'old',
        ]);

        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];

        $this->setupCollectionAndDocument($adapter, 'testCol', $existing, $attributes);
        $db = $this->buildDatabase($adapter);

        $requestTime = new NativeDateTime('2024-01-01T00:00:00.000+00:00');

        $this->expectException(ConflictException::class);
        $this->expectExceptionMessage('Document was updated after the request timestamp');

        $db->withRequestTimestamp($requestTime, function () use ($db) {
            $db->updateDocument('testCol', 'doc1', new Document([
                '$id' => 'doc1',
                '$collection' => 'testCol',
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                'name' => 'new',
            ]));
        });
    }

    public function testDeleteDocumentConflictThrows(): void
    {
        $adapter = $this->makeAdapter();
        $existing = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-06-15T12:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::delete(Role::any())],
            '$version' => 1,
            'name' => 'old',
        ]);

        $this->setupCollectionAndDocument($adapter, 'testCol', $existing);
        $db = $this->buildDatabase($adapter);

        $requestTime = new NativeDateTime('2024-01-01T00:00:00.000+00:00');

        $this->expectException(ConflictException::class);
        $this->expectExceptionMessage('Document was updated after the request timestamp');

        $db->withRequestTimestamp($requestTime, function () use ($db) {
            $db->deleteDocument('testCol', 'doc1');
        });
    }

    public function testUpdateDocumentNoConflict(): void
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

        $requestTime = new NativeDateTime('2024-06-15T12:00:00.000+00:00');

        $result = $db->withRequestTimestamp($requestTime, function () use ($db) {
            return $db->updateDocument('testCol', 'doc1', new Document([
                '$id' => 'doc1',
                '$collection' => 'testCol',
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                'name' => 'new',
            ]));
        });

        $this->assertSame('doc1', $result->getId());
        $this->assertSame(2, $result->getVersion());
    }
}
