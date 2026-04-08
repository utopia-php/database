<?php

namespace Tests\Unit\Indexes;

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
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Query\Schema\IndexType;

class IndexValidationTest extends TestCase
{
    private Adapter&Stub $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = self::createStub(Adapter::class);
        $this->adapter->method('getSharedTables')->willReturn(false);
        $this->adapter->method('getTenant')->willReturn(null);
        $this->adapter->method('getTenantPerDocument')->willReturn(false);
        $this->adapter->method('getNamespace')->willReturn('');
        $this->adapter->method('getIdAttributeType')->willReturn('string');
        $this->adapter->method('getMaxUIDLength')->willReturn(36);
        $this->adapter->method('getMinDateTime')->willReturn(new DateTime('0000-01-01'));
        $this->adapter->method('getMaxDateTime')->willReturn(new DateTime('9999-12-31'));
        $this->adapter->method('getLimitForString')->willReturn(16777215);
        $this->adapter->method('getLimitForInt')->willReturn(2147483647);
        $this->adapter->method('getLimitForAttributes')->willReturn(0);
        $this->adapter->method('getLimitForIndexes')->willReturn(64);
        $this->adapter->method('getMaxIndexLength')->willReturn(768);
        $this->adapter->method('getMaxVarcharLength')->willReturn(16383);
        $this->adapter->method('getDocumentSizeLimit')->willReturn(0);
        $this->adapter->method('getCountOfAttributes')->willReturn(0);
        $this->adapter->method('getCountOfIndexes')->willReturn(0);
        $this->adapter->method('getAttributeWidth')->willReturn(0);
        $this->adapter->method('getInternalIndexesKeys')->willReturn([]);
        $this->adapter->method('filter')->willReturnArgument(0);
        $this->adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::Index,
                Capability::IndexArray,
                Capability::UniqueIndex,
                Capability::DefinedAttributes,
                Capability::TTLIndexes,
            ]);
        });
        $this->adapter->method('castingBefore')->willReturnArgument(1);
        $this->adapter->method('castingAfter')->willReturnArgument(1);
        $this->adapter->method('startTransaction')->willReturn(true);
        $this->adapter->method('commitTransaction')->willReturn(true);
        $this->adapter->method('rollbackTransaction')->willReturn(true);
        $this->adapter->method('withTransaction')->willReturnCallback(function (callable $callback) {
            return $callback();
        });
        $this->adapter->method('createIndex')->willReturn(true);
        $this->adapter->method('deleteIndex')->willReturn(true);
        $this->adapter->method('renameIndex')->willReturn(true);
        $this->adapter->method('createDocument')->willReturnArgument(1);
        $this->adapter->method('updateDocument')->willReturnArgument(2);

        $cache = new Cache(new None());
        $this->database = new Database($this->adapter, $cache);
        $this->database->getAuthorization()->addRole(Role::any()->toString());
    }

    private function metaCollection(): Document
    {
        return new Document([
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any())],
            '$version' => 1,
            'name' => 'collections',
            'attributes' => [
                new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 256, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
                new Document(['$id' => 'attributes', 'key' => 'attributes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
                new Document(['$id' => 'indexes', 'key' => 'indexes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
                new Document(['$id' => 'documentSecurity', 'key' => 'documentSecurity', 'type' => 'boolean', 'size' => 0, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
            ],
            'indexes' => [],
            'documentSecurity' => true,
        ]);
    }

    private function setupCollection(string $id, array $attributes = [], array $indexes = []): void
    {
        $collection = new Document([
            '$id' => $id,
            '$collection' => Database::METADATA,
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            '$version' => 1,
            'name' => $id,
            'attributes' => $attributes,
            'indexes' => $indexes,
            'documentSecurity' => true,
        ]);
        $meta = $this->metaCollection();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($id, $collection, $meta) {
                if ($col->getId() === Database::METADATA && $docId === $id) {
                    return $collection;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return $meta;
                }

                return new Document();
            }
        );
        $this->adapter->method('updateDocument')->willReturnArgument(2);
    }

    public function testCreateIndexValidatesAttributeExists(): void
    {
        $this->setupCollection('testCol');

        $this->expectException(IndexException::class);
        $this->database->createIndex('testCol', new Index(
            key: 'idx1',
            type: IndexType::Key,
            attributes: ['nonexistent'],
        ));
    }

    public function testCreateIndexEnforcesIndexCountLimit(): void
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
        $adapter->method('getLimitForIndexes')->willReturn(1);
        $adapter->method('getMaxIndexLength')->willReturn(768);
        $adapter->method('getMaxVarcharLength')->willReturn(16383);
        $adapter->method('getDocumentSizeLimit')->willReturn(0);
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(5);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('getInternalIndexesKeys')->willReturn([]);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [Capability::Index, Capability::IndexArray, Capability::UniqueIndex, Capability::DefinedAttributes]);
        });
        $adapter->method('castingBefore')->willReturnArgument(1);
        $adapter->method('castingAfter')->willReturnArgument(1);
        $adapter->method('createIndex')->willReturn(true);

        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $collection = new Document([
            '$id' => 'testCol',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any())],
            'name' => 'testCol',
            'attributes' => $attributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);
        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection) {
                if ($col->getId() === Database::METADATA && $docId === 'testCol') {
                    return $collection;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return new Document(Database::COLLECTION);
                }

                return new Document();
            }
        );
        $adapter->method('updateDocument')->willReturnArgument(2);

        $db = new Database($adapter, new Cache(new None()));
        $db->getAuthorization()->addRole(Role::any()->toString());

        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Index limit');
        $db->createIndex('testCol', new Index(
            key: 'idx_name',
            type: IndexType::Key,
            attributes: ['name'],
        ));
    }

    public function testCreateIndexRejectsDuplicateKey(): void
    {
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $indexes = [
            new Document(['$id' => 'idx_name', 'key' => 'idx_name', 'type' => 'key', 'attributes' => ['name'], 'lengths' => [], 'orders' => []]),
        ];
        $this->setupCollection('testCol', $attributes, $indexes);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Index already exists');
        $this->database->createIndex('testCol', new Index(
            key: 'idx_name',
            type: IndexType::Key,
            attributes: ['name'],
        ));
    }

    public function testCreateIndexMissingAttributesThrows(): void
    {
        $this->setupCollection('testCol');
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Missing attributes');
        $this->database->createIndex('testCol', new Index(
            key: 'idx_empty',
            type: IndexType::Key,
            attributes: [],
        ));
    }

    public function testCreateIndexSucceedsWithValidConfig(): void
    {
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $attributes);

        $result = $this->database->createIndex('testCol', new Index(
            key: 'idx_name',
            type: IndexType::Key,
            attributes: ['name'],
        ));
        $this->assertTrue($result);
    }

    public function testDeleteIndexThrowsOnNotFound(): void
    {
        $this->setupCollection('testCol');
        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Index not found');
        $this->database->deleteIndex('testCol', 'nonexistent');
    }

    public function testRenameIndexThrowsOnNotFound(): void
    {
        $this->setupCollection('testCol');
        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Index not found');
        $this->database->renameIndex('testCol', 'nonexistent', 'newname');
    }

    public function testRenameIndexThrowsOnExistingName(): void
    {
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
            new Document(['$id' => 'title', 'key' => 'title', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $indexes = [
            new Document(['$id' => 'idx_name', 'key' => 'idx_name', 'type' => 'key', 'attributes' => ['name'], 'lengths' => [], 'orders' => []]),
            new Document(['$id' => 'idx_title', 'key' => 'idx_title', 'type' => 'key', 'attributes' => ['title'], 'lengths' => [], 'orders' => []]),
        ];
        $this->setupCollection('testCol', $attributes, $indexes);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Index name already used');
        $this->database->renameIndex('testCol', 'idx_name', 'idx_title');
    }

    public function testRenameIndexSucceeds(): void
    {
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $indexes = [
            new Document(['$id' => 'idx_name', 'key' => 'idx_name', 'type' => 'key', 'attributes' => ['name'], 'lengths' => [], 'orders' => []]),
        ];
        $this->setupCollection('testCol', $attributes, $indexes);

        $result = $this->database->renameIndex('testCol', 'idx_name', 'idx_new_name');
        $this->assertTrue($result);
    }
}
