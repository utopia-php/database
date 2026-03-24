<?php

namespace Tests\Unit\Collections;

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
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class CollectionValidationTest extends TestCase
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
        $this->adapter->method('createCollection')->willReturn(true);
        $this->adapter->method('deleteCollection')->willReturn(true);
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

    private function setupExistingCollection(string $id): void
    {
        $collection = new Document([
            '$id' => $id,
            '$collection' => Database::METADATA,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => $id,
            'attributes' => [],
            'indexes' => [],
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
    }

    private function setupEmptyMetadata(): void
    {
        $meta = $this->metaCollection();
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($meta) {
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return $meta;
                }

                return new Document();
            }
        );
    }

    public function testCreateCollectionThrowsOnDuplicateId(): void
    {
        $this->setupExistingCollection('existing');
        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('already exists');
        $this->database->createCollection('existing');
    }

    public function testCreateCollectionValidatesPermissionsFormat(): void
    {
        $this->setupEmptyMetadata();
        $this->database->enableValidation();

        $this->expectException(DatabaseException::class);
        $this->database->createCollection('newCol', permissions: ['bad-format']);
    }

    public function testCreateCollectionWithAttributeLimits(): void
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
        $adapter->method('getLimitForAttributes')->willReturn(1);
        $adapter->method('getLimitForIndexes')->willReturn(64);
        $adapter->method('getMaxIndexLength')->willReturn(768);
        $adapter->method('getMaxVarcharLength')->willReturn(16383);
        $adapter->method('getDocumentSizeLimit')->willReturn(0);
        $adapter->method('getCountOfAttributes')->willReturn(100);
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
        $adapter->method('createCollection')->willReturn(true);
        $adapter->method('deleteCollection')->willReturn(true);
        $adapter->method('getDocument')->willReturn(new Document());

        $db = new Database($adapter, new Cache(new None()));
        $db->getAuthorization()->addRole(Role::any()->toString());

        $attr = new \Utopia\Database\Attribute(
            key: 'name',
            type: \Utopia\Query\Schema\ColumnType::String,
            size: 128,
            required: false,
        );

        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Attribute limit');
        $db->createCollection('newCol', [$attr]);
    }

    public function testCreateCollectionWithIndexLimits(): void
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
        $adapter->method('getLimitForIndexes')->willReturn(0);
        $adapter->method('getMaxIndexLength')->willReturn(768);
        $adapter->method('getMaxVarcharLength')->willReturn(16383);
        $adapter->method('getDocumentSizeLimit')->willReturn(0);
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(100);
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
        $adapter->method('createCollection')->willReturn(true);
        $adapter->method('deleteCollection')->willReturn(true);
        $adapter->method('getDocument')->willReturn(new Document());

        $db = new Database($adapter, new Cache(new None()));
        $db->getAuthorization()->addRole(Role::any()->toString());

        $attr = new \Utopia\Database\Attribute(
            key: 'name',
            type: \Utopia\Query\Schema\ColumnType::String,
            size: 128,
            required: false,
        );
        $index = new \Utopia\Database\Index(
            key: 'idx_name',
            type: \Utopia\Query\Schema\IndexType::Key,
            attributes: ['name'],
        );

        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Index limit');
        $db->createCollection('newCol', [$attr], [$index]);
    }

    public function testDeleteCollectionThrowsOnNotFound(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');
        $this->database->deleteCollection('nonexistent');
    }

    public function testUpdateCollectionUpdatesPermissions(): void
    {
        $existingCol = new Document([
            '$id' => 'testCol',
            '$collection' => Database::METADATA,
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any())],
            '$version' => 1,
            'name' => 'testCol',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => false,
        ]);

        $metaAttributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 256, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
            new Document(['$id' => 'attributes', 'key' => 'attributes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
            new Document(['$id' => 'indexes', 'key' => 'indexes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
            new Document(['$id' => 'documentSecurity', 'key' => 'documentSecurity', 'type' => 'boolean', 'size' => 0, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
        ];
        $metaCollection = new Document([
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any())],
            'name' => 'collections',
            'attributes' => $metaAttributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($existingCol, $metaCollection) {
                if ($col->getId() === Database::METADATA && $docId === 'testCol') {
                    return $existingCol;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return $metaCollection;
                }

                return new Document();
            }
        );
        $this->adapter->method('updateDocument')->willReturnArgument(2);

        $newPermissions = [Permission::read(Role::any()), Permission::create(Role::user('admin'))];
        $result = $this->database->updateCollection('testCol', $newPermissions, true);
        $this->assertTrue($result->getAttribute('documentSecurity'));
    }

    public function testUpdateCollectionUpdatesDocumentSecurity(): void
    {
        $existingCol = new Document([
            '$id' => 'testCol',
            '$collection' => Database::METADATA,
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            '$version' => 1,
            'name' => 'testCol',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => false,
        ]);

        $metaAttributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 256, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
            new Document(['$id' => 'attributes', 'key' => 'attributes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
            new Document(['$id' => 'indexes', 'key' => 'indexes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
            new Document(['$id' => 'documentSecurity', 'key' => 'documentSecurity', 'type' => 'boolean', 'size' => 0, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
        ];
        $metaCollection = new Document([
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any())],
            'name' => 'collections',
            'attributes' => $metaAttributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($existingCol, $metaCollection) {
                if ($col->getId() === Database::METADATA && $docId === 'testCol') {
                    return $existingCol;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return $metaCollection;
                }

                return new Document();
            }
        );
        $this->adapter->method('updateDocument')->willReturnArgument(2);

        $result = $this->database->updateCollection('testCol', [Permission::read(Role::any())], true);
        $this->assertTrue($result->getAttribute('documentSecurity'));
    }

    public function testUpdateCollectionThrowsOnNotFound(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());
        $this->expectException(NotFoundException::class);
        $this->database->updateCollection('nonexistent', [Permission::read(Role::any())], true);
    }

    public function testListCollectionsReturnsCollectionDocuments(): void
    {
        $col1 = new Document([
            '$id' => 'col1',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'col1',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $metaAttributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 256, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
            new Document(['$id' => 'attributes', 'key' => 'attributes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
            new Document(['$id' => 'indexes', 'key' => 'indexes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
            new Document(['$id' => 'documentSecurity', 'key' => 'documentSecurity', 'type' => 'boolean', 'size' => 0, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
        ];

        $metaCollection = new Document([
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any())],
            'name' => 'collections',
            'attributes' => $metaAttributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($metaCollection) {
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return $metaCollection;
                }

                return new Document();
            }
        );
        $this->adapter->method('find')->willReturn([$col1]);

        $result = $this->database->listCollections();
        $this->assertCount(1, $result);
        $this->assertSame('col1', $result[0]->getId());
    }

    public function testGetCollectionReturnsCollectionDocument(): void
    {
        $this->setupExistingCollection('myCol');

        $result = $this->database->getCollection('myCol');
        $this->assertFalse($result->isEmpty());
        $this->assertSame('myCol', $result->getId());
    }

    public function testExistsDelegatesToAdapter(): void
    {
        $this->adapter->method('getDatabase')->willReturn('testdb');
        $this->adapter->method('exists')->willReturn(true);

        $result = $this->database->exists('testdb', 'testCol');
        $this->assertTrue($result);
    }
}
