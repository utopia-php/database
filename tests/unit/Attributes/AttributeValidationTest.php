<?php

namespace Tests\Unit\Attributes;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Query\Schema\ColumnType;

class AttributeValidationTest extends TestCase
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
        $this->adapter->method('createAttribute')->willReturn(true);

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

    private function setupCollection(string $id, array $attributes = []): void
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
        $this->adapter->method('updateDocument')->willReturnArgument(2);
    }

    public function testCreateAttributeOnMissingCollectionThrows(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());

        $this->expectException(NotFoundException::class);
        $this->database->createAttribute('nonexistent', new Attribute(
            key: 'name',
            type: ColumnType::String,
            size: 128,
        ));
    }

    public function testCreateAttributeRejectsDuplicateKey(): void
    {
        $existingAttrs = [
            new Document(['$id' => 'title', 'key' => 'title', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $existingAttrs);

        $this->expectException(DuplicateException::class);
        $this->database->createAttribute('testCol', new Attribute(
            key: 'title',
            type: ColumnType::String,
            size: 128,
        ));
    }

    public function testCreateAttributeValidatesSizeLimitsForStrings(): void
    {
        $this->setupCollection('testCol');

        $this->expectException(\Utopia\Database\Exception::class);
        $this->expectExceptionMessage('Max size allowed for string');

        $tooBig = $this->adapter->getLimitForString() + 1;
        $this->database->createAttribute('testCol', new Attribute(
            key: 'bigstr',
            type: ColumnType::String,
            size: $tooBig,
        ));
    }

    public function testCreateAttributeSucceedsWithValidString(): void
    {
        $this->setupCollection('testCol');

        $result = $this->database->createAttribute('testCol', new Attribute(
            key: 'name',
            type: ColumnType::String,
            size: 128,
        ));
        $this->assertTrue($result);
    }

    public function testCreateAttributeSucceedsWithInteger(): void
    {
        $this->setupCollection('testCol');

        $result = $this->database->createAttribute('testCol', new Attribute(
            key: 'age',
            type: ColumnType::Integer,
            size: 0,
        ));
        $this->assertTrue($result);
    }

    public function testCreateAttributeSucceedsWithBoolean(): void
    {
        $this->setupCollection('testCol');

        $result = $this->database->createAttribute('testCol', new Attribute(
            key: 'active',
            type: ColumnType::Boolean,
            size: 0,
        ));
        $this->assertTrue($result);
    }

    public function testCreateAttributeSucceedsWithDouble(): void
    {
        $this->setupCollection('testCol');

        $result = $this->database->createAttribute('testCol', new Attribute(
            key: 'score',
            type: ColumnType::Double,
            size: 0,
        ));
        $this->assertTrue($result);
    }

    public function testCreateAttributeEnforcesAttributeCountLimit(): void
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
        $adapter->method('getLimitForAttributes')->willReturn(2);
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
            return in_array($cap, [Capability::Index, Capability::IndexArray, Capability::UniqueIndex, Capability::DefinedAttributes]);
        });
        $adapter->method('castingBefore')->willReturnArgument(1);
        $adapter->method('castingAfter')->willReturnArgument(1);
        $adapter->method('startTransaction')->willReturn(true);
        $adapter->method('commitTransaction')->willReturn(true);
        $adapter->method('rollbackTransaction')->willReturn(true);
        $adapter->method('createAttribute')->willReturn(true);

        $collection = new Document([
            '$id' => 'testCol',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any())],
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
        $db->createAttribute('testCol', new Attribute(
            key: 'extra',
            type: ColumnType::String,
            size: 128,
        ));
    }

    public function testCreateAttributeEnforcesRowWidthLimit(): void
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
        $adapter->method('getDocumentSizeLimit')->willReturn(100);
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(200);
        $adapter->method('getInternalIndexesKeys')->willReturn([]);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [Capability::Index, Capability::IndexArray, Capability::UniqueIndex, Capability::DefinedAttributes]);
        });
        $adapter->method('castingBefore')->willReturnArgument(1);
        $adapter->method('castingAfter')->willReturnArgument(1);
        $adapter->method('startTransaction')->willReturn(true);
        $adapter->method('commitTransaction')->willReturn(true);
        $adapter->method('rollbackTransaction')->willReturn(true);
        $adapter->method('createAttribute')->willReturn(true);

        $collection = new Document([
            '$id' => 'testCol',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any())],
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
        $db->createAttribute('testCol', new Attribute(
            key: 'wide',
            type: ColumnType::String,
            size: 128,
        ));
    }

    public function testDeleteAttributeRemovesFromCollection(): void
    {
        $existingAttrs = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $existingAttrs);
        $this->adapter->method('deleteAttribute')->willReturn(true);

        $result = $this->database->deleteAttribute('testCol', 'name');
        $this->assertTrue($result);
    }

    public function testDeleteAttributeThrowsOnNotFound(): void
    {
        $this->setupCollection('testCol');
        $this->expectException(NotFoundException::class);
        $this->database->deleteAttribute('testCol', 'nonexistent');
    }

    public function testRenameAttributeThrowsOnDuplicateName(): void
    {
        $existingAttrs = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
            new Document(['$id' => 'title', 'key' => 'title', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $existingAttrs);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Attribute name already used');
        $this->database->renameAttribute('testCol', 'name', 'title');
    }

    public function testRenameAttributeThrowsOnNotFound(): void
    {
        $this->setupCollection('testCol');
        $this->expectException(NotFoundException::class);
        $this->database->renameAttribute('testCol', 'nonexistent', 'newname');
    }

    public function testCreateAttributesBatchValidatesEach(): void
    {
        $existingAttrs = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $existingAttrs);
        $this->adapter->method('createAttributes')->willReturn(true);

        $this->expectException(DuplicateException::class);
        $this->database->createAttributes('testCol', [
            new Attribute(key: 'name', type: ColumnType::String, size: 128),
        ]);
    }

    public function testCreateAttributesBatchWithEmptyListThrows(): void
    {
        $this->setupCollection('testCol');

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('No attributes to create');
        $this->database->createAttributes('testCol', []);
    }
}
