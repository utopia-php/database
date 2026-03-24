<?php

namespace Tests\Unit\Schemaless;

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
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\IndexType;

class SchemalessValidationTest extends TestCase
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
        $this->adapter->method('createDocument')->willReturnArgument(1);
        $this->adapter->method('createDocuments')->willReturnCallback(function (Document $col, array $docs) {
            return $docs;
        });
        $this->adapter->method('updateDocument')->willReturnArgument(2);
        $this->adapter->method('createIndex')->willReturn(true);
        $this->adapter->method('deleteIndex')->willReturn(true);
        $this->adapter->method('getSequences')->willReturnArgument(1);

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
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
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

    private function makeCollection(string $id, array $attributes = [], array $indexes = []): Document
    {
        return new Document([
            '$id' => $id,
            '$sequence' => $id,
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
    }

    private function setupCollections(array $collections): void
    {
        $meta = $this->metaCollection();
        $map = [];
        foreach ($collections as $col) {
            $map[$col->getId()] = $col;
        }

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($meta, $map) {
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return $meta;
                }
                if ($col->getId() === Database::METADATA && isset($map[$docId])) {
                    return $map[$docId];
                }

                return new Document();
            }
        );
    }

    public function testSchemalessDocumentInvalidInteralAttributeValidation(): void
    {
        $col = $this->makeCollection('schemaless1');
        $this->setupCollections([$col]);

        try {
            $docs = [
                new Document(['$id' => true, 'freeA' => 'doc1']),
                new Document(['$id' => true, 'freeB' => 'test']),
                new Document(['$id' => true]),
            ];
            $this->database->createDocuments('schemaless1', $docs);
            $this->fail('Expected StructureException for invalid $id type');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        try {
            $docs = [
                new Document(['$createdAt' => true, 'freeA' => 'doc1']),
                new Document(['$updatedAt' => true, 'freeB' => 'test']),
                new Document(['$permissions' => 12]),
            ];
            $this->database->createDocuments('schemaless1', $docs);
            $this->fail('Expected StructureException for invalid internal attribute');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }
    }

    public function testSchemalessIndexDuplicatePrevention(): void
    {
        $col = $this->makeCollection('sl_idx_dup');
        $this->setupCollections([$col]);

        $this->database->createDocument('sl_idx_dup', new Document([
            '$id' => 'a',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'x',
        ]));

        $this->assertTrue($this->database->createIndex(
            'sl_idx_dup',
            new Index(key: 'duplicate', type: IndexType::Key, attributes: ['name'], lengths: [0], orders: [OrderDirection::Asc->value])
        ));

        try {
            $this->database->createIndex(
                'sl_idx_dup',
                new Index(key: 'duplicate', type: IndexType::Key, attributes: ['name'], lengths: [0], orders: [OrderDirection::Asc->value])
            );
            $this->fail('Failed to throw exception');
        } catch (\Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }
    }

    public function testSchemalessInternalAttributes(): void
    {
        $col = $this->makeCollection('sl_internal');
        $this->setupCollections([$col]);

        $doc = $this->database->createDocument('sl_internal', new Document([
            '$id' => 'i1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'alpha',
        ]));

        $this->assertEquals('i1', $doc->getId());
        $this->assertEquals('sl_internal', $doc->getCollection());
        $this->assertNotEmpty($doc->getAttribute('$createdAt'));
        $this->assertNotEmpty($doc->getAttribute('$updatedAt'));
        $perms = $doc->getPermissions();
        $this->assertContains(Permission::read(Role::any()), $perms);
        $this->assertContains(Permission::update(Role::any()), $perms);
        $this->assertContains(Permission::delete(Role::any()), $perms);
    }

    public function testSchemalessTTLIndexDuplicatePrevention(): void
    {
        $col = $this->makeCollection('sl_ttl_dup');
        $this->setupCollections([$col]);

        $this->assertTrue($this->database->createIndex(
            'sl_ttl_dup',
            new Index(key: 'idx_ttl_expires', type: IndexType::Ttl, attributes: ['expiresAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 3600)
        ));

        try {
            $this->database->createIndex(
                'sl_ttl_dup',
                new Index(key: 'idx_ttl_expires_duplicate', type: IndexType::Ttl, attributes: ['expiresAt'], lengths: [], orders: [OrderDirection::Asc->value], ttl: 7200)
            );
            $this->fail('Expected exception for duplicate TTL index');
        } catch (\Exception $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('There can be only one TTL index in a collection', $e->getMessage());
        }
    }
}
