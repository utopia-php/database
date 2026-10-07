<?php

namespace Tests\Unit\Indexes;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
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
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class IndexValidationTest extends TestCase
{
    private Adapter&Stub $adapter;

    private Database $database;

    /** @var list<Document> */
    private array $metadataWrites = [];

    protected function setUp(): void
    {
        $this->adapter = self::createStub(Adapter::class);
        $this->adapter->method('getSharedTables')->willReturn(false);
        $this->adapter->method('getTenant')->willReturn(null);
        $this->adapter->method('getTenantPerDocument')->willReturn(false);
        $this->adapter->method('getNamespace')->willReturn('');
        $this->adapter->method('limits')->willReturn(new Limits(
            string: 16777215,
            varchar: 16383,
            integer: 2147483647,
            bigInteger: 0,
            attributes: 0,
            indexes: 64,
            defaultAttributes: 0,
            defaultIndexes: 0,
            indexLength: 768,
            uidLength: 36,
            documentSize: 0,
            minDateTime: new DateTime('0000-01-01'),
            maxDateTime: new DateTime('9999-12-31'),
            idType: ColumnType::String,
            keywords: [],
            internalIndexKeys: [],
        ));
        $this->adapter->method('getCountOfAttributes')->willReturn(0);
        $this->adapter->method('getCountOfIndexes')->willReturn(0);
        $this->adapter->method('getAttributeWidth')->willReturn(0);
        $this->adapter->method('filter')->willReturnArgument(0);
        $this->adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::IndexKey,
                Capability::IndexArray,
                Capability::IndexUnique,
                Capability::DefinedAttributes,
                Capability::IndexTtl,
            ]);
        });
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

    /**
     * @param  array<int, Document>  $attributes
     * @param  array<int, Document>  $indexes
     */
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
        $this->adapter->method('updateDocument')->willReturnCallback(function (Document $collection, string $id, Document $document): Document {
            $this->metadataWrites[] = $document;

            return $document;
        });
    }

    private function lastMetadataWrite(): Collection
    {
        $write = \end($this->metadataWrites);
        if ($write === false) {
            $this->fail('no metadata was written');
        }

        return Collection::fromDocument($write);
    }

    public function testCreateIndexValidatesAttributeExists(): void
    {
        $this->setupCollection('testCol');

        $this->expectException(IndexException::class);
        $this->database->createIndex('testCol', Index::key(key: 'idx1', attributes: ['nonexistent']));
    }

    public function testCreateIndexEnforcesIndexCountLimit(): void
    {
        $adapter = self::createStub(Adapter::class);
        $adapter->method('getSharedTables')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getTenantPerDocument')->willReturn(false);
        $adapter->method('getNamespace')->willReturn('');
        $adapter->method('limits')->willReturn(new Limits(
            string: 16777215,
            varchar: 16383,
            integer: 2147483647,
            bigInteger: 0,
            attributes: 0,
            indexes: 1,
            defaultAttributes: 0,
            defaultIndexes: 0,
            indexLength: 768,
            uidLength: 36,
            documentSize: 0,
            minDateTime: new DateTime('0000-01-01'),
            maxDateTime: new DateTime('9999-12-31'),
            idType: ColumnType::String,
            keywords: [],
            internalIndexKeys: [],
        ));
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(5);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [Capability::IndexKey, Capability::IndexArray, Capability::IndexUnique, Capability::DefinedAttributes]);
        });
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
                    return Database::collectionDefinition();
                }

                return new Document();
            }
        );
        $adapter->method('updateDocument')->willReturnArgument(2);

        $db = new Database($adapter, new Cache(new None()));
        $db->getAuthorization()->addRole(Role::any()->toString());

        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Index limit');
        $db->createIndex('testCol', Index::key(key: 'idx_name', attributes: ['name']));
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
        $this->database->createIndex('testCol', Index::key(key: 'idx_name', attributes: ['name']));
    }

    public function testCreateIndexMissingAttributesThrows(): void
    {
        $this->setupCollection('testCol');
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Missing attributes');
        $this->database->createIndex('testCol', Index::fromArray(['key' => 'idx_empty', 'type' => IndexType::Key]));
    }

    public function testCreateIndexSucceedsWithValidConfig(): void
    {
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $attributes);

        $created = $this->database->createIndex('testCol', Index::key(key: 'idx_name', attributes: ['name']));

        $this->assertSame('idx_name', $created->key);
        $this->assertSame(['name'], $created->attributes);
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

        $this->database->renameIndex('testCol', 'idx_name', 'idx_new_name');

        $indexes = $this->lastMetadataWrite()->indexes();
        $this->assertSame(['idx_new_name'], \array_map(static fn (Index $index): string => $index->key, $indexes));
        $this->assertSame(['name'], $indexes[0]->attributes);
    }
}
