<?php

namespace Tests\Unit\Attributes;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
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

    /** @var list<Document> */
    private array $metadataWrites = [];

    protected function setUp(): void
    {
        $this->adapter = self::createStub(Adapter::class);
        $this->adapter->method('hasSharedTables')->willReturn(false);
        $this->adapter->method('getTenant')->willReturn(null);
        $this->adapter->method('isTenantPerDocument')->willReturn(false);
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
            ]);
        });
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
     */
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

    public function testCreateAttributeOnMissingCollectionThrows(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());

        $this->expectException(NotFoundException::class);
        $this->database->createAttribute('nonexistent', Attribute::string(key: 'name', size: 128));
    }

    public function testCreateAttributeRejectsDuplicateKey(): void
    {
        $existingAttrs = [
            new Document(['$id' => 'title', 'key' => 'title', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $existingAttrs);

        $this->expectException(DuplicateException::class);
        $this->database->createAttribute('testCol', Attribute::string(key: 'title', size: 128));
    }

    public function testCreateAttributeValidatesSizeLimitsForStrings(): void
    {
        $this->setupCollection('testCol');

        $this->expectException(\Utopia\Database\Exception::class);
        $this->expectExceptionMessage('Max size allowed for string');

        $tooBig = $this->adapter->limits()->string + 1;
        $this->database->createAttribute('testCol', Attribute::string(key: 'bigstr', size: $tooBig));
    }

    public function testCreateAttributeSucceedsWithValidString(): void
    {
        $this->setupCollection('testCol');

        $created = $this->database->createAttribute('testCol', Attribute::string(key: 'name', size: 128));

        $this->assertSame('name', $created->key);
        $this->assertSame(Attribute::string(key: 'name', size: 128)->type, $created->type);
    }

    public function testCreateAttributeSucceedsWithInteger(): void
    {
        $this->setupCollection('testCol');

        $created = $this->database->createAttribute('testCol', Attribute::integer(key: 'age'));

        $this->assertSame('age', $created->key);
        $this->assertSame(Attribute::integer(key: 'age')->type, $created->type);
    }

    public function testCreateAttributeSucceedsWithBoolean(): void
    {
        $this->setupCollection('testCol');

        $created = $this->database->createAttribute('testCol', Attribute::boolean(key: 'active'));

        $this->assertSame('active', $created->key);
        $this->assertSame(Attribute::boolean(key: 'active')->type, $created->type);
    }

    public function testCreateAttributeSucceedsWithDouble(): void
    {
        $this->setupCollection('testCol');

        $created = $this->database->createAttribute('testCol', Attribute::double(key: 'score'));

        $this->assertSame('score', $created->key);
        $this->assertSame(Attribute::double(key: 'score')->type, $created->type);
    }

    public function testCreateAttributeEnforcesAttributeCountLimit(): void
    {
        $adapter = self::createStub(Adapter::class);
        $adapter->method('hasSharedTables')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('isTenantPerDocument')->willReturn(false);
        $adapter->method('getNamespace')->willReturn('');
        $adapter->method('limits')->willReturn(new Limits(
            string: 16777215,
            varchar: 16383,
            integer: 2147483647,
            bigInteger: 0,
            attributes: 2,
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
        $adapter->method('getCountOfAttributes')->willReturn(100);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [Capability::IndexKey, Capability::IndexArray, Capability::IndexUnique, Capability::DefinedAttributes]);
        });
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
                    return Database::collectionDefinition();
                }

                return new Document();
            }
        );
        $adapter->method('updateDocument')->willReturnArgument(2);

        $db = new Database($adapter, new Cache(new None()));
        $db->getAuthorization()->addRole(Role::any()->toString());

        $this->expectException(LimitException::class);
        $db->createAttribute('testCol', Attribute::string(key: 'extra', size: 128));
    }

    public function testCreateAttributeEnforcesRowWidthLimit(): void
    {
        $adapter = self::createStub(Adapter::class);
        $adapter->method('hasSharedTables')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('isTenantPerDocument')->willReturn(false);
        $adapter->method('getNamespace')->willReturn('');
        $adapter->method('limits')->willReturn(new Limits(
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
            documentSize: 100,
            minDateTime: new DateTime('0000-01-01'),
            maxDateTime: new DateTime('9999-12-31'),
            idType: ColumnType::String,
            keywords: [],
            internalIndexKeys: [],
        ));
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(200);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [Capability::IndexKey, Capability::IndexArray, Capability::IndexUnique, Capability::DefinedAttributes]);
        });
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
                    return Database::collectionDefinition();
                }

                return new Document();
            }
        );
        $adapter->method('updateDocument')->willReturnArgument(2);

        $db = new Database($adapter, new Cache(new None()));
        $db->getAuthorization()->addRole(Role::any()->toString());

        $this->expectException(LimitException::class);
        $db->createAttribute('testCol', Attribute::string(key: 'wide', size: 128));
    }

    public function testDeleteAttributeRemovesFromCollection(): void
    {
        $existingAttrs = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $existingAttrs);
        $this->adapter->method('deleteAttribute')->willReturn(true);

        $this->database->deleteAttribute('testCol', 'name');

        $this->assertSame([], $this->lastMetadataWrite()->attributes());
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
            Attribute::string(key: 'name', size: 128),
        ]);
    }

    public function testCreateAttributesBatchWithEmptyListThrows(): void
    {
        $this->setupCollection('testCol');

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('No attributes to create');
        $this->database->createAttributes('testCol', []);
    }

    public function testCreateAttributesOnMissingCollectionThrows(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');
        $this->database->createAttributes('nonexistent', [Attribute::string(key: 'name', size: 128)]);
    }
}
