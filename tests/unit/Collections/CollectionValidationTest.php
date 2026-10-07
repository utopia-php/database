<?php

namespace Tests\Unit\Collections;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Filter;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Query\Schema\ColumnType;

class CollectionValidationTest extends TestCase
{
    private Adapter&Stub $adapter;

    private Database $database;

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
        $this->database->createCollection(Collection::create(id: 'existing'));
    }

    public function testCreateCollectionValidatesPermissionsFormat(): void
    {
        $this->setupEmptyMetadata();
        $this->database->setValidation(true);

        $this->expectException(DatabaseException::class);
        $this->database->createCollection(Collection::create(id: 'newCol', permissions: ['bad-format']));
    }

    public function testCreateCollectionWithAttributeLimits(): void
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
            attributes: 1,
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
            return in_array($cap, [
                Capability::IndexKey,
                Capability::IndexArray,
                Capability::IndexUnique,
                Capability::DefinedAttributes,
            ]);
        });
        $adapter->method('createCollection')->willReturn(true);
        $adapter->method('deleteCollection')->willReturn(true);
        $adapter->method('getDocument')->willReturn(new Document());

        $db = new Database($adapter, new Cache(new None()));
        $db->getAuthorization()->addRole(Role::any()->toString());

        $attr = Attribute::string(
            key: 'name',
            size: 128,
            required: false,
        );

        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Attribute limit');
        $db->createCollection(Collection::create(id: 'newCol', attributes: [$attr]));
    }

    public function testCreateCollectionRejectsPointAttributeOnMemoryWhenValidateIsOn(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('testing')
            ->setNamespace('collections')
            ->setValidation(true);
        $database->create();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Spatial attributes are not supported');

        $database->createCollection(Collection::create(id: 'places', attributes: [
            Attribute::point(key: 'location'),
        ]));
    }

    public function testCreateCollectionAllowsJsonAndRequiredDefaultsWhenValidateIsOn(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('testing')
            ->setNamespace('collections')
            ->setValidation(true);
        $database->create();

        $collection = $database->createCollection(Collection::create(id: 'users', attributes: [
            Attribute::string(key: 'prefs', size: 65535, default: new \stdClass(), filters: [Filter::Json]),
            Attribute::string(key: 'status', size: 32, required: true, default: 'active'),
        ]));

        $this->assertSame('users', $collection->getId());
    }

    public function testCreateCollectionAcceptsCollectionModel(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('testing')
            ->setNamespace('collections')
            ->setValidation(true);
        $database->create();

        $collection = $database->createCollection(Collection::create(
            id: 'users',
            name: 'Users',
            attributes: [Attribute::string(key: 'name', required: true)],
        ));

        $this->assertSame('users', $collection->getId());
        $this->assertSame('Users', $collection->getAttribute('name'));

        $attributes = $collection->attributes();
        $this->assertSame(1, \count($attributes));
        $this->assertSame('name', $attributes[0]->key);
        $this->assertSame(ColumnType::String, $attributes[0]->type);
    }

    public function testCreateCollectionEmptyPermissionsUsesDefault(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('testing')
            ->setNamespace('collections')
            ->setValidation(true);
        $database->create();

        $anon = $database->createCollection(Collection::create(id: 'anon'));
        $control = $database->createCollection(Collection::create(id: 'control'));
        $locked = $database->createCollection(Collection::create(id: 'locked', permissions: []));

        $this->assertSame($control->getPermissions(), $anon->getPermissions());
        $this->assertSame([Permission::create(Role::any())], $anon->getPermissions());
        $this->assertSame([], $locked->getPermissions());
    }

    public function testCreateCollectionWithIndexLimits(): void
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
            indexes: 0,
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
        $adapter->method('getCountOfIndexes')->willReturn(100);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::IndexKey,
                Capability::IndexArray,
                Capability::IndexUnique,
                Capability::DefinedAttributes,
            ]);
        });
        $adapter->method('createCollection')->willReturn(true);
        $adapter->method('deleteCollection')->willReturn(true);
        $adapter->method('getDocument')->willReturn(new Document());

        $db = new Database($adapter, new Cache(new None()));
        $db->getAuthorization()->addRole(Role::any()->toString());

        $attr = Attribute::string(
            key: 'name',
            size: 128,
            required: false,
        );
        $index = Index::key(
            key: 'idx_name',
            attributes: ['name'],
        );

        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Index limit');
        $db->createCollection(Collection::create(id: 'newCol', attributes: [$attr], indexes: [$index]));
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
        $result = $this->database->updateCollection('testCol', new CollectionUpdate(permissions: $newPermissions, documentSecurity: true));
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

        $result = $this->database->updateCollection('testCol', new CollectionUpdate(permissions: [Permission::read(Role::any())], documentSecurity: true));
        $this->assertTrue($result->getAttribute('documentSecurity'));
    }

    public function testUpdateCollectionThrowsOnNotFound(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());
        $this->expectException(NotFoundException::class);
        $this->database->updateCollection('nonexistent', new CollectionUpdate(permissions: [Permission::read(Role::any())], documentSecurity: true));
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

        $result = $this->database->findCollection('myCol');
        $this->assertNotNull($result);
        $this->assertSame('myCol', $result->getId());
    }

    public function testCollectionExistsDelegatesToAdapter(): void
    {
        $this->adapter->method('getDatabase')->willReturn('testdb');
        $this->adapter->method('collectionExists')->willReturn(true);

        $result = $this->database->collectionExists('testCol', 'testdb');
        $this->assertTrue($result);
    }
}
