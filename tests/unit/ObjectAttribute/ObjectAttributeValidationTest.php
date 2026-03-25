<?php

namespace Tests\Unit\ObjectAttribute;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Query\Schema\ColumnType;

class ObjectAttributeValidationTest extends TestCase
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
                Capability::Objects,
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
        $this->adapter->method('updateDocument')->willReturnArgument(2);
        $this->adapter->method('createAttribute')->willReturn(true);
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

    private function makeCollection(string $id, array $attributes = []): Document
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
            'indexes' => [],
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
        $this->adapter->method('updateDocument')->willReturnArgument(2);
    }

    public function testObjectAttributeInvalidCases(): void
    {
        $metaAttr = new Document([
            '$id' => 'meta', 'key' => 'meta',
            'type' => ColumnType::Object->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->makeCollection('objInvalid', [$metaAttr]);
        $this->setupCollections([$col]);

        $exceptionThrown = false;
        try {
            $this->database->createDocument('objInvalid', new Document([
                '$id' => 'invalid1',
                '$permissions' => [Permission::read(Role::any())],
                'meta' => 'this is a string not an object',
            ]));
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(StructureException::class, $e);
        }
        $this->assertTrue($exceptionThrown, 'Expected Structure exception for string value');

        $exceptionThrown = false;
        try {
            $this->database->createDocument('objInvalid', new Document([
                '$id' => 'invalid2',
                '$permissions' => [Permission::read(Role::any())],
                'meta' => 12345,
            ]));
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(StructureException::class, $e);
        }
        $this->assertTrue($exceptionThrown, 'Expected Structure exception for integer value');

        $exceptionThrown = false;
        try {
            $this->database->createDocument('objInvalid', new Document([
                '$id' => 'invalid3',
                '$permissions' => [Permission::read(Role::any())],
                'meta' => true,
            ]));
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(StructureException::class, $e);
        }
        $this->assertTrue($exceptionThrown, 'Expected Structure exception for boolean value');
    }

    public function testObjectAttributeDefaults(): void
    {
        $emptyDefault = new Document([
            '$id' => 'metaDefaultEmpty', 'key' => 'metaDefaultEmpty',
            'type' => ColumnType::Object->value,
            'size' => 0, 'required' => false, 'default' => [],
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $settingsDefault = new Document([
            '$id' => 'settings', 'key' => 'settings',
            'type' => ColumnType::Object->value,
            'size' => 0, 'required' => false, 'default' => ['config' => ['theme' => 'light', 'lang' => 'en']],
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $profileRequired = new Document([
            '$id' => 'profile', 'key' => 'profile',
            'type' => ColumnType::Object->value,
            'size' => 0, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $profile2Default = new Document([
            '$id' => 'profile2', 'key' => 'profile2',
            'type' => ColumnType::Object->value,
            'size' => 0, 'required' => false, 'default' => ['name' => 'anon'],
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $miscNull = new Document([
            '$id' => 'misc', 'key' => 'misc',
            'type' => ColumnType::Object->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->makeCollection('objDefaults', [$emptyDefault, $settingsDefault, $profileRequired, $profile2Default, $miscNull]);
        $this->setupCollections([$col]);

        $exceptionThrown = false;
        try {
            $this->database->createDocument('objDefaults', new Document([
                '$id' => 'def1',
                '$permissions' => [Permission::read(Role::any())],
            ]));
        } catch (\Exception $e) {
            $exceptionThrown = true;
            $this->assertInstanceOf(StructureException::class, $e);
        }
        $this->assertTrue($exceptionThrown, 'Expected Structure exception for missing required object attribute');

        $doc = $this->database->createDocument('objDefaults', new Document([
            '$id' => 'def2',
            '$permissions' => [Permission::read(Role::any())],
            'profile' => ['name' => 'provided'],
        ]));

        $this->assertIsArray($doc->getAttribute('metaDefaultEmpty'));
        $this->assertEmpty($doc->getAttribute('metaDefaultEmpty'));

        $this->assertIsArray($doc->getAttribute('settings'));
        $this->assertEquals('light', $doc->getAttribute('settings')['config']['theme']);

        $this->assertEquals('provided', $doc->getAttribute('profile')['name']);

        $this->assertIsArray($doc->getAttribute('profile2'));
        $this->assertEquals('anon', $doc->getAttribute('profile2')['name']);

        $this->assertNull($doc->getAttribute('misc'));
    }
}
