<?php

namespace Tests\Unit\Spatial;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/** @internal */
abstract class SpatialAdapter extends Adapter implements Feature\Spatial
{
}

class SpatialValidationTest extends TestCase
{
    private SpatialAdapter&Stub $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = self::createStub(SpatialAdapter::class);
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
        $this->adapter->method('createIndex')->willReturn(true);
        $this->adapter->method('deleteIndex')->willReturn(true);
        $this->adapter->method('createDocument')->willReturnArgument(1);
        $this->adapter->method('updateDocument')->willReturnArgument(2);
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
        $this->adapter->method('updateDocument')->willReturnArgument(2);
    }

    public function testSpatialAttributeDefaults(): void
    {
        $ptAttr = new Document([
            '$id' => 'pt', 'key' => 'pt', 'type' => ColumnType::Point->value,
            'size' => 0, 'required' => false, 'default' => [1.0, 2.0],
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $lnAttr = new Document([
            '$id' => 'ln', 'key' => 'ln', 'type' => ColumnType::Linestring->value,
            'size' => 0, 'required' => false, 'default' => [[0.0, 0.0], [1.0, 1.0]],
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $pgAttr = new Document([
            '$id' => 'pg', 'key' => 'pg', 'type' => ColumnType::Polygon->value,
            'size' => 0, 'required' => false, 'default' => [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [0.0, 0.0]]],
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->makeCollection('spatial_defaults', [$ptAttr, $lnAttr, $pgAttr]);
        $this->setupCollections([$col]);

        $doc = $this->database->createDocument('spatial_defaults', new Document([
            '$id' => ID::custom('d1'),
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $this->assertEquals([1.0, 2.0], $doc->getAttribute('pt'));
        $this->assertEquals([[0.0, 0.0], [1.0, 1.0]], $doc->getAttribute('ln'));
        $this->assertEquals([[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [0.0, 0.0]]], $doc->getAttribute('pg'));
    }

    public function testInvalidSpatialTypes(): void
    {
        $pointAttr = new Document([
            '$id' => 'pointAttr', 'key' => 'pointAttr', 'type' => ColumnType::Point->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [ColumnType::Point->value],
        ]);
        $lineAttr = new Document([
            '$id' => 'lineAttr', 'key' => 'lineAttr', 'type' => ColumnType::Linestring->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [ColumnType::Linestring->value],
        ]);
        $polyAttr = new Document([
            '$id' => 'polyAttr', 'key' => 'polyAttr', 'type' => ColumnType::Polygon->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [ColumnType::Polygon->value],
        ]);

        $col = $this->makeCollection('test_invalid_spatial', [$pointAttr, $lineAttr, $polyAttr]);
        $this->setupCollections([$col]);

        try {
            $this->database->createDocument('test_invalid_spatial', new Document([
                'pointAttr' => [10.0],
            ]));
            $this->fail('Expected StructureException for invalid point');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        try {
            $this->database->createDocument('test_invalid_spatial', new Document([
                'lineAttr' => [[10.0, 20.0]],
            ]));
            $this->fail('Expected StructureException for invalid line');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        try {
            $this->database->createDocument('test_invalid_spatial', new Document([
                'polyAttr' => [10.0, 20.0],
            ]));
            $this->fail('Expected StructureException for invalid polygon');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }
    }

    public function testSpatialDistanceQueryOnNonSpatialAttribute(): void
    {
        $nameAttr = new Document([
            '$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $locAttr = new Document([
            '$id' => 'loc', 'key' => 'loc', 'type' => ColumnType::Point->value,
            'size' => 0, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->makeCollection('spatial_distance_error', [$nameAttr, $locAttr]);
        $this->setupCollections([$col]);

        try {
            $this->database->find('spatial_distance_error', [
                Query::distanceLessThan('name', [0.0, 0.0], 1000),
            ]);
            $this->fail('Expected QueryException');
        } catch (\Exception $e) {
            $this->assertInstanceOf(QueryException::class, $e);
            $msg = strtolower($e->getMessage());
            $this->assertStringContainsString('spatial', $msg);
        }
    }

    public function testSpatialIndexSingleAttributeOnly(): void
    {
        $locAttr = new Document([
            '$id' => 'loc', 'key' => 'loc', 'type' => ColumnType::Point->value,
            'size' => 0, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [ColumnType::Point->value],
        ]);
        $loc2Attr = new Document([
            '$id' => 'loc2', 'key' => 'loc2', 'type' => ColumnType::Point->value,
            'size' => 0, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [ColumnType::Point->value],
        ]);
        $titleAttr = new Document([
            '$id' => 'title', 'key' => 'title', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->makeCollection('spatial_idx_single', [$locAttr, $loc2Attr, $titleAttr]);
        $this->setupCollections([$col]);

        try {
            $this->database->createIndex('spatial_idx_single', new Index(
                key: 'idx_multi',
                type: IndexType::Spatial,
                attributes: ['loc', 'loc2']
            ));
            $this->fail('Expected exception for spatial index on multiple attributes');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(IndexException::class, $e);
        }
    }

    public function testSpatialIndexOnNonSpatial(): void
    {
        $locAttr = new Document([
            '$id' => 'loc', 'key' => 'loc', 'type' => ColumnType::Point->value,
            'size' => 0, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [ColumnType::Point->value],
        ]);
        $nameAttr = new Document([
            '$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value,
            'size' => 4, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->makeCollection('spatial_nonspatial', [$locAttr, $nameAttr]);
        $this->setupCollections([$col]);

        try {
            $this->database->createIndex('spatial_nonspatial', new Index(
                key: 'idx_name_spatial',
                type: IndexType::Spatial,
                attributes: ['name']
            ));
            $this->fail('Expected exception for spatial index on non-spatial attribute');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(IndexException::class, $e);
        }

        try {
            $this->database->createIndex('spatial_nonspatial', new Index(
                key: 'idx_loc_key',
                type: IndexType::Key,
                attributes: ['loc']
            ));
            $this->fail('Expected exception for non-spatial index on spatial attribute');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(IndexException::class, $e);
        }
    }

    public function testInvalidCoordinateDocuments(): void
    {
        $pointAttr = new Document([
            '$id' => 'pointAttr', 'key' => 'pointAttr', 'type' => ColumnType::Point->value,
            'size' => 0, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [ColumnType::Point->value],
        ]);

        $col = $this->makeCollection('test_invalid_coord', [$pointAttr]);
        $this->setupCollections([$col]);

        $this->expectException(StructureException::class);

        $this->database->createDocument('test_invalid_coord', new Document([
            '$id' => 'invalidDoc1',
            '$permissions' => [Permission::read(Role::any())],
            'pointAttr' => [200.0, 20.0],
        ]));
    }
}
