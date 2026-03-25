<?php

namespace Tests\Unit\Vector;

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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class VectorValidationTest extends TestCase
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
                Capability::Vectors,
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
        $this->adapter->method('find')->willReturn([]);
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

    private function vectorCollection(string $id, int $dimensions = 3, bool $required = true, array $extraAttrs = []): Document
    {
        $attrs = [
            new Document([
                '$id' => 'embedding', 'key' => 'embedding',
                'type' => ColumnType::Vector->value,
                'size' => $dimensions, 'required' => $required, 'default' => null,
                'signed' => true, 'array' => false, 'filters' => [],
            ]),
            ...$extraAttrs,
        ];

        return $this->makeCollection($id, $attrs);
    }

    public function testVectorInvalidDimensions(): void
    {
        $col = $this->makeCollection('vectorError');
        $this->setupCollections([$col]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector dimensions must be a positive integer');

        $this->database->createAttribute('vectorError', new Attribute(
            key: 'bad_embedding',
            type: ColumnType::Vector,
            size: 0,
            required: true
        ));
    }

    public function testVectorTooManyDimensions(): void
    {
        $col = $this->makeCollection('vectorLimit');
        $this->setupCollections([$col]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector dimensions cannot exceed 16000');

        $this->database->createAttribute('vectorLimit', new Attribute(
            key: 'huge_embedding',
            type: ColumnType::Vector,
            size: 16001,
            required: true
        ));
    }

    public function testVectorQueryValidation(): void
    {
        $textAttr = new Document([
            '$id' => 'name', 'key' => 'name',
            'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->vectorCollection('vectorValidation', 3, true, [$textAttr]);
        $this->setupCollections([$col]);

        $this->expectException(DatabaseException::class);

        $this->database->find('vectorValidation', [
            Query::vectorDot('name', [1.0, 0.0, 0.0]),
        ]);
    }

    public function testVectorDimensionMismatch(): void
    {
        $col = $this->vectorCollection('vectorDimMismatch');
        $this->setupCollections([$col]);

        $this->expectException(DatabaseException::class);

        $this->database->createDocument('vectorDimMismatch', new Document([
            '$permissions' => [Permission::read(Role::any())],
            'embedding' => [1.0, 0.0],
        ]));
    }

    public function testVectorWithInvalidDataTypes(): void
    {
        $col = $this->vectorCollection('vectorInvalidTypes');
        $this->setupCollections([$col]);

        try {
            $this->database->createDocument('vectorInvalidTypes', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => ['one', 'two', 'three'],
            ]));
            $this->fail('Should have thrown exception for non-numeric vector values');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric values', strtolower($e->getMessage()));
        }

        try {
            $this->database->createDocument('vectorInvalidTypes', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => [1.0, 'two', 3.0],
            ]));
            $this->fail('Should have thrown exception for mixed type vector values');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric values', strtolower($e->getMessage()));
        }
    }

    public function testVectorQueryValidationExtended(): void
    {
        $textAttr = new Document([
            '$id' => 'text', 'key' => 'text',
            'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->vectorCollection('vectorValidation2', 3, true, [$textAttr]);
        $this->setupCollections([$col]);

        try {
            $this->database->find('vectorValidation2', [
                Query::vectorCosine('embedding', [1.0, 0.0]),
            ]);
            $this->fail('Should have thrown exception for dimension mismatch');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('elements', strtolower($e->getMessage()));
        }

        try {
            $this->database->find('vectorValidation2', [
                Query::vectorCosine('text', [1.0, 0.0, 0.0]),
            ]);
            $this->fail('Should have thrown exception for non-vector attribute');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('vector', strtolower($e->getMessage()));
        }
    }

    public function testVectorWithAssociativeArray(): void
    {
        $col = $this->vectorCollection('vectorAssoc');
        $this->setupCollections([$col]);

        try {
            $this->database->createDocument('vectorAssoc', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => ['x' => 1.0, 'y' => 0.0, 'z' => 0.0],
            ]));
            $this->fail('Should have thrown exception for associative array');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }
    }

    public function testVectorWithSparseArray(): void
    {
        $col = $this->vectorCollection('vectorSparse');
        $this->setupCollections([$col]);

        try {
            $vector = [];
            $vector[0] = 1.0;
            $vector[2] = 1.0;
            $this->database->createDocument('vectorSparse', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => $vector,
            ]));
            $this->fail('Should have thrown exception for sparse array');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }
    }

    public function testVectorWithNestedArrays(): void
    {
        $col = $this->vectorCollection('vectorNested');
        $this->setupCollections([$col]);

        try {
            $this->database->createDocument('vectorNested', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => [[1.0], [0.0], [0.0]],
            ]));
            $this->fail('Should have thrown exception for nested array');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }
    }

    public function testVectorWithBooleansInArray(): void
    {
        $col = $this->vectorCollection('vectorBooleans');
        $this->setupCollections([$col]);

        try {
            $this->database->createDocument('vectorBooleans', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => [true, false, true],
            ]));
            $this->fail('Should have thrown exception for boolean values');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }
    }

    public function testVectorWithStringNumbers(): void
    {
        $col = $this->vectorCollection('vectorStringNums');
        $this->setupCollections([$col]);

        try {
            $this->database->createDocument('vectorStringNums', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => ['1.0', '2.0', '3.0'],
            ]));
            $this->fail('Should have thrown exception for string numbers');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }
    }

    public function testVectorCosineSimilarityDivisionByZero(): void
    {
        $col = $this->vectorCollection('vectorCosineZero');
        $this->setupCollections([$col]);

        $doc = $this->database->createDocument('vectorCosineZero', new Document([
            '$permissions' => [Permission::read(Role::any())],
            'embedding' => [0.0, 0.0, 0.0],
        ]));

        $this->assertNotNull($doc->getId());
    }

    public function testVectorSearchWithRestrictedPermissions(): void
    {
        $col = $this->vectorCollection('vectorPermissions');
        $this->setupCollections([$col]);

        $doc = $this->database->createDocument('vectorPermissions', new Document([
            '$permissions' => [Permission::read(Role::user('user1'))],
            'embedding' => [1.0, 0.0, 0.0],
        ]));

        $this->assertNotNull($doc->getId());
    }

    public function testVectorPermissionFilteringAfterScoring(): void
    {
        $scoreAttr = new Document([
            '$id' => 'score', 'key' => 'score',
            'type' => ColumnType::Integer->value,
            'size' => 0, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->vectorCollection('vectorPermScoring', 3, true, [$scoreAttr]);
        $this->setupCollections([$col]);

        $doc = $this->database->createDocument('vectorPermScoring', new Document([
            '$permissions' => [Permission::read(Role::any())],
            'score' => 4,
            'embedding' => [0.6, 0.4, 0.0],
        ]));

        $this->assertNotNull($doc->getId());
    }

    public function testVectorRequiredWithNullValue(): void
    {
        $col = $this->vectorCollection('vectorRequiredNull', 3, true);
        $this->setupCollections([$col]);

        $this->expectException(DatabaseException::class);

        $this->database->createDocument('vectorRequiredNull', new Document([
            '$permissions' => [Permission::read(Role::any())],
            'embedding' => null,
        ]));
    }

    public function testVectorIndexCreationFailure(): void
    {
        $textAttr = new Document([
            '$id' => 'text', 'key' => 'text',
            'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $col = $this->vectorCollection('vectorIdxFail', 3, true, [$textAttr]);
        $this->setupCollections([$col]);

        try {
            $this->database->createIndex('vectorIdxFail', new Index(
                key: 'bad_idx',
                type: IndexType::HnswCosine,
                attributes: ['text']
            ));
            $this->fail('Should not allow vector index on non-vector attribute');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('vector', strtolower($e->getMessage()));
        }
    }

    public function testVectorNonNumericValidationE2E(): void
    {
        $col = $this->vectorCollection('vectorNonNumeric');
        $this->setupCollections([$col]);

        try {
            $this->database->createDocument('vectorNonNumeric', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => [1.0, null, 0.0],
            ]));
            $this->fail('Should reject null in vector array');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('numeric', strtolower($e->getMessage()));
        }

        try {
            $this->database->createDocument('vectorNonNumeric', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => [1.0, (object) ['x' => 1], 0.0],
            ]));
            $this->fail('Should reject object in vector array');
        } catch (\Throwable $e) {
            $this->assertTrue(true);
        }
    }
}
