<?php

namespace Tests\Unit\Documents;

use DateTime;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Query\CursorDirection;

#[AllowMockObjectsWithoutExpectations]
class FindLogicTest extends TestCase
{
    private Adapter&MockObject $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = $this->createMock(Adapter::class);
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

        $cache = new Cache(new None());
        $this->database = new Database($this->adapter, $cache);
        $this->database->getAuthorization()->addRole(Role::any()->toString());
    }

    private function collectionDoc(string $id, array $attributes = [], array $indexes = [], array $permissions = []): Document
    {
        if (empty($permissions)) {
            $permissions = [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ];
        }

        return new Document([
            '$id' => $id,
            '$collection' => Database::METADATA,
            '$permissions' => $permissions,
            'name' => $id,
            'attributes' => $attributes,
            'indexes' => $indexes,
            'documentSecurity' => true,
        ]);
    }

    private function setupCollectionLookup(string $id, array $attributes = [], array $indexes = [], array $permissions = []): void
    {
        $collection = $this->collectionDoc($id, $attributes, $indexes, $permissions);
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($id, $collection) {
                if ($col->getId() === Database::METADATA && $docId === $id) {
                    return $collection;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return new Document(Database::COLLECTION);
                }

                return new Document();
            }
        );
    }

    public function testFindWithEmptyQueriesReturnsAdapterResults(): void
    {
        $this->setupCollectionLookup('testCol');
        $doc = new Document(['$id' => 'doc1', 'name' => 'test']);
        $this->adapter->method('find')->willReturn([$doc]);

        $results = $this->database->find('testCol');
        $this->assertCount(1, $results);
        $this->assertSame('doc1', $results[0]->getId());
    }

    public function testFindThrowsNotFoundExceptionForMissingCollection(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());
        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');
        $this->database->find('nonexistent');
    }

    public function testFindValidatesQueriesViaDocumentsValidator(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->database->enableValidation();
        $this->expectException(QueryException::class);
        $this->database->find('testCol', [Query::equal('nonexistent_attr', ['val'])]);
    }

    public function testFindRespectsDefaultLimit(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                25,
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol');
    }

    public function testFindRespectsCustomLimit(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                10,
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::limit(10)]);
    }

    public function testFindRespectsOffset(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                5,
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::offset(5)]);
    }

    public function testFindAddsSequenceToOrderByForUniqueness(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function ($orderAttributes) {
                    return in_array('$sequence', $orderAttributes);
                }),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol');
    }

    public function testFindSkipsSequenceWhenIdAlreadyInOrder(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function ($orderAttributes) {
                    return in_array('$id', $orderAttributes)
                        && ! in_array('$sequence', $orderAttributes);
                }),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::orderAsc('$id')]);
    }

    public function testFindSkipsSequenceWhenSequenceAlreadyInOrder(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function ($orderAttributes) {
                    $sequenceCount = array_count_values($orderAttributes)['$sequence'] ?? 0;

                    return $sequenceCount === 1;
                }),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::orderAsc('$sequence')]);
    }

    public function testFindCursorValidationThrowsOnEmptyCursorAttribute(): void
    {
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false]),
            new Document(['$id' => 'age', 'key' => 'age', 'type' => 'integer', 'size' => 0, 'required' => false, 'array' => false]),
        ];
        $this->setupCollectionLookup('testCol', $attributes);

        $cursorDoc = new Document([
            '$id' => 'cursor1',
            '$collection' => 'testCol',
            'name' => 'test',
        ]);

        $this->expectException(OrderException::class);
        $this->expectExceptionMessage('Order attribute');
        $this->database->skipValidation(fn () => $this->database->find('testCol', [
            Query::orderAsc('name'),
            Query::orderAsc('age'),
            Query::cursorAfter($cursorDoc),
        ]));
    }

    public function testFindCursorCollectionMismatchThrows(): void
    {
        $this->setupCollectionLookup('testCol');

        $cursorDoc = new Document([
            '$id' => 'cursor1',
            '$collection' => 'otherCollection',
            '$sequence' => '1',
        ]);

        $this->expectException(\Utopia\Database\Exception::class);
        $this->expectExceptionMessage('cursor Document must be from the same Collection');
        $this->database->find('testCol', [Query::cursorAfter($cursorDoc)]);
    }

    public function testFindPassesQueriesToAdapter(): void
    {
        $attributes = [
            new Document(['$id' => 'status', 'key' => 'status', 'type' => 'string', 'size' => 64, 'required' => false, 'array' => false]),
        ];
        $indexes = [
            new Document(['$id' => 'idx_status', 'key' => 'idx_status', 'type' => 'key', 'attributes' => ['status'], 'lengths' => [], 'orders' => []]),
        ];
        $this->setupCollectionLookup('testCol', $attributes, $indexes);

        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->callback(function ($queries) {
                    foreach ($queries as $q) {
                        if ($q->getAttribute() === 'status') {
                            return true;
                        }
                    }

                    return false;
                }),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::equal('status', ['active'])]);
    }

    public function testFindDecodesDocumentsAfterRetrieval(): void
    {
        $this->setupCollectionLookup('testCol');
        $rawDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
        ]);
        $this->adapter->method('find')->willReturn([$rawDoc]);

        $results = $this->database->find('testCol');
        $this->assertCount(1, $results);
        $this->assertSame('testCol', $results[0]->getAttribute('$collection'));
    }

    public function testFindEncodesCursorBeforePassingToAdapter(): void
    {
        $this->setupCollectionLookup('testCol');
        $cursorDoc = new Document([
            '$id' => 'c1',
            '$collection' => 'testCol',
            '$sequence' => '100',
        ]);

        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function ($cursor) {
                    return is_array($cursor) && ! empty($cursor);
                }),
                CursorDirection::After,
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::cursorAfter($cursorDoc)]);
    }

    public function testFindWithAggregationOnUnsupportedAdapterThrows(): void
    {
        $this->setupCollectionLookup('testCol');

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Aggregation queries are not supported');
        $this->database->skipValidation(fn () => $this->database->find('testCol', [
            Query::count('*', 'cnt'),
        ]));
    }

    public function testFindWithJoinOnUnsupportedAdapterThrows(): void
    {
        $this->setupCollectionLookup('testCol');

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Join queries are not supported');
        $this->database->skipValidation(fn () => $this->database->find('testCol', [
            Query::join('other', 'fk', '$id'),
        ]));
    }

    public function testFindAggregationWithCursorThrows(): void
    {
        $db = $this->buildDbWithCapabilities([
            Capability::Index, Capability::IndexArray, Capability::UniqueIndex,
            Capability::DefinedAttributes, Capability::Aggregations,
        ]);

        $cursorDoc = new Document([
            '$id' => 'c1',
            '$collection' => 'testCol',
            '$sequence' => '100',
        ]);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Cursor pagination is not supported with aggregation queries');
        $db->skipValidation(fn () => $db->find('testCol', [
            Query::count('*', 'cnt'),
            Query::cursorAfter($cursorDoc),
        ]));
    }

    public function testFindWithGroupBy(): void
    {
        $db = $this->buildDbWithCapabilities([
            Capability::Index, Capability::IndexArray, Capability::UniqueIndex,
            Capability::DefinedAttributes, Capability::Aggregations,
        ], function ($adapter) {
            $adapter->expects($this->once())
                ->method('find')
                ->with(
                    $this->anything(),
                    $this->callback(function ($queries) {
                        foreach ($queries as $q) {
                            if ($q->getMethod()->value === 'groupBy') {
                                return true;
                            }
                        }

                        return false;
                    }),
                    $this->anything(),
                    $this->anything(),
                    $this->anything(),
                    $this->anything(),
                    $this->anything(),
                    $this->anything(),
                    $this->anything()
                )
                ->willReturn([new Document(['status' => 'active', 'cnt' => 5])]);
        });

        $results = $db->skipValidation(fn () => $db->find('testCol', [
            Query::groupBy(['status']),
            Query::count('*', 'cnt'),
        ]));
        $this->assertCount(1, $results);
    }

    public function testFindWithDistinct(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->callback(function ($queries) {
                    foreach ($queries as $q) {
                        if ($q->getMethod()->value === 'distinct') {
                            return true;
                        }
                    }

                    return false;
                }),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->skipValidation(fn () => $this->database->find('testCol', [Query::distinct()]));
    }

    public function testFindWithSelectFiltersResults(): void
    {
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false]),
            new Document(['$id' => 'age', 'key' => 'age', 'type' => 'integer', 'size' => 0, 'required' => false, 'array' => false]),
        ];
        $this->setupCollectionLookup('testCol', $attributes);

        $rawDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            'name' => 'Alice',
            'age' => 30,
        ]);
        $this->adapter->method('find')->willReturn([$rawDoc]);

        $results = $this->database->find('testCol', [Query::select(['name'])]);
        $this->assertCount(1, $results);
    }

    public function testCountDelegatesToAdapter(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->expects($this->once())
            ->method('count')
            ->willReturn(42);

        $result = $this->database->count('testCol');
        $this->assertSame(42, $result);
    }

    public function testSumDelegatesToAdapter(): void
    {
        $attributes = [
            new Document(['$id' => 'amount', 'key' => 'amount', 'type' => 'double', 'size' => 0, 'required' => false, 'array' => false]),
        ];
        $this->setupCollectionLookup('testCol', $attributes);
        $this->adapter->expects($this->once())
            ->method('sum')
            ->willReturn(150.5);

        $result = $this->database->sum('testCol', 'amount');
        $this->assertSame(150.5, $result);
    }

    public function testCursorYieldsDocumentsFromBatches(): void
    {
        $this->setupCollectionLookup('testCol');

        $doc1 = new Document(['$id' => 'd1', '$collection' => 'testCol', '$sequence' => '1']);
        $doc2 = new Document(['$id' => 'd2', '$collection' => 'testCol', '$sequence' => '2']);
        $doc3 = new Document(['$id' => 'd3', '$collection' => 'testCol', '$sequence' => '3']);

        $callCount = 0;
        $this->adapter->method('find')->willReturnCallback(
            function () use (&$callCount, $doc1, $doc2, $doc3) {
                $callCount++;
                if ($callCount === 1) {
                    return [$doc1, $doc2];
                }
                if ($callCount === 2) {
                    return [$doc3];
                }

                return [];
            }
        );

        $results = [];
        foreach ($this->database->cursor('testCol', [], 2) as $doc) {
            $results[] = $doc;
        }
        $this->assertCount(3, $results);
    }

    public function testCursorStopsOnEmptyBatch(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->method('find')->willReturn([]);

        $results = [];
        foreach ($this->database->cursor('testCol', [], 10) as $doc) {
            $results[] = $doc;
        }
        $this->assertCount(0, $results);
    }

    public function testCursorStopsWhenBatchSmallerThanBatchSize(): void
    {
        $this->setupCollectionLookup('testCol');
        $doc1 = new Document(['$id' => 'd1', '$collection' => 'testCol', '$sequence' => '1']);

        $this->adapter->method('find')->willReturn([$doc1]);

        $results = [];
        foreach ($this->database->cursor('testCol', [], 5) as $doc) {
            $results[] = $doc;
        }
        $this->assertCount(1, $results);
    }

    public function testAggregateDelegatesToFind(): void
    {
        $db = $this->buildDbWithCapabilities([
            Capability::Index, Capability::IndexArray, Capability::UniqueIndex,
            Capability::DefinedAttributes, Capability::Aggregations,
        ], function ($adapter) {
            $aggResult = new Document(['cnt' => 10]);
            $adapter->expects($this->once())
                ->method('find')
                ->willReturn([$aggResult]);
        });

        $results = $db->skipValidation(fn () => $db->aggregate('testCol', [Query::count('*', 'cnt')]));
        $this->assertCount(1, $results);
        $this->assertSame(10, $results[0]->getAttribute('cnt'));
    }

    public function testFindWithValidationDisabledAllowsUnknownAttributes(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->method('find')->willReturn([]);

        $results = $this->database->skipValidation(
            fn () => $this->database->find('testCol', [Query::equal('nonexistent', ['val'])])
        );
        $this->assertCount(0, $results);
    }

    public function testFindAuthorizationCheckWhenNoPermission(): void
    {
        $collection = new Document([
            '$id' => 'restricted',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::user('admin'))],
            'name' => 'restricted',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => false,
        ]);
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection) {
                if ($col->getId() === Database::METADATA && $docId === 'restricted') {
                    return $collection;
                }

                return new Document();
            }
        );

        $db = new Database($this->adapter, new Cache(new None()));

        $this->expectException(AuthorizationException::class);
        $db->find('restricted');
    }

    public function testFindAllowsDocumentSecurityWhenCollectionPermissionFails(): void
    {
        $collection = new Document([
            '$id' => 'docSec',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::user('admin'))],
            'name' => 'docSec',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => true,
        ]);
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection) {
                if ($col->getId() === Database::METADATA && $docId === 'docSec') {
                    return $collection;
                }

                return new Document();
            }
        );
        $this->adapter->method('find')->willReturn([]);

        $db = new Database($this->adapter, new Cache(new None()));
        $results = $db->find('docSec');
        $this->assertCount(0, $results);
    }

    public function testFindSetsCollectionAttributeOnResults(): void
    {
        $this->setupCollectionLookup('testCol');
        $doc = new Document(['$id' => 'doc1']);
        $this->adapter->method('find')->willReturn([$doc]);

        $results = $this->database->find('testCol');
        $this->assertSame('testCol', $results[0]->getAttribute('$collection'));
    }

    public function testFindCursorBeforePassesDirection(): void
    {
        $this->setupCollectionLookup('testCol');
        $cursorDoc = new Document([
            '$id' => 'c1',
            '$collection' => 'testCol',
            '$sequence' => '100',
        ]);

        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                CursorDirection::Before,
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::cursorBefore($cursorDoc)]);
    }

    public function testFindMultipleOrderAttributes(): void
    {
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false]),
            new Document(['$id' => 'age', 'key' => 'age', 'type' => 'integer', 'size' => 0, 'required' => false, 'array' => false]),
        ];
        $this->setupCollectionLookup('testCol', $attributes);

        $this->adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function ($orderAttributes) {
                    return $orderAttributes[0] === 'name'
                        && $orderAttributes[1] === 'age'
                        && in_array('$sequence', $orderAttributes);
                }),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything()
            )
            ->willReturn([]);

        $this->database->find('testCol', [
            Query::orderAsc('name'),
            Query::orderDesc('age'),
        ]);
    }

    public function testCountThrowsAuthorizationForMissingCollection(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());
        $this->expectException(AuthorizationException::class);
        $this->database->count('nonexistent');
    }

    public function testSumThrowsAuthorizationForMissingCollection(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());
        $this->expectException(AuthorizationException::class);
        $this->database->sum('nonexistent', 'amount');
    }

    public function testSumValidatesQueries(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->database->enableValidation();

        $this->expectException(QueryException::class);
        $this->database->sum('testCol', 'amount', [Query::equal('unknown_field', ['val'])]);
    }

    private function buildDbWithCapabilities(array $capabilities, ?callable $adapterSetup = null): Database
    {
        $adapter = $this->createMock(Adapter::class);
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
        $adapter->method('getDocumentSizeLimit')->willReturn(0);
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('getInternalIndexesKeys')->willReturn([]);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('castingBefore')->willReturnArgument(1);
        $adapter->method('castingAfter')->willReturnArgument(1);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) use ($capabilities) {
            return in_array($cap, $capabilities);
        });

        $collection = new Document([
            '$id' => 'testCol',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any())],
            'name' => 'testCol',
            'attributes' => [
                new Document(['$id' => 'status', 'key' => 'status', 'type' => 'string', 'size' => 64, 'required' => false, 'array' => false]),
            ],
            'indexes' => [],
            'documentSecurity' => true,
        ]);
        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection) {
                if ($col->getId() === Database::METADATA && $docId === 'testCol') {
                    return $collection;
                }

                return new Document();
            }
        );

        if ($adapterSetup) {
            $adapterSetup($adapter);
        } else {
            $adapter->method('find')->willReturn([]);
        }

        $cache = new Cache(new None());
        $db = new Database($adapter, $cache);
        $db->getAuthorization()->addRole(Role::any()->toString());

        return $db;
    }
}
