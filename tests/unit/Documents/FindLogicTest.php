<?php

namespace Tests\Unit\Documents;

use DateTime;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Query\CursorDirection;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;

class FindLogicTest extends TestCase
{
    private Adapter&Stub $adapter;

    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $this->useAdapter(self::createStub(Adapter::class));
    }

    private function mockAdapter(): Adapter&MockObject
    {
        $adapter = $this->createMock(Adapter::class);
        $this->useAdapter($adapter);

        return $adapter;
    }

    private function useAdapter(Adapter&Stub $adapter): void
    {
        $this->adapter = $adapter;
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
                Capability::Vectors,
            ]);
        });

        $cache = new Cache(new None());
        $this->database = new Database($this->adapter, $cache);
        $this->database->getAuthorization()->addRole(Role::any()->toString());
    }

    /**
     * @param  array<Document>  $attributes
     * @param  array<Document>  $indexes
     * @param  array<string>  $permissions
     */
    private function collectionDoc(string $id, array $attributes = [], array $indexes = [], array $permissions = [], bool $documentSecurity = true): Document
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
            'documentSecurity' => $documentSecurity,
        ]);
    }

    /**
     * @param  array<Document>  $attributes
     * @param  array<Document>  $indexes
     * @param  array<string>  $permissions
     * @param  array<Document>  $collections
     */
    private function setupCollectionLookup(string $id, array $attributes = [], array $indexes = [], array $permissions = [], bool $documentSecurity = true, array $collections = []): void
    {
        $map = [$id => $this->collectionDoc($id, $attributes, $indexes, $permissions, $documentSecurity)];
        foreach ($collections as $collection) {
            $map[$collection->getId()] = $collection;
        }
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($map) {
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return Database::collectionDefinition();
                }
                if ($col->getId() === Database::METADATA && isset($map[$docId])) {
                    return $map[$docId];
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
        $this->database->setValidation(true);
        $this->expectException(QueryException::class);
        $this->database->find('testCol', [Query::equal('nonexistent_attr', ['val'])]);
    }

    public function testFindRespectsDefaultLimit(): void
    {
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $adapter->expects($this->once())
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
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $adapter->expects($this->once())
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
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $adapter->expects($this->once())
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
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (array $orderAttributes) {
                    return in_array('$sequence', $orderAttributes, true);
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
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (array $orderAttributes) {
                    return in_array('$id', $orderAttributes, true)
                        && ! in_array('$sequence', $orderAttributes, true);
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
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (mixed $orderAttributes): bool {
                    if (! \is_array($orderAttributes)) {
                        return false;
                    }
                    $values = \array_filter($orderAttributes, \is_string(...));
                    $sequenceCount = \array_count_values($values)[Document::SEQUENCE] ?? 0;

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

    public function testVectorFindOrdersByDistanceAloneWithoutCursor(): void
    {
        $adapter = $this->mockAdapter();
        $attributes = [
            new Document(['$id' => 'embedding', 'key' => 'embedding', 'type' => ColumnType::Vector->value, 'size' => 2, 'required' => false, 'array' => false]),
        ];
        $this->setupCollectionLookup('testCol', $attributes);
        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                [],
                [],
                $this->anything(),
                $this->anything(),
                $this->anything(),
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::vectorCosine('embedding', [0.1, 0.2])]);
    }

    public function testVectorFindWithCursorKeepsSequenceTieBreaker(): void
    {
        $adapter = $this->mockAdapter();
        $attributes = [
            new Document(['$id' => 'embedding', 'key' => 'embedding', 'type' => ColumnType::Vector->value, 'size' => 2, 'required' => false, 'array' => false]),
        ];
        $this->setupCollectionLookup('testCol', $attributes);
        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                ['$sequence'],
                [OrderDirection::Asc],
                $this->anything(),
                $this->anything(),
                $this->anything(),
            )
            ->willReturn([]);

        $cursor = new Document([
            '$id' => 'page',
            '$sequence' => 17,
            '$collection' => 'testCol',
            '$distance' => 0.25,
        ]);

        $this->database->find('testCol', [
            Query::vectorCosine('embedding', [0.1, 0.2]),
            Query::cursorAfter($cursor),
        ]);
    }

    public function testDateOrderAppendsMatchingSequenceTieBreakLast(): void
    {
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                ['$createdAt', '$updatedAt', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc, OrderDirection::Desc],
                $this->anything(),
                $this->anything(),
                $this->anything(),
            )
            ->willReturn([]);

        $this->database->find('testCol', [
            Query::orderDesc('$createdAt'),
            Query::orderAsc('$updatedAt'),
        ]);
    }

    public function testDescendingOrderAppendsDescendingSequenceTieBreak(): void
    {
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol', [
            new Document(['$id' => 'age', 'key' => 'age', 'type' => 'integer', 'size' => 0, 'required' => false, 'array' => false]),
        ]);
        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
                $this->anything(),
                $this->anything(),
                $this->anything(),
            )
            ->willReturn([]);

        $this->database->find('testCol', [Query::orderDesc('age')]);
    }

    public function testDateOrdersFollowTheirIndexDirection(): void
    {
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol', [], [
            new Document(['$id' => 'updated_desc', 'key' => 'updated_desc', 'type' => 'key', 'attributes' => ['$updatedAt'], 'lengths' => [], 'orders' => ['DESC']]),
        ]);

        $sent = [];
        $adapter->expects($this->exactly(3))->method('find')->willReturnCallback(function (...$arguments) use (&$sent): array {
            $sent[] = [$arguments[4], $arguments[5]];

            return [];
        });

        // The built-in _created_at index is ascending and not in the collection metadata, so a
        // date order with no declared index keeps following its own direction, as it always has.
        $this->database->find('testCol', [Query::orderDesc('$createdAt')]);
        $this->database->find('testCol', [Query::orderAsc('$createdAt')]);
        $this->database->find('testCol', [Query::orderDesc('$updatedAt')]);

        $this->assertSame([
            [['$createdAt', '$sequence'], [OrderDirection::Desc, OrderDirection::Desc]],
            [['$createdAt', '$sequence'], [OrderDirection::Asc, OrderDirection::Asc]],
            [['$updatedAt', '$sequence'], [OrderDirection::Desc, OrderDirection::Asc]],
        ], $sent);
    }

    public function testDescendingIndexKeepsAscendingSequenceTieBreak(): void
    {
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol', [
            new Document(['$id' => 'age', 'key' => 'age', 'type' => 'integer', 'size' => 0, 'required' => false, 'array' => false]),
        ], [
            new Document(['$id' => 'age_desc', 'key' => 'age_desc', 'type' => 'key', 'attributes' => ['age'], 'lengths' => [], 'orders' => ['DESC']]),
        ]);

        $sent = [];
        $adapter->expects($this->exactly(2))->method('find')->willReturnCallback(function (...$arguments) use (&$sent): array {
            $sent[] = [$arguments[4], $arguments[5]];

            return [];
        });

        $this->database->find('testCol', [Query::orderDesc('age')]);
        $this->database->find('testCol', [Query::orderAsc('age')]);

        // InnoDB appends the primary key ascending, so (age DESC) is read forward for
        // `age DESC, _id ASC` and backward for `age ASC, _id DESC`.
        $this->assertSame([
            [['age', '$sequence'], [OrderDirection::Desc, OrderDirection::Asc]],
            [['age', '$sequence'], [OrderDirection::Asc, OrderDirection::Desc]],
        ], $sent);
    }

    /**
     * Compound indexes whose leading columns are pinned by equality filters serve the order the same way an exact
     * index does: with `status = ?`, InnoDB reads `(status, age DESC, _id)` in `age DESC, _id ASC` order.
     *
     * @return array<string, array{list<array{attributes: list<string>, orders: list<string>, lengths?: list<int>, type?: string}>, list<Query>, list<string>, list<OrderDirection>}>
     */
    public static function compoundIndexTieBreakProvider(): array
    {
        $statusAgeDesc = ['attributes' => ['status', 'age'], 'orders' => ['ASC', 'DESC']];
        $statusAgeAsc = ['attributes' => ['status', 'age'], 'orders' => ['ASC', 'ASC']];
        $statusTypeAgeDesc = ['attributes' => ['status', 'type', 'age'], 'orders' => ['ASC', 'ASC', 'DESC']];

        return [
            // (status, age DESC): the regression case. The old code sent _id ASC here and was served.
            '(status, age DESC), status = ?, age DESC reads forward' => [
                [$statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, age DESC), status = ?, age ASC reads backward' => [
                [$statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderAsc('age')],
                ['age', '$sequence'],
                [OrderDirection::Asc, OrderDirection::Desc],
            ],

            // (status, age): ascending compound index, served by following the leading direction.
            '(status, age), status = ?, age DESC reads backward' => [
                [$statusAgeAsc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, age), status = ?, age ASC reads forward' => [
                [$statusAgeAsc],
                [Query::equal('status', ['active']), Query::orderAsc('age')],
                ['age', '$sequence'],
                [OrderDirection::Asc, OrderDirection::Asc],
            ],

            // Every leading column must be pinned by an equality filter for the index to serve the order.
            '(status, type, age DESC), both pinned, age DESC reads forward' => [
                [$statusTypeAgeDesc],
                [Query::equal('status', ['active']), Query::equal('type', ['a']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, type, age DESC), only status pinned, falls back to leading direction' => [
                [$statusTypeAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],

            // The leading column is not pinned to a single value, so the index cannot serve the order.
            '(status, age DESC), no status filter, falls back to leading direction' => [
                [$statusAgeDesc],
                [Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, age DESC), status IN (a, b), falls back to leading direction' => [
                [$statusAgeDesc],
                [Query::equal('status', ['active', 'archived']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, age DESC), status range filter, falls back to leading direction' => [
                [$statusAgeDesc],
                [Query::greaterThan('status', 'a'), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],

            // An exact index and a pinned compound index that agree.
            '(age DESC) and (status, age DESC), status = ?, age DESC reads forward' => [
                [['attributes' => ['age'], 'orders' => ['DESC']], $statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],

            // MySQL cannot read a prefix-length index in order, so it must not decide the direction.
            '(name(64) DESC) prefix index, name DESC falls back to leading direction' => [
                [['attributes' => ['name'], 'orders' => ['DESC'], 'lengths' => [64]]],
                [Query::orderDesc('name')],
                ['name', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(name(128) DESC) length covers the column, name DESC reads forward' => [
                [['attributes' => ['name'], 'orders' => ['DESC'], 'lengths' => [128]]],
                [Query::orderDesc('name')],
                ['name', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            // Every row matching status = ? shares the indexed prefix, so the index still orders them by age.
            '(status(8), age DESC) prefix on a pinned column, age DESC reads forward' => [
                [['attributes' => ['status', 'age'], 'orders' => ['ASC', 'DESC'], 'lengths' => [8, 0]]],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status(8), name(16) DESC) prefix on the order column, falls back to leading direction' => [
                [['attributes' => ['status', 'name'], 'orders' => ['ASC', 'DESC'], 'lengths' => [8, 16]]],
                [Query::equal('status', ['active']), Query::orderDesc('name')],
                ['name', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],

            // Several equality-filtered columns in front of the order column.
            '(status, type, name, age DESC), all three pinned, age DESC reads forward' => [
                [['attributes' => ['status', 'type', 'name', 'age'], 'orders' => ['ASC', 'ASC', 'ASC', 'DESC']]],
                [Query::equal('status', ['active']), Query::equal('type', ['a']), Query::equal('name', ['n']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, type, age DESC), filters in a different order than the index, still pinned' => [
                [$statusTypeAgeDesc],
                [Query::equal('type', ['a']), Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, age DESC), extra filters outside the index do not matter' => [
                [$statusAgeDesc],
                [Query::equal('status', ['active']), Query::equal('type', ['a']), Query::greaterThan('name', 'm'), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, type, age DESC), only the second column pinned, falls back' => [
                [$statusTypeAgeDesc],
                [Query::equal('type', ['a']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, type, age DESC), status = ? and type range, falls back' => [
                [$statusTypeAgeDesc],
                [Query::equal('status', ['active']), Query::greaterThan('type', 'a'), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, type, age DESC), status = ? and type IN (a, b), falls back' => [
                [$statusTypeAgeDesc],
                [Query::equal('status', ['active']), Query::equal('type', ['a', 'b']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],

            // The order column must be the index's last column: in (status, age DESC, type) _id follows type, not age.
            '(status, age DESC, type), status = ?, age DESC, order column not last, falls back' => [
                [['attributes' => ['status', 'age', 'type'], 'orders' => ['ASC', 'DESC', 'ASC']]],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, age DESC, type), status = ? and type = ?, order column not last, falls back' => [
                [['attributes' => ['status', 'age', 'type'], 'orders' => ['ASC', 'DESC', 'ASC']]],
                [Query::equal('status', ['active']), Query::equal('type', ['a']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],

            // A partly pinned index earlier in the list must not stop the search for a fully pinned one.
            '(type, age) unpinned, then (status, age DESC) pinned, the pinned one decides' => [
                [['attributes' => ['type', 'age'], 'orders' => ['ASC', 'ASC']], $statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, type, age) partly pinned, then (status, age DESC) pinned, the pinned one decides' => [
                [['attributes' => ['status', 'type', 'age'], 'orders' => ['ASC', 'ASC', 'ASC']], $statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],

            // Cursor pages must use the same tie break as the first page, or rows that tie on age are skipped or repeated.
            '(status, age DESC), status = ?, age DESC, cursorAfter' => [
                [$statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age'), Query::cursorAfter(new Document(['$id' => 'page', '$sequence' => 17, '$collection' => 'testCol', 'status' => 'active', 'age' => 30]))],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, age DESC), status = ?, age DESC, cursorBefore' => [
                [$statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age'), Query::cursorBefore(new Document(['$id' => 'page', '$sequence' => 17, '$collection' => 'testCol', 'status' => 'active', 'age' => 30]))],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],

            // Several order attributes: every direction must agree with the index, or every one must oppose it.
            '(status, name DESC, age), status = ?, name DESC, age ASC reads forward' => [
                [['attributes' => ['status', 'name', 'age'], 'orders' => ['ASC', 'DESC', 'ASC']]],
                [Query::equal('status', ['active']), Query::orderDesc('name'), Query::orderAsc('age')],
                ['name', 'age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc, OrderDirection::Asc],
            ],
            '(status, name DESC, age), status = ?, name ASC, age DESC reads backward' => [
                [['attributes' => ['status', 'name', 'age'], 'orders' => ['ASC', 'DESC', 'ASC']]],
                [Query::equal('status', ['active']), Query::orderAsc('name'), Query::orderDesc('age')],
                ['name', 'age', '$sequence'],
                [OrderDirection::Asc, OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, name DESC, age), status = ?, name DESC, age DESC mixes, falls back to leading direction' => [
                [['attributes' => ['status', 'name', 'age'], 'orders' => ['ASC', 'DESC', 'ASC']]],
                [Query::equal('status', ['active']), Query::orderDesc('name'), Query::orderDesc('age')],
                ['name', 'age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc, OrderDirection::Desc],
            ],

            // Filters that do not pin the leading column to one value.
            '(status, age DESC), status != ?, falls back to leading direction' => [
                [$statusAgeDesc],
                [Query::notEqual('status', 'archived'), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, age DESC), status IS NULL, falls back to leading direction' => [
                [$statusAgeDesc],
                [Query::isNull('status'), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            // An and() group pins its equalities like top-level filters, at any depth; an or() group pins nothing.
            '(status, age DESC), status = ? nested in and(), reads forward' => [
                [$statusAgeDesc],
                [Query::and([Query::equal('status', ['active']), Query::greaterThan('age', 1)]), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, type, age DESC), equalities split across top level and nested and(and()), reads forward' => [
                [$statusTypeAgeDesc],
                [Query::equal('status', ['active']), Query::and([Query::greaterThan('age', 1), Query::and([Query::equal('type', ['a']), Query::lessThan('age', 99)])]), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            '(status, age DESC), status = ? inside or(), falls back' => [
                [$statusAgeDesc],
                [Query::or([Query::equal('status', ['active']), Query::greaterThan('age', 1)]), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, age DESC), and() inside or(), falls back' => [
                [$statusAgeDesc],
                [Query::or([Query::and([Query::equal('status', ['active']), Query::greaterThan('age', 1)]), Query::equal('status', ['archived'])]), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],

            // Index types.
            'unique (status, age DESC), status = ?, age DESC reads forward' => [
                [['type' => 'unique'] + $statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
            'fulltext (name) is ignored, name DESC falls back to leading direction' => [
                [['type' => 'fulltext', 'attributes' => ['name'], 'orders' => []]],
                [Query::orderDesc('name')],
                ['name', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],

            // Two indexes that disagree: the first declared wins, and either one serves the order.
            '(status, age) before (status, age DESC), first wins' => [
                [$statusAgeAsc, $statusAgeDesc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Desc],
            ],
            '(status, age DESC) before (status, age), first wins' => [
                [$statusAgeDesc, $statusAgeAsc],
                [Query::equal('status', ['active']), Query::orderDesc('age')],
                ['age', '$sequence'],
                [OrderDirection::Desc, OrderDirection::Asc],
            ],
        ];
    }

    /**
     * @param  list<array{attributes: list<string>, orders: list<string>, lengths?: list<int>, type?: string}>  $indexes
     * @param  list<Query>  $queries
     * @param  list<string>  $expectedAttributes
     * @param  list<OrderDirection>  $expectedTypes
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('compoundIndexTieBreakProvider')]
    public function testCompoundIndexTieBreakDirection(array $indexes, array $queries, array $expectedAttributes, array $expectedTypes): void
    {
        $adapter = $this->mockAdapter();
        $attribute = static fn (string $key, string $type, int $size): Document => new Document(['$id' => $key, 'key' => $key, 'type' => $type, 'size' => $size, 'required' => false, 'array' => false]);

        $indexDocuments = [];
        foreach ($indexes as $position => $index) {
            $indexDocuments[] = new Document([
                '$id' => 'index_'.$position,
                'key' => 'index_'.$position,
                'type' => $index['type'] ?? 'key',
                'attributes' => $index['attributes'],
                'lengths' => $index['lengths'] ?? [],
                'orders' => $index['orders'],
            ]);
        }

        $this->setupCollectionLookup('testCol', [
            $attribute('status', 'string', 32),
            $attribute('type', 'string', 32),
            $attribute('name', 'string', 128),
            $attribute('age', 'integer', 0),
        ], $indexDocuments);

        $sent = null;
        $adapter->expects($this->once())->method('find')->willReturnCallback(function (...$arguments) use (&$sent): array {
            $sent = [$arguments[4], $arguments[5]];

            return [];
        });

        $this->database->find('testCol', $queries);

        $this->assertSame([$expectedAttributes, $expectedTypes], $sent);
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
        $adapter = $this->mockAdapter();
        $attributes = [
            new Document(['$id' => 'status', 'key' => 'status', 'type' => 'string', 'size' => 64, 'required' => false, 'array' => false]),
        ];
        $indexes = [
            new Document(['$id' => 'idx_status', 'key' => 'idx_status', 'type' => 'key', 'attributes' => ['status'], 'lengths' => [], 'orders' => []]),
        ];
        $this->setupCollectionLookup('testCol', $attributes, $indexes);

        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->callback(function (mixed $queries): bool {
                    if (! \is_array($queries)) {
                        return false;
                    }
                    foreach ($queries as $q) {
                        if ($q instanceof Query && $q->getAttribute() === 'status') {
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
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $cursorDoc = new Document([
            '$id' => 'c1',
            '$collection' => 'testCol',
            '$sequence' => '100',
        ]);

        $adapter->expects($this->once())
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

    public function testAggregateOnUnsupportedAdapterThrows(): void
    {
        $this->setupCollectionLookup('testCol');

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Aggregation queries are not supported');
        $this->database->skipValidation(fn () => $this->database->aggregate('testCol', [
            Query::count('*', 'cnt'),
        ]));
    }

    public function testFindRefusesAnAggregate(): void
    {
        $this->setupCollectionLookup('testCol');

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('find() does not run aggregate or groupBy queries: use aggregate()');
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
            Query::join('other', 'j0', [Query::on('fk', '$id')]),
        ]));
    }

    public function testAggregateWithCursorThrows(): void
    {
        $db = $this->buildDbWithCapabilities(self::createStub(Adapter::class), [
            Capability::IndexKey, Capability::IndexArray, Capability::IndexUnique,
            Capability::DefinedAttributes, Capability::Aggregations,
        ]);

        $cursorDoc = new Document([
            '$id' => 'c1',
            '$collection' => 'testCol',
            '$sequence' => '100',
        ]);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Cursor pagination is not supported with aggregation queries');
        $db->skipValidation(fn () => $db->aggregate('testCol', [
            Query::count('*', 'cnt'),
            Query::cursorAfter($cursorDoc),
        ]));
    }

    public function testAggregateWithGroupBy(): void
    {
        $db = $this->buildDbWithCapabilities($this->createMock(Adapter::class), [
            Capability::IndexKey, Capability::IndexArray, Capability::IndexUnique,
            Capability::DefinedAttributes, Capability::Aggregations,
        ], function (Adapter&MockObject $adapter): void {
            $adapter->expects($this->once())
                ->method('find')
                ->with(
                    $this->anything(),
                    $this->callback(function (mixed $queries): bool {
                        if (! \is_array($queries)) {
                            return false;
                        }
                        foreach ($queries as $q) {
                            if ($q instanceof Query && $q->getMethod()->value === 'groupBy') {
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

        $results = $db->skipValidation(fn () => $db->aggregate('testCol', [
            Query::groupBy(['status']),
            Query::count('*', 'cnt'),
        ]));
        $this->assertSame([['status' => 'active', 'cnt' => 5]], $results);
    }

    public function testFindWithDistinct(): void
    {
        $db = $this->buildDbWithCapabilities($this->createMock(Adapter::class), [
            Capability::IndexKey, Capability::IndexArray, Capability::IndexUnique,
            Capability::DefinedAttributes, Capability::Aggregations,
        ], function (Adapter&MockObject $adapter): void {
            $adapter->expects($this->once())
                ->method('find')
                ->with(
                    $this->anything(),
                    $this->callback(function (mixed $queries): bool {
                        if (! \is_array($queries)) {
                            return false;
                        }
                        foreach ($queries as $q) {
                            if ($q instanceof Query && $q->getMethod()->value === 'distinct') {
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
        });

        $db->skipValidation(fn () => $db->find('testCol', [Query::distinct()]));
    }

    public function testFindDistinctDoesNotAppendSequenceOrder(): void
    {
        $db = $this->buildDbWithCapabilities($this->createMock(Adapter::class), [
            Capability::IndexKey, Capability::IndexArray, Capability::IndexUnique,
            Capability::DefinedAttributes, Capability::Aggregations,
        ], function (Adapter&MockObject $adapter): void {
            $adapter->expects($this->once())
                ->method('find')
                ->with(
                    $this->anything(),
                    $this->anything(),
                    $this->anything(),
                    $this->anything(),
                    $this->callback(function (array $orderAttributes) {
                        return ! \in_array(Document::SEQUENCE, $orderAttributes, true);
                    }),
                    $this->anything(),
                    $this->anything(),
                    $this->anything(),
                    $this->anything()
                )
                ->willReturn([]);
        });

        $db->skipValidation(fn () => $db->find('testCol', [
            Query::distinct(),
        ]));
    }

    public function testFindSkipsAuthorizationWhenCollectionAllowsRead(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->adapter->method('find')->willReturnCallback(function () {
            $this->assertFalse($this->database->getAuthorization()->getStatus());

            return [];
        });

        $this->database->skipValidation(fn () => $this->database->find('testCol'));
    }

    public function testFindKeepsAuthorizationWithoutCollectionRead(): void
    {
        $this->setupCollectionLookup('testCol', permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);
        $this->adapter->method('find')->willReturnCallback(function () {
            $this->assertTrue($this->database->getAuthorization()->getStatus());

            return [];
        });

        $this->database->skipValidation(fn () => $this->database->find('testCol'));
    }

    public function testFindKeepsAuthorizationEnabledOnJoins(): void
    {
        $authOnFind = null;
        $db = null;
        $db = $this->buildDbWithCapabilities(self::createStub(Adapter::class), [
            Capability::IndexKey,
            Capability::IndexArray,
            Capability::IndexUnique,
            Capability::DefinedAttributes,
            Capability::Joins,
        ], function (Adapter&Stub $adapter) use (&$authOnFind, &$db): void {
            $adapter->method('find')->willReturnCallback(function () use (&$authOnFind, &$db) {
                $authOnFind = $db?->getAuthorization()->getStatus();

                return [];
            });
        }, extraCollections: ['other' => $this->collectionDoc('other')]);

        $db->skipValidation(fn () => $db->find('testCol', [
            Query::join('other', 'j0', [Query::on('fk', '$id')]),
        ]));

        $this->assertTrue($authOnFind);
    }

    public function testCountKeepsAuthorizationEnabledOnJoins(): void
    {
        $authOnCount = null;
        $captured = null;
        $db = null;
        $db = $this->buildDbWithCapabilities(self::createStub(Adapter::class), [
            Capability::IndexKey,
            Capability::IndexArray,
            Capability::IndexUnique,
            Capability::DefinedAttributes,
            Capability::Joins,
        ], function (Adapter&Stub $adapter) use (&$authOnCount, &$captured, &$db): void {
            $adapter->method('count')->willReturnCallback(function (Document $collection, array $queries) use (&$authOnCount, &$captured, &$db) {
                $authOnCount = $db?->getAuthorization()->getStatus();
                $captured = $queries;

                return 0;
            });
        }, extraCollections: ['other' => $this->collectionDoc('other')]);

        $db->skipValidation(fn () => $db->count('testCol', [
            Query::join('other', 'j0', [Query::on('fk', '$id')]),
            Query::equal('status', ['ok']),
        ]));

        $this->assertTrue($authOnCount);
        $this->assertIsArray($captured);
        $hasJoin = false;
        foreach ($captured as $query) {
            if ($query instanceof Query && $query->getMethod()->isJoin()) {
                $hasJoin = true;
                break;
            }
        }
        $this->assertTrue($hasJoin);
    }

    public function testSumKeepsAuthorizationEnabledOnJoins(): void
    {
        $authOnSum = null;
        $db = null;
        $db = $this->buildDbWithCapabilities(self::createStub(Adapter::class), [
            Capability::IndexKey,
            Capability::IndexArray,
            Capability::IndexUnique,
            Capability::DefinedAttributes,
            Capability::Joins,
        ], function (Adapter&Stub $adapter) use (&$authOnSum, &$db): void {
            $adapter->method('sum')->willReturnCallback(function () use (&$authOnSum, &$db) {
                $authOnSum = $db?->getAuthorization()->getStatus();

                return 0;
            });
        }, extraCollections: ['other' => $this->collectionDoc('other')]);

        $db->skipValidation(fn () => $db->sum('testCol', 'status', [
            Query::join('other', 'j0', [Query::on('fk', '$id')]),
        ]));

        $this->assertTrue($authOnSum);
    }

    public function testFindStampsJoinDocumentSecurityForPhysicalIds(): void
    {
        $captured = null;
        $join = $this->collectionDoc('jp_public', documentSecurity: false);
        $db = $this->buildDbWithCapabilities(
            self::createStub(Adapter::class),
            [
                Capability::IndexKey,
                Capability::IndexArray,
                Capability::IndexUnique,
                Capability::DefinedAttributes,
                Capability::Joins,
            ],
            function (Adapter&Stub $adapter) use (&$captured): void {
                $adapter->method('find')->willReturnCallback(function (Document $collection) use (&$captured) {
                    $captured = $collection->getAttribute('joinDocumentSecurity');

                    return [];
                });
            },
            extraCollections: [
                'database_1_collection_1' => $this->collectionDoc('database_1_collection_1', documentSecurity: false),
                'database_1_collection_2' => $join,
            ],
        );

        $db->skipValidation(fn () => $db->find('database_1_collection_1', [
            Query::leftJoin('database_1_collection_2', 'rev', [Query::on('$id', 'mainId')]),
        ]));

        $this->assertIsArray($captured);
        $this->assertSame(false, $captured['database_1_collection_2'] ?? true);
        $this->assertSame(false, $captured['jp_public'] ?? true);
    }

    public function testGetDocumentKeepsAuthorizationEnabledOnJoins(): void
    {
        $authOnGet = null;
        $db = null;
        $db = $this->buildDbWithCapabilities(
            self::createStub(Adapter::class),
            [
                Capability::IndexKey,
                Capability::IndexArray,
                Capability::IndexUnique,
                Capability::DefinedAttributes,
                Capability::Joins,
            ],
            extraCollections: ['other' => $this->collectionDoc('other')],
            getDocumentOverride: function (Document $col, string $docId) use (&$authOnGet, &$db): ?Document {
                if ($col->getId() === Database::METADATA) {
                    return null;
                }

                $authOnGet = $db?->getAuthorization()->getStatus();

                return new Document([
                    '$id' => $docId,
                    '$collection' => $col->getId(),
                ]);
            },
        );

        $db->skipValidation(fn () => $db->getDocument('testCol', 'doc1', [
            Query::join('other', 'j0', [Query::on('fk', '$id')]),
        ]));

        $this->assertTrue($authOnGet);
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
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $adapter->expects($this->once())
            ->method('count')
            ->willReturn(42);

        $result = $this->database->count('testCol');
        $this->assertSame(42, $result);
    }

    public function testSumDelegatesToAdapter(): void
    {
        $adapter = $this->mockAdapter();
        $attributes = [
            new Document(['$id' => 'amount', 'key' => 'amount', 'type' => 'double', 'size' => 0, 'required' => false, 'array' => false]),
        ];
        $this->setupCollectionLookup('testCol', $attributes);
        $adapter->expects($this->once())
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

    public function testAggregateReturnsTheAdapterRowsAsArrays(): void
    {
        $db = $this->buildDbWithCapabilities($this->createMock(Adapter::class), [
            Capability::IndexKey, Capability::IndexArray, Capability::IndexUnique,
            Capability::DefinedAttributes, Capability::Aggregations,
        ], function (Adapter&MockObject $adapter): void {
            $aggResult = new Document(['cnt' => 10]);
            $adapter->expects($this->once())
                ->method('find')
                ->willReturn([$aggResult]);
        });

        $results = $db->skipValidation(fn () => $db->aggregate('testCol', [Query::count('*', 'cnt')]));
        $this->assertSame([['cnt' => 10]], $results);
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
        $adapter = $this->mockAdapter();
        $this->setupCollectionLookup('testCol');
        $cursorDoc = new Document([
            '$id' => 'c1',
            '$collection' => 'testCol',
            '$sequence' => '100',
        ]);

        $adapter->expects($this->once())
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
        $adapter = $this->mockAdapter();
        $attributes = [
            new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 128, 'required' => false, 'array' => false]),
            new Document(['$id' => 'age', 'key' => 'age', 'type' => 'integer', 'size' => 0, 'required' => false, 'array' => false]),
        ];
        $this->setupCollectionLookup('testCol', $attributes);

        $adapter->expects($this->once())
            ->method('find')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (array $orderAttributes) {
                    return $orderAttributes[0] === 'name'
                        && $orderAttributes[1] === 'age'
                        && in_array('$sequence', $orderAttributes, true);
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

    public function testCountThrowsNotFoundExceptionForMissingCollection(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());
        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');
        $this->database->count('nonexistent');
    }

    public function testSumThrowsNotFoundExceptionForMissingCollection(): void
    {
        $this->adapter->method('getDocument')->willReturn(new Document());
        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');
        $this->database->sum('nonexistent', 'amount');
    }

    public function testSumValidatesQueries(): void
    {
        $this->setupCollectionLookup('testCol');
        $this->database->setValidation(true);

        $this->expectException(QueryException::class);
        $this->database->sum('testCol', 'amount', [Query::equal('unknown_field', ['val'])]);
    }

    /**
     * @template TAdapter of Adapter&Stub
     *
     * @param  TAdapter  $adapter
     * @param  list<Capability>  $capabilities
     * @param  (callable(TAdapter): void)|null  $adapterSetup
     * @param  array<string, Document>  $extraCollections
     * @param  (callable(Document, string): ?Document)|null  $getDocumentOverride
     */
    private function buildDbWithCapabilities(
        Adapter&Stub $adapter,
        array $capabilities,
        ?callable $adapterSetup = null,
        array $extraCollections = [],
        ?callable $getDocumentOverride = null,
    ): Database {
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
            documentSize: 0,
            minDateTime: new DateTime('0000-01-01'),
            maxDateTime: new DateTime('9999-12-31'),
            idType: ColumnType::String,
            keywords: [],
            internalIndexKeys: [],
        ));
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) use ($capabilities) {
            return in_array($cap, $capabilities);
        });

        $collectionMap = [
            'testCol' => new Document([
                '$id' => 'testCol',
                '$collection' => Database::METADATA,
                '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any())],
                'name' => 'testCol',
                'attributes' => [
                    new Document(['$id' => 'status', 'key' => 'status', 'type' => 'string', 'size' => 64, 'required' => false, 'array' => false]),
                ],
                'indexes' => [],
                'documentSecurity' => true,
            ]),
            ...$extraCollections,
        ];
        if ($adapterSetup !== null) {
            $adapterSetup($adapter);
        } else {
            $adapter->method('find')->willReturn([]);
        }

        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collectionMap, $getDocumentOverride) {
                if ($getDocumentOverride !== null) {
                    $override = $getDocumentOverride($col, $docId);
                    if ($override instanceof Document) {
                        return $override;
                    }
                }
                if ($col->getId() === Database::METADATA && isset($collectionMap[$docId])) {
                    return $collectionMap[$docId];
                }

                return new Document();
            }
        );

        $cache = new Cache(new None());
        $db = new Database($adapter, $cache);
        $db->getAuthorization()->addRole(Role::any()->toString());

        return $db;
    }

    public function testGetDocumentWithJoinOnUnsupportedAdapterThrows(): void
    {
        $this->setupCollectionLookup('testCol');

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Join queries are not supported by this adapter');
        $this->database->skipValidation(fn () => $this->database->getDocument('testCol', 'doc1', [
            Query::join('other', 'j0', [Query::on('fk', '$id')]),
        ]));
    }
}
