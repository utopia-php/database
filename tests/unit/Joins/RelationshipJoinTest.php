<?php

namespace Tests\Unit\Joins;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;

/**
 * The parent side of a one-to-many relationship holds no column: each child stores the parent's ID under the
 * relationship's two-way key. A join condition whose left column is such an attribute of the main collection compares
 * the main `$id` with the joined two-way key instead, keeping the condition's operator and the join's alias. Every
 * other relationship attribute is joined as it was in 8.0.3.
 */
final class RelationshipJoinTest extends TestCase
{
    private const string VIRTUAL = 'Cannot join on virtual relationship attribute: ';

    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase('relationship_join')
            ->setNamespace('relationship_join_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Relationships());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->createCollection('customers', [Attribute::string(key: 'name', size: 16)]);
        $this->createCollection('orders', [
            Attribute::integer(key: 'amount'),
            Attribute::string(key: 'status', size: 16),
            Attribute::string(key: 'region', size: 16),
        ]);
        $this->createCollection('regions', [Attribute::string(key: 'name', size: 16)]);

        $this->database->createRelationship('customers', Relationship::oneToMany(relatedCollection: 'orders', key: 'orders', twoWayKey: 'customer'));
        $this->database->createRelationship('customers', Relationship::manyToOne(relatedCollection: 'regions', key: 'region', twoWay: true, twoWayKey: 'customers'));

        $this->createDocument('regions', 'r1', ['name' => 'north']);
        $this->createDocument('regions', 'r2', ['name' => 'south']);
        $this->createDocument('customers', 'c1', ['name' => 'Ada', 'region' => 'r1']);
        $this->createDocument('customers', 'c2', ['name' => 'Bob', 'region' => 'r2']);
        $this->createDocument('customers', 'c3', ['name' => 'Cy', 'region' => 'r1']);

        foreach ([
            'o1' => [10, 'paid', 'r1', 'c1'],
            'o2' => [20, 'open', 'r1', 'c1'],
            'o3' => [30, 'paid', 'r1', 'c2'],
            'o4' => [40, 'paid', 'r2', 'c2'],
        ] as $id => [$amount, $status, $region, $customer]) {
            $this->createDocument('orders', $id, ['amount' => $amount, 'status' => $status, 'region' => $region, 'customer' => $customer]);
        }
    }

    public function testLeftJoinOnAOneToManyRelationshipPairsEachParentWithItsChildren(): void
    {
        $this->assertSame(
            ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4', 'c3/-'],
            $this->pairs('customers', [Query::leftJoin('orders', 'ord', [Query::on('orders', '$id')])], 'ord'),
        );
    }

    public function testJoinOnAOneToManyRelationshipMatchesTheParentIdAgainstTheTwoWayKey(): void
    {
        $this->assertSame(
            ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'],
            $this->pairs('customers', [Query::join('orders', 'ord', [Query::on('orders', '$id')])], 'ord'),
        );
    }

    public function testRightColumnOfTheConditionBecomesTheTwoWayKey(): void
    {
        $expected = ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'];

        $this->assertSame($expected, $this->pairs('customers', [Query::join('orders', 'ord', [Query::on('orders', 'ord.$id')])], 'ord'));
        $this->assertSame($expected, $this->pairs('customers', [Query::join('orders', 'ord', [Query::on('orders', 'status')])], 'ord'), 'the right column is replaced, as appwrite replaced it');
    }

    public function testRewrittenJoinKeepsItsAlias(): void
    {
        $rows = $this->database->find('customers', [
            Query::join('orders', 'purchase', [Query::on('orders', '$id')]),
            Query::select(['name', 'purchase.amount']),
            Query::orderAsc('purchase.amount'),
        ]);

        $this->assertSame([10, 20, 30, 40], \array_map(static fn (Document $row): mixed => $row->getAttribute('purchase.amount'), $rows));
        $this->assertSame(['Ada', 'Ada', 'Bob', 'Bob'], \array_map(static fn (Document $row): mixed => $row->getAttribute('name'), $rows));
    }

    public function testOnlyTheOneToManyConditionOfTheOnListIsRewritten(): void
    {
        $this->assertSame(
            ['c1/o1', 'c2/o4'],
            $this->pairs('customers', [Query::join('orders', 'ord', [
                Query::on('orders', '$id'),
                Query::on('region', 'region'),
                Query::equal('ord.status', ['paid']),
            ])], 'ord'),
            'the many-to-one column and the ON filter keep comparing what they named',
        );
    }

    public function testOperatorOfTheRewrittenConditionIsKept(): void
    {
        $this->assertSame(
            ['c1/o3', 'c1/o4', 'c2/o1', 'c2/o2', 'c3/o1', 'c3/o2', 'c3/o3', 'c3/o4'],
            $this->pairs('customers', [Query::join('orders', 'ord', [Query::on('orders', '$id', '!=')])], 'ord'),
        );
    }

    public function testConditionOnAnEarlierJoinsColumnIsNotTheMainRelationship(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage(self::VIRTUAL.'peer.orders');

        $this->database->find('customers', [
            Query::join('customers', 'peer', [Query::on('$id', '$id')]),
            Query::join('orders', 'sub', [Query::on('peer.orders', '$id')]),
        ]);
    }

    public function testOneToManyWithoutATwoWayKeyIsNotRewritten(): void
    {
        $this->createCollection('ledgers', [Attribute::relationship('orders', Relationship::oneToMany(relatedCollection: 'orders'), RelationshipSide::Parent)]);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage(self::VIRTUAL.'orders');

        $this->database->find('ledgers', [Query::join('orders', 'ord', [Query::on('orders', '$id')])]);
    }

    public function testOneToManyDeclaredInTheCollectionDefinitionIsRewritten(): void
    {
        $this->createCollection('areas', [
            Attribute::string(key: 'name', size: 16),
            Attribute::relationship('orders', Relationship::oneToMany(relatedCollection: 'orders', twoWayKey: 'region'), RelationshipSide::Parent),
        ]);
        $this->createDocument('areas', 'r1', ['name' => 'north']);
        $this->createDocument('areas', 'r2', ['name' => 'south']);

        $this->assertSame(
            ['r1/o1', 'r1/o2', 'r1/o3', 'r2/o4'],
            $this->pairs('areas', [Query::join('orders', 'ord', [Query::on('orders', '$id')])], 'ord'),
        );
    }

    public function testJoinOnTheChildSideOfAOneToManyKeepsItsColumn(): void
    {
        $this->assertSame(
            ['o1/c1', 'o2/c1', 'o3/c2', 'o4/c2'],
            $this->pairs('orders', [Query::join('customers', 'buyer', [Query::on('customer', '$id')])], 'buyer'),
            'the child side of a one-to-many holds the parent ID in its own column',
        );
    }

    public function testJoinOnTheParentSideOfAManyToOneKeepsItsColumn(): void
    {
        $this->assertSame(
            ['c1/r1', 'c2/r2', 'c3/r1'],
            $this->pairs('customers', [Query::join('regions', 'area', [Query::on('region', '$id')])], 'area'),
        );
    }

    /**
     * @return iterable<string, array{Closure(Database): mixed, string}>
     */
    public static function refusedRelationshipJoins(): iterable
    {
        yield 'the child side of a two-way many-to-one' => [
            static fn (Database $database): mixed => $database->find('regions', [Query::join('customers', 'resident', [Query::on('customers', '$id')])]),
            'customers',
        ];
        yield 'the parent side of a many-to-many' => [
            static function (Database $database): mixed {
                $database->createCollection(Collection::create(id: 'tags', permissions: [Permission::read(Role::any())]));
                $database->createRelationship('customers', Relationship::manyToMany(relatedCollection: 'tags', key: 'tags', twoWay: true, twoWayKey: 'customers'));

                return $database->find('customers', [Query::join('tags', 'tag', [Query::on('tags', '$id')])]);
            },
            'tags',
        ];
        yield 'the child side of a one-way one-to-one' => [
            static function (Database $database): mixed {
                $database->createCollection(Collection::create(id: 'profiles', permissions: [Permission::read(Role::any())]));
                $database->createRelationship('customers', Relationship::oneToOne(relatedCollection: 'profiles', key: 'profile', twoWayKey: 'owner'));

                return $database->find('profiles', [Query::join('customers', 'customer', [Query::on('owner', '$id')])]);
            },
            'owner',
        ];
    }

    /**
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('refusedRelationshipJoins')]
    public function testJoinOnAnotherRelationshipWithoutAColumnIsStillRefused(Closure $read, string $attribute): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage(self::VIRTUAL.$attribute);

        $read($this->database);
    }

    /**
     * @return iterable<string, array{Closure(Database): mixed, mixed}>
     */
    public static function reads(): iterable
    {
        $join = Query::join('orders', 'ord', [Query::on('orders', '$id')]);

        yield 'count' => [static fn (Database $database): int => $database->count('customers', [$join]), 4];
        yield 'sum' => [static fn (Database $database): int => (int) $database->sum('customers', 'ord.amount', [$join]), 100];
        yield 'aggregate' => [
            static fn (Database $database): array => \array_map(
                static fn (array $row): array => [$row['name'], self::integer($row['total'])],
                $database->aggregate('customers', [$join, Query::sum('ord.amount', 'total'), Query::groupBy(['name']), Query::orderAsc('name')]),
            ),
            [['Ada', 30], ['Bob', 70]],
        ];
        yield 'ungrouped aggregate' => [
            static fn (Database $database): array => \array_map(
                static fn (array $row): ?int => self::integer($row['rows']),
                $database->aggregate('customers', [$join, Query::count('*', 'rows')]),
            ),
            [4],
        ];
        yield 'getDocument' => [
            static fn (Database $database): mixed => $database->getDocument('customers', 'c1', [
                Query::join('orders', 'ord', [Query::on('orders', '$id'), Query::equal('ord.status', ['open'])]),
                Query::select(['name', 'ord.amount']),
            ])->getAttribute('ord.amount'),
            20,
        ];
    }

    /**
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('reads')]
    public function testEveryReadOfAOneToManyJoinMatchesTheTwoWayKey(Closure $read, mixed $expected): void
    {
        $this->assertSame($expected, $read($this->database));
    }

    public function testOneToManyJoinIsRewrittenWithoutValidation(): void
    {
        $pairs = $this->database->skipValidation(fn (): array => $this->pairs('customers', [Query::join('orders', 'ord', [Query::on('orders', '$id')])], 'ord'));

        $this->assertSame(['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'], $pairs);
    }

    public function testCallersJoinIsLeftAsGiven(): void
    {
        $condition = Query::on('orders', '$id');
        $join = Query::join('orders', 'ord', [$condition]);

        $this->assertSame(4, $this->database->count('customers', [$join]));
        $this->assertSame(['orders', '=', '$id'], $condition->getValues());
        $this->assertSame($condition, $join->getJoinOnQueries()[0]);
        $this->assertSame(['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'], $this->pairs('customers', [$join], 'ord'));
    }

    public function testJoinedRowsAreTheOnesTheCallerCanRead(): void
    {
        $authorization = $this->database->getAuthorization();
        $this->database->updateCollection('orders', new CollectionUpdate(permissions: [Permission::create(Role::any())], documentSecurity: true));
        $authorization->skip(fn (): Document => $this->database->updateDocument('orders', 'o2', new Document(['$permissions' => [Permission::read(Role::user('bob'))]])));
        $join = Query::leftJoin('orders', 'ord', [Query::on('orders', '$id')]);

        $this->assertSame(['c1/o1', 'c2/o3', 'c2/o4', 'c3/-'], $this->pairs('customers', [$join], 'ord'));

        $authorization->addRole(Role::user('bob')->toString());
        $this->assertSame(['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4', 'c3/-'], $this->pairs('customers', [$join], 'ord'));
    }

    public function testCursorPagingOverAOneToManyJoinReturnsEveryJoinedRowOnce(): void
    {
        $queries = [Query::leftJoin('orders', 'ord', [Query::on('orders', '$id')]), Query::orderAsc('ord.amount')];
        $all = \array_values($this->database->find('customers', [...$queries, Query::limit(100)]));
        $keys = \array_map(static fn (Document $row): string => self::pair($row, 'ord'), $all);
        $this->assertSame(['c3/-', 'c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'], $keys);

        foreach ($all as $index => $row) {
            $this->assertSame(\array_slice($keys, $index + 1), $this->keys([...$queries, Query::cursorAfter($row)]), "after {$keys[$index]}");
            $this->assertSame(\array_slice($keys, 0, $index), $this->keys([...$queries, Query::cursorBefore($row)]), "before {$keys[$index]}");
        }

        $forward = [];
        $cursor = null;
        for ($page = 0; $page <= \count($all); $page++) {
            $rows = $this->database->find('customers', [...$queries, Query::limit(2), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
            \array_push($forward, ...\array_map(static fn (Document $row): string => self::pair($row, 'ord'), $rows));
            if (\count($rows) < 2) {
                break;
            }
            $cursor = $rows[1];
        }
        $this->assertSame($keys, $forward, 'paging forward in pages of two');
    }

    public function testCursorResolvedFromAnIdPagesAOneToManyJoin(): void
    {
        $queries = [Query::join('orders', 'ord', [Query::on('orders', '$id')]), Query::orderAsc('ord.amount')];

        $this->assertSame(20, $this->database->resolveCursor('customers', 'c1', CursorDirection::After, $queries)->getAttribute('ord.amount'));
        $this->assertSame(30, $this->database->resolveCursor('customers', 'c2', CursorDirection::Before, $queries)->getAttribute('ord.amount'));

        $forward = [];
        $cursor = null;
        for ($page = 0; $page <= 4; $page++) {
            $rows = $this->database->find('customers', [
                ...$queries,
                Query::limit(2),
                ...($cursor === null ? [] : [Query::cursorAfter($this->database->resolveCursor('customers', $cursor, CursorDirection::After, $queries))]),
            ]);
            if ($rows === []) {
                break;
            }
            \array_push($forward, ...\array_map(static fn (Document $row): string => self::pair($row, 'ord'), $rows));
            $cursor = $rows[\count($rows) - 1]->getId();
        }
        $this->assertSame(['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'], $forward, 'paging by the ID of each page\'s last row returns every row once');

        $before = $this->database->find('customers', [...$queries, Query::cursorBefore($this->database->resolveCursor('customers', 'c2', CursorDirection::Before, $queries))]);
        $this->assertSame(['c1/o1', 'c1/o2'], \array_map(static fn (Document $row): string => self::pair($row, 'ord'), $before));
    }

    public function testPagingAOneToManyJoinItCannotPageFailsBeforeYieldingARow(): void
    {
        $rows = $this->database->cursor('customers', [
            Query::join('orders', 'purchase', [Query::on('orders', '$id')]),
            Query::select(['name', 'purchase.amount']),
            Query::orderAsc('purchase.amount'),
        ], 2);
        /** @var int $yielded */
        $yielded = 0;

        try {
            foreach ($rows as $row) {
                $yielded++;
            }
            $this->fail('A one-to-many join pairs a customer with several orders, so its next page orders by the joined $id the rows lack');
        } catch (OrderException $exception) {
            $this->assertSame('purchase.$id', $exception->getAttribute());
        }

        $this->assertSame(0, $yielded, 'The read must be refused before the caller acts on any of its rows');
    }

    public function testPagingAOneToManyJoinPagesEveryJoinedRowOnce(): void
    {
        $rows = $this->database->cursor('customers', [
            Query::join('orders', 'purchase', [Query::on('orders', '$id')]),
            Query::select(['name', 'purchase.$id', 'purchase.amount']),
            Query::orderAsc('purchase.amount'),
        ], 2);

        $this->assertSame(
            ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'],
            \array_map(static fn (Document $row): string => self::pair($row, 'purchase'), \iterator_to_array($rows, false)),
        );
    }

    /**
     * Each row as `<main $id>/<joined $id>`, sorted, with `-` for a row the outer join did not match.
     *
     * @param  list<Query>  $queries
     * @return list<string>
     */
    private function pairs(string $collection, array $queries, string $alias): array
    {
        $pairs = \array_map(
            static fn (Document $row): string => self::pair($row, $alias),
            $this->database->find($collection, [...$queries, Query::limit(100)]),
        );
        \sort($pairs);

        return $pairs;
    }

    /**
     * @param  list<Query>  $queries
     * @return list<string>
     */
    private function keys(array $queries): array
    {
        return \array_values(\array_map(
            static fn (Document $row): string => self::pair($row, 'ord'),
            $this->database->find('customers', [...$queries, Query::limit(100)]),
        ));
    }

    private static function pair(Document $row, string $alias): string
    {
        $joined = $row->getAttribute($alias.'.$id');

        return $row->getId().'/'.(\is_string($joined) ? $joined : '-');
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private function createCollection(string $id, array $attributes): void
    {
        $this->database->createCollection(Collection::create(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createDocument(string $collection, string $id, array $attributes): void
    {
        $this->database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            ...$attributes,
        ]));
    }

    private static function integer(mixed $value): ?int
    {
        return \is_numeric($value) ? (int) $value : null;
    }
}
