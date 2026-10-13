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
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * A one-to-many relationship attribute, and the child side of a two-way many-to-one, holds no column: the related
 * collection stores this document's ID. A join comparing such an attribute with the related collection's `$id`
 * compares this document's `$id` with the column that stores it, keeping the join's operator and alias. Every other
 * relationship side, and a join of another collection, is checked as before.
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

        foreach ([
            'o1' => [10, 'paid', 'r1'],
            'o2' => [20, 'open', 'r1'],
            'o3' => [30, 'paid', 'r1'],
            'o4' => [40, 'paid', 'r2'],
        ] as $id => [$amount, $status, $region]) {
            $this->createDocument('orders', $id, ['amount' => $amount, 'status' => $status, 'region' => $region]);
        }

        $this->createDocument('customers', 'c1', ['name' => 'Ada', 'region' => 'r1', 'orders' => ['o1', 'o2']]);
        $this->createDocument('customers', 'c2', ['name' => 'Bob', 'region' => 'r2', 'orders' => ['o3', 'o4']]);
        $this->createDocument('customers', 'c3', ['name' => 'Cy', 'region' => 'r1']);
    }

    public function testLeftJoinOnAOneToManyRelationshipPairsEachCustomerWithItsOrders(): void
    {
        $this->assertSame(
            ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4', 'c3/-'],
            $this->pairs('customers', [Query::leftJoin('orders', 'ord', [Query::on('orders', '$id')])], 'ord'),
        );
    }

    public function testJoinOnAOneToManyRelationshipMatchesTheMainIdAgainstTheRelatedKey(): void
    {
        $this->assertSame(
            ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'],
            $this->pairs('customers', [Query::join('orders', 'ord', [Query::on('orders', '$id')])], 'ord'),
            'a one-to-many relationship is stored on the related side, in its two-way key',
        );
    }

    public function testJoinNamingTheRelatedIdUnderItsAliasIsRewritten(): void
    {
        $this->assertSame(
            ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'],
            $this->pairs('customers', [Query::join('orders', 'ord', [Query::on('orders', 'ord.$id')])], 'ord'),
        );
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

    public function testOnlyTheRelationshipConditionOfTheOnListIsRewritten(): void
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

    public function testRelationshipDeclaredInTheCollectionDefinitionIsRewritten(): void
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

    public function testJoinOnAManyToOneRelationshipKeepsItsColumns(): void
    {
        $this->assertSame(
            ['c1/r1', 'c2/r2', 'c3/r1'],
            $this->pairs('customers', [Query::join('regions', 'reg', [Query::on('region', '$id')])], 'reg'),
            'the parent side of a many-to-one holds the related ID in its own column',
        );
    }

    public function testJoinOnTheChildSideOfATwoWayManyToOneMatchesTheMainIdAgainstTheParentKey(): void
    {
        $this->assertSame(
            ['r1/c1', 'r1/c3', 'r2/c2'],
            $this->pairs('regions', [Query::leftJoin('customers', 'resident', [Query::on('customers', '$id')])], 'resident'),
        );
    }

    public function testJoinOnTheChildSideOfAOneWayManyToOneIsRefused(): void
    {
        $this->createCollection('warehouses', [Attribute::string(key: 'name', size: 16)]);
        $this->database->createRelationship('orders', Relationship::manyToOne(relatedCollection: 'warehouses', key: 'warehouse', twoWayKey: 'stock'));

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage(self::VIRTUAL.'stock');

        $this->database->find('warehouses', [Query::join('orders', 'ord', [Query::on('stock', '$id')])]);
    }

    public function testJoinOfAnotherCollectionThanTheRelatedOneIsRefused(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage(self::VIRTUAL.'orders');

        $this->database->find('customers', [Query::join('regions', 'reg', [Query::on('orders', '$id')])]);
    }

    public function testRelationshipComparedWithAnotherColumnThanTheRelatedIdIsRefused(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage(self::VIRTUAL.'orders');

        $this->database->find('customers', [Query::join('orders', 'ord', [Query::on('orders', 'status')])]);
    }

    public function testJoinOnAManyToManyRelationshipIsRefused(): void
    {
        $this->createCollection('tags', [Attribute::string(key: 'name', size: 16)]);
        $this->database->createRelationship('customers', Relationship::manyToMany(relatedCollection: 'tags', key: 'tags', twoWay: true, twoWayKey: 'customers'));

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage(self::VIRTUAL.'tags');

        $this->database->find('customers', [Query::join('tags', 'tag', [Query::on('tags', '$id')])]);
    }

    public function testJoinOnTheChildSideOfAOneWayOneToOneIsRefused(): void
    {
        $this->createCollection('profiles', [Attribute::string(key: 'name', size: 16)]);
        $this->database->createRelationship('customers', Relationship::oneToOne(relatedCollection: 'profiles', key: 'profile', twoWayKey: 'owner'));

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage(self::VIRTUAL.'owner');

        $this->database->find('profiles', [Query::join('customers', 'customer', [Query::on('owner', '$id')])]);
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
    public function testEveryReadOfARelationshipJoinMatchesTheRelatedKey(Closure $read, mixed $expected): void
    {
        $this->assertSame($expected, $read($this->database));
    }

    public function testRelationshipJoinIsRewrittenWithoutValidation(): void
    {
        $pairs = $this->database->skipValidation(fn (): array => $this->pairs('customers', [Query::join('orders', 'ord', [Query::on('orders', '$id')])], 'ord'));

        $this->assertSame(['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'], $pairs);
    }

    public function testCallersJoinIsLeftAsGivenForTheNextRead(): void
    {
        $condition = Query::on('orders', '$id');
        $join = Query::join('orders', 'ord', [$condition]);

        $this->assertSame(4, $this->database->count('customers', [$join]));
        $this->assertSame(['orders', '=', '$id'], $condition->getValues());
        $this->assertSame($condition, $join->getJoinOnQueries()[0]);
        $this->assertSame(['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'], $this->pairs('customers', [$join], 'ord'));
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
            static function (Document $row) use ($alias): string {
                $joined = $row->getAttribute($alias.'.$id');

                return $row->getId().'/'.(\is_string($joined) ? $joined : '-');
            },
            $this->database->find($collection, [...$queries, Query::limit(100)]),
        );
        \sort($pairs);

        return $pairs;
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
