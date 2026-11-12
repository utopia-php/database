<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * count() and sum() over grouped, distinct and aggregate queries read the rows aggregate() and find() return, under
 * the same permission filters, whether the main collection grants the read or each document does.
 */
final class RowCountTest extends TestCase
{
    private const string PRODUCTS = 'products';

    private const string REVIEWS = 'reviews';

    private const string ADMIN = 'admin';

    private const string VIEWER = 'viewer';

    /**
     * Each product and review by its id: its attributes, and whether the viewer may read it.
     *
     * @var array<string, array<string, array{array<string, string|int>, bool}>>
     */
    private const array ROWS = [
        self::PRODUCTS => [
            'p1' => [['category' => 'electronics', 'price' => 1200], true],
            'p2' => [['category' => 'electronics', 'price' => 800], true],
            'p3' => [['category' => 'electronics', 'price' => 500], false],
            'p4' => [['category' => 'clothing', 'price' => 30], true],
            'p5' => [['category' => 'clothing', 'price' => 50], false],
            'p6' => [['category' => 'clothing', 'price' => 120], false],
            'p7' => [['category' => 'books', 'price' => 15], true],
            'p8' => [['category' => 'books', 'price' => 60], true],
            'p9' => [['category' => 'books', 'price' => 10], false],
            'p10' => [['category' => 'toys', 'price' => 25], false],
        ],
        self::REVIEWS => [
            'r1' => [['product' => 'p1', 'score' => 5], true],
            'r2' => [['product' => 'p1', 'score' => 4], true],
            'r3' => [['product' => 'p4', 'score' => 3], false],
            'r4' => [['product' => 'p7', 'score' => 5], true],
            'r5' => [['product' => 'p3', 'score' => 2], true],
            'r6' => [['product' => 'p10', 'score' => 1], true],
        ],
    ];

    /**
     * @return array<string, array{list<Query>, int, int}>
     */
    public static function shapes(): array
    {
        $join = Query::join(self::REVIEWS, 'r', [Query::on('$id', 'product')]);
        $leftJoin = Query::leftJoin(self::REVIEWS, 'r', [Query::on('$id', 'product')]);
        $fullOuterJoin = Query::fullOuterJoin(self::REVIEWS, 'r', [Query::on('$id', 'product')]);

        return [
            'groupBy' => [[Query::count('*', 'n'), Query::groupBy(['category'])], 4, 3],
            'groupBy with having' => [[Query::count('*', 'n'), Query::groupBy(['category']), Query::having([Query::greaterThan('n', 1)])], 3, 2],
            'distinct' => [[Query::distinct(), Query::select(['category'])], 4, 3],
            'plain aggregate' => [[Query::sum('price', 'total')], 1, 1],
            'left join with groupBy' => [[$leftJoin, Query::count('*', 'n'), Query::groupBy(['category'])], 4, 3],
            'join with groupBy' => [[$join, Query::count('*', 'n'), Query::groupBy(['category'])], 4, 2],
            'distinct over a join' => [[$join, Query::distinct(), Query::select(['category'])], 4, 2],
            'distinct joined attribute' => [[$join, Query::distinct(), Query::select(['r.score'])], 5, 2],
            'distinct over a join without a select' => [[$join, Query::distinct()], 6, 3],
            'emulated full outer join with groupBy' => [[$fullOuterJoin, Query::count('*', 'n'), Query::groupBy(['category'])], 4, 4],
            'emulated full outer join with distinct' => [[$fullOuterJoin, Query::distinct(), Query::select(['r.score'])], 6, 5],
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('shapes')]
    public function testCountIsTheNumberOfRowsTheReadReturns(array $queries, int $admin, int $viewer): void
    {
        $database = $this->database();

        foreach ([self::ADMIN => $admin, self::VIEWER => $viewer] as $role => $expected) {
            $this->actAs($database, $role);

            $this->assertSame($expected, $this->rowCount($database, $queries), $role);
            $this->assertSame($expected, $database->count(self::PRODUCTS, $queries), $role);
            $this->assertSame($expected, $database->count(self::PRODUCTS, [...$queries, Query::limit(1), Query::offset(1)]), $role.': the page is not counted');
            $this->assertSame(\min(2, $expected), $database->count(self::PRODUCTS, $queries, 2), $role.': capped at max');
        }
    }

    public function testDocumentCountsAreUnchanged(): void
    {
        $database = $this->database();
        $join = Query::join(self::REVIEWS, 'r', [Query::on('$id', 'product')]);

        foreach ([self::ADMIN => [10, 6, 6], self::VIEWER => [5, 3, 3]] as $role => [$all, $cheap, $joined]) {
            $this->actAs($database, $role);

            $this->assertSame($all, $database->count(self::PRODUCTS), $role);
            $this->assertSame($cheap, $database->count(self::PRODUCTS, [Query::lessThan('price', 100)]), $role);
            $this->assertSame($joined, $database->count(self::PRODUCTS, [$join]), $role);
        }
    }

    public function testSumAddsUpTheGroupedOrDistinctRows(): void
    {
        $database = $this->database();
        $join = Query::join(self::REVIEWS, 'r', [Query::on('$id', 'product')]);

        foreach ([self::ADMIN => [20, 15, 15], self::VIEWER => [17, 12, 9]] as $role => [$every, $distinct, $joined]) {
            $this->actAs($database, $role);

            $this->assertSame($every, $database->sum(self::REVIEWS, 'score'), $role);
            $this->assertSame($distinct, $database->sum(self::REVIEWS, 'score', [Query::count('*', 'n'), Query::groupBy(['score'])]), $role);
            $this->assertSame($distinct, $database->sum(self::REVIEWS, 'score', [Query::distinct(), Query::select(['score'])]), $role);
            $this->assertSame($joined, $database->sum(self::PRODUCTS, 'r.score', [$join, Query::distinct(), Query::select(['r.score'])]), $role);
        }
    }

    public function testSumOfAnAttributeTheGroupsDoNotHoldIsAnInvalidQuery(): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: sum() over grouped rows adds up one of their groupBy attributes, and score is not one of them');

        $database->sum(self::REVIEWS, 'score', [Query::count('*', 'n'), Query::groupBy(['product'])]);
    }

    public function testAnAdapterWithoutAggregatesRefusesToCountThemAsItRefusesToReadThem(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('rows')
            ->setNamespace('rows_'.\uniqid())
            ->setValidation(false);
        $database->create();
        $database->createCollection(Collection::create(
            id: self::PRODUCTS,
            attributes: [Attribute::string(key: 'category', size: 50, required: true)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createDocument(self::PRODUCTS, new Document(['category' => 'books']));
        $database->createDocument(self::PRODUCTS, new Document(['category' => 'books']));

        foreach ([
            'groupBy' => [Query::count('*', 'n'), Query::groupBy(['category'])],
            'distinct' => [Query::distinct(), Query::select(['category'])],
        ] as $label => $queries) {
            $error = null;
            try {
                $database->count(self::PRODUCTS, $queries);
            } catch (QueryException $caught) {
                $error = $caught;
            }

            $this->assertNotNull($error, $label.' was counted as documents');
        }
    }

    /**
     * @param  list<Query>  $queries
     */
    private function rowCount(Database $database, array $queries): int
    {
        $aggregates = \array_filter($queries, static fn (Query $query): bool => $query->getMethod()->isAggregate() || $query->getMethod() === Method::GroupBy);

        return \count($aggregates === []
            ? $database->find(self::PRODUCTS, [...$queries, Query::limit(100)])
            : $database->aggregate(self::PRODUCTS, [...$queries, Query::limit(100)]));
    }

    private function actAs(Database $database, string $role): void
    {
        $database->getAuthorization()->cleanRoles();
        $database->getAuthorization()->addRole(Role::user($role)->toString());
    }

    private function database(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('rows')
            ->setNamespace('rows_'.\uniqid());
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::user(self::ADMIN))];
        $database->createCollection(Collection::create(id: self::PRODUCTS, attributes: [
            Attribute::string(key: 'category', size: 50, required: true),
            Attribute::integer(key: 'price', required: true),
        ], permissions: $permissions, documentSecurity: true));
        $database->createCollection(Collection::create(id: self::REVIEWS, attributes: [
            Attribute::string(key: 'product', size: 36, required: true),
            Attribute::integer(key: 'score', required: true),
        ], permissions: $permissions, documentSecurity: true));

        foreach (self::ROWS as $collection => $documents) {
            foreach ($documents as $id => [$attributes, $visible]) {
                $database->createDocument($collection, new Document([
                    Document::ID => $id,
                    ...$attributes,
                    Document::PERMISSIONS => $visible ? [Permission::read(Role::user(self::VIEWER))] : [],
                ]));
            }
        }

        return $database;
    }
}
