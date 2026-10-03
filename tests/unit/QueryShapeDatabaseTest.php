<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Throwable;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class QueryShapeDatabaseTest extends TestCase
{
    private const string COLLECTION = 'orders';

    private function database(bool $nativeFullOuterJoin = false): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $database = new Database($nativeFullOuterJoin ? new NativeFullOuterJoinSQLite($pdo) : new SQLite($pdo), new Cache(new NoCache()));
        $database
            ->setDatabase('query_shape')
            ->setNamespace('query_shape_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'amount', required: true),
                Attribute::double(key: 'rating', default: 0.0),
                Attribute::string(key: 'status', size: 20, required: true),
                Attribute::string(key: 'body', size: 200, default: ''),
                Attribute::boolean(key: 'paid', default: false),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        foreach ([[5, 'paid'], [7, 'paid'], [3, 'open']] as [$amount, $status]) {
            $database->createDocument(self::COLLECTION, new Document([
                'amount' => $amount,
                'rating' => $amount / 2,
                'status' => $status,
                'body' => 'order of '.$amount,
                'paid' => $status === 'paid',
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        return $database;
    }

    /**
     * @return list<Query>
     */
    private static function crossJoins(int $count): array
    {
        return \array_map(fn (int $index): Query => Query::crossJoin(self::COLLECTION, 'joined'.$index), \range(1, $count));
    }

    /**
     * @return array<string, array{0: list<Query>, 1: string}>
     */
    public static function rejectedShapeProvider(): array
    {
        return [
            'more joins than allowed' => [
                self::crossJoins(9),
                'Too many joins: at most 8 are allowed',
            ],
            'having an attribute outside the schema' => [
                [Query::sum('amount', 'total'), Query::groupBy(['status']), Query::having([Query::equal('no_such_attribute', ['x'])])],
                'Invalid query: Having can only compare an aggregate alias or a groupBy attribute: no_such_attribute',
            ],
            'having an attribute that is not grouped' => [
                [Query::sum('amount', 'total'), Query::groupBy(['status']), Query::having([Query::equal('body', ['x'])])],
                'Invalid query: Having can only compare an aggregate alias or a groupBy attribute: body',
            ],
            'having a search without a fulltext index' => [
                [Query::count('*', 'rows'), Query::groupBy(['body']), Query::having([Query::search('body', 'order')])],
                'Searching by attribute "body" requires a fulltext index.',
            ],
            'having more values than allowed' => [
                [Query::count('*', 'rows'), Query::groupBy(['status']), Query::having([Query::equal('status', \array_map(fn (int $index): string => 'status'.$index, \range(1, 5001)))])],
                'Invalid query: Query on attribute has greater than 5000 values: status',
            ],
            'having a value of the wrong type' => [
                [Query::count('*', 'rows'), Query::groupBy(['paid']), Query::having([Query::greaterThan('paid', 'yes')])],
                'Invalid query: Query value is invalid for attribute "paid"',
            ],
            'having a numeric alias compared with text' => [
                [Query::sum('amount', 'total'), Query::groupBy(['status']), Query::having([Query::greaterThan('total', 'abc')])],
                'Invalid query: Query value is invalid for aggregate alias "total"',
            ],
            'having an alias inside a logical group' => [
                [Query::sum('amount', 'total'), Query::groupBy(['status']), Query::having([Query::or([Query::greaterThan('total', 10), Query::lessThan('total', 1)])])],
                'Invalid query: Aggregate alias "total" can only be compared at the top level of having',
            ],
            'sum of a string attribute' => [
                [Query::sum('status', 'total')],
                'Invalid query: Aggregate sum requires a numeric attribute that is not an array: status',
            ],
            'standard deviation of a string attribute' => [
                [Query::stddev('status', 'spread')],
                'Invalid query: Aggregate stddev requires a numeric attribute that is not an array: status',
            ],
            'average of a boolean attribute' => [
                [Query::avg('paid', 'mean')],
                'Invalid query: Aggregate avg requires a numeric attribute that is not an array: paid',
            ],
            'bitwise and of a double attribute' => [
                [Query::bitAnd('rating', 'bits')],
                'Invalid query: Aggregate bitAnd requires an integer attribute that is not an array: rating',
            ],
            'minimum of every row' => [
                [Query::min('*', 'least')],
                'Invalid query: Only count can aggregate "*"',
            ],
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('rejectedShapeProvider')]
    public function testFindRejectsTheShapeWithAQueryException(array $queries, string $message): void
    {
        $error = $this->capture(fn () => $this->database()->find(self::COLLECTION, $queries));

        $this->assertInstanceOf(QueryException::class, $error, $error === null ? 'find() accepted the query shape' : $error::class.': '.$error->getMessage());
        $this->assertSame($message, $error->getMessage());
    }

    private function capture(callable $call): ?Throwable
    {
        try {
            $call();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }

    public function testCountAndSumRejectTooManyJoins(): void
    {
        $database = $this->database();

        $this->assertSame(3, $database->count(self::COLLECTION, []));

        foreach ([
            'count' => fn () => $database->count(self::COLLECTION, self::crossJoins(9)),
            'sum' => fn () => $database->sum(self::COLLECTION, 'amount', self::crossJoins(9)),
        ] as $method => $call) {
            $error = $this->capture($call);

            $this->assertInstanceOf(QueryException::class, $error, $error === null ? $method.'() accepted nine joins' : $error::class.': '.$error->getMessage());
            $this->assertSame('Too many joins: at most 8 are allowed', $error->getMessage());
        }
    }

    public function testEightJoinsAreStillAllowed(): void
    {
        $database = $this->database();

        $rows = $database->find(self::COLLECTION, [
            Query::equal('status', ['open']),
            ...self::crossJoins(8),
            Query::limit(1),
        ]);

        $this->assertCount(1, $rows);
    }

    public function testEmptySetAggregatesFollowTheContract(): void
    {
        $database = $this->database();

        $queries = [
            Query::equal('status', ['nonexistent']),
            Query::count('*', 'rows'),
            Query::countDistinct('status', 'statuses'),
            Query::sum('amount', 'total'),
            Query::avg('amount', 'mean'),
            Query::min('amount', 'least'),
            Query::max('amount', 'most'),
        ];

        $results = $database->find(self::COLLECTION, $queries);

        $this->assertCount(1, $results);
        $this->assertSame(0, $results[0]->getAttribute('rows'));
        $this->assertSame(0, $results[0]->getAttribute('statuses'));
        foreach (['total', 'mean', 'least', 'most'] as $alias) {
            $this->assertTrue($results[0]->offsetExists($alias), $alias.' must be present');
            $this->assertNull($results[0]->getAttribute($alias), $alias.' over no rows must be null');
        }
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function fullOuterJoinModes(): iterable
    {
        yield 'emulated full outer join' => [false];
        yield 'native full outer join' => [true];
    }

    /**
     * The contract holds over a full outer join too, whose aggregation runs once over both halves of
     * the emulation on engines without one.
     */
    #[DataProvider('fullOuterJoinModes')]
    public function testEmptySetAggregatesOverAFullOuterJoinFollowTheContract(bool $nativeFullOuterJoin): void
    {
        $database = $this->database($nativeFullOuterJoin);
        $database->createCollection(new Collection(
            id: 'refunds',
            attributes: [
                Attribute::integer(key: 'amount', required: true),
                Attribute::string(key: 'status', size: 20, required: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createDocument('refunds', new Document([
            'amount' => 2,
            'status' => 'refunded',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $noRows = [
            Query::fullOuterJoin('refunds', 'status', 'status', '=', 'refund'),
            Query::equal('status', ['nonexistent']),
        ];

        $results = $database->find(self::COLLECTION, [
            ...$noRows,
            Query::count('*', 'rows'),
            Query::countDistinct('refund.status', 'statuses'),
            Query::sum('refund.amount', 'total'),
            Query::avg('refund.amount', 'mean'),
            Query::min('amount', 'least'),
            Query::max('refund.amount', 'most'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(0, $results[0]->getAttribute('rows'));
        $this->assertSame(0, $results[0]->getAttribute('statuses'));
        foreach (['total', 'mean', 'least', 'most'] as $alias) {
            $this->assertTrue($results[0]->offsetExists($alias), $alias.' must be present');
            $this->assertNull($results[0]->getAttribute($alias), $alias.' over no rows must be null');
        }
        $this->assertSame(0, $database->count(self::COLLECTION, $noRows));
        $this->assertSame(0, $database->sum(self::COLLECTION, 'amount', $noRows), 'Database::sum() still answers 0 over no rows');
    }
}
