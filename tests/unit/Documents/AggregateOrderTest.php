<?php

namespace Tests\Unit\Documents;

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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Query\Order;
use Utopia\Query\Schema\ColumnType;

/**
 * An aggregation query returns one row per group, so an order next to an aggregate or a groupBy
 * can name only an aggregate alias or an attribute the query groups by: no engine is asked to
 * order groups by a column the groups do not determine.
 */
final class AggregateOrderTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = $this->database(new SQLite(new PDO('sqlite::memory:')));
    }

    /**
     * @return iterable<string, array{list<Query>, string}>
     */
    public static function ungroupedOrders(): iterable
    {
        $note = Query::join('notes', '$id', 'customerId', '=', 'note');

        yield 'an attribute next to a count' => [[Query::count('*', 'rows'), Query::orderAsc('balance')], 'balance'];
        yield 'an attribute next to a sum' => [[Query::sum('balance', 'total'), Query::orderDesc('name')], 'name'];
        yield 'an internal attribute next to a count' => [[Query::count('*', 'rows'), Query::orderAsc('$createdAt')], '$createdAt'];
        yield 'an attribute outside the groups' => [[Query::count('*', 'rows'), Query::groupBy(['status']), Query::orderAsc('balance')], 'balance'];
        yield 'an attribute next to a groupBy without an aggregate' => [[Query::groupBy(['status']), Query::orderAsc('name')], 'name'];
        yield 'an attribute next to a distinct count' => [[Query::distinct(), Query::count('*', 'rows'), Query::orderAsc('name')], 'name'];
        yield 'a joined attribute next to a count' => [[$note, Query::count('*', 'rows'), Query::orderAsc('note.score')], 'note.score'];
        yield 'a joined attribute when the main one is grouped' => [[$note, Query::count('*', 'rows'), Query::groupBy(['name']), Query::orderAsc('note.name')], 'note.name'];
        yield 'a main attribute when the joined one is grouped' => [[$note, Query::count('*', 'rows'), Query::groupBy(['note.name']), Query::orderAsc('name')], 'name'];
        yield 'a joined internal attribute outside the groups' => [[$note, Query::count('*', 'rows'), Query::groupBy(['note.name']), Query::orderAsc('note.$id')], 'note.$id'];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('ungroupedOrders')]
    public function testOrderByAnUngroupedAttributeInAnAggregationQueryIsAnInvalidQuery(array $queries, string $attribute): void
    {
        $this->assertInvalidQuery(self::ungrouped($attribute), fn (): mixed => $this->database->find('customers', $queries));
    }

    public function testUngroupedOrderIsRejectedWhereverAQuerySetIsValidated(): void
    {
        $queries = [Query::count('*', 'rows'), Query::groupBy(['status']), Query::orderAsc('name')];

        $this->assertInvalidQuery(self::ungrouped('name'), fn (): mixed => $this->database->count('customers', $queries), 'count()');
        $this->assertInvalidQuery(self::ungrouped('name'), fn (): mixed => $this->database->sum('customers', 'balance', $queries), 'sum()');
    }

    /**
     * @return iterable<string, array{list<Query>, string, list<mixed>}>
     */
    public static function groupedOrders(): iterable
    {
        $note = Query::join('notes', '$id', 'customerId', '=', 'note');

        yield 'a group' => [[Query::count('*', 'rows'), Query::groupBy(['status']), Query::orderDesc('status')], 'status', ['b', 'a']];
        yield 'an aggregate alias' => [[Query::count('*', 'rows'), Query::groupBy(['status']), Query::orderDesc('rows')], 'rows', [2, 1]];
        yield 'an aggregate alias and a group' => [
            [Query::sum('balance', 'total'), Query::groupBy(['status']), Query::orderAsc('total'), Query::orderAsc('status')],
            'total',
            [20, 40],
        ];
        yield 'a joined group' => [[$note, Query::count('*', 'rows'), Query::groupBy(['note.name']), Query::orderDesc('note.name')], 'name', ['second', 'first']];
        yield 'a joined group named without its alias' => [
            [$note, Query::count('*', 'rows'), Query::groupBy(['score']), Query::orderDesc('note.score')],
            'score',
            [5, 4, 3],
        ];
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<mixed>  $expected
     */
    #[DataProvider('groupedOrders')]
    public function testOrderByAGroupOrAnAggregateReturnsTheGroupsInThatOrder(array $queries, string $attribute, array $expected): void
    {
        $groups = $this->database->find('customers', $queries);

        $this->assertSame($expected, \array_map(static fn (Document $group): mixed => $group->getAttribute($attribute), $groups));
    }

    public function testRandomOrderNextToAnAggregateIsAccepted(): void
    {
        $groups = $this->database->find('customers', [Query::count('*', 'rows'), Query::groupBy(['status']), Query::orderRandom()]);

        $this->assertCount(2, $groups);
    }

    public function testOrdersOutsideAnAggregationQueryAreUnchanged(): void
    {
        $customers = $this->database->find('customers', [Query::orderDesc('balance')]);

        $this->assertSame(['Three', 'Two', 'One'], \array_map(static fn (Document $customer): mixed => $customer->getAttribute('name'), $customers));
    }

    public function testOrderValidatorAcceptsOnlyGroupsAndAggregatesOfAnAggregationQuery(): void
    {
        $validator = new Order([
            new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value]),
            new Document(['$id' => 'status', 'key' => 'status', 'type' => ColumnType::String->value]),
        ]);

        $validator->addAggregationAliases(['rows']);
        $validator->setAggregations([Query::count('*', 'rows')]);
        $validator->setGroupBy(['status']);

        $this->assertTrue($validator->isValid(Query::orderAsc('status')), $validator->getDescription());
        $this->assertTrue($validator->isValid(Query::orderDesc('rows')), $validator->getDescription());
        $this->assertTrue($validator->isValid(Query::orderRandom()), $validator->getDescription());
        $this->assertFalse($validator->isValid(Query::orderAsc('name')));
        $this->assertSame('Cannot order by "name": an aggregation query can only order by its groups and aggregates', $validator->getDescription());

        $validator->setAggregations([]);
        $validator->setGroupBy([]);

        $this->assertTrue($validator->isValid(Query::orderAsc('name')), 'without an aggregate or a group an order names any attribute');
    }

    private static function ungrouped(string $attribute): string
    {
        return 'Invalid query: Cannot order by "'.$attribute.'": an aggregation query can only order by its groups and aggregates';
    }

    /**
     * @param  Closure(): mixed  $read
     */
    private function assertInvalidQuery(string $message, Closure $read, string $label = ''): void
    {
        $prefix = $label === '' ? '' : $label.': ';

        try {
            $read();
        } catch (QueryException $error) {
            $this->assertSame($message, $error->getMessage(), $prefix.'the rejection names the order');

            return;
        }

        $this->fail($prefix.'the shape was accepted: '.$message);
    }

    private function database(SQLite $adapter): Database
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setDatabase('aggregate_orders')
            ->setNamespace('aggregate_orders_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());
        $database->create();

        $this->createCollection($database, 'customers', [
            Attribute::string(key: 'name', size: 64),
            Attribute::string(key: 'status', size: 16),
            Attribute::integer(key: 'balance'),
        ]);
        $this->createCollection($database, 'notes', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::string(key: 'name', size: 64),
            Attribute::integer(key: 'score'),
        ]);

        $this->createDocument($database, 'customers', 'c1', ['name' => 'One', 'status' => 'a', 'balance' => 10]);
        $this->createDocument($database, 'customers', 'c2', ['name' => 'Two', 'status' => 'b', 'balance' => 20]);
        $this->createDocument($database, 'customers', 'c3', ['name' => 'Three', 'status' => 'a', 'balance' => 30]);
        $this->createDocument($database, 'notes', 'n1', ['customerId' => 'c1', 'name' => 'first', 'score' => 3]);
        $this->createDocument($database, 'notes', 'n2', ['customerId' => 'c1', 'name' => 'second', 'score' => 4]);
        $this->createDocument($database, 'notes', 'n3', ['customerId' => 'c2', 'name' => 'first', 'score' => 5]);
        $this->createDocument($database, 'notes', 'n4', ['customerId' => 'c9', 'name' => 'third', 'score' => 6]);

        return $database;
    }

    /**
     * @param  array<Attribute>  $attributes
     */
    private function createCollection(Database $database, string $id, array $attributes): void
    {
        $database->createCollection(new Collection(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createDocument(Database $database, string $collection, string $id, array $attributes): void
    {
        $database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            ...$attributes,
        ]));
    }
}
