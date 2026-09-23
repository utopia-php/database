<?php

namespace Tests\Unit\Documents;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
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
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Queries\Document as DocumentValidator;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Schema\ColumnType;

/**
 * An aggregation query, one with an aggregate or a groupBy, returns one row per group holding the
 * groups and the aggregates. A select next to them can name only an attribute the query groups by,
 * so no engine is asked for a column the groups do not determine. `*` and relationship wildcards at
 * any depth are accepted and add nothing to the rows.
 */
final class AggregateSelectTest extends TestCase
{
    private const string TENANT = '1';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = $this->database(new SQLite(new PDO('sqlite::memory:')));
    }

    /**
     * @return iterable<string, array{list<Query>, string}>
     */
    public static function ungroupedSelects(): iterable
    {
        $note = Query::join('notes', '$id', 'customerId', '=', 'note');

        yield 'an attribute next to a count' => [[Query::count('*', 'rows'), Query::select(['name'])], 'name'];
        yield 'an attribute next to a sum' => [[Query::sum('balance', 'total'), Query::select(['name'])], 'name'];
        yield 'an internal attribute next to a count' => [[Query::count('*', 'rows'), Query::select(['$id'])], '$id'];
        yield '$collection next to a count' => [[Query::count('*', 'rows'), Query::select(['$collection'])], '$collection'];
        yield 'an attribute outside the groups' => [[Query::count('*', 'rows'), Query::groupBy(['status']), Query::select(['name'])], 'name'];
        yield 'a group and an attribute outside the groups' => [[Query::count('*', 'rows'), Query::groupBy(['status']), Query::select(['status', 'name'])], 'name'];
        yield 'a wildcard and an attribute outside the groups' => [[Query::count('*', 'rows'), Query::groupBy(['status']), Query::select(['*', 'name'])], 'name'];
        yield 'an attribute next to a groupBy without an aggregate' => [[Query::groupBy(['status']), Query::select(['name'])], 'name'];
        yield 'an attribute next to a distinct count' => [[Query::distinct(), Query::count('*', 'rows'), Query::select(['name'])], 'name'];
        yield 'an attribute of a related document' => [[Query::count('*', 'rows'), Query::select(['account.plan'])], 'account.plan'];
        yield 'a wildcard under an attribute that is not a relationship' => [[Query::count('*', 'rows'), Query::select(['name.*'])], 'name.*'];
        yield 'a joined attribute next to a count' => [[$note, Query::count('*', 'rows'), Query::select(['note.score'])], 'note.score'];
        yield 'a joined attribute when the main one is grouped' => [[$note, Query::count('*', 'rows'), Query::groupBy(['name']), Query::select(['note.name'])], 'note.name'];
        yield 'a main attribute when the joined one is grouped' => [[$note, Query::count('*', 'rows'), Query::groupBy(['note.name']), Query::select(['name'])], 'name'];
        yield 'a joined internal attribute outside the groups' => [[$note, Query::count('*', 'rows'), Query::groupBy(['note.name']), Query::select(['note.$id'])], 'note.$id'];
        yield 'the wildcard of a join alias' => [[$note, Query::count('*', 'rows'), Query::select(['note.*'])], 'note.*'];
        yield 'the wildcard of a join alias named like a relationship' => [
            [Query::join('accounts', 'account', '$id', '=', 'account'), Query::count('*', 'rows'), Query::select(['account.*'])],
            'account.*',
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('ungroupedSelects')]
    public function testSelectOfAnUngroupedAttributeInAnAggregationQueryIsAnInvalidQuery(array $queries, string $attribute): void
    {
        $this->assertInvalidQuery(self::ungrouped($attribute), fn (): mixed => $this->database->find('customers', $queries));
    }

    public function testUngroupedSelectIsRejectedWhereverAQuerySetIsValidated(): void
    {
        $queries = [Query::count('*', 'rows'), Query::select(['name'])];

        $this->assertInvalidQuery(self::ungrouped('name'), fn (): mixed => $this->database->count('customers', $queries), 'count()');
        $this->assertInvalidQuery(self::ungrouped('name'), fn (): mixed => $this->database->sum('customers', 'balance', $queries), 'sum()');
    }

    /**
     * @return iterable<string, array{list<Query>, list<array<string, mixed>>}>
     */
    public static function wildcardSelects(): iterable
    {
        yield 'a count' => [[Query::count('*', 'rows'), Query::select(['*'])], [['rows' => 3]]];
        yield 'a count and a sum' => [[Query::count('*', 'rows'), Query::sum('balance', 'total'), Query::select(['*'])], [['rows' => 3, 'total' => 60]]];
        yield 'a count over a join' => [[Query::join('notes', '$id', 'customerId', '=', 'note'), Query::count('*', 'rows'), Query::select(['*'])], [['rows' => 3]]];
        yield 'a count over a left join' => [[Query::leftJoin('notes', '$id', 'customerId', '=', 'note'), Query::count('*', 'rows'), Query::select(['*'])], [['rows' => 4]]];
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<array<string, mixed>>  $expected
     */
    #[DataProvider('wildcardSelects')]
    public function testWildcardSelectNextToAnAggregateReturnsOnlyTheAggregates(array $queries, array $expected): void
    {
        $this->assertSame($expected, $this->rows($this->database->find('customers', $queries)));
    }

    /**
     * The selects a listing gets when it asks for every attribute and every related document: `*`
     * and a wildcard for each relationship, nested ones included.
     *
     * @return iterable<string, array{list<string>}>
     */
    public static function relationshipWildcards(): iterable
    {
        yield 'one level' => [['*', 'account.*']];
        yield 'two levels' => [['*', 'account.*', 'account.region.*']];
        yield 'a nested wildcard alone' => [['account.region.*']];
    }

    /**
     * @param  list<string>  $selects
     */
    #[DataProvider('relationshipWildcards')]
    public function testRelationshipWildcardsNextToAnAggregateAddNothingToTheRows(array $selects): void
    {
        $this->assertSame([['rows' => 3]], $this->rows($this->database->find('customers', [Query::count('*', 'rows'), Query::select($selects)])));
        $this->assertSame(
            [['rows' => 2, 'status' => 'a'], ['rows' => 1, 'status' => 'b']],
            $this->rows($this->database->find('customers', [Query::count('*', 'rows'), Query::groupBy(['status']), Query::select($selects), Query::orderAsc('status')])),
        );
        $this->assertSame(
            [['rows' => 2, 'name' => 'first'], ['rows' => 1, 'name' => 'second']],
            $this->rows($this->database->find('customers', [Query::join('notes', '$id', 'customerId', '=', 'note'), Query::count('*', 'rows'), Query::groupBy(['note.name']), Query::select($selects), Query::orderAsc('note.name')])),
        );
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function fullOuterJoins(): iterable
    {
        yield 'emulated full outer join' => [false];
        yield 'native full outer join' => [true];
    }

    #[DataProvider('fullOuterJoins')]
    public function testWildcardsNextToAnAggregateOverAFullOuterJoinAddNothingToTheRows(bool $native): void
    {
        if ($native) {
            $this->database = $this->database(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
        }
        $note = Query::fullOuterJoin('notes', '$id', 'customerId', '=', 'note');

        $this->assertSame([['rows' => 5]], $this->rows($this->database->find('customers', [$note, Query::count('*', 'rows'), Query::select(['*'])])));
        $this->assertSame([['rows' => 5]], $this->rows($this->database->find('customers', [$note, Query::count('*', 'rows'), Query::select(['*', 'account.*', 'account.region.*'])])));
        $this->assertSame(
            [['rows' => 1, 'name' => null], ['rows' => 2, 'name' => 'first'], ['rows' => 1, 'name' => 'second'], ['rows' => 1, 'name' => 'third']],
            $this->rows($this->database->find('customers', [$note, Query::count('*', 'rows'), Query::groupBy(['note.name']), Query::select(['note.name', '*', 'account.*', 'account.region.*']), Query::orderAsc('note.name')])),
        );
        $this->assertInvalidQuery(self::ungrouped('name'), fn (): mixed => $this->database->find('customers', [$note, Query::count('*', 'rows'), Query::select(['name'])]));
    }

    /**
     * @return iterable<string, array{list<Query>, list<array<string, mixed>>}>
     */
    public static function groupedSelects(): iterable
    {
        $note = Query::join('notes', '$id', 'customerId', '=', 'note');

        yield 'a grouped attribute' => [
            [Query::count('*', 'rows'), Query::groupBy(['status']), Query::select(['status']), Query::orderAsc('status')],
            [['rows' => 2, 'status' => 'a'], ['rows' => 1, 'status' => 'b']],
        ];
        yield 'a grouped attribute selected before its groupBy' => [
            [Query::select(['status']), Query::count('*', 'rows'), Query::groupBy(['status']), Query::orderAsc('status')],
            [['rows' => 2, 'status' => 'a'], ['rows' => 1, 'status' => 'b']],
        ];
        yield 'a grouped attribute and a wildcard' => [
            [Query::count('*', 'rows'), Query::groupBy(['status']), Query::select(['status', '*']), Query::orderAsc('status')],
            [['rows' => 2, 'status' => 'a'], ['rows' => 1, 'status' => 'b']],
        ];
        yield 'a wildcard next to a group' => [
            [Query::count('*', 'rows'), Query::groupBy(['status']), Query::select(['*']), Query::orderAsc('status')],
            [['rows' => 2, 'status' => 'a'], ['rows' => 1, 'status' => 'b']],
        ];
        yield 'a grouped attribute without an aggregate' => [
            [Query::groupBy(['status']), Query::select(['status']), Query::orderAsc('status')],
            [['status' => 'a'], ['status' => 'b']],
        ];
        yield 'two groups' => [
            [Query::count('*', 'rows'), Query::groupBy(['status', 'name']), Query::select(['name', 'status']), Query::orderAsc('name')],
            [['rows' => 1, 'status' => 'a', 'name' => 'One'], ['rows' => 1, 'status' => 'a', 'name' => 'Three'], ['rows' => 1, 'status' => 'b', 'name' => 'Two']],
        ];
        yield 'a grouped internal attribute' => [
            [Query::count('*', 'rows'), Query::groupBy(['$id']), Query::select(['$id']), Query::orderAsc('$id')],
            [['rows' => 1, Storage::UID => 'c1'], ['rows' => 1, Storage::UID => 'c2'], ['rows' => 1, Storage::UID => 'c3']],
        ];
        yield 'a grouped joined attribute' => [
            [$note, Query::count('*', 'rows'), Query::groupBy(['note.name']), Query::select(['note.name']), Query::orderAsc('note.name')],
            [['rows' => 2, 'name' => 'first'], ['rows' => 1, 'name' => 'second']],
        ];
        yield 'a joined attribute grouped by the bare name only its join declares' => [
            [$note, Query::count('*', 'rows'), Query::groupBy(['score']), Query::select(['note.score']), Query::orderAsc('note.score')],
            [['rows' => 1, 'score' => 3], ['rows' => 1, 'score' => 4], ['rows' => 1, 'score' => 5]],
        ];
        yield 'a grouped joined internal attribute' => [
            [$note, Query::count('*', 'rows'), Query::groupBy(['note.$id']), Query::select(['note.$id']), Query::orderAsc('note.$id')],
            [['rows' => 1, Storage::UID => 'n1'], ['rows' => 1, Storage::UID => 'n2'], ['rows' => 1, Storage::UID => 'n3']],
        ];
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<array<string, mixed>>  $expected
     */
    #[DataProvider('groupedSelects')]
    public function testGroupedSelectReturnsEachGroupOnceWithItsAggregates(array $queries, array $expected): void
    {
        $this->assertSame($expected, $this->rows($this->database->find('customers', $queries)));
    }

    #[DataProvider('fullOuterJoins')]
    public function testGroupedSelectOverAFullOuterJoinReturnsEachGroupOnce(bool $native): void
    {
        if ($native) {
            $this->database = $this->database(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
        }

        $this->assertSame(
            [['rows' => 1, 'name' => null], ['rows' => 2, 'name' => 'first'], ['rows' => 1, 'name' => 'second'], ['rows' => 1, 'name' => 'third']],
            $this->rows($this->database->find('customers', [
                Query::fullOuterJoin('notes', '$id', 'customerId', '=', 'note'),
                Query::count('*', 'rows'),
                Query::groupBy(['note.name']),
                Query::select(['note.name']),
                Query::orderAsc('note.name'),
            ])),
        );
    }

    public function testTenantIsSelectedNextToAnAggregateOnlyWhenGrouped(): void
    {
        $this->database = $this->database(new SQLite(new PDO('sqlite::memory:')), sharedTables: true);

        $this->assertInvalidQuery(self::ungrouped('$tenant'), fn (): mixed => $this->database->find('customers', [Query::count('*', 'rows'), Query::select(['$tenant'])]));
        $this->assertSame(
            [['rows' => 3, Storage::TENANT => 1]],
            $this->rows($this->database->find('customers', [Query::count('*', 'rows'), Query::groupBy(['$tenant']), Query::select(['$tenant'])])),
        );
    }

    /**
     * With validation skipped, a select still never reaches the statement of an aggregation query.
     */
    public function testSelectNextToAnAggregateNeverReachesTheEngine(): void
    {
        $this->assertSame(
            [['rows' => 3]],
            $this->rows($this->database->skipValidation(fn (): array => $this->database->find('customers', [Query::count('*', 'rows'), Query::select(['name'])]))),
        );
        $this->assertSame(
            [['rows' => 3]],
            $this->rows($this->database->skipValidation(fn (): array => $this->database->find('customers', [Query::count('*', 'rows'), Query::select(['*', 'account.*'])]))),
        );
    }

    public function testSelectsOutsideAnAggregationQueryAreUnchanged(): void
    {
        $customers = $this->database->find('customers', [Query::select(['name', '$collection']), Query::orderAsc('name')]);
        $this->assertSame(['One', 'Three', 'Two'], \array_map(static fn (Document $customer): mixed => $customer->getAttribute('name'), $customers));
        $this->assertSame(['customers', 'customers', 'customers'], \array_map(static fn (Document $customer): string => $customer->getCollection(), $customers));

        $statuses = $this->database->find('customers', [Query::distinct(), Query::select(['status']), Query::orderAsc('status')]);
        $this->assertSame(['a', 'b'], \array_map(static fn (Document $customer): mixed => $customer->getAttribute('status'), $statuses));

        $withAccount = $this->database->find('customers', [Query::select(['name', 'account.*', 'account.region.*']), Query::equal('$id', ['c1'])]);
        $this->assertCount(1, $withAccount);
        $account = $withAccount[0]->getAttribute('account');
        $this->assertInstanceOf(Document::class, $account);
        $this->assertSame('pro', $account->getAttribute('plan'));
        $region = $account->getAttribute('region');
        $this->assertInstanceOf(Document::class, $region);
        $this->assertSame('eu', $region->getAttribute('code'));

        $joined = $this->database->find('customers', [Query::join('notes', '$id', 'customerId', '=', 'note'), Query::select(['name', 'note.name']), Query::orderAsc('note.$id')]);
        $this->assertSame(['first', 'second', 'first'], \array_map(static fn (Document $customer): mixed => $customer->getAttribute('note.name'), $joined));
    }

    /**
     * A validator that cannot run aggregates, or a document read, keeps rejecting the aggregate
     * itself rather than the select next to it.
     */
    public function testValidatorsWithoutAggregatesRejectTheAggregateItself(): void
    {
        $attributes = [new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value, 'array' => false])];
        $queries = [Query::select(['name']), Query::count('*', 'rows')];

        $documents = new DocumentsValidator($attributes, [], ColumnType::String->value);
        $this->assertFalse($documents->isValid($queries));
        $this->assertSame('Invalid query method: count', $documents->getDescription());

        $document = new DocumentValidator($attributes);
        $this->assertFalse($document->isValid($queries));
        $this->assertSame('Invalid query method: count', $document->getDescription());
    }

    public function testSelectValidatorAcceptsOnlyGroupsAndWildcardsOfAnAggregationQuery(): void
    {
        $validator = new Select([
            new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value]),
            new Document(['$id' => 'status', 'key' => 'status', 'type' => ColumnType::String->value]),
            new Document(['$id' => 'account', 'key' => 'account', 'type' => ColumnType::Relationship->value]),
        ]);

        $validator->setAggregations([Query::count('*', 'rows')]);
        $validator->setGroupBy(['status']);

        $this->assertTrue($validator->isValid(Query::select(['status', '*', 'account.*', 'account.region.*'])), $validator->getDescription());
        $this->assertFalse($validator->isValid(Query::select(['name'])));
        $this->assertSame('Cannot select "name": an aggregation query can only select the attributes it groups by', $validator->getDescription());

        $validator->setAggregations([]);
        $validator->setGroupBy([]);

        $this->assertTrue($validator->isValid(Query::select(['name'])), 'without an aggregate or a group a select is a projection');

        $schemaless = new Select([], supportForAttributes: false);
        $schemaless->setAggregations([Query::count('*', 'rows')]);
        $schemaless->setGroupBy(['anything']);

        $this->assertTrue($schemaless->isValid(Query::select(['anything', '*'])), $schemaless->getDescription());
        $this->assertFalse($schemaless->isValid(Query::select(['other'])));
        $this->assertFalse($schemaless->isValid(Query::select(['unknown.*'])), 'a wildcard is a relationship wildcard only under a declared relationship');
    }

    private static function ungrouped(string $attribute): string
    {
        return 'Invalid query: Cannot select "'.$attribute.'": an aggregation query can only select the attributes it groups by';
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
            $this->assertSame($message, $error->getMessage(), $prefix.'the rejection names the select');

            return;
        }

        $this->fail($prefix.'the shape was accepted: '.$message);
    }

    /**
     * @param  array<Document>  $documents
     * @return list<array<string, mixed>>
     */
    private function rows(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): array => $document->getArrayCopy(), $documents));
    }

    private function database(SQLite $adapter, bool $sharedTables = false): Database
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setDatabase('aggregate_selects')
            ->setNamespace('aggregate_selects_'.\uniqid())
            ->setAuthorization(new Authorization());
        if ($sharedTables) {
            $database->setSharedTables(true)->setTenant(null);
        }
        $database->addHook(new Permissions());
        $database->addHook(new Relationships($database));
        $database->create();

        $this->createCollection($database, 'customers', [
            Attribute::string(key: 'name', size: 64),
            Attribute::string(key: 'status', size: 16),
            Attribute::integer(key: 'balance'),
        ]);
        $this->createCollection($database, 'accounts', [Attribute::string(key: 'plan', size: 16)]);
        $this->createCollection($database, 'regions', [Attribute::string(key: 'code', size: 16)]);
        $this->createCollection($database, 'notes', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::string(key: 'name', size: 64),
            Attribute::integer(key: 'score'),
        ]);
        $database->createRelationship(Relationship::oneToOne(collection: 'customers', relatedCollection: 'accounts', key: 'account', twoWayKey: 'customer'));
        $database->createRelationship(Relationship::manyToOne(collection: 'accounts', relatedCollection: 'regions', key: 'region', twoWayKey: 'accounts'));

        if ($sharedTables) {
            $database->setTenant(self::TENANT);
        }

        $this->createDocument($database, 'regions', 'r1', ['code' => 'eu']);
        $this->createDocument($database, 'accounts', 'a1', ['plan' => 'pro', 'region' => 'r1']);
        $this->createDocument($database, 'customers', 'c1', ['name' => 'One', 'status' => 'a', 'balance' => 10, 'account' => 'a1']);
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
