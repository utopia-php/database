<?php

namespace Tests\Unit\Joins;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\Profiler\Log;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Aggregate results keep the empty-set contract and their names whether an aggregate is aliased or
 * not, and whether its attribute belongs to the main collection or to a join.
 */
final class AggregateMinorsTest extends TestCase
{
    /**
     * MariaDB and MySQL answer BIT_AND over no values with every bit set and BIT_OR/BIT_XOR with 0.
     * The contract is null for both, aliased or not.
     */
    public function testUnaliasedBitwiseAggregateOfAnEmptySetFollowsTheContract(): void
    {
        [$rows, $statement] = $this->mariaDBFind(
            [Query::bitAnd('flags'), Query::bitOr('flags'), Query::bitXor('mask'), Query::bitAnd('flags', 'all_bits')],
            ['flags' => 0, 'mask' => 0],
        );

        $this->assertStringContainsString('COUNT(`flags`) AS `$inputs:0`', $statement);
        $this->assertStringContainsString('COUNT(`flags`) AS `$inputs:1`', $statement);
        $this->assertStringContainsString('COUNT(`mask`) AS `$inputs:2`', $statement);
        $this->assertStringContainsString('COUNT(`flags`) AS `$inputs:3`', $statement);
        $this->assertSame(
            [['BIT_AND(`flags`)' => null, 'BIT_OR(`flags`)' => null, 'BIT_XOR(`mask`)' => null, 'all_bits' => null]],
            $rows,
        );
    }

    public function testUnaliasedBitwiseAggregateOfValuesKeepsItsValue(): void
    {
        [$rows] = $this->mariaDBFind(
            [Query::bitAnd('flags'), Query::bitOr('mask'), Query::bitXor('$sequence')],
            ['flags' => 2, 'mask' => 0, '_id' => 3],
        );

        $this->assertSame(
            [['BIT_AND(`flags`)' => '18446744073709551615', 'BIT_OR(`mask`)' => null, 'BIT_XOR(`_id`)' => '0']],
            $rows,
        );
    }

    public function testSumResolvesAJoinDeclaredAttribute(): void
    {
        $database = $this->database();
        $item = Query::join('items', 'it', [Query::on('item', 'code')]);

        $this->assertSame(40, $database->sum('orders', 'price', [$item]), 'a name only the join declares');
        $this->assertSame(
            [['total' => 40]],
            $database->aggregate('orders', [$item, Query::sum('price', 'total')]),
            'aggregate() reads the same attribute',
        );
        $this->assertSame(40, $database->sum('orders', 'it.price', [$item]), 'the qualified name');
        $this->assertSame(20, $database->sum('orders', 'price', [$item, Query::equal('it.code', ['a'])]));
        $this->assertSame(6, $database->sum('orders', 'quantity', [$item]), 'a name the main collection declares reads the main table');
        $this->assertSame(0, $database->sum('orders', 'price', [$item, Query::equal('it.code', ['z'])]));
    }

    public function testSumReadsEachJoinedCollectionDefinitionOnce(): void
    {
        $database = $this->database();
        $database->setProfiling(true);

        foreach (['validated' => true, 'unvalidated' => false] as $case => $validate) {
            $validate ? $database->setValidation(true) : $database->setValidation(false);
            $database->getProfiler()?->reset();

            $this->assertSame(40, $database->sum('orders', 'price', [Query::join('items', 'it', [Query::on('item', 'code')])]), $case);

            $reads = \array_filter(
                $database->getProfiler()?->getLogs() ?? [],
                static fn (Log $log): bool => \str_contains($log->query, '_metadata') && \in_array('items', $log->bindings, true),
            );
            $this->assertCount(1, $reads, $case.': the definition resolved for the join serves the bare name too');
        }
    }

    public function testSumRefusesABareNameNoCollectionOrSeveralJoinsDeclare(): void
    {
        $database = $this->database();
        $item = Query::join('items', 'it', [Query::on('item', 'code')]);
        $extra = Query::join('extras', 'ex', [Query::on('item', 'code')]);

        foreach ([
            'two joins declare it' => [fn (): int|float => $database->sum('orders', 'price', [$item, $extra]), 'Invalid query: Attribute "price" is ambiguous across joins; qualify it with a join alias'],
            'no join' => [fn (): int|float => $database->sum('orders', 'price'), 'Invalid query: Attribute not found in schema: price'],
            'no collection declares it' => [fn (): int|float => $database->sum('orders', 'weight', [$item]), 'Invalid query: Attribute not found in schema: weight'],
        ] as $case => [$sum, $message]) {
            try {
                $sum();
                $this->fail($case.': the sum ran');
            } catch (QueryException $error) {
                $this->assertSame($message, $error->getMessage(), $case);
            }
        }

        $this->assertSame(200, $database->sum('orders', 'ex.price', [$item, $extra]), 'qualified, the ambiguous name reads its join');
    }

    public function testJoinedGroupKeepsItsQualifiedName(): void
    {
        foreach (['join' => [false, 'join'], 'emulated full outer join' => [false, 'fullOuterJoin'], 'native full outer join' => [true, 'fullOuterJoin']] as $case => [$native, $method]) {
            $database = $this->database($native);
            $item = Query::$method('items', 'it', [Query::on('item', 'code')]);
            $extra = Query::join('extras', 'ex', [Query::on('item', 'code')]);

            $this->assertSame(
                [['orders' => 2, 'name' => 'x', 'it.name' => 'apple'], ['orders' => 1, 'name' => 'y', 'it.name' => 'banana']],
                $database->aggregate('orders', [$item, Query::count('*', 'orders'), Query::groupBy(['name', 'it.name']), Query::orderAsc('name')]),
                $case.': the main group keeps the bare name',
            );
            $this->assertSame(
                [['orders' => 2, 'name' => 'x', 'it.name' => 'apple'], ['orders' => 1, 'name' => 'y', 'it.name' => 'banana']],
                $database->aggregate('orders', [$item, Query::count('*', 'orders'), Query::groupBy(['it.name', 'name']), Query::orderAsc('it.name')]),
                $case.': in either order',
            );
            $this->assertSame(
                [['orders' => 1, 'name' => 'y', 'it.name' => 'banana']],
                $database->aggregate('orders', [$item, Query::count('*', 'orders'), Query::groupBy(['name', 'it.name']), Query::having([Query::equal('it.name', ['banana'])])]),
                $case.': a having on the qualified group',
            );
            $this->assertSame(
                [['orders' => 2, 'name' => 'apple'], ['orders' => 1, 'name' => 'banana']],
                $database->aggregate('orders', [$item, Query::count('*', 'orders'), Query::groupBy(['it.name']), Query::orderAsc('it.name')]),
                $case.': a joined group alone keeps its bare name',
            );
            $this->assertSame(
                [['orders' => 2, 'code' => 'a'], ['orders' => 1, 'code' => 'b']],
                $database->aggregate('orders', [$item, Query::count('*', 'orders'), Query::groupBy(['code']), Query::orderAsc('it.code')]),
                $case.': a bare name only the join declares',
            );
        }

        $database = $this->database();
        $this->assertSame(
            [['orders' => 2, 'it.code' => 'a', 'ex.code' => 'a']],
            $database->aggregate('orders', [
                Query::join('items', 'it', [Query::on('item', 'code')]),
                Query::join('extras', 'ex', [Query::on('item', 'code')]),
                Query::count('*', 'orders'),
                Query::groupBy(['it.code', 'ex.code']),
            ]),
            'two joined groups of one name are both qualified',
        );
    }

    /**
     * Run a find on MariaDB, answered as MariaDB answers: an unaliased aggregate is named by its
     * expression, a count is the number of values $inputs gives its column, BIT_AND is every bit set
     * and BIT_OR/BIT_XOR are 0.
     *
     * @param  list<Query>  $queries
     * @param  array<string, int>  $inputs  The number of values each column holds
     * @return array{list<array<string, mixed>>, string}
     */
    private function mariaDBFind(array $queries, array $inputs): array
    {
        $sql = '';
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $statement->method('closeCursor')->willReturn(true);
        $statement->method('fetchAll')->willReturnCallback(function () use (&$sql, $inputs): array {
            \preg_match_all('/([A-Z_]+)\(`([^`]+)`\)(?: AS `([^`]+)`)?/', $sql, $matches, PREG_SET_ORDER);
            $this->assertNotSame([], $matches, 'no aggregate in: '.$sql);

            $row = [];
            foreach ($matches as $match) {
                [$expression, $function, $column] = $match;
                $name = $match[3] ?? $expression;
                $row[$name] = match ($function) {
                    'COUNT' => (string) ($inputs[$column] ?? 0),
                    'BIT_AND' => '18446744073709551615',
                    default => '0',
                };
            }

            return [$row];
        });

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$sql, $statement): PDOStatement {
            $sql = $query;

            return $statement;
        });

        $adapter = new MariaDB($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $rows = \array_values(\array_map(
            static fn (Document $document): array => $document->getArrayCopy(),
            $adapter->find(new Document(['$id' => 'collection']), $queries, limit: 25),
        ));

        return [$rows, $sql];
    }

    /**
     * Orders of items: o1 and o3 order a (price 10), o2 orders b (price 20); extras prices a at 100.
     */
    private function database(bool $nativeFullOuterJoin = false): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $database = new Database($nativeFullOuterJoin ? new NativeFullOuterJoinSQLite($pdo) : new SQLite($pdo), new Cache(new NoCache()));
        $database
            ->setDatabase('aggregate_minors')
            ->setNamespace('aggregate_minors_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: 'orders',
            attributes: [
                Attribute::string(key: 'item', size: 16),
                Attribute::integer(key: 'quantity'),
                Attribute::string(key: 'name', size: 16),
            ],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: 'items',
            attributes: [
                Attribute::string(key: 'code', size: 16),
                Attribute::integer(key: 'price'),
                Attribute::integer(key: 'quantity'),
                Attribute::string(key: 'name', size: 16),
            ],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: 'extras',
            attributes: [
                Attribute::string(key: 'code', size: 16),
                Attribute::integer(key: 'price'),
            ],
            permissions: $permissions,
        ));

        foreach ([['o1', 'a', 1, 'x'], ['o2', 'b', 2, 'y'], ['o3', 'a', 3, 'x']] as [$id, $item, $quantity, $name]) {
            $database->createDocument('orders', new Document(['$id' => $id, '$permissions' => [], 'item' => $item, 'quantity' => $quantity, 'name' => $name]));
        }
        foreach ([['a', 10, 'apple'], ['b', 20, 'banana']] as [$code, $price, $name]) {
            $database->createDocument('items', new Document(['$id' => $code, '$permissions' => [], 'code' => $code, 'price' => $price, 'quantity' => 100, 'name' => $name]));
        }
        $database->createDocument('extras', new Document(['$id' => 'a', '$permissions' => [], 'code' => 'a', 'price' => 100]));

        return $database;
    }
}
