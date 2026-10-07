<?php

namespace Tests\Unit;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * A filtered count() or sum() is one aggregate over the collection's table. Only a bound on the
 * rows ($max) or a join keeps the rows in a derived table the aggregate then reads.
 */
final class FlatAggregateTest extends TestCase
{
    private const string NAMESPACE = 'flat_aggregate';

    private PDO $pdo;

    private Database $database;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->database = new Database(new SQLite($this->pdo), new Cache(new NoCache()));
        $this->database
            ->setDatabase('flat_aggregate')
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();

        foreach (['items', 'labels'] as $collection) {
            $this->database->createCollection(Collection::create(
                id: $collection,
                attributes: [
                    Attribute::string(key: 'category', size: 16),
                    Attribute::integer(key: 'price'),
                ],
                permissions: [Permission::create(Role::any())],
                documentSecurity: true,
            ));
        }

        foreach ([
            ['i1', 'a', 10, Role::any()],
            ['i2', 'a', 20, Role::any()],
            ['i3', 'a', 30, Role::user('other')],
            ['i4', 'b', 40, Role::any()],
            ['i5', 'a', 50, Role::user('other')],
        ] as [$id, $category, $price, $reader]) {
            $this->database->createDocument('items', new Document([
                '$id' => $id,
                '$permissions' => [Permission::read($reader)],
                'category' => $category,
                'price' => $price,
            ]));
        }

        $this->database->createDocument('labels', new Document([
            '$id' => 'l1',
            '$permissions' => [Permission::read(Role::any())],
            'category' => 'a',
            'price' => 0,
        ]));
    }

    public function testFilteredCountRunsOneFlatStatement(): void
    {
        [$count, $statements] = $this->profile(fn (): int => $this->database->count('items', [Query::equal('category', ['a'])]));

        $this->assertSame(2, $count);
        $this->assertCount(1, $statements, \implode("\n", $statements));
        $this->assertFlat($statements[0]);
        $this->assertStringContainsString('COUNT(1)', $statements[0]);
        $this->assertStringContainsString('_perms', $statements[0], 'the permission subquery is part of the statement');
    }

    public function testFilteredSumRunsOneFlatStatement(): void
    {
        [$sum, $statements] = $this->profile(fn (): int|float => $this->database->sum('items', 'price', [Query::equal('category', ['a'])]));

        $this->assertSame(30, $sum);
        $this->assertCount(1, $statements, \implode("\n", $statements));
        $this->assertFlat($statements[0]);
        $this->assertStringContainsString('SUM(', $statements[0]);
        $this->assertStringContainsString('_perms', $statements[0], 'the permission subquery is part of the statement');
    }

    public function testNestedFiltersStayFlat(): void
    {
        [$count, $statements] = $this->profile(fn (): int => $this->database->count('items', [
            Query::or([Query::equal('category', ['b']), Query::lessThan('price', 15)]),
        ]));

        $this->assertSame(2, $count);
        $this->assertCount(1, $statements);
        $this->assertFlat($statements[0]);
    }

    public function testFilteredCountWithoutAuthorizationStaysFlat(): void
    {
        [$count, $statements] = $this->database->getAuthorization()->skip(fn (): array => $this->profile(
            fn (): int => $this->database->count('items', [Query::equal('category', ['a'])]),
        ));

        $this->assertSame(4, $count);
        $this->assertCount(1, $statements);
        $this->assertFlat($statements[0]);
    }

    public function testBoundedCountKeepsItsLimit(): void
    {
        [$count, $statements] = $this->database->getAuthorization()->skip(fn (): array => $this->profile(
            fn (): int => $this->database->count('items', [Query::equal('category', ['a'])], 2),
        ));

        $this->assertSame(2, $count);
        $this->assertCount(1, $statements);
        $this->assertStringContainsString('table_count', $statements[0], 'a bound on the rows keeps the derived table');
        $this->assertStringContainsString('LIMIT', $statements[0]);
    }

    public function testBoundedSumKeepsItsLimit(): void
    {
        [$sum, $statements] = $this->database->getAuthorization()->skip(fn (): array => $this->profile(
            fn (): int|float => $this->database->sum('items', 'price', [Query::equal('category', ['a'])], 2),
        ));

        $this->assertContains($sum, [30, 40, 50, 60, 70, 80], 'the sum of two of the four matching prices');
        $this->assertCount(1, $statements);
        $this->assertStringContainsString('table_count', $statements[0]);
        $this->assertStringContainsString('LIMIT', $statements[0]);
    }

    public function testJoinedCountKeepsTheDerivedTable(): void
    {
        $join = Query::join('labels', 'label', [Query::on('category', 'category')]);

        [$count, $statements] = $this->profile(fn (): int => $this->database->count('items', [$join]));
        $this->assertSame(2, $count);
        $this->assertCount(1, $statements);
        $this->assertStringContainsString('table_count', $statements[0], 'a join keeps the derived table');

        [$sum] = $this->profile(fn (): int|float => $this->database->sum('items', 'price', [$join]));
        $this->assertSame(30, $sum);
    }

    public function testBoundedJoinedCountKeepsItsLimit(): void
    {
        $this->assertSame(1, $this->database->count('items', [Query::join('labels', 'label', [Query::on('category', 'category')])], 1));
        $this->assertSame(3, $this->database->count('items', [Query::fullOuterJoin('labels', 'label', [Query::on('category', 'category')])]));
        $this->assertSame(2, $this->database->count('items', [Query::fullOuterJoin('labels', 'label', [Query::on('category', 'category')])], 2));
    }

    public function testANonNumericAggregateCountsAsZero(): void
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $statement->method('closeCursor')->willReturn(true);
        $statement->method('fetch')->willReturn(['sum' => 'not a number']);
        $statement->method('fetchAll')->willReturn([['sum' => 'not a number']]);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturn($statement);

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $collection = new Document(['$id' => 'items']);
        $filter = [Query::equal('category', ['a'])];

        $this->assertSame(0, $adapter->sum($collection, 'price'), 'unfiltered');
        $this->assertSame(0, $adapter->sum($collection, 'price', $filter), 'filtered');
        $this->assertSame(0, $adapter->sum($collection, 'price', $filter, 2), 'bounded');
        $this->assertSame(0, $adapter->count($collection), 'unfiltered');
        $this->assertSame(0, $adapter->count($collection, $filter), 'filtered');
        $this->assertSame(0, $adapter->count($collection, $filter, 2), 'bounded');
    }

    public function testPermissionFilteredCountIsUnchanged(): void
    {
        $this->assertSame(3, $this->database->count('items'));
        $this->assertSame(70, $this->database->sum('items', 'price'));
        $this->assertSame(0, $this->database->count('items', [Query::equal('category', ['c'])]));
        $this->assertSame(0, $this->database->sum('items', 'price', [Query::equal('category', ['c'])]));

        $this->database->getAuthorization()->addRole(Role::user('other')->toString());
        $this->assertSame(5, $this->database->count('items'));
        $this->assertSame(4, $this->database->count('items', [Query::equal('category', ['a'])]));
        $this->assertSame(110, $this->database->sum('items', 'price', [Query::equal('category', ['a'])]));
    }

    public function testABuilderRefusalIsAQueryException(): void
    {
        $join = new Query(Method::Join, 'labels', [Query::on('category', 'category'), Query::limit(1)], 'label');
        $this->database->disableValidation();

        foreach ([
            'count()' => fn (): int => $this->database->count('items', [$join]),
            'sum()' => fn (): int|float => $this->database->sum('items', 'price', [$join]),
        ] as $method => $read) {
            try {
                $read();
                $this->fail($method.': the builder\'s refusal was not raised');
            } catch (QueryException $error) {
                $this->assertSame('Unsupported join ON condition: limit', $error->getMessage(), $method);
            }
        }
    }

    public function testAnEngineErrorWhilePreparingIsMapped(): void
    {
        $this->pdo->exec('DROP TABLE `'.self::NAMESPACE.'_items`');

        foreach ([
            'filtered count()' => fn (): int => $this->database->count('items', [Query::equal('category', ['a'])]),
            'filtered sum()' => fn (): int|float => $this->database->sum('items', 'price', [Query::equal('category', ['a'])]),
            'bounded count()' => fn (): int => $this->database->count('items', [], 2),
            'unfiltered count()' => fn (): int => $this->database->getAuthorization()->skip(fn (): int => $this->database->count('items')),
            'unfiltered sum()' => fn (): int|float => $this->database->getAuthorization()->skip(fn (): int|float => $this->database->sum('items', 'price')),
        ] as $method => $read) {
            try {
                $read();
                $this->fail($method.': the missing table was not reported');
            } catch (NotFoundException $error) {
                $this->assertSame('Collection not found', $error->getMessage(), $method);
            }
        }
    }

    /**
     * Run $read with the profiler on and return its result with the statements it ran on the
     * collection's own table.
     *
     * @template T
     *
     * @param  callable(): T  $read
     * @return array{T, list<string>}
     */
    private function profile(callable $read): array
    {
        $profiler = $this->database->enableProfiling()->getProfiler();
        $this->assertNotNull($profiler);

        try {
            $profiler->reset();
            $result = $read();
        } finally {
            $this->database->disableProfiling();
        }

        $statements = [];
        foreach ($profiler->getLogs() as $log) {
            if (\str_contains($log->query, self::NAMESPACE.'_items`')) {
                $statements[] = $log->query;
            }
        }

        return [$result, $statements];
    }

    private function assertFlat(string $statement): void
    {
        $this->assertStringNotContainsString('table_count', $statement, 'no derived table');
        $this->assertStringNotContainsString('FROM (SELECT', $statement, 'no derived table');
    }
}
