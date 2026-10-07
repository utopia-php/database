<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

/**
 * A count() or sum() whose queries only narrow its rows is written out rather than built. It must
 * be the statement the builder makes for it: the same conditions for the filters, the tenant and
 * the permissions, in the same order, the same bound on the rows, with the same bindings.
 */
final class FilteredAggregateStatementTest extends TestCase
{
    private const array ROLES = ['any', 'user:reader'];

    /** @var list<string> */
    private array $statements = [];

    /** @var list<mixed> */
    private array $bindings = [];

    /**
     * @return array<string, Closure(): list<Query>>
     */
    private static function filters(): array
    {
        return [
            'equal' => static fn (): array => [Query::equal('category', ['c3'])],
            'equal several' => static fn (): array => [Query::equal('category', ['c1', 'c2', 'c3'])],
            'not equal' => static fn (): array => [Query::notEqual('category', 'c3')],
            'comparisons' => static fn (): array => [Query::greaterThan('score', 10), Query::lessThanEqual('price', 9.5)],
            'between' => static fn (): array => [Query::between('score', 1, 99)],
            'not between' => static fn (): array => [Query::notBetween('score', 1, 99)],
            'null checks' => static fn (): array => [Query::isNull('category'), Query::isNotNull('score')],
            'starts with an escaped value' => static fn (): array => [Query::startsWith('category', 'c_3%')],
            'ends with' => static fn (): array => [Query::notEndsWith('category', '3')],
            'contains in a string' => static fn (): array => [new Query(Method::Contains, 'category', ['c'])],
            'contains in an array' => static function (): array {
                $query = new Query(Method::Contains, 'tags', ['t1', 't2']);
                $query->setOnArray(true);

                return [$query];
            },
            'contains any in an array' => static function (): array {
                $query = Query::containsAny('tags', ['t1']);
                $query->setOnArray(true);

                return [$query];
            },
            'not contains in an array' => static function (): array {
                $query = Query::notContains('tags', ['t1']);
                $query->setOnArray(true);

                return [$query];
            },
            'regex' => static fn (): array => [Query::regex('category', '^c[0-9]$')],
            'internal attributes' => static fn (): array => [Query::equal('$id', ['d1']), Query::greaterThan('$sequence', 5), Query::lessThan('$createdAt', '2030-01-01 00:00:00.000')],
            'or' => static fn (): array => [Query::or([Query::equal('category', ['c1']), Query::greaterThan('score', 5)])],
            'and inside or' => static fn (): array => [Query::or([Query::and([Query::equal('category', ['c1']), Query::isNotNull('score')]), Query::equal('category', ['c2'])])],
            'object path' => static function (): array {
                $query = Query::equal('meta.level', ['x']);
                $query->setAttributeType(ColumnType::Object->value);

                return [$query];
            },
        ];
    }

    /**
     * @return iterable<string, array{Closure(PDO): (SQL&AggregateReference), string, string, string, bool, int|null, string, int|null}>
     */
    public static function aggregates(): iterable
    {
        $adapters = [
            'mariadb' => static fn (PDO $pdo): SQL&AggregateReference => new class ($pdo) extends MariaDB implements AggregateReference {
                use BuildsAggregates;
            },
            'mysql' => static fn (PDO $pdo): SQL&AggregateReference => new class ($pdo) extends MySQL implements AggregateReference {
                use BuildsAggregates;
            },
            'postgres' => static fn (PDO $pdo): SQL&AggregateReference => new class ($pdo) extends Postgres implements AggregateReference {
                use BuildsAggregates;
            },
            'sqlite' => static fn (PDO $pdo): SQL&AggregateReference => new class ($pdo) extends SQLite implements AggregateReference {
                use BuildsAggregates;
            },
        ];
        $tables = [
            'plain' => ['books', null],
            'shared' => ['books', 3],
            'shared metadata' => [Database::METADATA, 3],
        ];

        foreach ($adapters as $engine => $make) {
            foreach (\array_keys(self::filters()) as $filter) {
                foreach (['count', 'sum'] as $operation) {
                    foreach ($tables as $mode => [$collection, $tenant]) {
                        foreach (['unauthorized', 'document security'] as $authorization) {
                            foreach ([null, 25] as $max) {
                                $name = "{$engine} {$operation} {$filter} {$mode} {$authorization} max ".($max ?? 'none');
                                yield $name => [$make, $filter, $operation, $collection, $mode !== 'plain', $tenant, $authorization, $max];
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * @param  Closure(PDO): (SQL&AggregateReference)  $make
     */
    #[DataProvider('aggregates')]
    public function testAnAggregateWhoseQueriesNarrowItsRowsRunsTheStatementTheBuilderMakes(
        Closure $make,
        string $filter,
        string $operation,
        string $collection,
        bool $shared,
        ?int $tenant,
        string $authorization,
        ?int $max,
    ): void {
        $adapter = $this->adapter($make, $shared, $tenant, $authorization);
        $document = Collection::create(id: $collection, documentSecurity: $authorization === 'document security');
        $queries = self::filters()[$filter]();

        $expected = $adapter->builtAggregate($operation, $document, self::filters()[$filter](), $max);
        $result = $operation === 'count'
            ? $adapter->count($document, $queries, $max)
            : $adapter->sum($document, 'price', $queries, $max);

        $this->assertSame([$expected->query], $this->statements);
        $this->assertSame($adapter->boundValues($expected->bindings), $this->bindings);
        $this->assertSame(5, $result);
        $this->assertEquals(self::filters()[$filter](), $queries, 'The queries handed in are left as they were');
    }

    public function testAnAggregateWhoseQueriesNarrowItsRowsBuildsNoStatement(): void
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn([['sum' => '5']]);

            return $statement;
        });
        $adapter = new class ($pdo) extends MariaDB {
            public int $built = 0;

            protected function createBuilder(): SQLBuilder
            {
                return parent::createBuilder()->beforeBuild(function (): void {
                    $this->built++;
                });
            }
        };
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setAuthorization(new Authorization());
        $collection = Collection::create(id: 'books', documentSecurity: true);

        $adapter->count($collection, [Query::equal('category', ['c3'])]);
        $adapter->count($collection, [Query::greaterThan('score', 1)], 25);
        $adapter->sum($collection, 'price', [Query::isNotNull('category')]);
        $this->assertSame(0, $adapter->built);

        $adapter->count($collection, [Query::equal('category', ['c3']), Query::orderAsc('score'), Query::limit(5)]);
        $this->assertSame(2, $adapter->built, 'Rows bounded by their own limit are still counted from a built statement');
    }

    public function testAFilterTheBuilderCannotCompileIsRefusedAsAQueryError(): void
    {
        $adapter = $this->adapter(static fn (PDO $pdo): SQL => new MariaDB($pdo), false, null, 'unauthorized');

        $this->expectException(QueryException::class);
        $adapter->count(Collection::create(id: 'books'), [new Query(Method::ElemMatch, 'tags', [Query::equal('name', ['x'])])]);
    }

    /**
     * @template T of SQL
     *
     * @param  Closure(PDO): T  $make
     * @return T
     */
    private function adapter(Closure $make, bool $shared, ?int $tenant, string $authorization): SQL
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bindings[] = $value;

                return true;
            });
            $statement->method('fetch')->willReturn(['sum' => '5']);
            $statement->method('fetchAll')->willReturn([['sum' => '5']]);
            $statement->method('closeCursor')->willReturn(true);

            return $statement;
        });

        $adapter = $make($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables($shared);
        $adapter->setTenant($tenant);

        $roles = new Authorization();
        foreach (self::ROLES as $role) {
            $roles->addRole($role);
        }
        if ($authorization === 'unauthorized') {
            $roles->disable();
        }
        $adapter->setAuthorization($roles);

        return $adapter;
    }
}
