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
use Utopia\Database\Validator\Authorization;

/**
 * A count() or sum() without queries is written out rather than built. It must be the statement
 * the builder makes for it: the same tenant condition, the same permission condition, the same
 * bound on the rows, with the same bindings in the same order.
 */
final class UnfilteredAggregateStatementTest extends TestCase
{
    private const array ROLES = ['any', 'user:reader'];

    /** @var list<string> */
    private array $statements = [];

    /** @var list<mixed> */
    private array $bindings = [];

    /**
     * @return iterable<string, array{Closure(PDO): (SQL&AggregateReference), string, string, bool, int|null, string, int|null}>
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
            'shared without a tenant' => ['books', null],
        ];

        foreach ($adapters as $engine => $make) {
            foreach (['count', 'sum'] as $operation) {
                foreach ($tables as $mode => [$collection, $tenant]) {
                    foreach (['unauthorized', 'collection permissions', 'document security'] as $authorization) {
                        foreach ([null, 25, 0] as $max) {
                            $name = "{$engine} {$operation} {$mode} {$authorization} max ".($max ?? 'none');
                            yield $name => [$make, $operation, $collection, $mode !== 'plain', $tenant, $authorization, $max];
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
    public function testAnAggregateWithoutQueriesRunsTheStatementTheBuilderMakes(
        Closure $make,
        string $operation,
        string $collection,
        bool $shared,
        ?int $tenant,
        string $authorization,
        ?int $max,
    ): void {
        $adapter = $this->adapter($make, $shared, $tenant, $authorization);
        $document = Collection::create(id: $collection, documentSecurity: $authorization === 'document security');

        $expected = $adapter->builtAggregate($operation, $document, [], $max);
        $result = $operation === 'count'
            ? $adapter->count($document, [], $max)
            : $adapter->sum($document, 'price', [], $max);

        $this->assertSame([$expected->query], $this->statements);
        $this->assertSame($adapter->boundValues($expected->bindings), $this->bindings);
        $this->assertSame(5, $result);
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
