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
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder\Statement;

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
     * @return iterable<string, array{Closure(PDO): SQL, string, string, bool, int|null, string, int|null}>
     */
    public static function aggregates(): iterable
    {
        $adapters = [
            'mariadb' => static fn (PDO $pdo): SQL => new MariaDB($pdo),
            'mysql' => static fn (PDO $pdo): SQL => new MySQL($pdo),
            'postgres' => static fn (PDO $pdo): SQL => new Postgres($pdo),
            'sqlite' => static fn (PDO $pdo): SQL => new SQLite($pdo),
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
     * @param  Closure(PDO): SQL  $make
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
        $document = new Collection(id: $collection, documentSecurity: $authorization === 'document security');

        $expected = $this->built($adapter, $operation, $document, $max);
        $result = $operation === 'count'
            ? $adapter->count($document, [], $max)
            : $adapter->sum($document, 'price', [], $max);

        $this->assertSame([$expected->query], $this->statements);
        $this->assertSame($expected->bindings, $this->bindings);
        $this->assertSame(5, $result);
    }

    /**
     * The statement the builder makes for an aggregate without queries, as count() and sum()
     * built it before they wrote it out.
     */
    private function built(SQL $adapter, string $operation, Document $collection, ?int $max): Statement
    {
        $build = function () use ($operation, $collection, $max): Statement {
            $name = $this->filter($collection->getId());
            $builder = $this->newBuilder($name, Query::DEFAULT_ALIAS);

            $perDocument = $collection->getAttribute('documentSecurity', false) || $collection->getId() === Database::METADATA;
            if ($this->authorization->getStatus() && $perDocument) {
                $builder->addHook($this->newPermissionHook($name, $this->authorization->getRoles()));
            }

            if ($max === null) {
                $operation === 'count' ? $builder->count('1', 'sum') : $builder->sum('price', 'sum');

                return $builder->build();
            }

            $operation === 'count' ? $builder->selectRaw('1') : $builder->select(['price']);
            $builder->limit($max);
            $outer = $this->createBuilder();
            $outer->fromSub($builder, 'table_count');
            $operation === 'count' ? $outer->count('1', 'sum') : $outer->sum('price', 'sum');

            return $outer->build();
        };

        /** @var Statement */
        return $build->call($adapter);
    }

    /**
     * @param  Closure(PDO): SQL  $make
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
