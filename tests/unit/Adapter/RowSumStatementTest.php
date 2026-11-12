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
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * MariaDB and MySQL send a double sum that overflowed as `0`, so sum() over grouped or distinct rows adds them up
 * through the engine's range check, as it does over documents.
 */
final class RowSumStatementTest extends TestCase
{
    /**
     * @return iterable<string, array{Closure(PDO): SQL, string}>
     */
    public static function adapters(): iterable
    {
        yield 'mariadb' => [static fn (PDO $pdo): SQL => new MariaDB($pdo), 'SUM(`reading`) + 0 AS `sum`'];
        yield 'mysql' => [static fn (PDO $pdo): SQL => new MySQL($pdo), 'SUM(`reading`) + 0 AS `sum`'];
        yield 'postgres' => [static fn (PDO $pdo): SQL => new Postgres($pdo), 'SUM("reading") AS "sum"'];
    }

    /**
     * @return iterable<string, array{list<Query>}>
     */
    public static function shapes(): iterable
    {
        yield 'groupBy' => [[Query::count('*', 'n'), Query::groupBy(['reading'])]];
        yield 'distinct' => [[Query::distinct(), Query::select(['reading'])]];
    }

    /**
     * @return iterable<string, array{Closure(PDO): SQL, string, list<Query>}>
     */
    public static function sums(): iterable
    {
        foreach (self::adapters() as $adapter => [$create, $selection]) {
            foreach (self::shapes() as $shape => [$queries]) {
                yield "{$shape} on {$adapter}" => [$create, $selection, $queries];
            }
        }
    }

    /**
     * @param  Closure(PDO): SQL  $create
     * @param  list<Query>  $queries
     */
    #[DataProvider('sums')]
    public function testTheRowsAreAddedUpThroughTheRangeCheck(Closure $create, string $selection, array $queries): void
    {
        $statements = [];
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $sql) use (&$statements): PDOStatement {
            $statements[] = $sql;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn([['sum' => '3']]);
            $statement->method('closeCursor')->willReturn(true);

            return $statement;
        });

        $adapter = $create($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setAuthorization(new Authorization());

        $collection = Collection::create(id: 'readings', attributes: [Attribute::double(key: 'reading', required: true)]);

        $this->assertSame(3, $adapter->sum($collection, 'reading', $queries));
        $this->assertCount(1, $statements);
        $this->assertStringStartsWith('SELECT '.$selection.' FROM (', $statements[0]);
    }
}
