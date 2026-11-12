<?php

namespace Tests\Unit\Builder;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Builder\MariaDB;
use Utopia\Database\Builder\MySQL;
use Utopia\Database\Builder\Postgres;
use Utopia\Database\Query;
use Utopia\Query\Builder;

final class FloatOverflowTest extends TestCase
{
    /**
     * @return iterable<string, array{Builder}>
     */
    public static function builders(): iterable
    {
        yield 'mariadb' => [new MariaDB()];
        yield 'mysql' => [new MySQL()];
    }

    /**
     * @return iterable<string, array{Query, string}>
     */
    public static function aggregates(): iterable
    {
        yield 'sum' => [Query::sum('reading', 'total'), 'SUM(`reading`) + 0 AS `total`'];
        yield 'avg' => [Query::avg('reading', 'total'), 'AVG(`reading`) + 0 AS `total`'];
        yield 'variance' => [Query::variance('reading', 'total'), 'VARIANCE(`reading`) + 0 AS `total`'];
        yield 'varPop' => [Query::varPop('reading', 'total'), 'VAR_POP(`reading`) + 0 AS `total`'];
        yield 'varSamp' => [Query::varSamp('reading', 'total'), 'VAR_SAMP(`reading`) + 0 AS `total`'];
        yield 'stddev' => [Query::stddev('reading', 'total'), 'SQRT(VARIANCE(`reading`) + 0) AS `total`'];
        yield 'stddevPop' => [Query::stddevPop('reading', 'total'), 'SQRT(VAR_POP(`reading`) + 0) AS `total`'];
        yield 'stddevSamp' => [Query::stddevSamp('reading', 'total'), 'SQRT(VAR_SAMP(`reading`) + 0) AS `total`'];
        yield 'unaliased sum keeps the name the engine gives it' => [Query::sum('reading'), 'SUM(`reading`) + 0 AS `SUM(``reading``)`'];
        yield 'count is an integer' => [Query::count('*', 'total'), 'COUNT(*) AS `total`'];
        yield 'min cannot overflow' => [Query::min('reading', 'total'), 'MIN(`reading`) AS `total`'];
        yield 'max cannot overflow' => [Query::max('reading', 'total'), 'MAX(`reading`) AS `total`'];
        yield 'bitAnd is signed' => [Query::bitAnd('flags', 'bits'), 'CAST(BIT_AND(`flags`) AS SIGNED) AS `bits`'];
        yield 'bitOr is signed' => [Query::bitOr('flags', 'bits'), 'CAST(BIT_OR(`flags`) AS SIGNED) AS `bits`'];
        yield 'bitXor is signed' => [Query::bitXor('flags', 'bits'), 'CAST(BIT_XOR(`flags`) AS SIGNED) AS `bits`'];
        yield 'unaliased bitOr keeps the name the engine gives it' => [Query::bitOr('flags'), 'CAST(BIT_OR(`flags`) AS SIGNED) AS `BIT_OR(``flags``)`'];
    }

    /**
     * @return iterable<string, array{Builder, Query, string}>
     */
    public static function compilations(): iterable
    {
        foreach (self::builders() as $engine => [$builder]) {
            foreach (self::aggregates() as $name => [$query, $sql]) {
                yield "{$engine} {$name}" => [$builder, $query, $sql];
            }
        }
    }

    #[DataProvider('compilations')]
    public function testAnAggregateIsCompiledAsEveryEngineAnswersIt(Builder $builder, Query $query, string $sql): void
    {
        $this->assertSame($sql, $builder->compileAggregate($query));
    }

    public function testPostgresRaisesAnOverflowItselfAndSignsBitwiseAggregatesSoItsAggregatesAreLeftAsTheyAre(): void
    {
        $this->assertSame('BIT_OR("flags") AS "bits"', (new Postgres())->compileAggregate(Query::bitOr('flags', 'bits')));
        $this->assertSame('SUM("reading") AS "total"', (new Postgres())->compileAggregate(Query::sum('reading', 'total')));
        $this->assertSame('STDDEV_POP("reading") AS "total"', (new Postgres())->compileAggregate(Query::stddevPop('reading', 'total')));
    }
}
