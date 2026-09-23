<?php

namespace Tests\Unit;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * An aggregate over no input values is null, except a count, which is zero, on every engine.
 *
 * The statement the adapter builds is answered the way MySQL and MariaDB answer it: BIT_AND over
 * no input values has every bit set and BIT_OR / BIT_XOR are zero, where PostgreSQL answers null.
 */
final class EmptySetAggregateContractTest extends TestCase
{
    private const string EVERY_BIT = '18446744073709551615';

    /**
     * @return iterable<string, array{class-string<SQL>}>
     */
    public static function adapters(): iterable
    {
        yield 'mysql' => [MySQL::class];
        yield 'mariadb' => [MariaDB::class];
        yield 'postgres' => [Postgres::class];
    }

    /**
     * @param  class-string<SQL>  $adapter
     */
    #[DataProvider('adapters')]
    public function testBitwiseAggregatesOverNoInputValuesAreNull(string $adapter): void
    {
        $rows = $this->find($adapter, inputs: 0, queries: [
            Query::count('*', 'rows'),
            Query::bitAnd('flags', 'all_bits'),
            Query::bitOr('flags', 'any_bits'),
            Query::bitXor('flags', 'odd_bits'),
            Query::sum('flags', 'total'),
        ]);

        $this->assertSame([[
            'rows' => '0',
            'all_bits' => null,
            'any_bits' => null,
            'odd_bits' => null,
            'total' => null,
        ]], $rows);
    }

    /**
     * @param  class-string<SQL>  $adapter
     */
    #[DataProvider('adapters')]
    public function testBitwiseAggregatesOverInputValuesKeepTheirResult(string $adapter): void
    {
        $rows = $this->find($adapter, inputs: 2, queries: [
            Query::bitAnd('flags', 'all_bits'),
            Query::bitOr('flags', 'any_bits'),
            Query::bitXor('flags', 'odd_bits'),
            Query::groupBy(['kind']),
        ]);

        $this->assertSame([[
            'all_bits' => self::EVERY_BIT,
            'any_bits' => '0',
            'odd_bits' => '0',
        ]], $rows);
    }

    /**
     * MySQL and MariaDB aggregate an emulated full outer join once, over the union of its halves, and
     * that outer statement answers the same way; PostgreSQL joins natively.
     *
     * @param  class-string<SQL>  $adapter
     */
    #[DataProvider('adapters')]
    public function testBitwiseAggregatesOverAFullOuterJoinWithNoInputValuesAreNull(string $adapter): void
    {
        $rows = $this->find($adapter, inputs: 0, queries: [
            Query::fullOuterJoin('other', '$id', 'collectionId', '=', 'joined'),
            Query::count('*', 'rows'),
            Query::bitAnd('joined.flags', 'all_bits'),
            Query::bitOr('joined.flags', 'any_bits'),
            Query::bitXor('joined.flags', 'odd_bits'),
            Query::sum('joined.flags', 'total'),
        ]);

        $this->assertSame([[
            'rows' => '0',
            'all_bits' => null,
            'any_bits' => null,
            'odd_bits' => null,
            'total' => null,
        ]], $rows);
    }

    /**
     * Run a find whose statement is answered as if `$inputs` rows fed every aggregate.
     *
     * @param  class-string<SQL>  $adapter
     * @param  list<Query>  $queries
     * @return list<array<string, mixed>>
     */
    private function find(string $adapter, int $inputs, array $queries): array
    {
        $sql = '';
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $statement->method('closeCursor')->willReturn(true);
        $statement->method('fetchAll')->willReturnCallback(function () use (&$sql, $inputs): array {
            return [$this->answer($sql, $inputs)];
        });

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$sql, $statement): PDOStatement {
            $sql = $query;

            return $statement;
        });

        $instance = new $adapter($pdo);
        $instance->setDatabase('database');
        $instance->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $instance->setAuthorization($authorization);

        $rows = [];
        foreach ($instance->find(new Document(['$id' => 'collection']), $queries, limit: 25) as $document) {
            $rows[] = $document->getArrayCopy();
        }

        return $rows;
    }

    /**
     * The row MySQL returns for each aggregate of the statement, with the bitwise aggregates
     * answering a neutral value when there are no input values.
     *
     * @return array<string, mixed>
     */
    private function answer(string $sql, int $inputs): array
    {
        \preg_match_all('/([A-Z_]+)\((?:DISTINCT )?[^()]*\) AS [`"]([^`"]+)[`"]/', $sql, $matches, PREG_SET_ORDER);
        $this->assertNotSame([], $matches, 'no aggregate in: '.$sql);

        $row = [];
        foreach ($matches as [, $function, $alias]) {
            $row[$alias] = match ($function) {
                'COUNT' => (string) $inputs,
                'BIT_AND' => self::EVERY_BIT,
                'BIT_OR', 'BIT_XOR' => '0',
                default => $inputs === 0 ? null : '1',
            };
        }

        return $row;
    }
}
