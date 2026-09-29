<?php

namespace Tests\Unit\Joins;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Document;
use Utopia\Database\Query;
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
}
