<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;
use Utopia\Query\OrderDirection;

final class PostgresVectorCursorTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /** @var list<mixed> */
    private array $bound = [];

    public function testAVectorCursorWithoutItsDistanceIsRefused(): void
    {
        $this->assertRefused('Vector cursor is missing its distance', ['rank'], ['rank' => 3]);
    }

    public function testAVectorCursorWithoutAnOrderAttributeIsRefused(): void
    {
        $this->assertRefused('Vector cursor requires a unique order attribute', [], ['$distance' => 0.25]);
    }

    public function testAVectorCursorMissingAnOrderValueIsRefused(): void
    {
        $this->assertRefused("Vector cursor is missing order attribute 'rank'", ['rank', '$sequence'], ['$sequence' => '5', '$distance' => 0.25]);
    }

    public function testAVectorCursorMissingALaterOrderValueIsRefused(): void
    {
        $this->assertRefused("Vector cursor is missing order attribute '\$sequence'", ['rank', '$sequence'], ['rank' => 3, '$distance' => 0.25]);
    }

    /**
     * @return iterable<string, array{OrderDirection, CursorDirection, string, string}>
     */
    public static function pages(): iterable
    {
        yield 'descending after' => [OrderDirection::Desc, CursorDirection::After, '>', '<'];
        yield 'descending before' => [OrderDirection::Desc, CursorDirection::Before, '<', '>'];
        yield 'ascending after' => [OrderDirection::Asc, CursorDirection::After, '>', '>'];
        yield 'ascending before' => [OrderDirection::Asc, CursorDirection::Before, '<', '<'];
    }

    #[DataProvider('pages')]
    public function testAVectorCursorComparesTheDistanceThenTheOrderInThePageDirection(OrderDirection $order, CursorDirection $direction, string $distanceOperator, string $rankOperator): void
    {
        $this->find(['rank'], [$order], ['rank' => 3, '$distance' => 0.25], $direction);

        $this->assertCount(1, $this->statements);
        $where = $this->statements[0];
        $this->assertStringContainsString(') ' . $distanceOperator . ' ?', $where);
        $this->assertStringContainsString('"table_main"."rank" ' . $rankOperator . ' ?', $where);
        $this->assertContains('0.25', $this->bound);
        $this->assertContains(3, $this->bound);
    }

    /**
     * @param list<string> $orderAttributes
     * @param array<string, mixed> $cursor
     */
    private function assertRefused(string $message, array $orderAttributes, array $cursor): void
    {
        try {
            $this->find($orderAttributes, \array_fill(0, \count($orderAttributes), OrderDirection::Asc), $cursor, CursorDirection::After);
            $this->fail('The vector cursor must be refused');
        } catch (QueryException $error) {
            $this->assertSame($message, $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    /**
     * @param list<string> $orderAttributes
     * @param list<OrderDirection> $orderTypes
     * @param array<string, mixed> $cursor
     */
    private function find(array $orderAttributes, array $orderTypes, array $cursor, CursorDirection $direction): void
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('closeCursor')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bound[] = $value;

                return true;
            });

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $adapter->find(
            new Document(['$id' => 'items']),
            [Query::vectorCosine('embedding', [1.0, 0.0, 0.0])],
            limit: 2,
            orderAttributes: $orderAttributes,
            orderTypes: $orderTypes,
            cursor: $cursor,
            cursorDirection: $direction,
        );
    }
}
