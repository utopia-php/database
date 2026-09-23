<?php

namespace Tests\Unit;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;
use Utopia\Query\OrderDirection;

#[AllowMockObjectsWithoutExpectations]
final class JoinVectorCursorTest extends TestCase
{
    #[DataProvider('directions')]
    public function testCursorConditionQuotesJoinQualifiedOrderColumns(CursorDirection $direction): void
    {
        $sql = $this->captureFindSql(
            orderAttributes: ['meta.score', 'meta.$id', '$sequence'],
            cursor: [
                'meta.score' => 10,
                'meta.$id' => 'meta-1',
                '$sequence' => '5',
                '$distance' => 0.25,
            ],
            direction: $direction,
        );

        $where = $this->whereClause($sql);
        $this->assertStringNotContainsString('"table_main"."meta.', $where);
        $this->assertStringNotContainsString('"meta.score"', $where);
        $this->assertStringNotContainsString('"meta._uid"', $where);
        $this->assertSame(3, \substr_count($where, '"meta"."score"'), $sql);
        $this->assertSame(2, \substr_count($where, '"meta"."_uid"'), $sql);
        $this->assertSame(1, \substr_count($where, '"table_main"."_id"'), $sql);
    }

    public function testCursorConditionKeepsMainColumnsOnTheMainAlias(): void
    {
        $sql = $this->captureFindSql(
            orderAttributes: ['title', '$sequence'],
            cursor: [
                'title' => 'alpha',
                '$sequence' => '5',
                '$distance' => 0.25,
            ],
            direction: CursorDirection::After,
        );

        $where = $this->whereClause($sql);
        $this->assertSame(2, \substr_count($where, '"table_main"."title"'), $sql);
        $this->assertSame(1, \substr_count($where, '"table_main"."_id" >'), $sql);
    }

    /**
     * @return iterable<string, array{CursorDirection}>
     */
    public static function directions(): iterable
    {
        yield 'after' => [CursorDirection::After];
        yield 'before' => [CursorDirection::Before];
    }

    /**
     * @param  list<string>  $orderAttributes
     * @param  array<string, mixed>  $cursor
     */
    private function captureFindSql(array $orderAttributes, array $cursor, CursorDirection $direction): string
    {
        $statement = $this->createMock(PDOStatement::class);
        $statement->method('bindValue')->willReturn(true);
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);
        $statement->method('closeCursor')->willReturn(true);

        $sql = '';
        $pdo = $this->createMock(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$sql, $statement): PDOStatement {
            $sql = $query;

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $adapter->find(
            new Document(['$id' => 'collection']),
            [
                Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
                Query::join('meta', '$id', 'mainId', '=', 'meta'),
            ],
            limit: 5,
            orderAttributes: $orderAttributes,
            orderTypes: \array_fill(0, \count($orderAttributes), OrderDirection::Asc),
            cursor: $cursor,
            cursorDirection: $direction,
        );

        $this->assertNotSame('', $sql);

        return $sql;
    }

    private function whereClause(string $sql): string
    {
        $where = \strpos($sql, ' WHERE ');
        $order = \strrpos($sql, ' ORDER BY ');
        $this->assertNotFalse($where, $sql);
        $this->assertNotFalse($order, $sql);

        return \substr($sql, $where, $order - $where);
    }
}
