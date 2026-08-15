<?php

namespace Tests\Unit;

use Exception;
use PDOException;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Document;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\OrderDirection;

#[AllowMockObjectsWithoutExpectations]
final class SQLFindTest extends TestCase
{
    #[DataProvider('paths')]
    public function testFetchFailureIsTranslatedAndWinsOverCloseFailure(bool $fast): void
    {
        $timeout = $this->createTimeoutException();
        $statement = $this->statement();
        $statement->expects($this->once())->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willThrowException($timeout);
        $statement->expects($this->once())
            ->method('closeCursor')
            ->willThrowException(new PDOException('Failed to close cursor'));

        try {
            $this->find($this->adapter($statement), $fast);
        } catch (TimeoutException $exception) {
            $this->assertSame($timeout, $exception->getPrevious());

            return;
        }

        $this->fail('Expected a timeout exception.');
    }

    #[DataProvider('paths')]
    public function testCloseFailureIsTranslated(bool $fast): void
    {
        $close = new PDOException('Lost connection while closing cursor');
        $statement = $this->statement();
        $statement->expects($this->once())->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willReturn([]);
        $statement->expects($this->once())->method('closeCursor')->willThrowException($close);

        try {
            $this->find($this->adapter($statement), $fast);
        } catch (PDOException $exception) {
            $this->assertSame($close, $exception);

            return;
        }

        $this->fail('Expected a close cursor exception.');
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function paths(): iterable
    {
        yield 'fast path' => [true];
        yield 'builder path' => [false];
    }

    public function testEmulatesFullOuterJoinWithOuterLimit(): void
    {
        $sql = $this->captureFindSql(
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 2,
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
    }

    public function testEmulatesFullOuterJoinOrderByIsUnambiguousAfterUnion(): void
    {
        $sql = $this->captureFindSql(
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 2,
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
        $this->assertUnambiguousUnionOrderBy($sql);
    }

    public function testEmulatesFullOuterJoinOrderByMatchesProjectedUnionOutput(): void
    {
        $sql = $this->captureFindSql(
            [
                Query::fullOuterJoin('orders', '$id', 'customerId'),
                Query::select(['name']),
            ],
            limit: 2,
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
        $this->assertUnambiguousUnionOrderBy($sql);
    }

    public function testEmulatesFullOuterJoinOrderByHandlesMultipleAttributes(): void
    {
        $sql = $this->captureFindSql(
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 2,
            orderAttributes: ['name', Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc, OrderDirection::Asc],
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
        $this->assertUnambiguousUnionOrderBy($sql, expectedTerms: 2);
    }

    public function testEmulatesFullOuterJoinStripsOrderAliasesFromDocuments(): void
    {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([
            [
                '_uid' => 'doc1',
                '_id' => 1,
                '_permissions' => '[]',
                '_createdAt' => '2020-01-01 00:00:00.000',
                '_updatedAt' => '2020-01-01 00:00:00.000',
                'name' => 'Alice',
                'foj_ord_0' => 1,
            ],
        ]);
        $statement->method('closeCursor')->willReturn(true);

        $results = $this->adapter($statement)->find(
            new Document(['$id' => 'collection']),
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 1,
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
        );

        $this->assertSame(1, \count($results));
        $this->assertSame('doc1', $results[0]->getId());
        $this->assertSame(false, $results[0]->isSet('foj_ord_0'));
    }

    public function testEmulatesFullOuterJoinRemapsQualifiedUnionColumns(): void
    {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([
            [
                'table_main._uid' => 'doc1',
                'table_main._id' => '1',
                'table_main._permissions' => '[]',
                'table_main._createdAt' => '2020-01-01 00:00:00.000',
                'table_main._updatedAt' => '2020-01-01 00:00:00.000',
                'table_main.name' => 'Alice',
                'foj_ord_0' => 1,
            ],
        ]);
        $statement->method('closeCursor')->willReturn(true);

        $results = $this->adapter($statement)->find(
            new Document(['$id' => 'collection']),
            [
                Query::fullOuterJoin('orders', '$id', 'customerId'),
                Query::select(['name']),
            ],
            limit: 1,
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
        );

        $this->assertSame(1, \count($results));
        $this->assertSame('doc1', $results[0]->getId());
        $this->assertSame('1', $results[0]->getSequence());
        $this->assertSame('Alice', $results[0]->getAttribute('name'));
        $this->assertSame(false, $results[0]->isSet('foj_ord_0'));
        $this->assertSame(false, $results[0]->isSet('table_main._uid'));
    }

    /**
     * @param  array<Query>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     */
    private function captureFindSql(
        array $queries,
        ?int $limit = 25,
        array $orderAttributes = [],
        array $orderTypes = [],
    ): string {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);
        $statement->method('closeCursor')->willReturn(true);

        $sql = '';
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturnCallback(function (string $query) use (&$sql, $statement): \PDOStatement {
                $sql = $query;

                return $statement;
            });

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $adapter->find(
            new Document(['$id' => 'collection']),
            $queries,
            limit: $limit,
            orderAttributes: $orderAttributes,
            orderTypes: $orderTypes,
        );

        $this->assertNotSame('', $sql);

        return $sql;
    }

    private function assertEmulatedFullOuterJoin(string $sql): void
    {
        $this->assertStringContainsString('UNION ALL', $sql);
        $this->assertSame(0, \preg_match_all('/UNION(?! ALL)/i', $sql), $sql);
        $this->assertStringContainsString('LEFT JOIN', $sql);
        $this->assertStringContainsString('RIGHT JOIN', $sql);
        $this->assertStringContainsString('IS NULL', $sql);
        $this->assertStringNotContainsString('FULL OUTER JOIN', $sql);
        $this->assertDoesNotMatchRegularExpression('/FROM\s*\(\s*SELECT\s+\*/i', $sql);
    }

    private function countLimitsAfterUnion(string $sql): int
    {
        $unionPosition = \stripos($sql, 'UNION');
        $this->assertNotFalse($unionPosition);

        $limitMatches = \preg_match_all('/\bLIMIT\s+(?:2|\?)/i', $sql, $matches, PREG_OFFSET_CAPTURE);
        $this->assertNotFalse($limitMatches);
        foreach ($matches[0] as $match) {
            $this->assertGreaterThan($unionPosition, $match[1], $sql);
        }

        return $limitMatches;
    }

    private function assertUnambiguousUnionOrderBy(string $sql, int $expectedTerms = 1): void
    {
        $unionPosition = \stripos($sql, 'UNION');
        $this->assertNotFalse($unionPosition);

        $afterUnion = \substr($sql, $unionPosition);
        $this->assertMatchesRegularExpression('/ORDER BY/i', $afterUnion, $sql);

        $this->assertDoesNotMatchRegularExpression(
            '/ORDER BY\s+`_id`\b/i',
            $afterUnion,
            $sql,
        );

        $aliasMatches = \preg_match_all('/`foj_ord_\d+`/', $afterUnion);
        $positionalMatches = \preg_match_all('/ORDER BY\s+\d+/i', $afterUnion);

        $this->assertTrue(
            $aliasMatches >= $expectedTerms || $positionalMatches === 1,
            $sql,
        );

        if ($aliasMatches >= $expectedTerms) {
            $orderByPosition = \stripos($afterUnion, 'ORDER BY');
            $this->assertNotFalse($orderByPosition);
            $selectSql = \substr($sql, 0, $unionPosition + $orderByPosition);
            for ($index = 0; $index < $expectedTerms; $index++) {
                $this->assertStringContainsString('foj_ord_'.$index, $selectSql, $sql);
                $this->assertStringContainsString('`foj_ord_'.$index.'`', $afterUnion, $sql);
            }
        }

        $this->assertDoesNotMatchRegularExpression(
            '/ORDER BY\s+`table_main`\.`_id`/i',
            $afterUnion,
            $sql,
        );
    }

    private function find(MySQL $adapter, bool $fast): void
    {
        $adapter->find(
            new Document(['$id' => 'collection']),
            $fast ? [] : [Query::equal('title', ['value'])],
            orderAttributes: ['$sequence'],
            orderTypes: [OrderDirection::Asc],
        );
    }

    private function adapter(\PDOStatement $statement): MySQL
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())->method('prepare')->willReturn($statement);

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        return $adapter;
    }

    private function statement(): \PDOStatement&MockObject
    {
        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->method('bindValue')->willReturn(true);

        return $statement;
    }

    private function createTimeoutException(): PDOException
    {
        $exception = new PDOException('Query execution was interrupted');
        $code = new ReflectionProperty(Exception::class, 'code');
        $code->setValue($exception, 'HY000');
        $exception->errorInfo = ['HY000', 3024, 'Query execution was interrupted'];

        return $exception;
    }
}
