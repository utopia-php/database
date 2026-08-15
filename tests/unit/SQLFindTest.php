<?php

namespace Tests\Unit;

use Exception;
use PDOException;
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
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 2,
        );

        $this->assertNotSame('', $sql);
        $this->assertStringContainsString('UNION', $sql);
        $this->assertStringContainsString('LEFT JOIN', $sql);
        $this->assertStringContainsString('RIGHT JOIN', $sql);
        $this->assertStringContainsString('IS NULL', $sql);
        $this->assertStringNotContainsString('FULL OUTER JOIN', $sql);

        $unionPosition = stripos($sql, 'UNION');
        $this->assertNotFalse($unionPosition);

        $limitMatches = preg_match_all('/\bLIMIT\s+(?:2|\?)\b/i', $sql, $matches, PREG_OFFSET_CAPTURE);
        $this->assertSame(1, $limitMatches, $sql);
        $this->assertGreaterThan($unionPosition, $matches[0][0][1], $sql);
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
