<?php

namespace Tests\Unit;

use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Exception\Transaction as TransactionException;

final class SQLTransactionTest extends TestCase
{
    /**
     * A pooled connection (e.g. Swoole PDOProxy) can keep its own transaction
     * counter that survives a reconnect, so it reports an open transaction the
     * underlying connection no longer holds. The cleanup rollBack() then throws
     * "There is no active transaction". startTransaction() must swallow that and
     * still begin a fresh transaction instead of surfacing it as a failure.
     */
    public function testStartTransactionRecoversFromDesyncedRollback(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdo->method('inTransaction')->willReturn(true);
        $pdo->method('rollBack')->willThrowException(
            new PDOException('There is no active transaction')
        );
        $pdo->expects($this->once())
            ->method('beginTransaction')
            ->willReturn(true);

        $adapter = new MySQL($pdo);

        $this->assertTrue($adapter->startTransaction());
        $this->assertTrue($adapter->inTransaction());
    }

    public function testStartTransactionDoesNotMaskBeginFailureAfterDesyncedRollback(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdo->method('inTransaction')->willReturn(true);
        $pdo->method('rollBack')->willThrowException(
            new PDOException('There is no active transaction')
        );
        $pdo->expects($this->once())
            ->method('beginTransaction')
            ->willThrowException(new PDOException('Connection lost'));

        $adapter = new MySQL($pdo);

        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('Failed to start transaction: Connection lost');

        $adapter->startTransaction();
    }

    public function testPostgresStartTransactionRecoversFromDesyncedRollback(): void
    {
        $method = new ReflectionMethod(Postgres::class, 'startTransaction');
        $this->assertSame(SQL::class, $method->getDeclaringClass()->getName());

        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdo->method('inTransaction')->willReturn(true);
        $pdo->method('rollBack')->willThrowException(
            new PDOException('There is no active transaction')
        );
        $pdo->expects($this->once())
            ->method('beginTransaction')
            ->willReturn(true);

        $adapter = new Postgres($pdo);

        $this->assertTrue($adapter->startTransaction());
        $this->assertTrue($adapter->inTransaction());
    }

    public function testPostgresStartTransactionPreservesFalseResultFailure(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdo->method('inTransaction')->willReturn(true);
        $pdo->expects($this->once())
            ->method('rollBack')
            ->willReturn(true);
        $pdo->expects($this->once())
            ->method('beginTransaction')
            ->willReturn(false);

        $adapter = new Postgres($pdo);

        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('Failed to start transaction');

        $adapter->startTransaction();
    }

    public function testPostgresRollbackPreservesFalseResultFailure(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('rollBack')
            ->willReturn(false);

        $adapter = new Postgres($pdo);
        $inTransaction = new ReflectionProperty(SQL::class, 'inTransaction');
        $inTransaction->setValue($adapter, 1);

        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('Failed to rollback transaction');

        $adapter->rollbackTransaction();
    }
}
