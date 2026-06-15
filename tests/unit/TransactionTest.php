<?php

namespace Tests\Unit;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Exception\Transaction as TransactionException;

class TransactionTest extends TestCase
{
    /**
     * Regression: when PDO::inTransaction() returns true but the server-side
     * transaction is already gone (stale persistent connection, implicit
     * commit, etc.), the defensive rollBack() in startTransaction() must not
     * propagate "There is no active transaction" — it should silently recover
     * and begin a fresh transaction.
     */
    public function testStartTransactionHandlesStaleInTransactionFlag(): void
    {
        $pdoMock = $this->createMock(PDO::class);

        // PDO thinks a transaction is active, but rollBack() disagrees
        $pdoMock->method('inTransaction')
            ->willReturn(true);

        $pdoMock->method('rollBack')
            ->willThrowException(new PDOException('There is no active transaction'));

        $pdoMock->expects($this->once())
            ->method('beginTransaction')
            ->willReturn(true);

        $adapter = new MariaDB($pdoMock);

        $result = $adapter->startTransaction();

        $this->assertTrue($result);
    }

    /**
     * Regression: when rollbackTransaction() fails during a withTransaction()
     * retry, the inTransaction counter must be reset to 0 before the next
     * attempt so startTransaction() takes the fresh-transaction path.
     */
    public function testWithTransactionResetsCounterOnRollbackFailureDuringRetry(): void
    {
        $pdoMock = $this->createMock(PDO::class);
        $stmtMock = $this->createMock(PDOStatement::class);
        $stmtMock->method('execute')->willReturn(true);

        $callCount = 0;

        // First call: beginTransaction succeeds, then callback fails,
        // then rollBack also fails (simulating connection issue).
        // Second call: fresh beginTransaction succeeds, callback succeeds.
        $pdoMock->method('inTransaction')
            ->willReturnCallback(function () use (&$callCount) {
                // After the first failed rollback, PDO no longer thinks
                // it's in a transaction.
                return false;
            });

        $pdoMock->method('prepare')
            ->willReturn($stmtMock);

        $pdoMock->method('beginTransaction')
            ->willReturn(true);

        $pdoMock->method('commit')
            ->willReturn(true);

        // rollBack fails the first time (simulating stale transaction)
        $pdoMock->method('rollBack')
            ->willThrowException(new PDOException('There is no active transaction'));

        $adapter = new MariaDB($pdoMock);

        $result = $adapter->withTransaction(function () use (&$callCount) {
            $callCount++;
            if ($callCount === 1) {
                throw new \RuntimeException('Transient error');
            }
            return 'success';
        });

        $this->assertSame('success', $result);
        $this->assertSame(2, $callCount);
    }
}
