<?php

namespace Tests\Unit;

use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Exception\Transaction as TransactionException;

final class SQLTransactionTest extends TestCase
{
    /**
     * A deadlock (SQLSTATE 40001) inside withTransaction should be retried
     * more aggressively than a generic exception (which gets only 2 retries).
     */
    public function testWithTransactionRetriesDeadlockMoreThanGenericError(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdo->method('inTransaction')->willReturn(true);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('rollBack')->willReturn(true);

        $adapter = new MySQL($pdo);

        // First, verify a generic RuntimeException gets only 3 attempts (initial + 2 retries)
        $genericAttempts = 0;
        try {
            $adapter->withTransaction(function () use (&$genericAttempts) {
                $genericAttempts++;
                throw new \RuntimeException('Generic failure');
            });
        } catch (\RuntimeException) {
        }
        $this->assertEquals(3, $genericAttempts, 'Generic exception should get 3 attempts (initial + 2 retries)');

        // Now verify a deadlock PDOException gets more attempts
        $deadlockAttempts = 0;
        try {
            $adapter->withTransaction(function () use (&$deadlockAttempts) {
                $deadlockAttempts++;
                throw new PDOException(
                    'SQLSTATE[40001]: Serialization failure: 1213 Deadlock found when trying to get lock; try restarting transaction',
                    '40001'
                );
            });
        } catch (PDOException) {
        }
        $this->assertGreaterThan(
            $genericAttempts,
            $deadlockAttempts,
            'Deadlock should get more retry attempts than a generic exception'
        );
        $this->assertEquals(6, $deadlockAttempts, 'Deadlock should get 6 attempts (initial + 5 retries)');
    }

    /**
     * When a deadlock resolves on a retry, the transaction should succeed.
     */
    public function testWithTransactionSucceedsAfterTransientDeadlock(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdo->method('inTransaction')->willReturn(true);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('rollBack')->willReturn(true);
        $pdo->method('commit')->willReturn(true);

        $adapter = new MySQL($pdo);

        $attempts = 0;
        $result = $adapter->withTransaction(function () use (&$attempts) {
            $attempts++;
            if ($attempts < 3) {
                throw new PDOException(
                    'SQLSTATE[40001]: Serialization failure: 1213 Deadlock found when trying to get lock; try restarting transaction',
                    '40001'
                );
            }
            return 'success';
        });

        $this->assertEquals('success', $result);
        $this->assertEquals(3, $attempts, 'Should succeed on the 3rd attempt after 2 deadlocks');
    }

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
}
