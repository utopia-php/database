<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Timeout as TimeoutException;

/**
 * Covers the retry policy of Adapter::withTransaction(): which exceptions abort
 * immediately versus which are retried up to the built-in attempt budget.
 */
class TransactionRetryTest extends TestCase
{
    private DatabaseMemory $adapter;

    protected function setUp(): void
    {
        $this->adapter = new DatabaseMemory();
    }

    /**
     * A statement timeout already spent the full timeout budget on this attempt;
     * retrying re-runs it for another full budget and amplifies lock convoys.
     * It must abort after a single attempt.
     */
    public function testTimeoutIsNotRetried(): void
    {
        $attempts = 0;

        try {
            $this->adapter->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new TimeoutException('Query timed out');
            });
        } catch (TimeoutException) {
            $this->assertSame(1, $attempts);
        }
    }

    /**
     * A duplicate is deterministic, so it also aborts on the first attempt.
     * Anchors the timeout case against an existing no-retry exception.
     */
    public function testDuplicateIsNotRetried(): void
    {
        $attempts = 0;

        try {
            $this->adapter->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new DuplicateException('Duplicate');
            });
        } catch (DuplicateException) {
            $this->assertSame(1, $attempts);
        }
    }

    /**
     * A transient/unknown failure is still retried across the full attempt
     * budget (3 attempts: initial + 2 retries) before the error propagates.
     */
    public function testGenericFailureIsRetried(): void
    {
        $attempts = 0;

        try {
            $this->adapter->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new \RuntimeException('transient');
            });
        } catch (\RuntimeException) {
            $this->assertSame(3, $attempts);
        }
    }

    /**
     * Rollback cleanup can itself fail (adapters throw a DatabaseException from
     * rollbackTransaction()). A non-retriable action must still abort after a
     * single attempt and propagate the original action, not be retried or
     * masked by the rollback error.
     */
    public function testNonRetriableActionAbortsWhenRollbackFails(): void
    {
        $adapter = new class () extends DatabaseMemory {
            public function rollbackTransaction(): bool
            {
                throw new \RuntimeException('rollback failed');
            }
        };

        $attempts = 0;
        $thrown = null;

        try {
            $adapter->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new TimeoutException('Query timed out');
            });
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        $this->assertInstanceOf(TimeoutException::class, $thrown);
        $this->assertSame(1, $attempts);
    }

    public function testRedisRollbackFailureEndsTheTransaction(): void
    {
        if (!\extension_loaded('redis')) {
            $this->markTestSkipped('redis extension not loaded');
        }

        $adapter = new class (new \Redis()) extends RedisAdapter {
            public bool $failReplay = true;

            protected function rollbackJournal(): void
            {
                if ($this->failReplay) {
                    $this->failReplay = false;

                    throw new \RuntimeException('rollback replay failed');
                }

                parent::rollbackJournal();
            }
        };

        $adapter->startTransaction();
        $adapter->startTransaction();

        $thrown = null;
        try {
            $adapter->rollbackTransaction();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        $this->assertInstanceOf(\RuntimeException::class, $thrown);
        $this->assertFalse($adapter->inTransaction());
        $this->assertFalse($adapter->commitTransaction(), 'No transaction is left open to commit');
        $this->assertFalse($adapter->rollbackTransaction(), 'No transaction is left open to roll back');

        $this->assertTrue($adapter->startTransaction());
        $this->assertTrue($adapter->startTransaction());
        $this->assertTrue($adapter->commitTransaction());
        $this->assertTrue($adapter->rollbackTransaction());
        $this->assertFalse($adapter->inTransaction(), 'A later nested transaction must unwind to no transaction');
    }
}
