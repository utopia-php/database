<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
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
        $thrown = null;

        try {
            $this->adapter->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new TimeoutException('Query timed out');
            });
        } catch (TimeoutException $e) {
            $thrown = $e;
        }

        $this->assertInstanceOf(TimeoutException::class, $thrown);
        $this->assertSame(1, $attempts);
    }

    /**
     * A duplicate is deterministic, so it also aborts on the first attempt.
     * Anchors the timeout case against an existing no-retry exception.
     */
    public function testDuplicateIsNotRetried(): void
    {
        $attempts = 0;
        $thrown = null;

        try {
            $this->adapter->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new DuplicateException('Duplicate');
            });
        } catch (DuplicateException $e) {
            $thrown = $e;
        }

        $this->assertInstanceOf(DuplicateException::class, $thrown);
        $this->assertSame(1, $attempts);
    }

    /**
     * A transient/unknown failure is still retried across the full attempt
     * budget (3 attempts: initial + 2 retries) before the error propagates.
     */
    public function testGenericFailureIsRetried(): void
    {
        $attempts = 0;
        $thrown = null;

        try {
            $this->adapter->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new \RuntimeException('transient');
            });
        } catch (\RuntimeException $e) {
            $thrown = $e;
        }

        $this->assertInstanceOf(\RuntimeException::class, $thrown);
        $this->assertSame(3, $attempts);
    }
}
