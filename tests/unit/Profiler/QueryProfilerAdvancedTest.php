<?php

namespace Tests\Unit\Profiler;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Profiler\QueryProfiler;

class QueryProfilerAdvancedTest extends TestCase
{
    private QueryProfiler $profiler;

    protected function setUp(): void
    {
        $this->profiler = new QueryProfiler();
    }

    public function testBacktraceCaptureWhenEnabled(): void
    {
        $this->profiler->enable();
        $this->profiler->enableBacktrace(true);
        $this->profiler->log('SELECT 1', [], 1.0);

        $logs = $this->profiler->getLogs();
        $this->assertCount(1, $logs);
        $this->assertNotNull($logs[0]->backtrace);
        $this->assertIsArray($logs[0]->backtrace);
        $this->assertNotEmpty($logs[0]->backtrace);
    }

    public function testBacktraceIsNullWhenDisabled(): void
    {
        $this->profiler->enable();
        $this->profiler->log('SELECT 1', [], 1.0);

        $logs = $this->profiler->getLogs();
        $this->assertNull($logs[0]->backtrace);
    }

    public function testEnableBacktraceToggle(): void
    {
        $this->profiler->enable();

        $this->profiler->enableBacktrace(true);
        $this->profiler->log('Q1', [], 1.0);
        $this->assertNotNull($this->profiler->getLogs()[0]->backtrace);

        $this->profiler->enableBacktrace(false);
        $this->profiler->log('Q2', [], 1.0);
        $this->assertNull($this->profiler->getLogs()[1]->backtrace);
    }

    public function testMultipleSlowQueryCallbacks(): void
    {
        $this->profiler->enable();
        $this->profiler->setSlowThreshold(10.0);

        $received = null;
        $this->profiler->onSlowQuery(function ($entry) use (&$received) {
            $received = $entry;
        });

        $this->profiler->log('fast', [], 5.0);
        $this->assertNull($received);

        $this->profiler->log('slow', [], 20.0);
        $this->assertNotNull($received);
        $this->assertEquals('slow', $received->query);
    }

    public function testDetectNPlusOneWithVariedQueryPatterns(): void
    {
        $this->profiler->enable();

        for ($i = 0; $i < 10; $i++) {
            $this->profiler->log("SELECT * FROM users WHERE id = {$i}", [], 1.0);
        }

        for ($i = 0; $i < 3; $i++) {
            $this->profiler->log("SELECT * FROM posts WHERE id = {$i}", [], 1.0);
        }

        $violations = $this->profiler->detectNPlusOne(5);
        $this->assertNotEmpty($violations);

        $hasUsersPattern = false;
        foreach ($violations as $pattern => $count) {
            if ($count >= 10) {
                $hasUsersPattern = true;
            }
        }
        $this->assertTrue($hasUsersPattern);
    }

    public function testDetectNPlusOneBelowThresholdReturnsEmpty(): void
    {
        $this->profiler->enable();

        $this->profiler->log('SELECT * FROM users WHERE id = 1', [], 1.0);
        $this->profiler->log('SELECT * FROM users WHERE id = 2', [], 1.0);

        $violations = $this->profiler->detectNPlusOne(5);
        $this->assertEmpty($violations);
    }

    public function testGetTotalTimeWithNoLogsReturnsZero(): void
    {
        $this->assertEquals(0.0, $this->profiler->getTotalTime());
    }

    public function testGetSlowQueriesReturnsEmptyWhenNoneExceedThreshold(): void
    {
        $this->profiler->enable();
        $this->profiler->setSlowThreshold(50.0);

        $this->profiler->log('fast1', [], 10.0);
        $this->profiler->log('fast2', [], 20.0);

        $slow = $this->profiler->getSlowQueries();
        $this->assertEmpty($slow);
    }

    public function testLogWithAllParameters(): void
    {
        $this->profiler->enable();
        $this->profiler->log('SELECT * FROM orders', ['active'], 15.5, 'orders', 'find');

        $logs = $this->profiler->getLogs();
        $this->assertCount(1, $logs);
        $this->assertEquals('SELECT * FROM orders', $logs[0]->query);
        $this->assertEquals(['active'], $logs[0]->bindings);
        $this->assertEquals(15.5, $logs[0]->durationMs);
        $this->assertEquals('orders', $logs[0]->collection);
        $this->assertEquals('find', $logs[0]->operation);
    }

    public function testResetClearsEverything(): void
    {
        $this->profiler->enable();
        $this->profiler->log('Q1', [], 10.0);
        $this->profiler->log('Q2', [], 20.0);

        $this->profiler->reset();

        $this->assertCount(0, $this->profiler->getLogs());
        $this->assertEquals(0, $this->profiler->getQueryCount());
        $this->assertEquals(0.0, $this->profiler->getTotalTime());
        $this->assertEmpty($this->profiler->getSlowQueries());
    }

    public function testSlowQueryCallbackReceivesQueryLogEntry(): void
    {
        $this->profiler->enable();
        $this->profiler->setSlowThreshold(10.0);

        $received = null;
        $this->profiler->onSlowQuery(function ($entry) use (&$received) {
            $received = $entry;
        });

        $this->profiler->log('SELECT slow', ['param'], 50.0, 'users', 'find');

        $this->assertNotNull($received);
        $this->assertEquals('SELECT slow', $received->query);
        $this->assertEquals(50.0, $received->durationMs);
        $this->assertEquals('users', $received->collection);
    }

    public function testDetectNPlusOneNormalizesQueryParameters(): void
    {
        $this->profiler->enable();

        for ($i = 0; $i < 6; $i++) {
            $this->profiler->log("SELECT * FROM users WHERE name = 'user_{$i}'", [], 1.0);
        }

        $violations = $this->profiler->detectNPlusOne(5);
        $this->assertNotEmpty($violations);
    }

    public function testGetSlowQueriesAtExactThreshold(): void
    {
        $this->profiler->enable();
        $this->profiler->setSlowThreshold(50.0);

        $this->profiler->log('exact', [], 50.0);

        $slow = $this->profiler->getSlowQueries();
        $this->assertCount(1, $slow);
    }

    public function testEnabledProfilerLogsTotalTimeCorrectly(): void
    {
        $this->profiler->enable();

        $this->profiler->log('Q1', [], 1.5);
        $this->profiler->log('Q2', [], 2.5);
        $this->profiler->log('Q3', [], 3.0);

        $this->assertEquals(7.0, $this->profiler->getTotalTime());
    }
}
