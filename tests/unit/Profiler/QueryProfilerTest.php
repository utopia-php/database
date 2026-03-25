<?php

namespace Tests\Unit\Profiler;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Profiler\QueryProfiler;

class QueryProfilerTest extends TestCase
{
    private QueryProfiler $profiler;

    protected function setUp(): void
    {
        $this->profiler = new QueryProfiler();
    }

    public function testDisabledByDefault(): void
    {
        $this->assertFalse($this->profiler->isEnabled());
    }

    public function testEnableDisable(): void
    {
        $this->profiler->enable();
        $this->assertTrue($this->profiler->isEnabled());

        $this->profiler->disable();
        $this->assertFalse($this->profiler->isEnabled());
    }

    public function testLogWhenDisabled(): void
    {
        $this->profiler->log('SELECT 1', [], 1.0);
        $this->assertCount(0, $this->profiler->getLogs());
    }

    public function testLogWhenEnabled(): void
    {
        $this->profiler->enable();
        $this->profiler->log('SELECT * FROM users', [], 5.5, 'users', 'find');
        $this->profiler->log('SELECT * FROM posts', [], 3.2, 'posts', 'find');

        $logs = $this->profiler->getLogs();
        $this->assertCount(2, $logs);
        $this->assertEquals('SELECT * FROM users', $logs[0]->query);
        $this->assertEquals(5.5, $logs[0]->durationMs);
        $this->assertEquals('users', $logs[0]->collection);
    }

    public function testQueryCount(): void
    {
        $this->profiler->enable();
        $this->profiler->log('Q1', [], 1.0);
        $this->profiler->log('Q2', [], 2.0);
        $this->profiler->log('Q3', [], 3.0);

        $this->assertEquals(3, $this->profiler->getQueryCount());
    }

    public function testTotalTime(): void
    {
        $this->profiler->enable();
        $this->profiler->log('Q1', [], 10.0);
        $this->profiler->log('Q2', [], 20.0);

        $this->assertEquals(30.0, $this->profiler->getTotalTime());
    }

    public function testSlowQueryDetection(): void
    {
        $this->profiler->enable();
        $this->profiler->setSlowThreshold(50.0);

        $this->profiler->log('fast', [], 10.0);
        $this->profiler->log('slow', [], 100.0);
        $this->profiler->log('medium', [], 49.0);

        $slow = $this->profiler->getSlowQueries();
        $this->assertCount(1, $slow);
        $slowEntry = \array_values($slow)[0];
        $this->assertEquals('slow', $slowEntry->query);
    }

    public function testSlowQueryCallback(): void
    {
        $this->profiler->enable();
        $this->profiler->setSlowThreshold(50.0);

        $called = false;
        $this->profiler->onSlowQuery(function () use (&$called) {
            $called = true;
        });

        $this->profiler->log('fast', [], 10.0);
        $this->assertFalse($called);

        $this->profiler->log('slow', [], 100.0);
        $this->assertTrue($called);
    }

    public function testNPlusOneDetection(): void
    {
        $this->profiler->enable();

        for ($i = 0; $i < 10; $i++) {
            $this->profiler->log('SELECT * FROM users WHERE id = ?', [$i], 1.0);
        }

        $violations = $this->profiler->detectNPlusOne(5);
        $this->assertNotEmpty($violations);
    }

    public function testReset(): void
    {
        $this->profiler->enable();
        $this->profiler->log('Q1', [], 1.0);
        $this->profiler->reset();

        $this->assertCount(0, $this->profiler->getLogs());
        $this->assertEquals(0, $this->profiler->getQueryCount());
    }
}
