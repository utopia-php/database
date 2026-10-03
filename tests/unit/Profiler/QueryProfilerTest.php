<?php

namespace Tests\Unit\Profiler;

use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Profiler\QueryLog;
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

        $called = new \stdClass();
        $called->value = false;
        $this->profiler->onSlowQuery(function () use ($called) {
            $called->value = true;
        });

        $this->profiler->log('fast', [], 10.0);
        $this->assertFalse($called->value);

        $this->profiler->log('slow', [], 100.0);
        $this->assertTrue($called->value);
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

    public function testLogsKeepOnlyTheNewestEntriesWithinTheCapacity(): void
    {
        $this->profiler->enable();
        $this->profiler->setCapacity(3);

        foreach (['Q1', 'Q2', 'Q3', 'Q4', 'Q5'] as $query) {
            $this->profiler->log($query, [], 1.0);
        }

        $this->assertSame(['Q3', 'Q4', 'Q5'], $this->loggedQueries());
        $this->assertSame(3, $this->profiler->getCapacity());
    }

    public function testDefaultCapacityBoundsTheLogs(): void
    {
        $this->profiler->enable();

        for ($index = 0; $index <= QueryProfiler::DEFAULT_CAPACITY; $index++) {
            $this->profiler->log("Q{$index}", [], 1.0);
        }

        $logs = $this->profiler->getLogs();
        $this->assertCount(QueryProfiler::DEFAULT_CAPACITY, $logs);
        $this->assertSame('Q1', $logs[0]->query);
        $this->assertSame('Q'.QueryProfiler::DEFAULT_CAPACITY, $logs[QueryProfiler::DEFAULT_CAPACITY - 1]->query);
    }

    public function testCountAndTotalTimeCoverEntriesPastTheCapacity(): void
    {
        $this->profiler->enable();
        $this->profiler->setCapacity(2);

        $this->profiler->log('Q1', [], 1.0);
        $this->profiler->log('Q2', [], 2.0);
        $this->profiler->log('Q3', [], 4.0);

        $this->assertSame(['Q2', 'Q3'], $this->loggedQueries());
        $this->assertSame(3, $this->profiler->getQueryCount());
        $this->assertSame(7.0, $this->profiler->getTotalTime());
    }

    public function testShrinkingTheCapacityKeepsTheNewestEntries(): void
    {
        $this->profiler->enable();
        $this->profiler->setCapacity(3);

        foreach (['Q1', 'Q2', 'Q3', 'Q4'] as $query) {
            $this->profiler->log($query, [], 1.0);
        }

        $this->profiler->setCapacity(2);
        $this->assertSame(['Q3', 'Q4'], $this->loggedQueries());

        $this->profiler->log('Q5', [], 1.0);
        $this->assertSame(['Q4', 'Q5'], $this->loggedQueries());
    }

    public function testGrowingTheCapacityKeepsTheOrder(): void
    {
        $this->profiler->enable();
        $this->profiler->setCapacity(2);

        foreach (['Q1', 'Q2', 'Q3'] as $query) {
            $this->profiler->log($query, [], 1.0);
        }

        $this->profiler->setCapacity(3);
        $this->profiler->log('Q4', [], 1.0);
        $this->assertSame(['Q2', 'Q3', 'Q4'], $this->loggedQueries());

        $this->profiler->log('Q5', [], 1.0);
        $this->assertSame(['Q3', 'Q4', 'Q5'], $this->loggedQueries());
    }

    public function testCapacityMustBeAtLeastOne(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->profiler->setCapacity(0);
    }

    public function testDisableStopsRecordingAndKeepsTheLogsForInspection(): void
    {
        $this->profiler->enable();
        $this->profiler->log('Q1', [], 1.0);

        $this->profiler->disable();
        $this->profiler->log('Q2', [], 1.0);

        $this->assertSame(['Q1'], $this->loggedQueries());
        $this->assertSame(1, $this->profiler->getQueryCount());
    }

    /**
     * @return array<string>
     */
    private function loggedQueries(): array
    {
        return \array_map(static fn (QueryLog $log): string => $log->query, $this->profiler->getLogs());
    }
}
