<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\DateTime;

final class DateTimeTest extends TestCase
{
    public function testNowAfterAdvancesAtMillisecondPrecision(): void
    {
        $future = '2999-01-01 00:00:00.123';

        $this->assertSame('2999-01-01 00:00:00.124', DateTime::nowAfter($future));
    }

    public function testNowAfterUsesCurrentTimeForPriorTimestamp(): void
    {
        $result = DateTime::nowAfter('2000-01-01 00:00:00.000');

        $this->assertGreaterThan('2000-01-01 00:00:00.000', $result);
    }
}
