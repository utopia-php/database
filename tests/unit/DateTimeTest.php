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

    public function testFormatTzReturnsUnparseableInputUnchanged(): void
    {
        $this->assertSame('not a date', DateTime::formatTz('not a date'));
        $this->assertNull(DateTime::formatTz(null));
        $this->assertSame('2024-05-06T07:08:09.123+02:00', DateTime::formatTz('2024-05-06 07:08:09.123+02:00'));
    }
}
