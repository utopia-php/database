<?php

namespace Tests\Unit;

use DateMalformedStringException;
use PHPUnit\Framework\TestCase;
use Utopia\Database\DateTime;
use Utopia\Database\Exception as DatabaseException;

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

    public function testNowAfterRejectsAnUnparseablePreviousTimestamp(): void
    {
        try {
            DateTime::nowAfter('not a date');
            $this->fail('nowAfter() accepted an unparseable previous timestamp');
        } catch (DatabaseException $error) {
            $previous = $error->getPrevious();
            $this->assertInstanceOf(DateMalformedStringException::class, $previous);
            $this->assertSame($previous->getMessage(), $error->getMessage());
        }
    }

    public function testSetTimezoneWrapsAnUnparseableValue(): void
    {
        try {
            DateTime::setTimezone('not a date');
            $this->fail('setTimezone() accepted an unparseable value');
        } catch (DatabaseException $error) {
            $previous = $error->getPrevious();
            $this->assertInstanceOf(DateMalformedStringException::class, $previous);
            $this->assertSame($previous->getMessage(), $error->getMessage());
        }
    }
}
