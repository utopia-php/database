<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\DateTime;

class DateTimeTest extends TestCase
{
    public function testFormatAcceptsAnyDateTimeInterface(): void
    {
        $date = '2022-07-02 18:31:52.680';

        $this->assertEquals($date, DateTime::format(new \DateTime($date)));
        $this->assertEquals($date, DateTime::format(new \DateTimeImmutable($date)));
    }

    public function testAddSecondsAcceptsAnyDateTimeInterface(): void
    {
        $date = '2022-07-02 18:31:52.680';

        $this->assertEquals('2022-07-02 18:32:02.680', DateTime::addSeconds(new \DateTime($date), 10));
        $this->assertEquals('2022-07-02 18:32:02.680', DateTime::addSeconds(new \DateTimeImmutable($date), 10));
        $this->assertEquals('2022-07-02 18:31:42.680', DateTime::addSeconds(new \DateTimeImmutable($date), -10));
    }

    public function testAddSecondsDoesNotMutateInput(): void
    {
        $date = new \DateTime('2022-07-02 18:31:52.680');

        DateTime::addSeconds($date, 10);

        $this->assertEquals('2022-07-02 18:31:52.680', $date->format('Y-m-d H:i:s.v'));
    }
}
