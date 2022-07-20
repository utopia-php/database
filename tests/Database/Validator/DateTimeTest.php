<?php

namespace Utopia\Tests\Validator;

use Exception;
use Utopia\Database\DateTime;
use PHPUnit\Framework\TestCase;

class DateTimeTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testCreateDatetime()
    {
        $now = DateTime::now();
        $this->assertGreaterThan(DateTime::addSeconds(new \DateTime(), -3), DateTime::now());
        $this->assertEquals(false, DateTime::isValid("2022-13-04 11:31:52.680"));
        $this->assertGreaterThan('2022-7-2', '2022-7-2 11:31:52.680');
        $this->assertEquals(23, strlen($now));
        $this->assertGreaterThan('2020-1-1 11:31:52.680', $now);

        $date = '2022-07-02 18:31:52.680';
        $dateObject = new \DateTime($date);
        $this->assertEquals(DateTime::format($dateObject), $date);
        $this->assertEquals('2022', $dateObject->format('Y'));
        $this->assertEquals('07', $dateObject->format('m'));
        $this->assertEquals('02', $dateObject->format('d'));
        $this->assertEquals('18', $dateObject->format('H'));
        $this->assertEquals('31', $dateObject->format('i'));
        $this->assertEquals('52', $dateObject->format('s'));
        $this->assertEquals('680', $dateObject->format('v'));


    }

    public function testDisableConstructor()
    {
        $this->expectException(Exception::class);
        new DateTime();
    }

}