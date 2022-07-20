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
        $this->assertGreaterThan(DateTime::addSeconds(new \DateTime(), -3), DateTime::now());
        $this->assertEquals(false, DateTime::isValid("2022-13-04 11:31:52.680"));
        $this->assertGreaterThan('2022-7-2', '2022-7-2 11:31:52.680');
        $this->assertEquals(23, strlen(DateTime::now()));
    }

    public function testDisableConstructor()
    {
        $this->expectException(Exception::class);
        new DateTime();
    }

}