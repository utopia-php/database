<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\DateTime;
use Utopia\Database\Validator\Datetime as DatetimeValidator;

class DateTimeTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testCreateDatetime(): void
    {
        $dateValidator = new DatetimeValidator();

        $this->assertGreaterThan(DateTime::addSeconds(new \DateTime(), -3), DateTime::now());
        $this->assertEquals(true, $dateValidator->isValid("2022-12-04"));
        $this->assertEquals(true, $dateValidator->isValid("2022-1-4 11:31"));
        $this->assertEquals(true, $dateValidator->isValid("2022-12-04 11:31:52"));
        $this->assertEquals(true, $dateValidator->isValid("2022-1-4 11:31:52.123456789"));
        $this->assertGreaterThan('2022-7-2', '2022-7-2 11:31:52.680');
        $now = DateTime::now();
        $this->assertEquals(23, strlen($now));
        $this->assertGreaterThan('2020-1-1 11:31:52.680', $now);
        $this->assertEquals('Value must be valid date.', $dateValidator->getDescription());

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

        $this->assertEquals(true, $dateValidator->isValid("2022-12-04 11:31:52.680+02:00"));
        $this->assertEquals('UTC', date_default_timezone_get());
        $this->assertEquals("2022-12-04 09:31:52.680", DateTime::setTimezone("2022-12-04 11:31:52.680+02:00"));
        $this->assertEquals("2022-12-04T09:31:52.681+00:00", DateTime::formatTz("2022-12-04 09:31:52.681"));

        /**
         * Test for Failure
         */
        $this->assertEquals(false, $dateValidator->isValid("2022-13-04 11:31:52.680"));
    }

    public function testPastDateValidation(): void
    {
        $dateValidator = new DatetimeValidator(requireDateInFuture: true);
        $this->assertEquals(false, $dateValidator->isValid(DateTime::addSeconds(new \DateTime(), -3)));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::addSeconds(new \DateTime(), 5)));
        $this->assertEquals('Value must be valid date in future.', $dateValidator->getDescription());

        $dateValidator = new DatetimeValidator(requireDateInFuture: false);
        $this->assertEquals(true, $dateValidator->isValid(DateTime::addSeconds(new \DateTime(), -3)));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::addSeconds(new \DateTime(), 5)));
        $this->assertEquals('Value must be valid date.', $dateValidator->getDescription());
    }

    public function testDatePrecision(): void
    {
        $dateValidator = new DatetimeValidator(precision: DatetimeValidator::PRECISION_SECONDS);
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.255'))));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.000'))));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26'))));
        $this->assertEquals('Value must be valid date with seconds precision.', $dateValidator->getDescription());

        $dateValidator = new DatetimeValidator(precision: DatetimeValidator::PRECISION_MINUTES);
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.255'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.000'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26'))));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:00'))));
        $this->assertEquals('Value must be valid date with minutes precision.', $dateValidator->getDescription());

        $dateValidator = new DatetimeValidator(precision: DatetimeValidator::PRECISION_HOURS);
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.255'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.000'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:00'))));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:00:00'))));
        $this->assertEquals('Value must be valid date with hours precision.', $dateValidator->getDescription());

        $dateValidator = new DatetimeValidator(precision: DatetimeValidator::PRECISION_DAYS);
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.255'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.000'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:00'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:00:00'))));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 00:00:00'))));
        $this->assertEquals('Value must be valid date with days precision.', $dateValidator->getDescription());

        $dateValidator = new DatetimeValidator(true, DatetimeValidator::PRECISION_MINUTES);
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2019-04-08 19:36:26.255'))));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format(new \DateTime('2100-04-08 19:36:26.255'))));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format(new \DateTime('2100-04-08 19:36:00'))));
        $this->assertEquals('Value must be valid date in future with minutes precision.', $dateValidator->getDescription());
    }

    public function testOffset(): void
    {
        $dateValidator = new DatetimeValidator(offset: 60);

        $time = (new \DateTime());
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format($time)));
        $time = $time->add(new \DateInterval('PT50S'));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format($time)));
        $time = $time->add(new \DateInterval('PT20S'));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format($time)));
        $this->assertEquals('Value must be valid date at least 60 seconds in future.', $dateValidator->getDescription());

        $dateValidator = new DatetimeValidator(offset: -60);

        $time = (new \DateTime());
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format($time)));
        $time = $time->sub(new \DateInterval('PT50S'));
        $this->assertEquals(false, $dateValidator->isValid(DateTime::format($time)));
        $time = $time->sub(new \DateInterval('PT20S'));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format($time)));
        $this->assertEquals('Value must be valid date at least 60 seconds in past.', $dateValidator->getDescription());

        $dateValidator = new DatetimeValidator(requireDateInFuture: true, offset: 60);

        $time = (new \DateTime());
        $time = $time->add(new \DateInterval('PT50S'));
        $time = $time->add(new \DateInterval('PT20S'));
        $this->assertEquals(true, $dateValidator->isValid(DateTime::format($time)));
        $this->assertEquals('Value must be valid date at least 60 seconds in future.', $dateValidator->getDescription());
    }
}
