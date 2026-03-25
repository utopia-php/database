<?php

namespace Utopia\Database;

use DateInterval;
use DateTime as PhpDateTime;
use DateTimeZone;
use Throwable;
use Utopia\Database\Exception as DatabaseException;

/**
 * Utility class for formatting and manipulating date-time values in the database.
 */
class DateTime
{
    protected static string $formatDb = 'Y-m-d H:i:s.v';

    protected static string $formatTz = 'Y-m-d\TH:i:s.vP';

    private function __construct()
    {
    }

    /**
     * Get the current date-time formatted for database storage.
     *
     * @return string
     */
    public static function now(): string
    {
        $date = new PhpDateTime();

        return self::format($date);
    }

    /**
     * Format a DateTime object into the database storage format.
     *
     * @param PhpDateTime $date The date to format
     * @return string
     */
    public static function format(PhpDateTime $date): string
    {
        return $date->format(self::$formatDb);
    }

    /**
     * Add seconds to a DateTime and return the formatted result.
     *
     * @param PhpDateTime $date The base date
     * @param int $seconds Number of seconds to add
     * @return string
     * @throws DatabaseException
     */
    public static function addSeconds(PhpDateTime $date, int $seconds): string
    {
        $interval = DateInterval::createFromDateString($seconds.' seconds');

        if (! $interval) {
            throw new DatabaseException('Invalid interval');
        }

        $date->add($interval);

        return self::format($date);
    }

    /**
     * Parse a datetime string and convert it to the system's default timezone.
     *
     * @param string $datetime The datetime string to convert
     * @return string
     * @throws DatabaseException
     */
    public static function setTimezone(string $datetime): string
    {
        try {
            $value = new PhpDateTime($datetime);
            $value->setTimezone(new DateTimeZone(date_default_timezone_get()));

            return DateTime::format($value);
        } catch (Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Convert a database-format date string to a timezone-aware ISO 8601 format.
     *
     * @param string|null $dbFormat The date string in database format, or null
     * @return string|null The formatted date string with timezone, or null if input is null
     */
    public static function formatTz(?string $dbFormat): ?string
    {
        if (is_null($dbFormat)) {
            return null;
        }

        try {
            $value = new PhpDateTime($dbFormat);

            return $value->format(self::$formatTz);
        } catch (Throwable) {
            return $dbFormat;
        }
    }
}
