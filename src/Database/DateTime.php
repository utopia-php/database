<?php

namespace Utopia\Database;

use Utopia\Database\Exception as DatabaseException;

class DateTime
{
    protected static string $formatDb = 'Y-m-d H:i:s.v';
    protected static string $formatTz = 'Y-m-d\TH:i:s.vP';

    private function __construct()
    {
    }

    /**
     * @return string
     */
    public static function now(): string
    {
        $date = new \DateTime();
        return self::format($date);
    }

    /**
     * @param \DateTimeInterface $date
     * @return string
     */
    public static function format(\DateTimeInterface $date): string
    {
        return $date->format(self::$formatDb);
    }

    /**
     * Returns the given date shifted by $seconds. The given date is left untouched.
     *
     * @param \DateTimeInterface $date
     * @param int $seconds
     * @return string
     * @throws DatabaseException
     */
    public static function addSeconds(\DateTimeInterface $date, int $seconds): string
    {
        $interval  = \DateInterval::createFromDateString($seconds . ' seconds');

        if (!$interval) {
            throw new DatabaseException('Invalid interval');
        }

        return self::format(\DateTimeImmutable::createFromInterface($date)->add($interval));
    }

    /**
     * @param string $datetime
     * @return string
     * @throws DatabaseException
     */
    public static function setTimezone(string $datetime): string
    {
        try {
            $value = new \DateTime($datetime);
            $value->setTimezone(new \DateTimeZone(date_default_timezone_get()));
            return DateTime::format($value);
        } catch (\Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param string|null $dbFormat
     * @return string|null
     */
    public static function formatTz(?string $dbFormat): ?string
    {
        if (is_null($dbFormat)) {
            return null;
        }

        try {
            $value = new \DateTime($dbFormat);
            return $value->format(self::$formatTz);
        } catch (\Throwable) {
            return $dbFormat;
        }
    }
}
