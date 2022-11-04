<?php

namespace Utopia\Database;

use Utopia\Database\Exception;

class DateTime
{
    protected static string $formatDb = 'Y-m-d H:i:s.v';
    protected static string $formatTz = 'Y-m-d\TH:i:s.vP';

    private function __construct()
    {
    }

    /**
     * @param string|null $datetime
     * @return bool
     */
    public static function isValid(?string $datetime): bool
    {
        if (empty($datetime)) {
            return false;
        }

        try {
            new \DateTime($datetime);
        }
        catch(Exception $e) {
            return false;
        }

        return true;
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
     * @param \DateTime $date
     * @return string
     */
    public static function format(\DateTime $date): string
    {
        return $date->format(self::$formatDb);
    }

    /**
     * @param \DateTime $date
     * @param int $seconds
     * @return string
     */
    public static function addSeconds(\DateTime $date, int $seconds): string
    {
        $date->add(\DateInterval::createFromDateString($seconds . ' seconds'));
        return self::format($date);
    }

    /**
     * @param string $datetime
     * @return string
     * @throws Exception
     */
    public static function setTimezone(string $datetime): string
    {
        $value = new \DateTime($datetime);
        $value->setTimezone(new \DateTimeZone(date_default_timezone_get()));
        return DateTime::format($value);
    }

    /**
     * @param string|null $dbFormat
     * @return string|null
     */
    public static function formatTz(?string $dbFormat): ?string
    {
        if (is_null($dbFormat)) return null;

        try {
            $value = new \DateTime($dbFormat);
            return $value->format(self::$formatTz);
        } catch (\Throwable $th) {
            return $dbFormat;
        }
    }
}
