<?php

namespace Utopia\Database\Adapter\Feature;

/**
 * Provides the ability to cast datetime strings to UTC for storage.
 */
interface UTCCasting
{
    /**
     * Convert a datetime string to a UTC representation suitable for the database.
     *
     * @param string $value The datetime string to convert.
     * @return mixed The converted value in the adapter's native format.
     */
    public function setUTCDatetime(string $value): mixed;
}
