<?php

namespace Utopia\Database\Helpers;

class ID
{
    /**
     * Create a new unique ID
     *
     * @throws \Exception
     */
    public static function unique(int $padding = 7): string
    {
        $uniqid = \uniqid();

        if ($padding > 0) {
            $bytes = \random_bytes((int)\ceil(($padding / 2))); // one byte expands to two chars
            $uniqid .= \substr(\bin2hex($bytes), 0, $padding);
        }

        return $uniqid;
    }

    /**
     * Create a new ID from a string
     */
    public static function custom(string $id): string
    {
        return $id;
    }
}
