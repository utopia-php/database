<?php

namespace Utopia\Database\Helpers;

use Exception;
use Utopia\Database\Exception as DatabaseException;

/**
 * Helper class for generating and creating document identifiers.
 */
class ID
{
    /**
     * Create a new unique ID using uniqid with optional random padding.
     *
     * @param int $padding Number of random hex characters to append for uniqueness
     * @return string The generated unique identifier
     * @throws DatabaseException If random bytes generation fails
     */
    public static function unique(int $padding = 7): string
    {
        $uniqid = \uniqid();

        if ($padding > 0) {
            try {
                $bytes = \random_bytes(\max(1, (int) \ceil(($padding / 2)))); // one byte expands to two chars
            } catch (Exception $e) {
                throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
            }

            $uniqid .= \substr(\bin2hex($bytes), 0, $padding);
        }

        return $uniqid;
    }

    /**
     * Create an ID from a custom string value.
     *
     * @param string $id The custom identifier string
     * @return string The provided identifier
     */
    public static function custom(string $id): string
    {
        return $id;
    }
}
