<?php

namespace Utopia\Database;

class ID
{

    /**
     * Create a new unique ID
     */
    public static function unique(): string
    {
        return uniqid();
    }

    /**
     * Create a new ID from a string
     */
    public static function custom(string $id): string
    {
        return $id;
    }
}