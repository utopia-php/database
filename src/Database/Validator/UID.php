<?php

namespace Utopia\Database\Validator;

class UID extends Key
{
    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return 'UID must contain at most ' . self::KEY_MAX_LENGTH . ' chars. Valid chars are a-z, A-Z, 0-9, and underscore. Can\'t start with a leading underscore';
    }
}
