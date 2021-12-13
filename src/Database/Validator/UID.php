<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Validator\Key;

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
        return 'UID must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, and underscore. Can\'t start with a leading underscore';
    }
}
