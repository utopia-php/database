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
    public function getDescription()
    {
        return 'UID must contain only alphanumeric chars or non-leading underscore, shorter than 36 chars';
    }
}
