<?php

namespace Utopia\Database\Validator;

class Cursor extends Key
{
    protected int $maxLength = 100;

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        $message = 'Cursor must contain at most %u chars. Valid chars are a-z, A-Z, 0-9, and underscore. Can\'t start with a leading underscore';
        return sprintf($message, $this->maxLength);
    }
}
