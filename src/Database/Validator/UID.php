<?php

namespace Utopia\Database\Validator;

class UID extends Key
{
    /**
     * Expression constructor
     */
    public function __construct(int $maxLength = 255)
    {
        parent::__construct(false, $maxLength);
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return 'UID must contain at most ' . $this->maxLength . ' chars. Valid chars are a-z, A-Z, 0-9, and underscore. Can\'t start with a leading underscore';
    }
}
