<?php

namespace Utopia\Database\Validator;

class Label extends Key
{
    public function __construct(bool $allowInternal = false, int $maxLength = 255)
    {
        parent::__construct($allowInternal, $maxLength);
        $this->message = 'Value must be a valid string between 1 and ' . $this->maxLength . ' chars containing only alphanumeric chars';
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param $value
     *
     * @return bool
     */
    public function isValid($value): bool
    {
        if (!parent::isValid($value)) {
            return false;
        }

        // Valid chars: A-Z, a-z, 0-9
        if (\preg_match('/[^A-Za-z0-9]/', $value)) {
            return false;
        }

        return true;
    }
}
