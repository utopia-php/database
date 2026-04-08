<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;

/**
 * Validates label strings ensuring they contain only alphanumeric characters.
 */
class Label extends Key
{
    /**
     * Create a new label validator.
     *
     * @param bool $allowInternal Whether to allow internal attribute names starting with $
     * @param int $maxLength Maximum allowed string length
     */
    public function __construct(
        bool $allowInternal = false,
        int $maxLength = Database::MAX_UID_DEFAULT_LENGTH
    ) {
        parent::__construct($allowInternal, $maxLength);
        $this->message = 'Value must be a valid string between 1 and '.$this->maxLength.' chars containing only alphanumeric chars';
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     */
    public function isValid($value): bool
    {
        if (! parent::isValid($value)) {
            return false;
        }

        if (! \is_string($value)) {
            return false;
        }

        // Valid chars: A-Z, a-z, 0-9
        if (\preg_match('/[^A-Za-z0-9]/', $value)) {
            return false;
        }

        return true;
    }
}
