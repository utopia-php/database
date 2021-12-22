<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class Key extends Validator
{
    /**
     * @var string
     */
    protected $message = 'Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char';

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
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
        if (!\is_string($value)) {
            return false;
        }

        // no leading special characters
        $leading = \mb_substr($value, 0, 1);
        if($leading === '_' || $leading === '.' || $leading === '-') {
            return false;
        }

        // Valid chars: A-Z, a-z, 0-9, underscore, hyphen, period
        if (\preg_match('/[^A-Za-z0-9\_\-\.]/', $value)) {
            return false;
        }

        if (\mb_strlen($value) > 36) {
            return false;
        }

        return true;
    }
    
    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_STRING;
    }
}
