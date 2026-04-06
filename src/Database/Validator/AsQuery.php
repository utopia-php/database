<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class AsQuery extends Validator
{
    protected string $message = '"as" must contain at most 64 chars. Valid chars are a-z, A-Z, 0-9, and underscore.';

    /**
     * @param string $attribute
     */
    public function __construct(
        private readonly string $attribute,
    ) {

    }

    /**
     * Validator Description.
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is valid.
     * Returns true if valid or false if not.
     * @param mixed $value
     * @return bool
     */
    public function isValid($value): bool
    {
        if (! \is_string($value)) {
            return false;
        }

        if (empty($value)) {
            return true;
        }

        if (! preg_match('/^[a-zA-Z0-9_]+$/', $value)) {
            return false;
        }

        if (\strlen($value) >= 64) {
            return false;
        }

        if ($this->attribute === '*') {
            $this->message = 'Invalid "as" on attribute "*"';
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
