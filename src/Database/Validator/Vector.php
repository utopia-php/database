<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

/**
 * Validates vector values ensuring they are numeric arrays of the expected dimension size.
 */
class Vector extends Validator
{
    protected int $size;

    /**
     * Vector constructor.
     *
     * @param  int  $size  The size (number of elements) the vector should have
     */
    public function __construct(int $size)
    {
        $this->size = $size;
    }

    /**
     * Get Description
     *
     * Returns validator description
     */
    public function getDescription(): string
    {
        return "Value must be an array of {$this->size} numeric values";
    }

    /**
     * Is valid
     *
     * Validation will pass when $value is a valid vector array or JSON string
     */
    public function isValid(mixed $value): bool
    {
        if (is_string($value)) {
            $decoded = json_decode($value, true);
            if (! is_array($decoded)) {
                return false;
            }
            $value = $decoded;
        }

        if (! is_array($value)) {
            return false;
        }

        if (! \array_is_list($value)) {
            return false;
        }

        if (count($value) !== $this->size) {
            return false;
        }

        // Check that all values are int or float (not strings, booleans, null, arrays, objects)
        foreach ($value as $component) {
            if (! \is_int($component) && ! \is_float($component)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}
