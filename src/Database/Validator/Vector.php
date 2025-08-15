<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class Vector extends Validator
{
    protected int $dimensions;

    /**
     * Vector constructor.
     *
     * @param int $dimensions The number of dimensions the vector should have
     */
    public function __construct(int $dimensions)
    {
        $this->dimensions = $dimensions;
    }

    /**
     * Get Description
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return "Value must be an array of floats with {$this->dimensions} dimensions";
    }

    /**
     * Is valid
     *
     * Validation will pass when $value is a valid vector array
     *
     * @param mixed $value
     * @return bool
     */
    public function isValid(mixed $value): bool
    {
        if (!is_array($value)) {
            return false;
        }

        if (count($value) !== $this->dimensions) {
            return false;
        }

        // Check that all values are numeric (can be converted to float)
        foreach ($value as $component) {
            if (!is_numeric($component)) {
                return false;
            }
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
        return self::TYPE_ARRAY;
    }
}