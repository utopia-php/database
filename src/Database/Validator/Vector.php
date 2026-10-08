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
     * @param  int  $size  The size (number of elements) the vector should have
     */
    public function __construct(int $size)
    {
        $this->size = $size;
    }

    #[\Override]
    public function getDescription(): string
    {
        return "Value must be an array of {$this->size} numeric values";
    }

    /**
     * Validation will pass when $value is a valid vector array or JSON string
     */
    #[\Override]
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

        foreach ($value as $component) {
            if (! \is_int($component) && ! \is_float($component)) {
                return false;
            }
        }

        return true;
    }

    #[\Override]
    public function isArray(): bool
    {
        return false;
    }

    #[\Override]
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}
