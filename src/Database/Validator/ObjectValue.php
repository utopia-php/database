<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

/**
 * Validates that a value is a valid object (associative array or valid JSON string).
 */
class ObjectValue extends Validator
{
    public function getDescription(): string
    {
        return 'Value must be a valid object';
    }

    public function isValid(mixed $value): bool
    {
        if (is_string($value)) {
            $decoded = json_decode($value);

            if (json_last_error() !== JSON_ERROR_NONE) {
                return false;
            }

            $value = $decoded;
        }

        if ($value instanceof \stdClass) {
            return true;
        }

        return is_array($value) && (count($value) === 0 || ! array_is_list($value));
    }

    public function isArray(): bool
    {
        return false;
    }

    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }
}
