<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

/**
 * Validates a dotted path into an object attribute, such as `meta.address.city`: every key of the
 * path holds only alphanumeric chars, underscores and hyphens.
 */
class ObjectPath extends Validator
{
    public const string KEY_PATTERN = '/^[a-zA-Z0-9_\-]+$/D';

    public function getDescription(): string
    {
        return 'Object path keys must be non-empty and contain only a-z, A-Z, 0-9, underscore and hyphen';
    }

    public function isValid(mixed $value): bool
    {
        if (! \is_string($value)) {
            return false;
        }

        foreach (\explode('.', $value) as $key) {
            if (\preg_match(self::KEY_PATTERN, $key) !== 1) {
                return false;
            }
        }

        return true;
    }

    public function isArray(): bool
    {
        return false;
    }

    public function getType(): string
    {
        return self::TYPE_STRING;
    }
}
