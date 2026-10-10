<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class ObjectValue extends Validator
{
    /**
     * @param  bool  $stored  Whether the value is one already stored, which 7.x accepted as any JSON string or any
     *                        empty value too
     */
    public function __construct(private readonly bool $stored = false)
    {
    }

    #[\Override]
    public function getDescription(): string
    {
        return 'Value must be a valid object';
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        if ($this->stored && (empty($value) || (\is_string($value) && \json_validate($value)))) {
            return true;
        }

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

    #[\Override]
    public function isArray(): bool
    {
        return false;
    }

    #[\Override]
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }
}
