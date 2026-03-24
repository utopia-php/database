<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator;
use Utopia\Validator\Range;

/**
 * Validates sequence/ID values based on the configured ID attribute type (UUID7 or integer).
 */
class Sequence extends Validator
{
    private string $idAttributeType;

    private bool $primary;

    /**
     * Get the validator description.
     *
     * @return string
     */
    public function getDescription(): string
    {
        return 'Invalid sequence value';
    }

    /**
     * Expression constructor
     */
    public function __construct(string $idAttributeType, bool $primary)
    {
        $this->primary = $primary;
        $this->idAttributeType = $idAttributeType;
    }

    /**
     * Is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get the validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_STRING;
    }

    /**
     * Validate a sequence value against the configured ID attribute type.
     *
     * @param mixed $value The value to validate
     * @return bool
     */
    public function isValid($value): bool
    {
        if ($this->primary && empty($value)) {
            return false;
        }

        if ($value === null) {
            return true;
        }

        if (! \is_string($value) && ! \is_int($value)) {
            return false;
        }

        if (! $this->primary) {
            return true;
        }

        $idType = ColumnType::tryFrom($this->idAttributeType);

        return match ($idType) {
            ColumnType::Uuid7 => \is_string($value) && preg_match('/^[a-f0-9]{8}-[a-f0-9]{4}-7[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i', $value) === 1,
            ColumnType::Integer => (new Range($this->primary ? 1 : 0, Database::MAX_BIG_INT, ColumnType::Integer->value))->isValid($value),
            default => false,
        };
    }
}
