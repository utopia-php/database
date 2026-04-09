<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Helpers\BigInt as BigIntHelper;
use Utopia\Validator;

class BigInt extends Validator
{
    public function __construct(
        private readonly bool $signed,
        private readonly bool $supportUnsigned64Bit = true
    ) {
    }

    public function getDescription(): string
    {
        if ($this->signed) {
            return 'Value must be a valid signed 64-bit integer between ' .
                BigIntHelper::formatIntegerString(BigIntHelper::SIGNED_MIN) .
                ' and ' . BigIntHelper::formatIntegerString(BigIntHelper::SIGNED_MAX);
        }

        $max = $this->supportUnsigned64Bit ? BigIntHelper::UNSIGNED_MAX : BigIntHelper::SIGNED_MAX;
        return 'Value must be a valid unsigned 64-bit integer between 0 and ' .
            BigIntHelper::formatIntegerString($max);
    }

    public function isArray(): bool
    {
        return false;
    }

    public function getType(): string
    {
        return Database::VAR_BIGINT;
    }

    public function isValid(mixed $value): bool
    {
        if (\is_int($value)) {
            return $this->signed ? $value >= \PHP_INT_MIN && $value <= \PHP_INT_MAX : $value >= 0;
        }

        if (!\is_string($value)) {
            return false;
        }

        return BigIntHelper::fitsBigIntRange($value, $this->signed, $this->supportUnsigned64Bit);
    }
}
