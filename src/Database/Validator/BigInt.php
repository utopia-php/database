<?php

namespace Utopia\Database\Validator;

use InvalidArgumentException;
use Utopia\Database\OperatorType;
use Utopia\Validator;

class BigInt extends Validator
{
    public const string SIGNED_MIN = '-9223372036854775808';
    public const string SIGNED_MAX = '9223372036854775807';
    public const string UNSIGNED_MAX = '18446744073709551615';

    public function __construct(
        private readonly bool $signed,
        private readonly bool $supportUnsigned64Bit = true
    ) {
    }

    public function getDescription(): string
    {
        if ($this->signed) {
            return 'Value must be a valid signed 64-bit integer between ' .
                self::formatIntegerString(self::SIGNED_MIN) .
                ' and ' . self::formatIntegerString(self::SIGNED_MAX);
        }

        $max = $this->supportUnsigned64Bit ? self::UNSIGNED_MAX : self::SIGNED_MAX;
        return 'Value must be a valid unsigned 64-bit integer between 0 and ' .
            self::formatIntegerString($max);
    }

    public function isArray(): bool
    {
        return false;
    }

    public function getType(): string
    {
        return \Utopia\Query\Schema\ColumnType::BigInteger->value;
    }

    public function isValid(mixed $value): bool
    {
        if (\is_int($value)) {
            return $this->signed ? $value >= \PHP_INT_MIN && $value <= \PHP_INT_MAX : $value >= 0;
        }

        if (!\is_string($value)) {
            return false;
        }

        return self::fitsBigIntRange($value, $this->signed, $this->supportUnsigned64Bit);
    }

    public static function isIntegerString(string $value, bool $signed = true): bool
    {
        return \preg_match($signed ? '/^-?\d+$/' : '/^\d+$/', $value) === 1;
    }

    public static function fitsPhpInt(string $value, bool $signed = true): bool
    {
        if (!self::isIntegerString($value, $signed)) {
            return false;
        }

        $phpMax = (string)\PHP_INT_MAX;
        $phpMinAbs = \ltrim((string)\PHP_INT_MIN, '-');

        if ($signed && \str_starts_with($value, '-')) {
            $digits = self::normalizeUnsignedString(\substr($value, 1));
            return self::compareUnsignedStrings($digits, $phpMinAbs) <= 0;
        }

        $digits = self::normalizeUnsignedString($value);
        return self::compareUnsignedStrings($digits, $phpMax) <= 0;
    }

    public static function fitsBigIntRange(string $value, bool $signed, bool $supportUnsigned64Bit = true): bool
    {
        if (!self::isIntegerString($value, $signed)) {
            return false;
        }

        if ($signed) {
            if (\str_starts_with($value, '-')) {
                $digits = self::normalizeUnsignedString(\substr($value, 1));
                $minAbs = \ltrim(\str_replace('-', '', self::SIGNED_MIN), '0');
                return self::compareUnsignedStrings($digits, $minAbs) <= 0;
            }

            return self::compareUnsignedStrings($value, self::SIGNED_MAX) <= 0;
        }

        $max = $supportUnsigned64Bit ? self::UNSIGNED_MAX : self::SIGNED_MAX;
        return self::compareUnsignedStrings($value, $max) <= 0;
    }

    public static function normalizeUnsignedString(string $value): string
    {
        $value = \trim($value);
        $value = \ltrim($value, '0');
        return $value === '' ? '0' : $value;
    }

    public static function compareUnsignedStrings(string $a, string $b): int
    {
        $a = self::normalizeUnsignedString($a);
        $b = self::normalizeUnsignedString($b);

        $lenA = \strlen($a);
        $lenB = \strlen($b);
        if ($lenA < $lenB) {
            return -1;
        }
        if ($lenA > $lenB) {
            return 1;
        }
        if ($a === $b) {
            return 0;
        }

        return $a < $b ? -1 : 1;
    }

    public static function normalizeInteger(mixed $value): string
    {
        if (\is_float($value)) {
            if (! \is_finite($value) || \floor($value) !== $value || $value < \PHP_INT_MIN || $value > \PHP_INT_MAX) {
                throw new InvalidArgumentException('Value must be an integer.');
            }
            $value = (int) $value;
        }
        if (! \is_int($value) && ! \is_string($value)) {
            throw new InvalidArgumentException('Value must be an integer.');
        }
        $value = (string) $value;
        if (! self::isIntegerString($value)) {
            throw new InvalidArgumentException('Value must be an integer.');
        }

        $negative = \str_starts_with($value, '-');
        $digits = self::normalizeUnsignedString($negative ? \substr($value, 1) : $value);

        return $negative && $digits !== '0' ? '-'.$digits : $digits;
    }

    public static function toNative(mixed $value): int|string
    {
        $value = self::normalizeInteger($value);

        return self::fitsPhpInt($value) ? (int) $value : $value;
    }

    public static function compare(int|float|string $a, int|float|string $b): int
    {
        $a = self::normalizeInteger($a);
        $b = self::normalizeInteger($b);
        $aNegative = \str_starts_with($a, '-');
        $bNegative = \str_starts_with($b, '-');

        if ($aNegative !== $bNegative) {
            return $aNegative ? -1 : 1;
        }

        $comparison = self::compareUnsignedStrings(\ltrim($a, '-'), \ltrim($b, '-'));

        return $aNegative ? -$comparison : $comparison;
    }

    public static function add(int|float|string $a, int|float|string $b): int|string
    {
        $a = self::normalizeInteger($a);
        $b = self::normalizeInteger($b);
        $aNegative = \str_starts_with($a, '-');
        $bNegative = \str_starts_with($b, '-');
        $aDigits = \ltrim($a, '-');
        $bDigits = \ltrim($b, '-');

        if ($aNegative === $bNegative) {
            $result = self::addUnsignedStrings($aDigits, $bDigits);
            if ($aNegative && $result !== '0') {
                $result = '-'.$result;
            }

            return self::toNative($result);
        }

        $comparison = self::compareUnsignedStrings($aDigits, $bDigits);
        if ($comparison === 0) {
            return 0;
        }

        $aLarger = $comparison > 0;
        $result = self::subtractUnsignedStrings(
            $aLarger ? $aDigits : $bDigits,
            $aLarger ? $bDigits : $aDigits,
        );
        if (($aLarger ? $aNegative : $bNegative) && $result !== '0') {
            $result = '-'.$result;
        }

        return self::toNative($result);
    }

    public static function subtract(int|float|string $a, int|float|string $b): int|string
    {
        return self::add($a, self::negate($b));
    }

    public static function negate(int|float|string $value): int|string
    {
        $value = self::normalizeInteger($value);
        if ($value === '0') {
            return 0;
        }

        return self::toNative(\str_starts_with($value, '-') ? \substr($value, 1) : '-'.$value);
    }

    public static function multiply(int|float|string $a, int|float|string $b): int|string
    {
        $a = self::normalizeInteger($a);
        $b = self::normalizeInteger($b);
        $negative = \str_starts_with($a, '-') !== \str_starts_with($b, '-');
        $result = self::multiplyUnsignedStrings(\ltrim($a, '-'), \ltrim($b, '-'));
        if ($negative && $result !== '0') {
            $result = '-'.$result;
        }

        return self::toNative($result);
    }

    public static function divide(int|float|string $a, int|float|string $b): int|string
    {
        $a = self::normalizeInteger($a);
        $b = self::normalizeInteger($b);
        if ($b === '0') {
            throw new InvalidArgumentException('Division by zero is not allowed.');
        }

        $negative = \str_starts_with($a, '-') !== \str_starts_with($b, '-');
        [$quotient] = self::divideUnsignedStrings(\ltrim($a, '-'), \ltrim($b, '-'));
        if ($negative && $quotient !== '0') {
            $quotient = '-'.$quotient;
        }

        return self::toNative($quotient);
    }

    public static function modulo(int|float|string $a, int|float|string $b): int|string
    {
        $a = self::normalizeInteger($a);
        $b = self::normalizeInteger($b);
        if ($b === '0') {
            throw new InvalidArgumentException('Modulo by zero is not allowed.');
        }

        [, $remainder] = self::divideUnsignedStrings(\ltrim($a, '-'), \ltrim($b, '-'));
        if (\str_starts_with($a, '-') && $remainder !== '0') {
            $remainder = '-'.$remainder;
        }

        return self::toNative($remainder);
    }

    public static function power(int|float|string $base, int|float|string $exponent): int|string
    {
        $base = self::normalizeInteger($base);
        $exponent = self::normalizeInteger($exponent);
        if (\str_starts_with($exponent, '-')) {
            throw new InvalidArgumentException('Integer power exponent must not be negative.');
        }
        if ($exponent === '0') {
            return 1;
        }
        if ($base === '0' || $base === '1') {
            return (int) $base;
        }
        if ($base === '-1') {
            return ((int) \substr($exponent, -1)) % 2 === 0 ? 1 : -1;
        }
        if (! self::fitsPhpInt($exponent, false) || (int) $exponent > 64) {
            return \str_starts_with($base, '-') && ((int) \substr($exponent, -1)) % 2 !== 0
                ? '-'.self::UNSIGNED_MAX.'0'
                : self::UNSIGNED_MAX.'0';
        }

        $result = 1;
        $factor = $base;
        $remaining = (int) $exponent;
        while ($remaining > 0) {
            if ($remaining % 2 === 1) {
                $result = self::multiply($result, $factor);
            }
            $remaining = \intdiv($remaining, 2);
            if ($remaining > 0) {
                $factor = self::multiply($factor, $factor);
            }
        }

        return $result;
    }

    public static function calculate(OperatorType $method, int|float|string $current, int|float|string $operand): int|string
    {
        return match ($method) {
            OperatorType::Increment => self::add($current, $operand),
            OperatorType::Decrement => self::subtract($current, $operand),
            OperatorType::Multiply => self::multiply($current, $operand),
            OperatorType::Divide => self::divide($current, $operand),
            OperatorType::Modulo => self::modulo($current, $operand),
            OperatorType::Power => self::power($current, $operand),
            default => throw new InvalidArgumentException('Operator must be numeric.'),
        };
    }

    public static function calculateOutsideNative(OperatorType $method, mixed $current, mixed $operand): int|string|null
    {
        if (! self::isIntegerValue($current) || ! self::isIntegerValue($operand)) {
            return null;
        }

        try {
            $result = self::calculate($method, $current, $operand);
        } catch (InvalidArgumentException) {
            return null;
        }

        $currentOutside = \is_string($current) && ! self::fitsPhpInt($current);
        $operandOutside = \is_string($operand) && ! self::fitsPhpInt($operand);

        return $currentOutside || $operandOutside || \is_string($result) ? $result : null;
    }

    /**
     * @phpstan-assert-if-true int|string $value
     */
    public static function isIntegerValue(mixed $value): bool
    {
        return \is_int($value) || (\is_string($value) && self::isIntegerString($value));
    }

    private static function addUnsignedStrings(string $a, string $b): string
    {
        $aIndex = \strlen($a) - 1;
        $bIndex = \strlen($b) - 1;
        $carry = 0;
        $result = '';

        while ($aIndex >= 0 || $bIndex >= 0 || $carry > 0) {
            $sum = ($aIndex >= 0 ? (int) $a[$aIndex--] : 0)
                + ($bIndex >= 0 ? (int) $b[$bIndex--] : 0)
                + $carry;
            $result = (string) ($sum % 10).$result;
            $carry = \intdiv($sum, 10);
        }

        return self::normalizeUnsignedString($result);
    }

    private static function subtractUnsignedStrings(string $a, string $b): string
    {
        $aIndex = \strlen($a) - 1;
        $bIndex = \strlen($b) - 1;
        $borrow = 0;
        $result = '';

        while ($aIndex >= 0) {
            $digit = (int) $a[$aIndex--] - $borrow - ($bIndex >= 0 ? (int) $b[$bIndex--] : 0);
            if ($digit < 0) {
                $digit += 10;
                $borrow = 1;
            } else {
                $borrow = 0;
            }
            $result = (string) $digit.$result;
        }

        return self::normalizeUnsignedString($result);
    }

    private static function multiplyUnsignedStrings(string $a, string $b): string
    {
        $a = self::normalizeUnsignedString($a);
        $b = self::normalizeUnsignedString($b);
        if ($a === '0' || $b === '0') {
            return '0';
        }

        $digits = \array_fill(0, \strlen($a) + \strlen($b), 0);
        for ($aIndex = \strlen($a) - 1; $aIndex >= 0; $aIndex--) {
            for ($bIndex = \strlen($b) - 1; $bIndex >= 0; $bIndex--) {
                $position = $aIndex + $bIndex + 1;
                $product = (int) $a[$aIndex] * (int) $b[$bIndex] + $digits[$position];
                $digits[$position] = $product % 10;
                $digits[$position - 1] += \intdiv($product, 10);
            }
        }

        return self::normalizeUnsignedString(\implode('', $digits));
    }

    /**
     * @return array{string, string}
     */
    private static function divideUnsignedStrings(string $dividend, string $divisor): array
    {
        $dividend = self::normalizeUnsignedString($dividend);
        $divisor = self::normalizeUnsignedString($divisor);
        if ($divisor === '0') {
            throw new InvalidArgumentException('Division by zero is not allowed.');
        }

        $quotient = '';
        $remainder = '0';
        for ($index = 0, $length = \strlen($dividend); $index < $length; $index++) {
            $remainder = self::normalizeUnsignedString($remainder.$dividend[$index]);
            $digit = 0;
            while (self::compareUnsignedStrings($remainder, $divisor) >= 0) {
                $remainder = self::subtractUnsignedStrings($remainder, $divisor);
                $digit++;
            }
            $quotient .= (string) $digit;
        }

        return [self::normalizeUnsignedString($quotient), $remainder];
    }

    public static function formatIntegerString(string $value): string
    {
        $negative = \str_starts_with($value, '-');
        if ($negative) {
            $value = \substr($value, 1);
        }

        $value = self::normalizeUnsignedString($value);
        $formatted = \preg_replace('/\B(?=(\d{3})+(?!\d))/', ',', $value) ?? $value;

        return $negative ? "-{$formatted}" : $formatted;
    }
}
