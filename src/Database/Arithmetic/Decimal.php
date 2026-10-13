<?php

namespace Utopia\Database\Arithmetic;

use Utopia\Database\Validator\BigInt;

/**
 * A number read exactly from its text, at any size, without passing through a float.
 */
final readonly class Decimal
{
    /**
     * The furthest power of ten from the units a number is written out to: far past a BIGINT and the precision of
     * any fraction an engine kept. A number further out is never expanded (1e1000000000 would take a billion
     * digits); it is beyond every result, or closer to zero than any.
     */
    private const int MAXIMUM_DIGITS = 64;

    private const int MAXIMUM_EXPONENT_DIGITS = 18;

    private const string WHITESPACE = " \t\n\r\v\f";

    private const string EXPONENT_MARKERS = 'eE';

    private const string SATURATED = BigInt::UNSIGNED_MAX . '0';

    private function __construct(
        private bool $negative,
        private string $whole,
        private bool $fractional,
    ) {
    }

    /**
     * The decimal a number's text writes, or null when the text is not a decimal number. A magnitude beyond the
     * unsigned 64-bit range saturates just past it.
     */
    public static function parse(int|float|string $value): ?self
    {
        $value = \trim((string) $value, self::WHITESPACE);
        $unsigned = self::unsigned($value);
        $marker = \strcspn($unsigned, self::EXPONENT_MARKERS);
        $exponent = \substr($unsigned, $marker + 1);
        if ($marker < \strlen($unsigned) && ! \ctype_digit(self::unsigned($exponent))) {
            return null;
        }

        [$integer, $fraction] = \explode('.', \substr($unsigned, 0, $marker), 2) + ['', ''];
        if ($integer . $fraction === '' || ! self::isDigits($integer) || ! self::isDigits($fraction)) {
            return null;
        }

        $digits = \ltrim($integer . $fraction, '0');
        if ($digits === '') {
            return new self(false, '0', false);
        }

        $negative = \str_starts_with($value, '-');
        $exponentLimit = \strlen($integer . $fraction) + \strlen(BigInt::UNSIGNED_MAX) + 1;
        $point = \strlen($digits) - \strlen($fraction) + self::exponent($exponent, $exponentLimit);
        if ($point > \strlen(BigInt::UNSIGNED_MAX)) {
            return new self($negative, self::SATURATED, false);
        }

        $whole = $point > 0 ? \str_pad(\substr($digits, 0, $point), $point, '0') : '0';
        $fractional = $point <= 0 || \ltrim(\substr($digits, $point), '0') !== '';

        return new self($negative, $whole, $fractional);
    }

    public function floor(): int|string
    {
        return $this->fractional && $this->negative
            ? BigInt::subtract($this->truncated(), 1)
            : BigInt::toNative($this->truncated());
    }

    public function ceil(): int|string
    {
        return $this->fractional && ! $this->negative
            ? BigInt::add($this->truncated(), 1)
            : BigInt::toNative($this->truncated());
    }

    /**
     * A number as an integer numerator and the power of ten that divides it, from its text; null for text that is
     * not numeric or a number whose leading digit is more than MAXIMUM_DIGITS powers of ten from the units.
     *
     * @return array{int|string, int}|null
     */
    public static function fraction(int|float|string $value): ?array
    {
        if (\is_string($value) && ! \is_numeric($value)) {
            return null;
        }

        $order = self::order($value);
        if ($order === null) {
            return [0, 0];
        }
        if (\abs($order) > self::MAXIMUM_DIGITS) {
            return null;
        }

        [$mantissa, $exponent] = self::split($value);
        [$whole, $fraction] = \explode('.', $mantissa, 2) + [1 => ''];
        $scale = \strlen($fraction) - (int) $exponent;
        $digits = \ltrim($whole, '+') . $fraction;

        if ($scale < 0) {
            $digits .= \str_repeat('0', -$scale);
            $scale = 0;
        }

        return [BigInt::toNative(BigInt::normalizeInteger($digits)), $scale];
    }

    /**
     * Whether a number fraction() leaves out is closer to zero than any result rather than beyond every result.
     */
    public static function isTiny(int|float|string $value): bool
    {
        $order = self::order($value);

        return $order !== null && $order < -self::MAXIMUM_DIGITS;
    }

    /**
     * The power of ten of a number's leading digit (2 for 123, -3 for 0.001), from its text; null for zero. An
     * exponent too long to read as an integer saturates.
     */
    public static function order(int|float|string $value): ?int
    {
        [$mantissa, $exponent] = self::split($value);
        [$whole, $fraction] = \explode('.', \ltrim($mantissa, '+-'), 2) + [1 => ''];
        if (\ltrim($whole . $fraction, '0') === '') {
            return null;
        }

        $magnitude = \ltrim(\ltrim($exponent, '+-'), '0');
        if (\strlen($magnitude) > self::MAXIMUM_EXPONENT_DIGITS) {
            return \str_starts_with($exponent, '-') ? \PHP_INT_MIN : \PHP_INT_MAX;
        }

        $significant = \ltrim($whole, '0');
        $leading = $significant !== '' ? \strlen($significant) - 1 : -(\strspn($fraction, '0') + 1);

        return $leading + (int) $exponent;
    }

    /**
     * @return array{string, string}
     */
    private static function split(int|float|string $value): array
    {
        $parts = \explode('e', \strtolower(\trim((string) $value)), 2);

        return [$parts[0], $parts[1] ?? '0'];
    }

    private function truncated(): string
    {
        return ($this->negative ? '-' : '') . $this->whole;
    }

    private static function unsigned(string $value): string
    {
        return \str_starts_with($value, '+') || \str_starts_with($value, '-') ? \substr($value, 1) : $value;
    }

    private static function isDigits(string $value): bool
    {
        return $value === '' || \ctype_digit($value);
    }

    private static function exponent(string $exponent, int $limit): int
    {
        $digits = \ltrim($exponent, '+-0');
        $magnitude = \strlen($digits) > \strlen((string) $limit) ? $limit : \min((int) $digits, $limit);

        return \str_starts_with($exponent, '-') ? -$magnitude : $magnitude;
    }
}
