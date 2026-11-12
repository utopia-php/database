<?php

namespace Utopia\Database;

use Utopia\Database\Validator\BigInt;

/**
 * @internal
 */
enum Rounding
{
    case HalfAwayFromZero;
    case Floor;
    case Ceil;

    private const string DECIMAL = '/^\s*([+-]?)(?=\.?\d)(\d*)\.(\d*)\s*$/D';

    /**
     * @template T
     *
     * @param  T  $value
     * @return T|int|string $value as a whole number, exact at any size; anything but a finite float or plain decimal
     *                      text is returned as given, for the caller to accept or refuse
     */
    public function round(mixed $value): mixed
    {
        if (\is_float($value) && \is_finite($value)) {
            $negative = $value < 0;
            $magnitude = \floor(\abs($value));
            $fraction = \abs($value) - $magnitude;
            $whole = BigInt::integralValue($negative ? -$magnitude : $magnitude);
            $half = $fraction >= 0.5;
            $fractional = $fraction > 0;
        } elseif (\is_string($value) && \preg_match(self::DECIMAL, $value, $parts) === 1) {
            $negative = $parts[1] === '-';
            $whole = BigInt::toNative(($negative ? '-' : '').($parts[2] ?: '0'));
            $half = $parts[3] !== '' && $parts[3][0] >= '5';
            $fractional = \trim($parts[3], '0') !== '';
        } else {
            return $value;
        }

        if ($whole === null) {
            return $value;
        }

        $awayFromZero = match ($this) {
            self::HalfAwayFromZero => $half,
            self::Floor => $negative && $fractional,
            self::Ceil => ! $negative && $fractional,
        };

        return $awayFromZero ? BigInt::add($whole, $negative ? -1 : 1) : $whole;
    }
}
