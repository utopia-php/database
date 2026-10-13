<?php

namespace Utopia\Database\Arithmetic;

use Utopia\Database\Arithmetic;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Validator\BigInt;

/**
 * Fractional amounts and limits on an integer column, answered as database 7.4.1 answered them: it handed them to
 * the engine, which stored the result rounded its own way. Each is resolved from the stored value into the
 * whole-number change that stores the same result.
 */
final readonly class Fractional
{
    private const array LIMITED_BY_MAXIMUM = [OperatorType::Increment, OperatorType::Multiply, OperatorType::Power];

    private const array LIMITED_BY_MINIMUM = [OperatorType::Decrement, OperatorType::Divide];

    private const array SHIFTS = [OperatorType::Increment, OperatorType::Decrement];

    public function __construct(private Arithmetic $arithmetic)
    {
    }

    /**
     * Whether a value is a finite number with a fractional part.
     *
     * @phpstan-assert-if-true float|numeric-string $value
     */
    public static function isFractional(mixed $value): bool
    {
        return match (true) {
            \is_float($value) => \is_finite($value) && \floor($value) !== $value,
            \is_string($value) && \is_numeric($value) => ! BigInt::isIntegerString($value) && \floor((float) $value) !== (float) $value,
            default => false,
        };
    }

    /**
     * Whether a value is an integer, integer text or a finite float without a fractional part.
     */
    public static function isWhole(int|float|string $value): bool
    {
        return match (true) {
            \is_int($value) => true,
            \is_string($value) => BigInt::isIntegerString($value),
            default => \is_finite($value) && \floor($value) === $value,
        };
    }

    /**
     * Whether operator() needs the stored value: a numeric operator with a fractional amount or limit.
     */
    public static function needsStored(Operator $operator): bool
    {
        if (! $operator->isNumericOperation()) {
            return false;
        }

        foreach (\array_slice($operator->getValues(), 0, 2) as $value) {
            if (self::isFractional($value)) {
                return true;
            }
        }

        return false;
    }

    /**
     * The whole-number increment or decrement that stores what the engine stored for $operator applied to
     * $stored, or an increment by 0 where its limit held the value back. The operator comes back unchanged where
     * the engine refused it, an operand is not a number, or the result is not finite, so the caller refuses it.
     */
    public function operator(Operator $operator, int|float|string|null $stored): Operator
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();
        $amount = self::number($values[0] ?? 1);
        $limit = isset($values[1]) ? self::number($values[1]) : null;
        $current = self::integer($stored ?? 0);

        if (! $method->isNumeric() || $current === null || $amount === null || ($limit === null && isset($values[1]))) {
            return $operator;
        }

        $whole = ! self::isFractional($amount);
        if ($limit === null && ! $whole && \in_array($method, $this->arithmetic->refusedWithoutLimit, true)) {
            return $operator;
        }

        $rounding = $this->arithmetic->rounding($method, $whole);
        if (\in_array($method, $this->arithmetic->exact, true)) {
            $exact = self::rational($method, $current, $amount);
            if ($exact === null) {
                return $operator;
            }
            if ($limit !== null && self::isBeyondRational($method, $exact, $amount, $limit)) {
                return self::change($operator, 0);
            }
            $stores = $rounding->ratio(...$exact);
        } else {
            $exact = self::compute($method, (float) $current, (float) $amount);
            if ($limit !== null && (self::isUndefinedPower($method, $current, $amount) || self::isBeyond($method, $exact, (float) $amount, (float) $limit))) {
                return self::change($operator, 0);
            }
            if (! \is_finite($exact)) {
                return $operator;
            }
            $stores = \in_array($method, self::SHIFTS, true)
                ? self::shift($rounding, $current, (float) $amount, $method === OperatorType::Increment)
                : $rounding->round($exact);
        }

        return self::change($operator, BigInt::subtract($stores, $current));
    }

    /**
     * The whole-number change, never negative, that stores $stored plus or minus $value as the engine stored a
     * counter change. $stored never passes through a float; only the fraction of $value decides the rounding.
     *
     * @throws TypeException when $value is not a number greater than 0, or is fractional and the engine refused
     *                       a fractional counter change
     */
    public function counter(int|string $stored, int|float|string $value, bool $increase): int|string
    {
        if (! \is_numeric($value) || (BigInt::isIntegerValue($value) ? BigInt::compare($value, 0) <= 0 : (float) $value <= 0)) {
            throw new TypeException('Value must be numeric and greater than 0');
        }

        $whole = BigInt::integralValue($value);
        if ($whole !== null) {
            return $whole;
        }

        if (! self::isFractional($value) || $this->arithmetic->refusesCounterFractions) {
            throw new TypeException('Change value must be an integer.');
        }

        $value = (float) $value;
        $fraction = $value - \floor($value);
        $wholePart = self::integral(\floor($value));
        $result = $increase
            ? $this->arithmetic->counter->apply(BigInt::add($stored, $wholePart), $fraction)
            : $this->arithmetic->counter->apply(BigInt::subtract(BigInt::subtract($stored, $wholePart), 1), 1 - $fraction);

        return BigInt::compare($result, $stored) >= 0 ? BigInt::subtract($result, $stored) : BigInt::subtract($stored, $result);
    }

    /**
     * A maximum on an integer column rounded down to the whole number it allows. Integer text and anything that
     * is not a finite number come back unchanged.
     */
    public static function maximum(int|float|string|null $bound): int|float|string|null
    {
        return match (true) {
            \is_string($bound) => self::decimal($bound)?->floor() ?? $bound,
            \is_float($bound) && \is_finite($bound) => self::integral(\floor($bound)),
            default => $bound,
        };
    }

    /**
     * A minimum on an integer column rounded up to the whole number it allows. Integer text and anything that
     * is not a finite number come back unchanged.
     */
    public static function minimum(int|float|string|null $bound): int|float|string|null
    {
        return match (true) {
            \is_string($bound) => self::decimal($bound)?->ceil() ?? $bound,
            \is_float($bound) && \is_finite($bound) => self::integral(\ceil($bound)),
            default => $bound,
        };
    }

    /**
     * An operator adding items to an integer array, with each fractional item cut toward zero as every engine
     * read it back.
     */
    public static function whole(Operator $operator): Operator
    {
        $values = $operator->getValues();

        return match ($operator->getMethod()) {
            OperatorType::ArrayAppend, OperatorType::ArrayPrepend => (clone $operator)->setValues(\array_map(
                static fn (mixed $value): mixed => \is_array($value) ? \array_map(self::truncate(...), $value) : self::truncate($value),
                $values,
            )),
            OperatorType::ArrayInsert => \array_key_exists(1, $values)
                ? (clone $operator)->setValues([$values[0], self::truncate($values[1])])
                : $operator,
            default => $operator,
        };
    }

    private static function truncate(mixed $item): mixed
    {
        return (\is_float($item) || \is_string($item)) && self::isFractional($item) ? (int) (float) $item : $item;
    }

    private static function change(Operator $operator, int|string $change): Operator
    {
        return BigInt::compare($change, 0) >= 0
            ? new Operator(OperatorType::Increment, $operator->getAttribute(), [$change])
            : new Operator(OperatorType::Decrement, $operator->getAttribute(), [BigInt::negate($change)]);
    }

    private static function number(mixed $value): int|float|string|null
    {
        return \is_int($value) || \is_float($value) || (\is_string($value) && \is_numeric($value)) ? $value : null;
    }

    /**
     * A stored integer, or a stored fraction cut toward zero as every read of it was; null for anything else.
     */
    private static function integer(int|float|string $stored): int|string|null
    {
        if (\is_string($stored) && ! \is_numeric($stored)) {
            return null;
        }

        return BigInt::integralValue($stored) ?? (self::isFractional($stored) ? self::integral((float) $stored < 0 ? \ceil((float) $stored) : \floor((float) $stored)) : null);
    }

    private static function compute(OperatorType $method, float $current, float $amount): float
    {
        return match ($method) {
            OperatorType::Increment => $current + $amount,
            OperatorType::Decrement => $current - $amount,
            OperatorType::Multiply => $current * $amount,
            OperatorType::Divide => $amount === 0.0 ? \NAN : $current / $amount,
            OperatorType::Modulo => $amount === 0.0 ? \NAN : \fmod($current, $amount),
            OperatorType::Power => $current === 0.0 && $amount < 0 ? \NAN : $current ** $amount,
            default => \NAN,
        };
    }

    /**
     * The exact result as a numerator and a positive denominator, or null for a division or modulo by zero or an
     * operand too long to write out.
     *
     * @return array{int|string, int|string}|null
     */
    private static function rational(OperatorType $method, int|string $current, int|float|string $amount): ?array
    {
        $decimal = Decimal::fraction($amount);
        if ($decimal === null) {
            return Decimal::isTiny($amount) && $method !== OperatorType::Divide ? [0, 1] : null;
        }
        [$numerator, $scale] = $decimal;
        $power = self::powerOfTen($scale);

        if ($method === OperatorType::Multiply) {
            return [BigInt::multiply($current, $numerator), $power];
        }
        if (BigInt::compare($numerator, 0) === 0) {
            return null;
        }
        if ($method === OperatorType::Modulo) {
            return [BigInt::modulo(BigInt::multiply($current, $power), $numerator), $power];
        }

        $dividend = BigInt::multiply($current, $power);

        return BigInt::compare($numerator, 0) < 0
            ? [BigInt::negate($dividend), BigInt::negate($numerator)]
            : [$dividend, $numerator];
    }

    /**
     * @param  array{int|string, int|string}  $exact
     */
    private static function isBeyondRational(OperatorType $method, array $exact, int|float|string $amount, int|float|string $limit): bool
    {
        $factor = Decimal::fraction($amount);
        if ($factor === null || BigInt::compare($factor[0], 0) === 0) {
            return false;
        }

        [$numerator, $denominator] = $exact;
        $bound = Decimal::fraction($limit);
        $comparison = $bound === null
            ? self::compareWithOversized($numerator, $limit)
            : BigInt::compare(
                BigInt::multiply($numerator, self::powerOfTen($bound[1])),
                BigInt::multiply($bound[0], $denominator),
            );

        return match (true) {
            \in_array($method, self::LIMITED_BY_MAXIMUM, true) => $comparison > 0,
            \in_array($method, self::LIMITED_BY_MINIMUM, true) => $comparison < 0,
            default => false,
        };
    }

    /**
     * How a result compares with a limit Decimal::fraction() leaves out: one beyond every result in magnitude, or
     * one closer to zero than any of them but zero.
     */
    private static function compareWithOversized(int|string $numerator, int|float|string $limit): int
    {
        $negative = \str_starts_with(\ltrim((string) $limit), '-');
        if (! Decimal::isTiny($limit)) {
            return $negative ? 1 : -1;
        }

        $sign = BigInt::compare($numerator, 0);

        return $sign !== 0 ? $sign : ($negative ? 1 : -1);
    }

    private static function isBeyond(OperatorType $method, float $exact, float $amount, float $limit): bool
    {
        return match (true) {
            $method === OperatorType::Multiply && $amount === 0.0, $method === OperatorType::Divide && $amount === 0.0 => false,
            \in_array($method, self::LIMITED_BY_MAXIMUM, true) => $exact > $limit,
            \in_array($method, self::LIMITED_BY_MINIMUM, true) => $exact < $limit,
            default => false,
        };
    }

    /**
     * Where a limit came with it, 7.4.1 left the value as it was for a power without a value: zero to a negative
     * power, or a negative base to a fractional one.
     */
    private static function isUndefinedPower(OperatorType $method, int|string $current, int|float|string $amount): bool
    {
        return $method === OperatorType::Power
            && ((BigInt::compare($current, 0) === 0 && (float) $amount < 0) || (BigInt::compare($current, 0) < 0 && self::isFractional($amount)));
    }

    /**
     * The stored value plus or minus a fractional amount, exact at any size of the stored value.
     */
    private static function shift(Rounding $rounding, int|string $current, float $amount, bool $increase): int|string
    {
        $whole = self::integral(\floor($amount));
        $fraction = $amount - \floor($amount);

        if ($increase) {
            return $rounding->apply(BigInt::add($current, $whole), $fraction);
        }

        return $fraction === 0.0
            ? BigInt::subtract($current, $whole)
            : $rounding->apply(BigInt::subtract(BigInt::subtract($current, $whole), 1), 1 - $fraction);
    }

    private static function decimal(string $bound): ?Decimal
    {
        return BigInt::isIntegerString($bound) ? null : Decimal::parse($bound);
    }

    private static function powerOfTen(int $exponent): int|string
    {
        return BigInt::toNative('1' . \str_repeat('0', $exponent));
    }

    private static function integral(float $whole): int|string
    {
        return BigInt::toNative(\sprintf('%.0f', $whole));
    }
}
