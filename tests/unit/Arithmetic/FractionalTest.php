<?php

namespace Tests\Unit\Arithmetic;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Tests\Unit\Support\FractionalEngine;
use Tests\Unit\Support\FractionalResults;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Arithmetic\Fractional;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Validator\BigInt;
use Utopia\Mongo\Client;

final class FractionalTest extends TestCase
{
    /**
     * @return iterable<string, array{FractionalEngine, int, OperatorType, list<int|float>, int|null}>
     */
    public static function operators(): iterable
    {
        return FractionalResults::operators();
    }

    /**
     * @param  list<int|float>  $values
     */
    #[DataProvider('operators')]
    public function testAFractionalOperandStoresWhatTheEngineStored(FractionalEngine $engine, int $stored, OperatorType $method, array $values, ?int $expected): void
    {
        $operator = new Operator($method, 'count', $values);
        $this->assertTrue(Fractional::needsStored($operator));

        $resolved = self::fractional($engine)->operator($operator, $stored);

        if ($expected === null) {
            $this->assertSame($operator, $resolved, 'The engine refused it, so the caller must see it unchanged');

            return;
        }

        $this->assertContains($resolved->getMethod(), [OperatorType::Increment, OperatorType::Decrement]);
        $this->assertSame('count', $resolved->getAttribute());
        $this->assertCount(1, $resolved->getValues(), 'A resolved change carries no limit');
        $this->assertSame($expected, self::stores($stored, $resolved));
    }

    public function testAWholeOperandAndLimitNeedNoStoredValue(): void
    {
        $this->assertFalse(Fractional::needsStored(Operator::increment(2, 10)));
        $this->assertFalse(Fractional::needsStored(Operator::multiply(2.0)));
        $this->assertFalse(Fractional::needsStored(Operator::divide('4')));
        $this->assertFalse(Fractional::needsStored(Operator::arrayAppend([1.5])));
        $this->assertFalse(Fractional::needsStored(Operator::increment(\INF)));
        $this->assertTrue(Fractional::needsStored(Operator::increment(2, 10.5)));
        $this->assertTrue(Fractional::needsStored(Operator::divide('2.5')));
    }

    public function testAMissingStoredValueCountsAsZero(): void
    {
        $resolved = self::fractional(FractionalEngine::MariaDB)->operator(Operator::increment(1.5), null);

        $this->assertSame(OperatorType::Increment, $resolved->getMethod());
        $this->assertSame(2, $resolved->getValue());
    }

    public function testAFractionalChangeBeyondFloatPrecisionStaysExact(): void
    {
        $resolved = self::fractional(FractionalEngine::MariaDB)->operator(Operator::increment(1.5), 9007199254740993);

        $this->assertSame(1, $resolved->getValue(), '9007199254740993 + 1.5 rounds half to even to 9007199254740994');
    }

    /**
     * @return iterable<string, array{Operator, int, int}>
     */
    public static function numericOnPostgreSQLBeyondFloatPrecision(): iterable
    {
        yield 'multiply' => [Operator::multiply(1.5, 99999999999999999), 9007199254740993, 13510798882111490];
        yield 'divide' => [Operator::divide(0.5, 0), 9007199254740993, 18014398509481986];
        yield 'modulo' => [Operator::modulo(2.5), 9007199254740993, 1];
    }

    #[DataProvider('numericOnPostgreSQLBeyondFloatPrecision')]
    public function testPostgreSQLComputesFractionalFactorsExactlyAsNumericDid(Operator $operator, int $stored, int $expected): void
    {
        $resolved = self::fractional(FractionalEngine::PostgreSQL)->operator($operator, $stored);

        $this->assertSame($expected, self::stores($stored, $resolved));
    }

    public function testAnOperandTooLongForAnIntegerIsLeftForTheCallerToRefuse(): void
    {
        $fractional = self::fractional(FractionalEngine::PostgreSQL);

        foreach ([Operator::multiply('1e1000000000', 1.5), new Operator(OperatorType::Divide, 'count', ['1e-1000000000', 0.5]), Operator::multiply('1e99999999999999999999', 1.5)] as $operator) {
            $this->assertSame($operator, $fractional->operator($operator, 10));
        }
    }

    public function testAResultThatIsNotFiniteIsLeftForTheCallerToRefuse(): void
    {
        $operator = Operator::power(100.5);

        $this->assertSame($operator, self::fractional(FractionalEngine::MariaDB)->operator($operator, \PHP_INT_MAX));
    }

    /**
     * @return iterable<string, array{Operator}>
     */
    public static function nonFiniteOperands(): iterable
    {
        foreach (['an infinite' => \INF, 'a negative infinite' => -\INF, 'a not-a-number' => \NAN] as $name => $value) {
            yield "{$name} increment under a fractional maximum" => [new Operator(OperatorType::Increment, 'count', [$value, 10.5])];
            yield "{$name} multiplication under a fractional maximum" => [new Operator(OperatorType::Multiply, 'count', [$value, 10.5])];
            yield "{$name} division above a fractional minimum" => [new Operator(OperatorType::Divide, 'count', [$value, 0.5])];
            yield "a fractional increment under {$name} maximum" => [new Operator(OperatorType::Increment, 'count', [1.5, $value])];
            yield "a fractional multiplication under {$name} maximum" => [new Operator(OperatorType::Multiply, 'count', [1.5, $value])];
            yield "a fractional division above {$name} minimum" => [new Operator(OperatorType::Divide, 'count', [0.5, $value])];
            yield "a fractional power under {$name} maximum" => [new Operator(OperatorType::Power, 'count', [0.5, $value])];
        }
    }

    #[DataProvider('nonFiniteOperands')]
    public function testANonFiniteOperandOrLimitIsLeftForTheCallerToRefuse(Operator $operator): void
    {
        foreach (FractionalEngine::cases() as $engine) {
            $this->assertSame($operator, self::fractional($engine)->operator($operator, 10), $engine->name);
        }
    }

    /**
     * @return iterable<string, array{Operator}>
     */
    public static function overlongOperands(): iterable
    {
        yield 'a factor' => [Operator::multiply('1.' . \str_repeat('3', 10000), 100)];
        yield 'a divisor' => [Operator::divide('1.' . \str_repeat('3', 10000), 0)];
        yield 'a modulus' => [Operator::modulo('1.' . \str_repeat('3', 10000))];
        yield 'a maximum' => [Operator::multiply(1.5, '15.' . \str_repeat('9', 200))];
        yield 'a minimum' => [Operator::divide(0.5, '-1.' . \str_repeat('9', 200))];
    }

    #[DataProvider('overlongOperands')]
    public function testANumberWithMoreDigitsThanAnExactEngineReadIsLeftForTheCallerToRefuse(Operator $operator): void
    {
        $this->assertSame($operator, self::fractional(FractionalEngine::PostgreSQL)->operator($operator, 10));
    }

    public function testTrailingZerosPastTheFractionAreReadByTheirValue(): void
    {
        $resolved = self::fractional(FractionalEngine::PostgreSQL)->operator(Operator::multiply('1.5' . \str_repeat('0', 10000), 100), 10);

        $this->assertSame(15, self::stores(10, $resolved));
    }

    /**
     * @return iterable<string, array{Operator, int}>
     */
    public static function whitespacePaddedOperands(): iterable
    {
        yield 'a factor led by a form feed' => [Operator::multiply("\f1.5", 100), 15];
        yield 'a factor padded with every numeric whitespace' => [Operator::multiply(" \t\n\r\v\f1.5 \t\n\r\v\f", 100), 15];
        yield 'a huge negative maximum led by a form feed' => [Operator::multiply(1.5, "\f-1e1000000000"), 10];
        yield 'a tiny negative minimum led by a form feed' => [Operator::divide(0.5, "\f-1e-1000000000"), 20];
    }

    #[DataProvider('whitespacePaddedOperands')]
    public function testNumericTextPaddedWithWhitespaceIsReadByItsValue(Operator $operator, int $expected): void
    {
        $this->assertSame($expected, self::stores(10, self::fractional(FractionalEngine::PostgreSQL)->operator($operator, 10)));
    }

    /**
     * @return iterable<string, array{int|string, Operator, int|string|null}>
     */
    public static function truncatedModuli(): iterable
    {
        yield 'a modulus cut toward zero' => [11, Operator::modulo(4.5), 3];
        yield 'a negative modulus cut toward zero' => [7, Operator::modulo(-2.5), 1];
        yield 'a negative value keeps its sign' => [-11, Operator::modulo(3.5), -2];
        yield 'modulus text cut toward zero' => [13, Operator::modulo('3.5'), 1];
        yield 'a value beyond float precision' => ['9007199254740993', Operator::modulo(2.5), 1];
        yield 'a modulus cut to zero is left for the caller to refuse' => [10, Operator::modulo(0.5), null];
        yield 'a negative modulus cut to zero is left for the caller to refuse' => [10, Operator::modulo('-0.75'), null];
    }

    #[DataProvider('truncatedModuli')]
    public function testAnEngineThatCutAFractionalModulusTakesTheRemainderOfItsWholePart(int|string $stored, Operator $operator, int|string|null $expected): void
    {
        $resolved = self::fractional(FractionalEngine::Memory)->operator($operator, $stored);

        if ($expected === null) {
            $this->assertSame($operator, $resolved);

            return;
        }

        $change = $resolved->getValue();
        $this->assertTrue(BigInt::isIntegerValue($change));
        $this->assertSame($expected, $resolved->getMethod() === OperatorType::Increment ? BigInt::add($stored, $change) : BigInt::subtract($stored, $change));
    }

    /**
     * @return iterable<string, array{Operator, int}>
     */
    public static function oversizedLimits(): iterable
    {
        yield 'a huge maximum holds nothing back' => [Operator::multiply(1.5, '1e1000000000'), 15];
        yield 'a huge negative maximum holds everything back' => [Operator::multiply(1.5, '-1e1000000000'), 10];
        yield 'a tiny positive maximum holds back a positive result' => [Operator::multiply(1.5, '1e-1000000000'), 10];
        yield 'a huge negative minimum holds nothing back' => [Operator::divide(0.5, '-1e1000000000'), 20];
        yield 'a tiny negative minimum lets a positive result through' => [Operator::divide(0.5, '-1e-1000000000'), 20];
        yield 'a maximum with many fractional digits just above one holds back 15' => [Operator::multiply(1.5, '1.' . \str_repeat('0', 80) . '1'), 10];
        yield 'a maximum with many fractional digits just below sixteen lets 15 through' => [Operator::multiply(1.5, '15.' . \str_repeat('9', 80)), 15];
        yield 'a tiny factor leaves zero' => [Operator::multiply('1e-1000000000', 100), 0];
        yield 'an exponent written with leading zeros is read by its value' => [Operator::multiply('1.5e-00000', 100), 15];
        yield 'a long fraction scaled up by its exponent is read by its value' => [Operator::multiply('0.' . \str_repeat('0', 99) . '15e100', 100), 15];
    }

    #[DataProvider('oversizedLimits')]
    public function testALimitTooLongToWriteOutStillBoundsTheResult(Operator $operator, int $expected): void
    {
        $resolved = self::fractional(FractionalEngine::PostgreSQL)->operator($operator, 10);

        $this->assertSame($expected, self::stores(10, $resolved));
    }

    public function testFractionalArrayItemsAreCutTowardZero(): void
    {
        $this->assertSame([1, -1, -2, 2, 0, 7], Fractional::whole(Operator::arrayAppend([1.5, -1.5, -2.5, 2.5, 0.9999, 7]))->getValues());
        $this->assertSame([0, 3], Fractional::whole(Operator::arrayPrepend([0.5, 3.7]))->getValues());
        $this->assertSame([[1, 2], 3], Fractional::whole(Operator::arrayAppend([[1.5, 2], '3.9']))->getValues());
        $this->assertSame([1, 3], Fractional::whole(Operator::arrayInsert(1, 3.5))->getValues());
        $this->assertSame(['a', 2], Fractional::whole(Operator::arrayAppend(['a', 2]))->getValues());
        $this->assertSame([1.5], Fractional::whole(Operator::increment(1.5))->getValues());
    }

    public function testCuttingArrayItemsLeavesTheGivenOperatorAsItWas(): void
    {
        $operator = Operator::arrayAppend([1.5]);

        Fractional::whole($operator);

        $this->assertSame([1.5], $operator->getValues());
    }

    /**
     * @return iterable<string, array{FractionalEngine, bool, int, float, int|float|null, int|null}>
     */
    public static function counters(): iterable
    {
        return FractionalResults::counters();
    }

    #[DataProvider('counters')]
    public function testAFractionalCounterChangeStoresWhatTheEngineStored(FractionalEngine $engine, bool $increase, int $start, float $value, int|float|null $bound, ?int $stored): void
    {
        $fractional = self::fractional($engine);

        if ($stored === null) {
            $this->expectException(TypeException::class);
            $this->expectExceptionMessage('Change value must be an integer.');
        }

        $change = $fractional->counter($start, $value, $increase);

        $this->assertSame($stored, $increase ? BigInt::add($start, $change) : BigInt::subtract($start, $change));
    }

    /**
     * @return iterable<string, array{int|float|string, int|string}>
     */
    public static function wholeCounterChanges(): iterable
    {
        yield 'integer' => [2, 2];
        yield 'whole float' => [2.0, 2];
        yield 'integer text' => ['2', 2];
        yield 'integer text beyond the native range' => ['18446744073709551615', '18446744073709551615'];
        yield 'exponent text' => ['1e3', 1000];
        yield 'whole decimal exponent text' => ['2.50e1', 25];
        yield 'exponent text beyond the native range' => ['1e19', '10000000000000000000'];
    }

    #[DataProvider('wholeCounterChanges')]
    public function testAWholeCounterChangeIsTheChangeOnEveryEngine(int|float|string $value, int|string $expected): void
    {
        foreach (FractionalEngine::cases() as $engine) {
            $this->assertSame($expected, self::fractional($engine)->counter(5, $value, true), $engine->name);
        }
    }

    public function testFractionalTextChangesAsItsNumber(): void
    {
        $this->assertSame(1, self::fractional(FractionalEngine::MariaDB)->counter(5, '1.5', true));
    }

    /**
     * @return iterable<string, array{int|float|string, string}>
     */
    public static function unusableCounterChanges(): iterable
    {
        yield 'infinite' => [\INF, 'Change value must be an integer.'];
        yield 'not a number' => [\NAN, 'Change value must be an integer.'];
        yield 'non-numeric text' => ['ten', 'Value must be numeric and greater than 0'];
        yield 'zero' => [0, 'Value must be numeric and greater than 0'];
        yield 'negative' => [-1, 'Value must be numeric and greater than 0'];
        yield 'negative fraction' => [-1.5, 'Value must be numeric and greater than 0'];
        yield 'negative fractional text' => ['-0.5', 'Value must be numeric and greater than 0'];
        yield 'negative integer text' => ['-18446744073709551615', 'Value must be numeric and greater than 0'];
    }

    #[DataProvider('unusableCounterChanges')]
    public function testACounterChangeThatIsNotAPositiveNumberIsRefused(int|float|string $value, string $message): void
    {
        $this->expectException(TypeException::class);
        $this->expectExceptionMessage($message);

        self::fractional(FractionalEngine::MariaDB)->counter(5, $value, true);
    }

    public function testAFractionalStoredValueIsReadCutTowardZero(): void
    {
        $resolved = self::fractional(FractionalEngine::MongoDB)->operator(Operator::increment(1.5), 6.5);

        $this->assertSame(7, self::stores(6, $resolved));
    }

    public function testAnOperatorThatIsNotNumericOrHasANonNumericOperandIsUnchanged(): void
    {
        $fractional = self::fractional(FractionalEngine::MariaDB);

        foreach ([Operator::arrayAppend([1.5]), Operator::increment('ten', 10.5), Operator::multiply(1.5, 'ten')] as $operator) {
            $this->assertSame($operator, $fractional->operator($operator, 10));
        }

        $operator = Operator::increment(1.5);
        $this->assertSame($operator, $fractional->operator($operator, 'ten'), 'A stored value that is not a number is left for the caller');
    }

    /**
     * @return iterable<string, array{int|float|string, bool}>
     */
    public static function changes(): iterable
    {
        yield 'integer' => [2, true];
        yield 'whole float' => [2.0, true];
        yield 'integer text' => ['2', true];
        yield 'fractional' => [1.5, false];
        yield 'fractional text' => ['1.5', false];
        yield 'infinite' => [\INF, false];
    }

    #[DataProvider('changes')]
    public function testOnlyAWholeChangeIsWhole(int|float|string $value, bool $whole): void
    {
        $this->assertSame($whole, Fractional::isWhole($value));
    }

    /**
     * @return iterable<string, array{mixed, bool}>
     */
    public static function values(): iterable
    {
        yield 'fractional float' => [1.5, true];
        yield 'fractional text' => ['-0.25', true];
        yield 'fractional exponent text' => ['1.25e1', true];
        yield 'integer' => [2, false];
        yield 'whole float' => [2.0, false];
        yield 'whole decimal text' => ['2.000', false];
        yield 'integer text' => ['2', false];
        yield 'infinite' => [\INF, false];
        yield 'not a number' => [\NAN, false];
        yield 'non-numeric text' => ['1.5x', false];
        yield 'null' => [null, false];
        yield 'array' => [[1.5], false];
    }

    #[DataProvider('values')]
    public function testOnlyAFiniteNumberWithAFractionIsFractional(mixed $value, bool $fractional): void
    {
        $this->assertSame($fractional, Fractional::isFractional($value));
    }

    /**
     * @return iterable<string, array{int|float|string|null, int|float|string|null, int|float|string|null}>
     */
    public static function integerBounds(): iterable
    {
        yield 'fractional' => [10.5, 10, 11];
        yield 'negative fractional' => [-2.5, -3, -2];
        yield 'just under a whole number' => [9.999, 9, 10];
        yield 'whole float' => [10.0, 10, 10];
        yield 'integer' => [10, 10, 10];
        yield 'fractional text' => ['10.5', 10, 11];
        yield 'integer text' => ['10', '10', '10'];
        yield 'beyond the native integer range' => [1.0e19, '10000000000000000000', '10000000000000000000'];
        yield 'fractional text beyond float precision' => ['9007199254740993.5', 9007199254740993, 9007199254740994];
        yield 'negative fractional text beyond float precision' => ['-9007199254740993.5', -9007199254740994, -9007199254740993];
        yield 'negative fractional text' => ['-5.5', -6, -5];
        yield 'negative fraction of one text' => ['-0.5', -1, 0];
        yield 'fraction of one text' => ['0.5', 0, 1];
        yield 'whole decimal text' => ['10.000', 10, 10];
        yield 'leading zeros text' => ['0000000000000000000005.5', 5, 6];
        yield 'fractional text above the native integer range' => ['9223372036854775807.5', 9223372036854775807, '9223372036854775808'];
        yield 'fractional text below the native integer range' => ['-9223372036854775808.5', '-9223372036854775809', \PHP_INT_MIN];
        yield 'fractional text at the unsigned limit' => ['18446744073709551615.9', '18446744073709551615', '18446744073709551616'];
        yield 'exponent text' => ['1e3', 1000, 1000];
        yield 'capital exponent text' => ['1E3', 1000, 1000];
        yield 'fractional exponent text' => ['1.25e1', 12, 13];
        yield 'negative fractional exponent text' => ['-1.25e1', -13, -12];
        yield 'negative exponent text' => ['1e-3', 0, 1];
        yield 'negative value with a negative exponent text' => ['-1e-3', -1, 0];
        yield 'vanishing exponent text' => ['1e-999999999', 0, 1];
        yield 'negative value with a vanishing exponent text' => ['-1e-999999999', -1, 0];
        yield 'exponent beyond the native integer range text' => ['5e-9223372036854775808', 0, 1];
        yield 'zero exponent text beyond float precision' => ['9007199254740993.5e0', 9007199254740993, 9007199254740994];
        yield 'padded fractional text' => [' 10.5 ', 10, 11];
        yield 'fractional text padded with form feed and vertical tab' => ["\f10.5\v", 10, 11];
        yield 'signed fractional text' => ['+10.5', 10, 11];
        yield 'fraction without a whole part text' => ['.5', 0, 1];
        yield 'whole part without a fraction text' => ['5.', 5, 5];
        yield 'negative zero text' => ['-0.0', 0, 0];
        yield 'zero with a huge exponent text' => ['0e999999999', 0, 0];
        yield 'zero decimal with an exponent text' => ['0.000e50', 0, 0];
        yield 'negative zero with an exponent text' => ['-0e5', 0, 0];
        yield 'no bound' => [null, null, null];
    }

    #[DataProvider('integerBounds')]
    public function testAFractionalMaximumRoundsDownAndAMinimumUp(int|float|string|null $bound, int|float|string|null $maximum, int|float|string|null $minimum): void
    {
        $this->assertSame($maximum, Fractional::maximum($bound));
        $this->assertSame($minimum, Fractional::minimum($bound));
    }

    /**
     * @return iterable<string, array{int|float|string}>
     */
    public static function nonNumericBounds(): iterable
    {
        yield 'empty' => [''];
        yield 'exponent without digits' => ['1e'];
        yield 'hexadecimal' => ['0x1A'];
        yield 'digit separators' => ['1_000'];
        yield 'two decimal points' => ['1.2.3'];
        yield 'two signs' => ['--5'];
        yield 'a word' => ['ten'];
        yield 'infinite' => [\INF];
    }

    #[DataProvider('nonNumericBounds')]
    public function testABoundThatIsNotAFiniteNumberIsPassedOnUnchanged(int|float|string $bound): void
    {
        $this->assertSame($bound, Fractional::maximum($bound));
        $this->assertSame($bound, Fractional::minimum($bound));
    }

    private static function fractional(FractionalEngine $engine): Fractional
    {
        $adapter = match ($engine) {
            FractionalEngine::MariaDB => new MariaDB(new stdClass()),
            FractionalEngine::PostgreSQL => new Postgres(new stdClass()),
            FractionalEngine::Memory => new Memory(),
            FractionalEngine::MongoDB => new Mongo(new class () extends Client {
                public function __construct()
                {
                }

                #[\Override]
                public function connect(): self
                {
                    return $this;
                }

                #[\Override]
                public function close(): void
                {
                }
            }),
        };

        return new Fractional($adapter->arithmetic());
    }

    private static function stores(int $stored, Operator $resolved): int|string
    {
        $change = $resolved->getValue();
        self::assertTrue(BigInt::isIntegerValue($change), 'A resolved change is a whole number');

        return $resolved->getMethod() === OperatorType::Increment
            ? BigInt::add($stored, $change)
            : BigInt::subtract($stored, $change);
    }
}
