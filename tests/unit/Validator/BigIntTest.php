<?php

namespace Tests\Unit\Validator;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\BigInt;
use Utopia\Query\Schema\ColumnType;

final class BigIntTest extends TestCase
{
    public function testTypeIsTheBigIntegerColumnType(): void
    {
        $this->assertSame(ColumnType::BigInteger->value, (new BigInt(true))->getType());
        $this->assertSame(ColumnType::BigInteger->value, (new BigInt(false))->getType());
    }

    public function testNegatingZeroGivesTheIntegerZero(): void
    {
        $this->assertSame(0, BigInt::negate(0));
        $this->assertSame(0, BigInt::negate('0'));
        $this->assertSame(0, BigInt::negate('-0'));
        $this->assertSame(5, BigInt::subtract(5, 0));
    }

    public function testDivisionSignsTheQuotient(): void
    {
        $this->assertSame(-3, BigInt::divide(-10, 3));
        $this->assertSame(-3, BigInt::divide(10, -3));
        $this->assertSame(3, BigInt::divide(-10, -3));
        $this->assertSame(0, BigInt::divide(-1, 3));
    }

    public function testDivisionByZeroIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Division by zero is not allowed.');

        BigInt::divide(1, 0);
    }

    public function testModuloByZeroIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Modulo by zero is not allowed.');

        BigInt::modulo(5, 0);
    }

    public function testNormalizingAcceptsWholeFloatsAndIntegerStrings(): void
    {
        $this->assertSame('5', BigInt::normalizeInteger(5.0));
        $this->assertSame('-5', BigInt::normalizeInteger(-5.0));
        $this->assertSame('7', BigInt::normalizeInteger('007'));
        $this->assertSame('0', BigInt::normalizeInteger('-0'));
        $this->assertSame(BigInt::UNSIGNED_MAX, BigInt::normalizeInteger(BigInt::UNSIGNED_MAX));
    }

    #[DataProvider('valuesThatAreNotIntegers')]
    public function testNormalizingRejectsAValueThatIsNotAnInteger(mixed $value): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value must be an integer.');

        BigInt::normalizeInteger($value);
    }

    /**
     * @return iterable<string, array{mixed}>
     */
    public static function valuesThatAreNotIntegers(): iterable
    {
        yield 'a fractional float' => [1.5];
        yield 'infinity' => [\INF];
        yield 'not a number' => [\NAN];
        yield 'a float above the integer range' => [1e20];
        yield 'a float below the integer range' => [-1e20];
        yield 'a boolean' => [true];
        yield 'null' => [null];
        yield 'a word' => ['abc'];
        yield 'a decimal string' => ['1.5'];
    }

    public function testPowerRefusesANegativeExponent(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Integer power exponent must not be negative.');

        BigInt::power(5, -1);
    }

    public function testPowerOfZeroExponentAndUnitBases(): void
    {
        $this->assertSame(1, BigInt::power(2, 0));
        $this->assertSame(1, BigInt::power(0, 0));
        $this->assertSame(0, BigInt::power(0, 99));
        $this->assertSame(1, BigInt::power(1, 99));
        $this->assertSame(-1, BigInt::power(-1, 3));
        $this->assertSame(1, BigInt::power(-1, 4));
        $this->assertSame(-1, BigInt::power(-1, '99999999999999999999999'));
    }

    public function testPowerAboveSixtyFourIsAnOverflowSentinel(): void
    {
        $this->assertSame(BigInt::UNSIGNED_MAX.'0', BigInt::power(2, 65));
        $this->assertSame('-'.BigInt::UNSIGNED_MAX.'0', BigInt::power(-2, 65));
        $this->assertSame(BigInt::UNSIGNED_MAX.'0', BigInt::power(-2, 66));
        $this->assertSame(BigInt::UNSIGNED_MAX.'0', BigInt::power(2, '99999999999999999999'));
    }

    public function testNativeIntegerArithmeticAtTheSignedLimits(): void
    {
        $this->assertSame(\PHP_INT_MAX, BigInt::add(\PHP_INT_MAX - 1, 1));
        $this->assertSame(\PHP_INT_MIN, BigInt::add(\PHP_INT_MIN + 1, -1));
        $this->assertSame(\PHP_INT_MAX, BigInt::subtract(\PHP_INT_MAX - 1, -1));
        $this->assertSame(12, BigInt::add('5', '7'));
        $this->assertSame('9223372036854775808', BigInt::add(\PHP_INT_MAX, 1));
        $this->assertSame('-9223372036854775809', BigInt::add(\PHP_INT_MIN, -1));
        $this->assertSame(\PHP_INT_MIN, BigInt::subtract(\PHP_INT_MIN + 1, 1));
        $this->assertSame('-9223372036854775809', BigInt::subtract(\PHP_INT_MIN, 1));
        $this->assertSame('9223372036854775808', BigInt::negate(\PHP_INT_MIN));
        $this->assertSame(-\PHP_INT_MAX, BigInt::negate(\PHP_INT_MAX));
        $this->assertSame(0, BigInt::add(5, -5));
    }

    public function testComparingNativeAndStringIntegersAgrees(): void
    {
        $this->assertSame(-1, BigInt::compare(-2, 1));
        $this->assertSame(0, BigInt::compare(7, '7'));
        $this->assertSame(1, BigInt::compare(\PHP_INT_MAX, \PHP_INT_MIN));
        $this->assertSame(-1, BigInt::compare(\PHP_INT_MAX, BigInt::UNSIGNED_MAX));
    }

    public function testNativeIntegersNormalizeToTheirDigits(): void
    {
        $this->assertSame('-42', BigInt::normalizeInteger(-42));
        $this->assertSame('0', BigInt::normalizeInteger(0));
        $this->assertSame(\PHP_INT_MIN, BigInt::toNative(\PHP_INT_MIN));
        $this->assertSame(12, BigInt::toNative(12));
    }
}
