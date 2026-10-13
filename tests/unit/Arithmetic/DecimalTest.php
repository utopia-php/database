<?php

namespace Tests\Unit\Arithmetic;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Arithmetic\Decimal;

final class DecimalTest extends TestCase
{
    /**
     * @return iterable<string, array{string}>
     */
    public static function nonDecimals(): iterable
    {
        yield 'empty' => [''];
        yield 'whitespace' => [' '];
        yield 'sign without digits' => ['+'];
        yield 'point without digits' => ['.'];
        yield 'exponent without a mantissa' => ['e5'];
        yield 'exponent without digits' => ['1e'];
        yield 'exponent without a mantissa digit' => ['.e5'];
        yield 'exponent with two signs' => ['1e-+5'];
        yield 'two exponent markers' => ['1ee5'];
        yield 'fractional exponent' => ['1e5.0'];
        yield 'hexadecimal' => ['0x1A'];
        yield 'digit separators' => ['1_000'];
        yield 'two decimal points' => ['1.2.3'];
        yield 'two signs' => ['--5'];
        yield 'mixed signs' => ['+-1'];
        yield 'infinity' => ['INF'];
        yield 'padded with a null byte' => ["10.5\0"];
        yield 'integer padded with a null byte' => ["1\0"];
    }

    #[DataProvider('nonDecimals')]
    public function testTextThatIsNotADecimalIsNotParsed(string $value): void
    {
        $this->assertNull(Decimal::parse($value));
    }

    /**
     * @return iterable<string, array{int|float|string, int|string, int|string}>
     */
    public static function decimals(): iterable
    {
        yield 'fractional' => ['10.5', 10, 11];
        yield 'negative fraction of one' => ['-0.5', -1, 0];
        yield 'whole' => ['10.000', 10, 10];
        yield 'padded with every numeric whitespace' => [" \t\n\r\v\f10.5 \t\n\r\v\f", 10, 11];
        yield 'zero with an exponent' => ['-0.000e50', 0, 0];
        yield 'explicitly positive exponent' => ['1e+5', 100000, 100000];
        yield 'upper case exponent marker' => ['1.5E+2', 150, 150];
        yield 'point before the exponent' => ['1.e5', 100000, 100000];
        yield 'fraction without a whole part and an exponent' => ['-.5e1', -5, -5];
        yield 'negative exponent' => ['1.5e-1', 0, 1];
        yield 'fractional at the unsigned limit' => ['18446744073709551615.9', '18446744073709551615', '18446744073709551616'];
        yield 'beyond the unsigned limit' => ['1e400', '184467440737095516150', '184467440737095516150'];
        yield 'below the negative unsigned limit' => ['-1e400', '-184467440737095516150', '-184467440737095516150'];
        yield 'an integer' => [-7, -7, -7];
        yield 'a float' => [-2.5, -3, -2];
    }

    #[DataProvider('decimals')]
    public function testADecimalRoundsDownByFloorAndUpByCeiling(int|float|string $value, int|string $floor, int|string $ceiling): void
    {
        $decimal = Decimal::parse($value);

        $this->assertInstanceOf(Decimal::class, $decimal);
        $this->assertSame($floor, $decimal->floor());
        $this->assertSame($ceiling, $decimal->ceil());
    }

    /**
     * @return iterable<string, array{int|float|string, array{int|string, int}|null}>
     */
    public static function fractions(): iterable
    {
        yield 'fractional text' => ['1.5', [15, 1]];
        yield 'fractional float' => [1.5, [15, 1]];
        yield 'negative fraction' => ['-2.75', [-275, 2]];
        yield 'signed fraction' => ['+2.75', [275, 2]];
        yield 'exponent scaling a fraction up' => ['1.5e2', [150, 0]];
        yield 'exponent scaling down' => ['15e-3', [15, 3]];
        yield 'integer' => [10, [10, 0]];
        yield 'zero' => ['0.000', [0, 0]];
        yield 'beyond float precision' => ['9007199254740993.5', [90071992547409935, 1]];
        yield 'beyond the native integer range' => ['18446744073709551615.5', ['184467440737095516155', 1]];
        yield 'too large to write out' => ['1e1000000000', null];
        yield 'too close to zero to write out' => ['1e-1000000000', null];
    }

    /**
     * @param  array{int|string, int}|null  $expected
     */
    #[DataProvider('fractions')]
    public function testANumberIsAnIntegerOverAPowerOfTen(int|float|string $value, ?array $expected): void
    {
        $this->assertSame($expected, Decimal::fraction($value));
    }

    /**
     * @return iterable<string, array{int|float|string, int|null, bool}>
     */
    public static function orders(): iterable
    {
        yield 'hundreds' => [123, 2, false];
        yield 'thousandths' => ['0.001', -3, false];
        yield 'a negative fraction' => [-0.5, -1, false];
        yield 'zero' => [0, null, false];
        yield 'a zero with an exponent' => ['0e5', null, false];
        yield 'a huge exponent' => ['1e1000000000', 1000000000, false];
        yield 'a vanishing exponent' => ['1e-1000000000', -1000000000, true];
        yield 'just inside the written-out range' => ['1e-64', -64, false];
        yield 'just past the written-out range' => ['1e-65', -65, true];
        yield 'an exponent too long for an integer' => ['1e99999999999999999999', \PHP_INT_MAX, false];
        yield 'a negative exponent too long for an integer' => ['1e-99999999999999999999', \PHP_INT_MIN, true];
    }

    #[DataProvider('orders')]
    public function testTheOrderOfANumberIsThePowerOfTenOfItsLeadingDigit(int|float|string $value, ?int $order, bool $tiny): void
    {
        $this->assertSame($order, Decimal::order($value));
        $this->assertSame($tiny, Decimal::isTiny($value));
    }
}
