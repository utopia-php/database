<?php

namespace Tests\Unit\Arithmetic;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Arithmetic\Rounding;

final class RoundingTest extends TestCase
{
    /**
     * @return iterable<string, array{float, int, int, int}>
     */
    public static function values(): iterable
    {
        yield 'a half below an even number' => [2.5, 2, 3, 2];
        yield 'a half below an odd number' => [3.5, 4, 4, 3];
        yield 'a negative half' => [-2.5, -2, -3, -2];
        yield 'a negative half below an odd number' => [-3.5, -4, -4, -3];
        yield 'above a half' => [2.6, 3, 3, 2];
        yield 'below a half' => [2.4, 2, 2, 2];
        yield 'a negative value above a half' => [-2.6, -3, -3, -2];
        yield 'a negative value below a half' => [-2.4, -2, -2, -2];
        yield 'a whole number' => [7.0, 7, 7, 7];
        yield 'a half of zero' => [0.5, 0, 1, 0];
        yield 'a negative half of zero' => [-0.5, 0, -1, 0];
    }

    #[DataProvider('values')]
    public function testAFloatIsRoundedAsEachEngineStoredIt(float $value, int $halfEven, int $halfAwayFromZero, int $towardZero): void
    {
        $this->assertSame($halfEven, Rounding::HalfEven->round($value));
        $this->assertSame($halfAwayFromZero, Rounding::HalfAwayFromZero->round($value));
        $this->assertSame($towardZero, Rounding::TowardZero->round($value));
    }

    /**
     * @return iterable<string, array{int|string, int|string, int|string, int|string, int|string}>
     */
    public static function ratios(): iterable
    {
        yield 'a half below an even number' => [5, 2, 2, 3, 2];
        yield 'a half below an odd number' => [7, 2, 4, 4, 3];
        yield 'a negative half' => [-5, 2, -2, -3, -2];
        yield 'a negative half below an odd number' => [-7, 2, -4, -4, -3];
        yield 'above a half' => [11, 4, 3, 3, 2];
        yield 'below a half' => [9, 4, 2, 2, 2];
        yield 'a negative value above a half' => [-11, 4, -3, -3, -2];
        yield 'exact' => [6, 3, 2, 2, 2];
        yield 'a half beyond float precision' => ['18014398509481987', 2, 9007199254740994, 9007199254740994, 9007199254740993];
        yield 'a half beyond the native integer range' => ['36893488147419103231', 2, '18446744073709551616', '18446744073709551616', '18446744073709551615'];
    }

    #[DataProvider('ratios')]
    public function testARatioIsRoundedExactly(int|string $numerator, int|string $denominator, int|string $halfEven, int|string $halfAwayFromZero, int|string $towardZero): void
    {
        $this->assertSame($halfEven, Rounding::HalfEven->ratio($numerator, $denominator));
        $this->assertSame($halfAwayFromZero, Rounding::HalfAwayFromZero->ratio($numerator, $denominator));
        $this->assertSame($towardZero, Rounding::TowardZero->ratio($numerator, $denominator));
    }

    /**
     * @return iterable<string, array{int|string, float, int|string, int|string, int|string}>
     */
    public static function fractions(): iterable
    {
        yield 'no fraction keeps the base' => [9007199254740993, 0.0, 9007199254740993, 9007199254740993, 9007199254740993];
        yield 'a half above an odd base beyond float precision' => [9007199254740993, 0.5, 9007199254740994, 9007199254740994, 9007199254740993];
        yield 'a half above an even base beyond float precision' => [9007199254740994, 0.5, 9007199254740994, 9007199254740995, 9007199254740994];
        yield 'above a half beyond float precision' => [9007199254740993, 0.75, 9007199254740994, 9007199254740994, 9007199254740993];
        yield 'a half above a negative base' => [-9007199254740995, 0.5, -9007199254740994, -9007199254740995, -9007199254740994];
        yield 'a half above the unsigned limit' => ['18446744073709551615', 0.5, '18446744073709551616', '18446744073709551616', '18446744073709551615'];
        yield 'a half above the native integer maximum' => [\PHP_INT_MAX, 0.5, '9223372036854775808', '9223372036854775808', \PHP_INT_MAX];
    }

    #[DataProvider('fractions')]
    public function testABaseAndItsFractionAreRoundedWithoutPassingThroughAFloat(int|string $base, float $fraction, int|string $halfEven, int|string $halfAwayFromZero, int|string $towardZero): void
    {
        $this->assertSame($halfEven, Rounding::HalfEven->apply($base, $fraction));
        $this->assertSame($halfAwayFromZero, Rounding::HalfAwayFromZero->apply($base, $fraction));
        $this->assertSame($towardZero, Rounding::TowardZero->apply($base, $fraction));
    }
}
