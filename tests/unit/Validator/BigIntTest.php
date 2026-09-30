<?php

namespace Tests\Unit\Validator;

use InvalidArgumentException;
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
}
