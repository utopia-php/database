<?php

namespace Tests\Unit\Exception;

use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Database\Exception;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Order;
use Utopia\Database\Exception\Structure;

final class StateTest extends TestCase
{
    /**
     * @return array<string, array{int|string, int, ?string}>
     */
    public static function codes(): array
    {
        return [
            'integer' => [1062, 1062, null],
            'zero' => [0, 0, null],
            'alphanumeric sqlstate' => ['HY000', 0, 'HY000'],
            'numeric sqlstate' => ['23000', 23000, '23000'],
            'sqlstate with a leading zero' => ['08S01', 0, '08S01'],
            'empty string' => ['', 0, ''],
        ];
    }

    #[DataProvider('codes')]
    public function testTheCodeAndTheState(int|string $code, int $expectedCode, ?string $expectedState): void
    {
        $exception = new Exception('failed', $code);

        $this->assertSame($expectedCode, $exception->getCode());
        $this->assertSame($expectedState, $exception->state);
    }

    public function testEveryArgumentIsOptional(): void
    {
        $exception = new Exception();

        $this->assertSame('', $exception->getMessage());
        $this->assertSame(0, $exception->getCode());
        $this->assertNull($exception->state);
        $this->assertNull($exception->getPrevious());
    }

    public function testAMappedDriverErrorKeepsItsSqlstate(): void
    {
        $driver = new class ('SQLSTATE[HY000]: General error: 2006 MySQL server has gone away') extends PDOException {
            protected $code = 'HY000';
        };

        $exception = new Duplicate('Document already exists', $driver->getCode(), $driver);

        $this->assertSame('HY000', $exception->state);
        $this->assertSame(0, $exception->getCode());
        $this->assertSame($driver, $exception->getPrevious());
    }

    public function testSubclassesKeepTheState(): void
    {
        $previous = new RuntimeException('previous');
        $exception = new Structure('Invalid document structure', '42S22', $previous);

        $this->assertSame('42S22', $exception->state);
        $this->assertSame($previous, $exception->getPrevious());
    }

    public function testOrderTakesTheAttributeBeforeTheCode(): void
    {
        $previous = new RuntimeException('previous');
        $exception = new Order('Order attribute is empty', 'name', 'HY000', $previous);

        $this->assertSame('Order attribute is empty', $exception->getMessage());
        $this->assertSame('name', $exception->getAttribute());
        $this->assertSame(0, $exception->getCode());
        $this->assertSame('HY000', $exception->state);
        $this->assertSame($previous, $exception->getPrevious());
    }

    public function testOrderWithoutAnAttribute(): void
    {
        $exception = new Order('Invalid order');

        $this->assertNull($exception->getAttribute());
        $this->assertSame(0, $exception->getCode());
        $this->assertNull($exception->state);
    }
}
