<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Collection;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Validator\Authorization;

/**
 * The value of a sum() as the engine sends it over the text protocol: MariaDB and MySQL write a large double with an
 * exponent and no decimal point, and a DECIMAL sum past the range of an integer in full, which is refused.
 */
final class SumResultTest extends TestCase
{
    /**
     * @return iterable<string, array{string|null, int|float}>
     */
    public static function sums(): iterable
    {
        yield 'integer' => ['2785', 2785];
        yield 'decimal' => ['10.50', 10.5];
        yield 'double with an exponent' => ['1e308', 1e308];
        yield 'negative double with an exponent' => ['-1.5e300', -1.5e300];
        yield 'decimal at the top of the range of an integer' => ['9223372036854775807', PHP_INT_MAX];
        yield 'decimal at the bottom of the range of an integer' => ['-9223372036854775808', PHP_INT_MIN];
        yield 'no rows' => [null, 0];
        yield 'infinity' => ['INF', \INF];
        yield 'negative infinity' => ['-INF', -\INF];
        yield 'infinity in another case' => ['Inf', \INF];
        yield 'not a number word' => ['overflow', 0];
    }

    #[DataProvider('sums')]
    public function testASumIsReadAsTheNumberTheEngineSent(?string $sent, int|float $expected): void
    {
        $this->assertSame($expected, $this->sumOf($sent));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function integerSumsPastTheRangeOfAnInteger(): iterable
    {
        yield 'above, as MySQL and PostgreSQL sum it' => ['18446744073709551614'];
        yield 'above, as MariaDB sums it' => ['18446744073709551614.0000'];
        yield 'one above' => ['9223372036854775808'];
        yield 'one below' => ['-9223372036854775809'];
    }

    #[DataProvider('integerSumsPastTheRangeOfAnInteger')]
    public function testAnIntegerSumPastTheRangeOfAnIntegerIsRefused(string $sent): void
    {
        $this->expectException(LimitException::class);
        $this->sumOf($sent);
    }

    public function testANotANumberSumIsReadAsNan(): void
    {
        $this->assertNan($this->sumOf('NAN'));
    }

    private function sumOf(?string $sent): int|float
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function () use ($sent): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn([['sum' => $sent]]);
            $statement->method('closeCursor')->willReturn(true);

            return $statement;
        });

        $adapter = new MariaDB($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setAuthorization(new Authorization());

        return $adapter->sum(Collection::create(id: 'readings'), 'reading');
    }
}
