<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Collection;
use Utopia\Database\Validator\Authorization;

/**
 * The value of a sum() as the engine sends it over the text protocol: MariaDB and MySQL write a large double with an
 * exponent and no decimal point, and a DECIMAL sum past the range of an integer in full.
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
        yield 'decimal past the range of an integer' => ['18446744073709551614', 18446744073709551614.0];
        yield 'no rows' => [null, 0];
    }

    #[DataProvider('sums')]
    public function testASumIsReadAsTheNumberTheEngineSent(?string $sent, int|float $expected): void
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

        $this->assertSame($expected, $adapter->sum(Collection::create(id: 'readings'), 'reading'));
    }
}
