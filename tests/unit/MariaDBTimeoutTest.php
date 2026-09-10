<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;

final class MariaDBTimeoutTest extends TestCase
{
    public function testRepeatedSetAndClearOnlyWriteChangedSessionValues(): void
    {
        $statements = [];
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))
            ->method('exec')
            ->willReturnCallback(function (string $sql) use (&$statements): int {
                $statements[] = $sql;

                return 0;
            });

        $adapter = new MariaDB($pdo);
        $adapter->setTimeout(1000);
        $adapter->setTimeout(1000);
        $adapter->clearTimeout();
        $adapter->clearTimeout();

        $this->assertSame([
            'SET max_statement_time = 1.000000',
            'SET max_statement_time = 0.000000',
        ], $statements);
    }

    public function testTimeoutUsesFixedPointFormatting(): void
    {
        $milliseconds = PHP_INT_MAX;
        $expected = 'SET max_statement_time = '.\sprintf('%.6F', $milliseconds / 1000.0);

        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('exec')
            ->with($expected)
            ->willReturn(0);

        $adapter = new MariaDB($pdo);
        $adapter->setTimeout($milliseconds);
    }
}
