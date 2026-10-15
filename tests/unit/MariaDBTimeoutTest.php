<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Swoole\Database\PDOProxy;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\PDO as DatabasePDO;

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

    /**
     * @return iterable<string, array{class-string<MariaDB>, string, string}>
     */
    public static function engines(): iterable
    {
        yield 'MariaDB' => [MariaDB::class, 'SET max_statement_time = 1.000000', 'SET max_statement_time = 0.000000'];
        yield 'MySQL' => [MySQL::class, 'SET SESSION MAX_EXECUTION_TIME = 1000', 'SET SESSION MAX_EXECUTION_TIME = 0'];
    }

    /**
     * The wrapper runs its configured session again on the connection a transparent
     * reconnect opens, before it retries the call that found the old one gone.
     *
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testTimeoutIsConfiguredAsSessionStateOfTheDatabasePDO(string $class, string $set, string $clear): void
    {
        $configured = [];
        $pdo = $this->createMock(DatabasePDO::class);
        $pdo->expects($this->exactly(2))
            ->method('configure')
            ->willReturnCallback(function (string $setting, string $statement) use (&$configured): void {
                $configured[] = [$setting, $statement];
            });

        $adapter = new $class($pdo);
        $adapter->setTimeout(1000);
        $adapter->setTimeout(1000);
        $adapter->clearTimeout();

        $this->assertSame([$set, $clear], \array_column($configured, 1));
        $this->assertCount(1, \array_unique(\array_column($configured, 0)), 'Clearing must replace the setting, not add another');
    }

    /**
     * Swoole's PDOProxy reconnects on its own, counting each reconnect as a round,
     * and its new session starts from the server default.
     *
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testTimeoutIsSetAgainAfterSwooleProxyReconnects(string $class, string $set, string $clear): void
    {
        if (! \class_exists(PDOProxy::class)) {
            $this->markTestSkipped('Swoole\'s library is not loaded');
        }

        $round = 0;
        $statements = [];
        $proxy = $this->createStub(PDOProxy::class);
        $proxy->method('getRound')->willReturnCallback(function () use (&$round): int {
            return $round;
        });
        $proxy->method('__call')->willReturnCallback(function (string $name, array $arguments) use (&$statements): int {
            $statements[] = [$name, ...$arguments];

            return 0;
        });

        $adapter = new $class($proxy);
        $adapter->setTimeout(1000);
        $round = 1;
        $adapter->setTimeout(1000);
        $adapter->clearTimeout();
        $round = 2;
        $adapter->clearTimeout();

        $this->assertSame([['exec', $set], ['exec', $set], ['exec', $clear]], $statements);
    }
}
