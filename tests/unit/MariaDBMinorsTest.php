<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Swoole\Database\PDOProxy;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Event;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

final class MariaDBMinorsTest extends TestCase
{
    /**
     * @return iterable<string, array{class-string<MariaDB>}>
     */
    public static function adapters(): iterable
    {
        yield 'MariaDB' => [MariaDB::class];
        yield 'MySQL' => [MySQL::class];
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
     * An object that is no driver fails on the first access, so any read of the
     * driver while no timeout is requested shows up as an exception.
     *
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('adapters')]
    public function testNoTimeoutNeverTouchesTheDriver(string $class): void
    {
        $adapter = new $class(new \stdClass());

        $adapter->clearTimeout();
        $adapter->clearTimeout(Event::DocumentFind);
        $adapter->setTimeout(1000, Event::DocumentFind);
        $adapter->clearTimeout(Event::DocumentFind);
        $adapter->clearTimeout();

        $this->assertSame(0, $adapter->getTimeout());
        $this->assertSame(0, $adapter->getTimeout(Event::DocumentFind));
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('adapters')]
    public function testPooledBorrowWithoutATimeoutNeverTouchesTheDriver(string $class): void
    {
        $connection = new $class(new \stdClass());
        $adapter = new Pool(new UtopiaPool(new Stack(), 'no-driver', 1, fn (): MariaDB => $connection, timeout: 0.0));
        $adapter->setAuthorization(new Authorization());

        $this->assertSame($connection->getMaxVarcharLength(), $adapter->getMaxVarcharLength());
        $this->assertSame($connection->getMaxVarcharLength(), $adapter->getMaxVarcharLength());
    }

    /**
     * Swoole's PDOProxy counts each reconnect as a round, and the session it opens
     * starts at the server default: a timeout set before the reconnect is written
     * again, and clearing it there needs no statement.
     *
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testTimeoutIsReappliedAfterAReconnect(string $class, string $set, string $clear): void
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
        $adapter->clearTimeout();
        $adapter->setTimeout(1000);
        $round = 1;
        $adapter->setTimeout(1000);
        $round = 2;
        $adapter->clearTimeout();
        $adapter->setTimeout(1000);

        $this->assertSame([
            ['exec', $set],
            ['exec', $clear],
            ['exec', $set],
            ['exec', $set],
            ['exec', $set],
        ], $statements);
    }
}
