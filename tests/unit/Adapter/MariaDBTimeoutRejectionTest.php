<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;

final class MariaDBTimeoutRejectionTest extends TestCase
{
    /**
     * @return iterable<string, array{class-string<MariaDB>, int, Event}>
     */
    public static function nonPositiveTimeouts(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class] as $engine => $class) {
            yield $engine . ' zero' => [$class, 0, Event::All];
            yield $engine . ' negative' => [$class, -1, Event::All];
            yield $engine . ' zero for one event' => [$class, 0, Event::DocumentFind];
            yield $engine . ' negative for one event' => [$class, -250, Event::DocumentFind];
        }
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('nonPositiveTimeouts')]
    public function testNonPositiveTimeoutIsRejected(string $class, int $milliseconds, Event $event): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->never())->method('exec');

        $adapter = new $class($pdo);

        try {
            $adapter->setTimeout($milliseconds, $event);
            $this->fail('A timeout that is not positive must be rejected');
        } catch (DatabaseException $error) {
            $this->assertSame('Timeout must be greater than 0', $error->getMessage());
        }

        $this->assertSame(0, $adapter->getTimeout());
        $this->assertSame(0, $adapter->getTimeout($event));
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('nonPositiveTimeouts')]
    public function testRejectedTimeoutKeepsTheOneSetBefore(string $class, int $milliseconds, Event $event): void
    {
        $statements = [];
        $pdo = $this->createStub(\PDO::class);
        $pdo->method('exec')->willReturnCallback(function (string $statement) use (&$statements): int {
            $statements[] = $statement;

            return 0;
        });

        $adapter = new $class($pdo);
        $adapter->setTimeout(1000);
        $adapter->setTimeout(400, Event::DocumentFind);
        $applied = $statements;

        try {
            $adapter->setTimeout($milliseconds, $event);
            $this->fail('A timeout that is not positive must be rejected');
        } catch (DatabaseException $error) {
            $this->assertSame('Timeout must be greater than 0', $error->getMessage());
        }

        $this->assertSame($applied, $statements);
        $this->assertSame(1000, $adapter->getTimeout());
        $this->assertSame(400, $adapter->getTimeout(Event::DocumentFind));
    }
}
