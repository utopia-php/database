<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Event;

final class SQLTimeoutScopeTest extends TestCase
{
    public function testMySQLGlobalTimeoutUsesOnlyTheMySQLSessionVariable(): void
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

        $adapter = new MySQL($pdo);
        $adapter->setTimeout(1000);
        $adapter->clearTimeout();

        $this->assertSame([
            'SET SESSION MAX_EXECUTION_TIME = 1000',
            'SET SESSION MAX_EXECUTION_TIME = 0',
        ], $statements);
    }

    public function testClearingOneScopePreservesTheGlobalTimeout(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))->method('exec')->willReturn(0);

        $adapter = new MariaDB($pdo);
        $adapter->setTimeout(1000);
        $adapter->setTimeout(25, Event::DocumentFind);

        $this->assertSame(25, $adapter->getTimeout(Event::DocumentFind));
        $this->assertSame(1000, $adapter->getTimeout(Event::DocumentCreate));

        $adapter->clearTimeout(Event::DocumentFind);

        $this->assertSame(1000, $adapter->getTimeout(Event::DocumentFind));
        $this->assertSame(1000, $adapter->getTimeout(Event::DocumentCreate));

        $adapter->clearTimeout();
        $this->assertSame(0, $adapter->getTimeout(Event::DocumentFind));
    }

    /**
     * @param class-string<SQL> $adapterClass
     * @param list<string> $expected
     */
    #[DataProvider('adapters')]
    public function testReadScopeIsAppliedOnlyAroundMatchingOperations(string $adapterClass, array $expected): void
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

        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->exactly(2))->method('execute')->willReturn(true);

        $adapter = new $adapterClass($pdo);
        $adapter->setTimeout(25, Event::DocumentFind);
        $execute = new ReflectionMethod($adapter, 'execute');

        $this->assertTrue($execute->invoke($adapter, $statement, Event::DocumentFind));
        $this->assertTrue($execute->invoke($adapter, $statement, Event::DocumentCreate));
        $this->assertSame($expected, $statements);
    }

    /**
     * @return iterable<string, array{class-string<SQL>, list<string>}>
     */
    public static function adapters(): iterable
    {
        yield 'MariaDB' => [MariaDB::class, [
            'SET max_statement_time = 0.025000',
            'SET max_statement_time = 0.000000',
        ]];
        yield 'MySQL' => [MySQL::class, [
            'SET SESSION MAX_EXECUTION_TIME = 25',
            'SET SESSION MAX_EXECUTION_TIME = 0',
        ]];
        yield 'PostgreSQL' => [Postgres::class, [
            "SET statement_timeout = '25ms'",
            'RESET statement_timeout',
        ]];
    }
}
