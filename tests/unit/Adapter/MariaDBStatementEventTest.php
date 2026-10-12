<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Event;

final class MariaDBStatementEventTest extends TestCase
{
    /** @var list<string> */
    private array $sessionStatements = [];

    public function testAFirstStatementWithoutAnEventRunsUnderTheBaselineTimeout(): void
    {
        $adapter = $this->adapter();
        $adapter->setTimeout(50, Event::CollectionRead);

        $adapter->rawQuery('SELECT 1');

        $this->assertSame([], $this->sessionStatements);
    }

    public function testAStatementWithAnEventRunsUnderThatEventsTimeout(): void
    {
        $adapter = $this->adapter();
        $adapter->setTimeout(50, Event::CollectionRead);

        $adapter->rawQuery('SELECT 1');
        $adapter->getSizeOfCollection('notes');

        $this->assertSame([
            'SET max_statement_time = 0.050000',
            'SET max_statement_time = 0.000000',
        ], \array_values(\array_unique($this->sessionStatements)));
    }

    private function adapter(): MariaDB
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('fetchColumn')->willReturn('0');

            return $statement;
        });
        $pdo->method('exec')->willReturnCallback(function (string $statement): int {
            $this->sessionStatements[] = $statement;

            return 0;
        });

        $adapter = new MariaDB($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
