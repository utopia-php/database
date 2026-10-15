<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Validator\Authorization;

/**
 * A LOCAL setting lasts until its transaction ends or a savepoint set before it
 * is rolled back to, so inside a transaction the adapter writes the statement
 * timeout only when the next statement needs a different one.
 */
final class PostgresTimeoutStatementTest extends TestCase
{
    private const string SET_LOCAL = "SET LOCAL statement_timeout = '25ms'";

    private const string DEFAULT_LOCAL = 'SET LOCAL statement_timeout = DEFAULT';

    /** @var list<string> */
    private array $log = [];

    public function testStatementsOfOneTransactionShareOneLocalTimeout(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25, Event::DocumentFind);

        $adapter->startTransaction();
        $this->find($adapter);
        $this->find($adapter);
        $this->find($adapter);
        $adapter->commitTransaction();

        $this->assertSame([self::SET_LOCAL, 'SELECT', 'SELECT', 'SELECT'], $this->log);
    }

    public function testGlobalTimeoutIsSetOncePerTransaction(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25);

        $adapter->startTransaction();
        $this->find($adapter);
        $this->create($adapter);
        $this->find($adapter);
        $adapter->commitTransaction();

        $this->assertSame([self::SET_LOCAL, 'SELECT', 'INSERT', 'SELECT'], $this->log);
    }

    public function testReadScopedTimeoutDoesNotCarryToACreateInTheSameTransaction(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25, Event::DocumentFind);

        $adapter->startTransaction();
        $this->find($adapter);
        $this->create($adapter);
        $this->create($adapter);
        $adapter->commitTransaction();

        $this->assertSame([self::SET_LOCAL, 'SELECT', self::DEFAULT_LOCAL, 'INSERT', 'INSERT'], $this->log);
    }

    public function testStatementsWithoutATimeoutInATransactionWriteNone(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25, Event::DocumentFind);

        $adapter->startTransaction();
        $this->create($adapter);
        $this->create($adapter);
        $adapter->commitTransaction();

        $this->assertSame(['INSERT', 'INSERT'], $this->log);
    }

    public function testNextTransactionSetsItsTimeoutAgain(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25);

        $adapter->startTransaction();
        $this->find($adapter);
        $adapter->commitTransaction();
        $adapter->startTransaction();
        $this->find($adapter);
        $adapter->rollbackTransaction();
        $adapter->startTransaction();
        $this->find($adapter);
        $adapter->commitTransaction();

        $this->assertSame([self::SET_LOCAL, 'SELECT', self::SET_LOCAL, 'SELECT', self::SET_LOCAL, 'SELECT'], $this->log);
    }

    public function testRollbackToASavepointUndoesATimeoutSetAfterIt(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25, Event::DocumentFind);

        $adapter->startTransaction();
        $adapter->startTransaction();
        $this->find($adapter);
        $adapter->rollbackTransaction();
        $this->find($adapter);
        $adapter->commitTransaction();

        $this->assertSame([self::SET_LOCAL, 'SELECT', 'ROLLBACK TO transaction1', self::SET_LOCAL, 'SELECT'], $this->log);
    }

    /**
     * The rollback brings back the read timeout set before the savepoint, so the
     * create after it has to clear that timeout again.
     */
    public function testCreateAfterARollbackToASavepointRunsWithoutTheReadTimeout(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25, Event::DocumentFind);

        $adapter->startTransaction();
        $this->find($adapter);
        $adapter->startTransaction();
        $this->create($adapter);
        $adapter->rollbackTransaction();
        $this->create($adapter);
        $adapter->commitTransaction();

        $this->assertSame([
            self::SET_LOCAL,
            'SELECT',
            self::DEFAULT_LOCAL,
            'INSERT',
            'ROLLBACK TO transaction1',
            self::DEFAULT_LOCAL,
            'INSERT',
        ], $this->log);
    }

    public function testReconnectInsideATransactionForgetsTheLocalTimeout(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25);

        $adapter->startTransaction();
        $this->find($adapter);
        $adapter->reconnect();
        $adapter->startTransaction();
        $this->find($adapter);
        $adapter->commitTransaction();

        $this->assertSame([self::SET_LOCAL, 'SELECT', self::SET_LOCAL, 'SELECT'], $this->log);
    }

    public function testNestedCommitKeepsTheTimeoutSetInsideIt(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25, Event::DocumentFind);

        $adapter->startTransaction();
        $adapter->startTransaction();
        $this->find($adapter);
        $adapter->commitTransaction();
        $this->find($adapter);
        $adapter->commitTransaction();

        $this->assertSame([self::SET_LOCAL, 'SELECT', 'SELECT'], $this->log);
    }

    public function testStatementsOutsideATransactionSetAndResetTheSessionTimeout(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setTimeout(25, Event::DocumentFind);

        $this->find($adapter);
        $this->create($adapter);
        $this->find($adapter);

        $this->assertSame([
            "SET statement_timeout = '25ms'",
            'SELECT',
            'RESET statement_timeout',
            'INSERT',
            "SET statement_timeout = '25ms'",
            'SELECT',
            'RESET statement_timeout',
        ], $this->log);
    }

    private function createAdapter(): Postgres
    {
        $this->log = [];
        $open = false;

        $pdo = $this->createStub(\PDO::class);
        $pdo->method('inTransaction')->willReturnCallback(function () use (&$open): bool {
            return $open;
        });
        $pdo->method('beginTransaction')->willReturnCallback(function () use (&$open): bool {
            $open = true;

            return true;
        });
        $pdo->method('commit')->willReturnCallback(function () use (&$open): bool {
            $open = false;

            return true;
        });
        $pdo->method('rollBack')->willReturnCallback(function () use (&$open): bool {
            $open = false;

            return true;
        });
        $pdo->method('exec')->willReturnCallback(function (string $sql): int {
            if (! \str_starts_with($sql, 'SAVEPOINT')) {
                $this->log[] = $sql;
            }

            return 0;
        });
        $pdo->method('lastInsertId')->willReturn('1');
        $pdo->method('prepare')->willReturnCallback(function (string $sql): \PDOStatement {
            $statement = $this->createStub(\PDOStatement::class);
            $statement->method('bindValue')->willReturn(true);
            $statement->method('execute')->willReturnCallback(function () use ($sql): bool {
                $keyword = \strtoupper(\strtok(\ltrim($sql), " \n") ?: '');
                if ($keyword !== 'ROLLBACK') {
                    $this->log[] = $keyword;
                }

                return true;
            });
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('fetch')->willReturn(false);
            $statement->method('rowCount')->willReturn(1);
            $statement->method('closeCursor')->willReturn(true);

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        return $adapter;
    }

    private function find(Postgres $adapter): void
    {
        $adapter->find(new Document(['$id' => 'movies']), orderAttributes: ['$sequence']);
    }

    private function create(Postgres $adapter): void
    {
        $adapter->createDocument(new Document(['$id' => 'movies']), new Document(['$id' => 'movie', 'title' => 'Alien']));
    }
}
