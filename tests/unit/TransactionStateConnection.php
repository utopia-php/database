<?php

namespace Tests\Unit;

use PDO;
use PDOException;
use PDOStatement;

/**
 * A MySQL connection as a reconnecting driver presents it to the adapter: the first
 * statement that finds the session ended reconnects and rethrows, and the new session
 * holds no transaction.
 */
final class TransactionStateConnection extends PDO
{
    public int $begins = 0;

    public int $commits = 0;

    private bool $transaction = false;

    private bool $ended = false;

    /**
     * @var array<string>
     */
    private array $savepoints = [];

    public function __construct(private readonly PDOStatement $statement)
    {
    }

    public function endSession(): void
    {
        $this->ended = true;
    }

    public function reconnectSilently(): void
    {
        $this->transaction = false;
        $this->savepoints = [];
    }

    public function beginTransaction(): bool
    {
        $this->reconnectIfEnded();
        $this->begins++;
        $this->transaction = true;

        return true;
    }

    public function commit(): bool
    {
        if (! $this->transaction) {
            throw new PDOException('There is no active transaction');
        }

        $this->commits++;
        $this->reconnectSilently();

        return true;
    }

    public function rollBack(): bool
    {
        if (! $this->transaction) {
            throw new PDOException('There is no active transaction');
        }

        $this->reconnectSilently();

        return true;
    }

    public function inTransaction(): bool
    {
        return $this->transaction;
    }

    public function exec(string $statement): int
    {
        $this->reconnectIfEnded();

        if (\str_starts_with($statement, 'SAVEPOINT ')) {
            $this->savepoints[] = \substr($statement, \strlen('SAVEPOINT '));

            return 0;
        }

        if (\str_starts_with($statement, 'ROLLBACK TO ')) {
            $savepoint = \substr($statement, \strlen('ROLLBACK TO '));
            if (! \in_array($savepoint, $this->savepoints, true)) {
                throw new PDOException("SQLSTATE[42000]: Syntax error or access violation: 1305 SAVEPOINT {$savepoint} does not exist");
            }
        }

        return 0;
    }

    /**
     * @param array<mixed> $options
     */
    public function prepare(string $query, array $options = []): PDOStatement
    {
        return $this->statement;
    }

    private function reconnectIfEnded(): void
    {
        if (! $this->ended) {
            return;
        }

        $this->ended = false;
        $this->reconnectSilently();

        throw new PDOException('SQLSTATE[HY000]: General error: 2006 MySQL server has gone away');
    }
}
