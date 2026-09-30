<?php

namespace Tests\Unit;

use PDO;
use PDOException;
use PDOStatement;

/**
 * A MySQL connection as a reconnecting driver presents it to the adapter: the first
 * statement that finds the session ended reconnects and rethrows, and the new session
 * holds no transaction. Prepared statements run through executeStatement(), which can
 * lose a deadlock.
 */
final class TransactionStateConnection extends PDO
{
    public int $begins = 0;

    public int $commits = 0;

    private bool $transaction = false;

    private bool $ended = false;

    private bool $deadlocked = false;

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
        $this->discardTransaction();
    }

    /**
     * Make the next statement lose a deadlock, as MariaDB and MySQL report it: the engine
     * rolls the whole transaction back, savepoints included.
     */
    public function deadlock(): void
    {
        $this->deadlocked = true;
    }

    public function executeStatement(): bool
    {
        if (! $this->deadlocked) {
            return true;
        }

        $this->deadlocked = false;
        $this->discardTransaction();

        $message = 'SQLSTATE[40001]: Serialization failure: 1213 Deadlock found when trying to get lock; try restarting transaction';
        $error = new class ($message) extends PDOException {
            public function __construct(string $message)
            {
                parent::__construct($message);
                $this->code = '40001';
            }
        };
        $error->errorInfo = ['40001', 1213, $message];

        throw $error;
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
        $this->discardTransaction();

        return true;
    }

    public function rollBack(): bool
    {
        if (! $this->transaction) {
            throw new PDOException('There is no active transaction');
        }

        $this->discardTransaction();

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

    private function discardTransaction(): void
    {
        $this->transaction = false;
        $this->savepoints = [];
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
