<?php

namespace Tests\Unit;

use PDOException;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\EngineError;
use Throwable;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Transaction as TransactionException;

final class TransactionStateTest extends TestCase
{
    /**
     * The server ends the session after the outer callback wrote A. The nested SAVEPOINT
     * finds the connection gone, the connection reconnects and rethrows, and rolling back
     * on the new connection fails: the outer transaction and A are gone, so neither call
     * may begin a fresh transaction or commit.
     */
    public function testNestedTransactionAfterALostConnectionFailsTheOuterCall(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $writes = [];
        $nested = null;

        $outer = $this->capture(function () use ($adapter, $connection, &$writes, &$nested): void {
            $adapter->withTransaction(function () use ($adapter, $connection, &$writes, &$nested): void {
                $writes[] = 'A';
                $connection->endSession();

                $nested = $this->capture(function () use ($adapter, &$writes): void {
                    $adapter->withTransaction(function () use (&$writes): void {
                        $writes[] = 'B';
                    });
                });

                if ($nested !== null) {
                    throw $nested;
                }
            });
        });

        $this->assertInstanceOf(TransactionException::class, $nested, 'The nested call must fail once the enclosing transaction is lost');
        $this->assertInstanceOf(TransactionException::class, $outer, 'The outer call must fail once its transaction is lost');
        $this->assertSame(['A'], $writes, 'The nested work must not run in a fresh transaction of its own');
        $this->assertSame(1, $connection->begins, 'Only the outer call may begin a transaction');
        $this->assertSame(0, $connection->commits, 'Nothing may be committed after the transaction was lost');
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A reconnect the callback did not surface leaves the driver without the transaction
     * the adapter still counts. The nested commit cannot release a savepoint of a
     * transaction the connection no longer holds.
     */
    public function testNestedCommitAfterTheDriverLostTheTransactionFailsTheOuterCall(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $nested = null;

        $outer = $this->capture(function () use ($adapter, $connection, &$nested): void {
            $adapter->withTransaction(function () use ($adapter, $connection, &$nested): void {
                $nested = $this->capture(function () use ($adapter, $connection): void {
                    $adapter->withTransaction(function () use ($connection): void {
                        $connection->reconnectSilently();
                    });
                });

                if ($nested !== null) {
                    throw $nested;
                }
            });
        });

        $this->assertInstanceOf(TransactionException::class, $nested, 'The nested commit must fail when the driver lost the transaction');
        $this->assertInstanceOf(TransactionException::class, $outer, 'The outer call must fail once its transaction is lost');
        $this->assertSame(1, $connection->begins, 'Only the outer call may begin a transaction');
        $this->assertSame(0, $connection->commits, 'Nothing may be committed after the transaction was lost');
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A caller that expects a duplicate from the nested call and carries on must not
     * receive it when the savepoint rollback found the enclosing transaction gone.
     */
    public function testNestedDuplicateAfterALostConnectionFailsTheOuterCall(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);

        $outer = $this->capture(function () use ($adapter, $connection): void {
            $adapter->withTransaction(function () use ($adapter, $connection): void {
                try {
                    $adapter->withTransaction(function () use ($connection): void {
                        $connection->endSession();

                        throw new DuplicateException('Document already exists');
                    });
                } catch (DuplicateException) {
                }
            });
        });

        $this->assertInstanceOf(TransactionException::class, $outer, 'A lost transaction must not surface as the duplicate the caller expects');
        $this->assertSame(1, $connection->begins, 'Only the outer call may begin a transaction');
        $this->assertSame(0, $connection->commits, 'Nothing may be committed after the transaction was lost');
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * While the enclosing transaction holds, a nested attempt that failed transiently rolls
     * back to its savepoint and runs again inside the same outer transaction. A lock wait
     * timeout rolls back only the statement, so the savepoint holds.
     */
    public function testNestedTransactionRetriesWhileTheOuterTransactionHolds(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $attempts = 0;
        $stored = \uniqid();

        $result = $adapter->withTransaction(function () use ($adapter, &$attempts, $stored): string {
            return $adapter->withTransaction(function () use (&$attempts, $stored): string {
                $attempts++;
                if ($attempts === 1) {
                    throw new ContentionException('Lock wait timeout exceeded');
                }

                return $stored;
            });
        });

        $this->assertSame($stored, $result);
        $this->assertSame(2, $attempts);
        $this->assertSame(1, $connection->begins);
        $this->assertSame(1, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A top-level transaction that could not begin holds no work yet, so it begins again.
     */
    public function testTopLevelTransactionRetriesAfterItFailedToBegin(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $connection->endSession();
        $stored = \uniqid();

        $result = $adapter->withTransaction(fn (): string => $stored);

        $this->assertSame($stored, $result);
        $this->assertSame(1, $connection->begins);
        $this->assertSame(1, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A reconnect the callback did not surface leaves the driver without the top-level
     * transaction the adapter still counts. Its commit has nothing to commit, so the call
     * must fail instead of returning as if the work were stored, and must not run the
     * callback again: statements after the reconnect may already have run on their own.
     */
    public function testTopLevelCommitAfterTheDriverLostTheTransactionFails(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $attempts = 0;

        $error = $this->capture(function () use ($adapter, $connection, &$attempts): void {
            $adapter->withTransaction(function () use ($connection, &$attempts): void {
                $attempts++;
                $connection->reconnectSilently();
            });
        });

        $this->assertInstanceOf(TransactionException::class, $error, 'A commit of a transaction the driver no longer holds must fail');
        $this->assertSame(1, $attempts, 'The work of a lost transaction must not run again');
        $this->assertSame(1, $connection->begins);
        $this->assertSame(0, $connection->commits, 'Nothing may be committed after the transaction was lost');
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * MariaDB and MySQL roll the whole transaction back when a statement loses a deadlock,
     * and the nested call's savepoint goes with it. Nothing of the attempt is stored, so the
     * nested call surfaces the deadlock without running again in a transaction that no
     * longer exists, and the outermost call runs the whole unit again.
     */
    public function testOutermostTransactionRetriesAfterADeadlockRolledBackANestedCall(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        /** @var int $attempts */
        $attempts = 0;
        /** @var list<int> $nested */
        $nested = [];
        /** @var list<Throwable> $failures */
        $failures = [];
        $stored = \uniqid();

        $result = $adapter->withTransaction(function () use ($adapter, $connection, &$attempts, &$nested, &$failures, $stored): string {
            $attempts++;
            $attempt = $attempts;

            try {
                return $adapter->withTransaction(function () use ($adapter, $connection, $attempt, &$nested, $stored): string {
                    $nested[] = $attempt;
                    if ($attempt === 1) {
                        $connection->deadlock();
                    }
                    $adapter->exists('database', 'aggregations');

                    return $stored;
                });
            } catch (Throwable $error) {
                $failures[] = $error;

                throw $error;
            }
        });

        $this->assertSame($stored, $result);
        $this->assertSame(2, $attempts, 'The outermost call must run again after the engine rolled its transaction back');
        $this->assertSame([1, 2], $nested, 'The nested call must not run again inside the rolled-back transaction');
        $this->assertCount(1, $failures);
        $this->assertInstanceOf(ContentionException::class, $failures[0]);
        $this->assertSame('Deadlock detected', $failures[0]->getMessage(), 'The nested call must surface the deadlock, not a lost transaction');
        $this->assertSame(2, $connection->begins);
        $this->assertSame(1, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * An outermost call that keeps losing deadlocks gives up after as many attempts as a
     * failed top-level call, and rethrows the deadlock.
     */
    public function testOutermostTransactionRethrowsTheDeadlockAfterItsRetries(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $attempts = 0;

        $error = $this->capture(function () use ($adapter, $connection, &$attempts): void {
            $adapter->withTransaction(function () use ($adapter, $connection, &$attempts): void {
                $attempts++;
                $adapter->withTransaction(function () use ($adapter, $connection): void {
                    $connection->deadlock();
                    $adapter->exists('database', 'aggregations');
                });
            });
        });

        $this->assertInstanceOf(ContentionException::class, $error);
        $this->assertSame('Deadlock detected', $error->getMessage());
        $this->assertSame(3, $attempts);
        $this->assertSame(3, $connection->begins);
        $this->assertSame(0, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A deadlock in the top-level callback itself leaves nothing to roll back either, and
     * the call runs again, as it did in 7.x.
     */
    public function testTopLevelTransactionRetriesAfterADeadlock(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $attempts = 0;

        $adapter->withTransaction(function () use ($adapter, $connection, &$attempts): void {
            $attempts++;
            if ($attempts === 1) {
                $connection->deadlock();
            }
            $adapter->exists('database', 'aggregations');
        });

        $this->assertSame(2, $attempts);
        $this->assertSame(2, $connection->begins);
        $this->assertSame(1, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * An invalid write fails the same way however often it runs: the call rethrows it at
     * once without beginning another transaction.
     */
    public function testDeterministicFailureIsNotRetried(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $failure = new StructureException('Invalid document structure');
        $attempts = 0;

        $error = $this->capture(function () use ($adapter, $failure, &$attempts): void {
            $adapter->withTransaction(function () use ($failure, &$attempts): never {
                $attempts++;

                throw $failure;
            });
        });

        $this->assertSame($failure, $error);
        $this->assertSame(1, $attempts);
        $this->assertSame(1, $connection->begins);
        $this->assertSame(0, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A nested call rolls a deterministic failure back to its savepoint without running it
     * again, and the outermost call does not run the whole unit again for it either.
     */
    public function testDeterministicFailureInANestedCallIsNotRetried(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $failure = new NotFoundException('Collection not found');
        $outer = 0;
        $nested = 0;

        $error = $this->capture(function () use ($adapter, $failure, &$outer, &$nested): void {
            $adapter->withTransaction(function () use ($adapter, $failure, &$outer, &$nested): void {
                $outer++;
                $adapter->withTransaction(function () use ($failure, &$nested): never {
                    $nested++;

                    throw $failure;
                });
            });
        });

        $this->assertSame($failure, $error);
        $this->assertSame(1, $outer);
        $this->assertSame(1, $nested);
        $this->assertSame(1, $connection->begins);
        $this->assertSame(0, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A deadlock the driver raised without the adapter mapping it is still a lock conflict,
     * and the call runs again.
     */
    public function testUnmappedDeadlockIsRetried(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $failure = EngineError::create('40001', 1213, 'SQLSTATE[40001]: Serialization failure: 1213 Deadlock found when trying to get lock');
        $attempts = 0;

        $error = $this->capture(function () use ($adapter, $failure, &$attempts): void {
            $adapter->withTransaction(function () use ($failure, &$attempts): never {
                $attempts++;

                throw $failure;
            });
        });

        $this->assertSame($failure, $error);
        $this->assertSame(3, $attempts);
        $this->assertSame(3, $connection->begins);
        $this->assertSame(0, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    public function testUnmappedDeterministicDriverErrorIsNotRetried(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $failure = EngineError::create('42000', 1064, 'SQLSTATE[42000]: Syntax error or access violation: 1064');
        $attempts = 0;

        $error = $this->capture(function () use ($adapter, $failure, &$attempts): void {
            $adapter->withTransaction(function () use ($failure, &$attempts): never {
                $attempts++;

                throw $failure;
            });
        });

        $this->assertSame($failure, $error);
        $this->assertSame(1, $attempts);
        $this->assertSame(1, $connection->begins);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A top-level callback that lost its connection stored nothing, so it runs again on a
     * fresh transaction, as it did in 7.x, even though rolling back the lost transaction
     * fails.
     */
    public function testTopLevelTransactionRetriesAfterTheCallbackLostTheConnection(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $attempts = 0;
        $stored = \uniqid();

        $result = $adapter->withTransaction(function () use ($connection, &$attempts, $stored): string {
            $attempts++;
            if ($attempts === 1) {
                $connection->reconnectSilently();

                throw new PDOException('SQLSTATE[HY000]: General error: 2006 MySQL server has gone away');
            }

            return $stored;
        });

        $this->assertSame($stored, $result);
        $this->assertSame(2, $attempts);
        $this->assertSame(2, $connection->begins);
        $this->assertSame(1, $connection->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * @param callable(): mixed $callback
     */
    private function capture(callable $callback): ?Throwable
    {
        try {
            $callback();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }

    private function createConnection(): TransactionStateConnection
    {
        $statement = $this->createStub(PDOStatement::class);
        $connection = new TransactionStateConnection($statement);
        $statement->method('execute')->willReturnCallback($connection->executeStatement(...));

        return $connection;
    }
}
