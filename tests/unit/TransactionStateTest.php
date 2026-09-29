<?php

namespace Tests\Unit;

use PDOStatement;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Exception\Duplicate as DuplicateException;
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
                $nested = $this->capture(fn (): mixed => $adapter->withTransaction(function () use ($connection): void {
                    $connection->reconnectSilently();
                }));

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

        $outer = $this->capture(fn (): mixed => $adapter->withTransaction(function () use ($adapter, $connection): void {
            try {
                $adapter->withTransaction(function () use ($connection): void {
                    $connection->endSession();

                    throw new DuplicateException('Document already exists');
                });
            } catch (DuplicateException) {
            }
        }));

        $this->assertInstanceOf(TransactionException::class, $outer, 'A lost transaction must not surface as the duplicate the caller expects');
        $this->assertSame(1, $connection->begins, 'Only the outer call may begin a transaction');
        $this->assertSame(0, $connection->commits, 'Nothing may be committed after the transaction was lost');
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * While the enclosing transaction holds, a failed nested attempt rolls back to its
     * savepoint and runs again inside the same outer transaction.
     */
    public function testNestedTransactionRetriesWhileTheOuterTransactionHolds(): void
    {
        $connection = $this->createConnection();
        $adapter = new MariaDB($connection);
        $attempts = 0;

        $result = $adapter->withTransaction(function () use ($adapter, &$attempts): string {
            return $adapter->withTransaction(function () use (&$attempts): string {
                $attempts++;
                if ($attempts === 1) {
                    throw new RuntimeException('Transient failure');
                }

                return 'stored';
            });
        });

        $this->assertSame('stored', $result);
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

        $result = $adapter->withTransaction(fn (): string => 'stored');

        $this->assertSame('stored', $result);
        $this->assertSame(1, $connection->begins);
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
        $statement->method('execute')->willReturn(true);

        return new TransactionStateConnection($statement);
    }
}
