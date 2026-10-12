<?php

namespace Tests\Unit;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\ReplicaSetClient;
use Throwable;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Mongo\UnsentException;

/**
 * Covers the retry policy of Mongo::withTransaction(), which runs its own loop over the replica set's sessions.
 */
final class MongoTransactionRetryTest extends TestCase
{
    private const int ATTEMPTS = 3;

    private const string TRANSIENT_TRANSACTION_ERROR = 'TransientTransactionError';

    private const int WRITE_CONFLICT = 112;

    private const int BAD_VALUE = 2;

    private const int SOCKET_EXCEPTION = 9001;

    /**
     * @return array<string, array{Throwable}>
     */
    public static function deterministicFailures(): array
    {
        return [
            'structure' => [new StructureException('Invalid document structure')],
            'not found' => [new NotFoundException('Collection not found')],
            'duplicate' => [new DuplicateException('Document already exists')],
            'timeout' => [new TimeoutException('Query timed out')],
            'database' => [new DatabaseException('Missing ID')],
            'invalid argument' => [new InvalidArgumentException('Invalid argument')],
            'server error' => [new MongoException('BadValue', self::BAD_VALUE)],
        ];
    }

    /**
     * @return array<string, array{Throwable}>
     */
    public static function transientFailures(): array
    {
        return [
            'contention' => [new ContentionException('Write conflict')],
            'transaction' => [new TransactionException('Transaction aborted')],
            'labelled transient' => [new MongoException('Transaction was aborted', 251, null, [self::TRANSIENT_TRANSACTION_ERROR])],
            'network error' => [new MongoException('Socket error', self::SOCKET_EXCEPTION)],
            'unsent' => [new UnsentException('Connection to MongoDB has been lost')],
            'failure wrapping a transient error' => [new DatabaseException('Failed to commit transaction', previous: new MongoException('Transaction was aborted', 0, null, [self::TRANSIENT_TRANSACTION_ERROR]))],
        ];
    }

    #[DataProvider('deterministicFailures')]
    public function testDeterministicFailureRunsOnce(Throwable $failure): void
    {
        $client = new ReplicaSetClient();
        $adapter = new Mongo($client);

        [$thrown, $attempts] = $this->attempt($adapter, $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(1, $attempts, 'A deterministic failure must not run again');
        $this->assertSame(1, $client->sessions);
        $this->assertSame(1, $client->aborts);
        $this->assertFalse($adapter->inTransaction());
    }

    #[DataProvider('transientFailures')]
    public function testTransientFailureRunsEveryAttempt(Throwable $failure): void
    {
        $client = new ReplicaSetClient();
        $adapter = new Mongo($client);

        [$thrown, $attempts] = $this->attempt($adapter, $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(self::ATTEMPTS, $attempts);
        $this->assertSame(self::ATTEMPTS, $client->sessions);
        $this->assertFalse($adapter->inTransaction());
    }

    public function testAWriteConflictRunsUntilItsRetriesForWriteConflictsRunOut(): void
    {
        $client = new ReplicaSetClient();
        $adapter = new Mongo($client);
        $failure = new MongoException('WriteConflict', self::WRITE_CONFLICT);

        [$thrown, $attempts] = $this->attempt($adapter, $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(21, $attempts);
        $this->assertFalse($adapter->inTransaction());
    }

    public function testTransientFailureSucceedsWhenItRunsAgain(): void
    {
        $client = new ReplicaSetClient();
        $adapter = new Mongo($client);
        $attempts = 0;
        $stored = \uniqid();

        $result = $adapter->withTransaction(function () use (&$attempts, $stored): string {
            $attempts++;
            if ($attempts === 1) {
                throw new MongoException('WriteConflict', self::WRITE_CONFLICT, null, [self::TRANSIENT_TRANSACTION_ERROR]);
            }

            return $stored;
        });

        $this->assertSame($stored, $result);
        $this->assertSame(2, $attempts);
        $this->assertSame(1, $client->commits);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A standalone server has no transactions, so withTransaction() runs the callback once and retries nothing: a
     * caller that retries on its own must not take a transient failure for one the transaction already retried.
     */
    public function testAStandaloneServerRetriesNothing(): void
    {
        $adapter = new Mongo(new ReplicaSetClient(replicaSet: false));
        $failure = new ContentionException('Write conflict');

        [$thrown, $attempts] = $this->attempt($adapter, $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(1, $attempts);
        $this->assertFalse($adapter->isRetryable($failure));
        $this->assertTrue((new Mongo(new ReplicaSetClient()))->isRetryable($failure));
    }

    /**
     * Without savepoints a nested call runs inside the caller's transaction, so only the outermost call decides.
     */
    public function testDeterministicFailureInANestedCallRunsOnce(): void
    {
        $client = new ReplicaSetClient();
        $adapter = new Mongo($client);
        $failure = new StructureException('Invalid document structure');
        $outer = 0;
        $nested = 0;

        $thrown = null;
        try {
            $adapter->withTransaction(function () use ($adapter, $failure, &$outer, &$nested): void {
                $outer++;
                $adapter->withTransaction(function () use ($failure, &$nested): never {
                    $nested++;

                    throw $failure;
                });
            });
        } catch (Throwable $error) {
            $thrown = $error;
        }

        $this->assertSame($failure, $thrown);
        $this->assertSame(1, $outer);
        $this->assertSame(1, $nested);
        $this->assertSame(1, $client->sessions);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * @return array{?Throwable, int} What the transaction threw and how many times it ran the callback
     */
    private function attempt(Mongo $adapter, Throwable $failure): array
    {
        $attempts = 0;
        $thrown = null;

        try {
            $adapter->withTransaction(function () use ($failure, &$attempts): never {
                $attempts++;

                throw $failure;
            });
        } catch (Throwable $error) {
            $thrown = $error;
        }

        return [$thrown, $attempts];
    }
}
