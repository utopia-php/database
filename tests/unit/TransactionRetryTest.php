<?php

namespace Tests\Unit;

use Closure;
use InvalidArgumentException;
use LogicException;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Support\EngineError;
use Throwable;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Mismatch as MismatchException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Mirror;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

/**
 * Covers the retry policy of withTransaction(): only a failure that can succeed when the attempt runs again is
 * retried, up to the built-in attempt budget, on every entry point that runs a transaction.
 */
class TransactionRetryTest extends TestCase
{
    private const int ATTEMPTS = 3;

    private const string LOST_CONNECTION = 'SQLSTATE[HY000]: General error: 2006 MySQL server has gone away';

    private const string ADAPTER = 'adapter';

    private const string SQLITE = 'sqlite';

    private const string DATABASE = 'database';

    private const string POOL = 'pool';

    private const string READ_WRITE_POOL = 'read-write pool';

    private const string MIRROR = 'mirror';

    /**
     * @return array<string, array{Throwable}>
     */
    public static function deterministicFailures(): array
    {
        return [
            'structure' => [new StructureException('Invalid document structure: Unknown attribute: "name"')],
            'not found' => [new NotFoundException('Collection not found')],
            'query' => [new QueryException('Invalid query')],
            'type' => [new TypeException('Invalid operation')],
            'index' => [new IndexException('Index already exists')],
            'dependency' => [new DependencyException('Attribute cannot be deleted because it is used in an index')],
            'order' => [new OrderException('Invalid order')],
            'character' => [new CharacterException('Invalid character')],
            'truncate' => [new TruncateException('Resize would result in data truncation')],
            'operator' => [new OperatorException('Invalid operator')],
            'mismatch' => [new MismatchException('Document already exists with different data')],
            'unique' => [new UniqueException(UniqueException::MESSAGE)],
            'duplicate' => [new DuplicateException('Document already exists')],
            'restricted' => [new RestrictedException('Restricted')],
            'authorization' => [new AuthorizationException('Missing "create" permission')],
            'relationship' => [new RelationshipException('Invalid relationship')],
            'conflict' => [new ConflictException('Document was updated after the request timestamp')],
            'limit' => [new LimitException('Value out of range')],
            'timeout' => [new TimeoutException('Query timed out')],
            'typed failure wrapping a lost connection' => [new StructureException('Invalid document', previous: new PDOException(self::LOST_CONNECTION))],
            'database' => [new DatabaseException('Missing ID')],
            'invalid argument' => [new InvalidArgumentException('Invalid argument')],
            'logic' => [new LogicException('Unsupported')],
            'runtime' => [new RuntimeException('Unknown failure')],
            'driver error' => [EngineError::create('42000', 1064, 'SQLSTATE[42000]: Syntax error or access violation: 1064')],
        ];
    }

    /**
     * @return array<string, array{Throwable}>
     */
    public static function transientFailures(): array
    {
        return [
            'contention' => [new ContentionException('Deadlock detected')],
            'transaction' => [new TransactionException('Failed to start transaction')],
            'lost connection' => [new PDOException(self::LOST_CONNECTION)],
            'lost connection by driver code' => [EngineError::create('HY000', 2013, 'Lost')],
            'failure wrapping a lost connection' => [new DatabaseException('Failed to commit transaction', previous: new PDOException(self::LOST_CONNECTION))],
            'statement refused after a lost connection' => [new PDOException('The transaction was lost with the connection: roll it back before running another statement', previous: new PDOException(self::LOST_CONNECTION))],
        ];
    }

    /**
     * @return array<string, array{string}>
     */
    public static function entries(): array
    {
        return [
            self::ADAPTER => [self::ADAPTER],
            self::SQLITE => [self::SQLITE],
            self::DATABASE => [self::DATABASE],
            self::POOL => [self::POOL],
            self::READ_WRITE_POOL => [self::READ_WRITE_POOL],
            self::MIRROR => [self::MIRROR],
        ];
    }

    /**
     * @return array<string, array{string}>
     */
    public static function pools(): array
    {
        return [
            self::POOL => [self::POOL],
            self::READ_WRITE_POOL => [self::READ_WRITE_POOL],
        ];
    }

    /**
     * Running the attempt again fails the same way: the caller gets the failure at once.
     */
    #[DataProvider('deterministicFailures')]
    public function testDeterministicFailureRunsOnce(Throwable $failure): void
    {
        $adapter = new DatabaseMemory();
        [$thrown, $attempts] = $this->attempt($adapter->withTransaction(...), $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(1, $attempts, 'A deterministic failure must not run again');
        $this->assertFalse($adapter->inTransaction());
    }

    #[DataProvider('transientFailures')]
    public function testTransientFailureRunsEveryAttempt(Throwable $failure): void
    {
        $adapter = new DatabaseMemory();
        [$thrown, $attempts] = $this->attempt($adapter->withTransaction(...), $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(self::ATTEMPTS, $attempts);
        $this->assertFalse($adapter->inTransaction());
    }

    public function testTransientFailureSucceedsWhenItRunsAgain(): void
    {
        $adapter = new DatabaseMemory();
        $attempts = 0;
        $stored = \uniqid();

        $result = $adapter->withTransaction(function () use (&$attempts, $stored): string {
            $attempts++;
            if ($attempts === 1) {
                throw new ContentionException('Lock wait timeout exceeded');
            }

            return $stored;
        });

        $this->assertSame($stored, $result);
        $this->assertSame(2, $attempts);
        $this->assertFalse($adapter->inTransaction());
    }

    #[DataProvider('entries')]
    public function testEveryEntryPointRunsADeterministicFailureOnce(string $entry): void
    {
        foreach ([new StructureException('Invalid document structure'), new InvalidArgumentException('Invalid argument')] as $failure) {
            [$transaction, $adapter] = $this->entry($entry);
            [$thrown, $attempts] = $this->attempt($transaction, $failure);

            $this->assertSame($failure, $thrown);
            $this->assertSame(1, $attempts, "{$entry} must not run a deterministic failure again");
            $this->assertFalse($adapter->inTransaction());
        }
    }

    #[DataProvider('entries')]
    public function testEveryEntryPointRetriesContention(string $entry): void
    {
        [$transaction, $adapter] = $this->entry($entry);
        $failure = new ContentionException('Deadlock detected');
        [$thrown, $attempts] = $this->attempt($transaction, $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(self::ATTEMPTS, $attempts, "{$entry} must retry a lock conflict");
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A lock conflict the driver raised without the adapter mapping it is still a lock conflict.
     */
    public function testUnmappedDriverContentionIsRetried(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $failure = EngineError::create('HY000', 5, 'SQLSTATE[HY000]: General error: 5 database is locked');
        [$thrown, $attempts] = $this->attempt($adapter->withTransaction(...), $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(self::ATTEMPTS, $attempts);
        $this->assertFalse($adapter->inTransaction());
    }

    public function testUnmappedDeterministicDriverErrorRunsOnce(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $failure = EngineError::create('HY000', 1, 'SQLSTATE[HY000]: General error: 1 no such table: missing');
        [$thrown, $attempts] = $this->attempt($adapter->withTransaction(...), $failure);

        $this->assertSame($failure, $thrown);
        $this->assertSame(1, $attempts);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A pool answers whether a failure is retried as the adapters it lends out do.
     */
    #[DataProvider('pools')]
    public function testPoolClassifiesFailuresAsItsAdapterDoes(string $entry): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $pool = $entry === self::POOL
            ? new Pool($this->connections($adapter))
            : new ReadWritePool($this->connections($adapter), $this->connections(new DatabaseMemory()));
        $pool->setAuthorization(new Authorization());

        $this->assertTrue($pool->isRetryable(EngineError::create('HY000', 5, 'SQLSTATE[HY000]: General error: 5 database is locked')));
        $this->assertFalse($pool->isRetryable(EngineError::create('HY000', 1, 'SQLSTATE[HY000]: General error: 1 no such table: missing')));
        $this->assertTrue($pool->isRetryable(new ContentionException('Deadlock detected')));
        $this->assertFalse($pool->isRetryable(new StructureException('Invalid document structure')));
    }

    /**
     * A nested call does not retry a deterministic failure in its savepoint, and the enclosing call does not run
     * the whole unit again for it either.
     */
    public function testDeterministicFailureInANestedCallRunsOnce(): void
    {
        $adapter = new DatabaseMemory();
        $failure = new StructureException('Invalid document structure');
        $outer = 0;
        $nested = 0;

        $thrown = $this->capture(function () use ($adapter, $failure, &$outer, &$nested): void {
            $adapter->withTransaction(function () use ($adapter, $failure, &$outer, &$nested): void {
                $outer++;
                $adapter->withTransaction(function () use ($failure, &$nested): never {
                    $nested++;

                    throw $failure;
                });
            });
        });

        $this->assertSame($failure, $thrown);
        $this->assertSame(1, $nested);
        $this->assertSame(1, $outer);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * Only the outermost call retries: a nested call rolls a lock conflict back to its savepoint
     * and rethrows it, and the outermost call runs the whole unit again.
     */
    public function testTransientFailureInANestedCallRunsTheOuterTransactionAgain(): void
    {
        $adapter = new DatabaseMemory();
        $outer = 0;
        $nested = 0;
        $stored = \uniqid();

        $result = $adapter->withTransaction(function () use ($adapter, &$outer, &$nested, $stored): string {
            $outer++;

            return $adapter->withTransaction(function () use (&$nested, $stored): string {
                $nested++;
                if ($nested === 1) {
                    throw new ContentionException('Lock not available');
                }

                return $stored;
            });
        });

        $this->assertSame($stored, $result);
        $this->assertSame(2, $nested);
        $this->assertSame(2, $outer);
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * A lock conflict that keeps failing runs the nested callback once per attempt of the
     * outermost call, on every entry point, instead of once per attempt of each call.
     */
    #[DataProvider('entries')]
    public function testEveryEntryPointRunsAPersistentNestedLockConflictOncePerAttempt(string $entry): void
    {
        [$transaction, $adapter] = $this->entry($entry);
        $failure = new ContentionException('Lock wait timeout exceeded');
        $outer = 0;
        $nested = 0;

        $thrown = $this->capture(function () use ($transaction, $failure, &$outer, &$nested): void {
            $transaction(function () use ($transaction, $failure, &$outer, &$nested): void {
                $outer++;
                $transaction(function () use ($failure, &$nested): never {
                    $nested++;

                    throw $failure;
                });
            });
        });

        $this->assertSame($failure, $thrown);
        $this->assertSame(self::ATTEMPTS, $outer, "{$entry} must retry the outermost call");
        $this->assertSame(self::ATTEMPTS, $nested, "{$entry} must run the nested call once per attempt");
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * The count of enclosing calls unwinds with every call, so a later top-level call owns its
     * retries again.
     */
    public function testATopLevelCallAfterANestedFailureRetriesAgain(): void
    {
        $adapter = new DatabaseMemory();
        $this->capture(fn () => $adapter->withTransaction(fn () => $adapter->withTransaction(static function (): never {
            throw new ContentionException('Lock not available');
        })));

        [$thrown, $attempts] = $this->attempt($adapter->withTransaction(...), new ContentionException('Lock not available'));

        $this->assertInstanceOf(ContentionException::class, $thrown);
        $this->assertSame(self::ATTEMPTS, $attempts);
    }

    /**
     * Rollback cleanup can itself fail (adapters throw a DatabaseException from
     * rollbackTransaction()). A non-retriable action must still abort after a
     * single attempt and propagate the original action, not be retried or
     * masked by the rollback error.
     */
    public function testNonRetriableActionAbortsWhenRollbackFails(): void
    {
        $adapter = new class () extends DatabaseMemory {
            public function rollbackTransaction(): bool
            {
                throw new \RuntimeException('rollback failed');
            }
        };

        $attempts = 0;
        $thrown = null;

        try {
            $adapter->withTransaction(function () use (&$attempts) {
                $attempts++;
                throw new TimeoutException('Query timed out');
            });
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        $this->assertInstanceOf(TimeoutException::class, $thrown);
        $this->assertSame(1, $attempts);
    }

    public function testTransientActionRunsAgainWhenRollbackFails(): void
    {
        $adapter = new class () extends DatabaseMemory {
            public function rollbackTransaction(): bool
            {
                parent::rollbackTransaction();

                throw new DatabaseException('Failed to rollback transaction');
            }
        };
        $attempts = 0;
        $stored = \uniqid();

        $result = $adapter->withTransaction(function () use (&$attempts, $stored): string {
            $attempts++;
            if ($attempts === 1) {
                throw new ContentionException('Deadlock detected');
            }

            return $stored;
        });

        $this->assertSame($stored, $result);
        $this->assertSame(2, $attempts);
        $this->assertFalse($adapter->inTransaction());
    }

    public function testRedisRollbackFailureEndsTheTransaction(): void
    {
        if (!\extension_loaded('redis')) {
            $this->markTestSkipped('redis extension not loaded');
        }

        $adapter = new class (new \Redis()) extends RedisAdapter {
            public bool $failReplay = true;

            protected function rollbackJournal(): void
            {
                if ($this->failReplay) {
                    $this->failReplay = false;

                    throw new \RuntimeException('rollback replay failed');
                }

                parent::rollbackJournal();
            }
        };

        $adapter->startTransaction();
        $adapter->startTransaction();

        $thrown = null;
        try {
            $adapter->rollbackTransaction();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        $this->assertInstanceOf(\RuntimeException::class, $thrown);
        $this->assertFalse($adapter->inTransaction());
        $this->assertFalse($adapter->commitTransaction(), 'No transaction is left open to commit');
        $this->assertFalse($adapter->rollbackTransaction(), 'No transaction is left open to roll back');

        $this->assertTrue($adapter->startTransaction());
        $this->assertTrue($adapter->startTransaction());
        $this->assertTrue($adapter->commitTransaction());
        $this->assertTrue($adapter->rollbackTransaction());
        $this->assertFalse($adapter->inTransaction(), 'A later nested transaction must unwind to no transaction');
    }

    /**
     * @return array{Closure(callable(): mixed): mixed, Adapter} The entry point's withTransaction() and the adapter
     *                                                            that holds its transaction
     */
    private function entry(string $entry): array
    {
        $adapter = $entry === self::SQLITE ? new SQLite(new PDO('sqlite::memory:')) : new DatabaseMemory();
        $adapter->setAuthorization(new Authorization());

        $target = match ($entry) {
            self::ADAPTER, self::SQLITE => $adapter,
            self::DATABASE => new Database($adapter, new Cache(new NoCache())),
            self::POOL => new Pool($this->connections($adapter)),
            self::READ_WRITE_POOL => new ReadWritePool($this->connections($adapter), $this->connections(new DatabaseMemory())),
            self::MIRROR => new Mirror(new Database($adapter, new Cache(new NoCache()))),
            default => throw new LogicException("Unknown entry point {$entry}"),
        };
        if ($target instanceof Pool) {
            $target->setAuthorization(new Authorization());
        }

        return [$target->withTransaction(...), $adapter];
    }

    /**
     * @return UtopiaPool<Adapter>&Stub
     */
    private function connections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        return $connections;
    }

    /**
     * Run a callback that always fails through a withTransaction().
     *
     * @param callable(callable(): never): mixed $transaction
     * @return array{?Throwable, int} What the transaction threw and how many times it ran the callback
     */
    private function attempt(callable $transaction, Throwable $failure): array
    {
        $attempts = 0;

        $thrown = $this->capture(function () use ($transaction, $failure, &$attempts): void {
            $transaction(function () use ($failure, &$attempts): never {
                $attempts++;

                throw $failure;
            });
        });

        return [$thrown, $attempts];
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
}
