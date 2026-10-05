<?php

namespace Tests\Unit;

use ArrayObject;
use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Database\Exception\Unconfirmed as UnconfirmedException;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Mongo\UnsentException;

/**
 * Covers how Mongo::withTransaction() handles a commit whose result is unknown: the commit alone is retried, and the
 * callback never runs again unless the server reports the transaction aborted or the commit was never sent.
 */
final class MongoCommitRetryTest extends TestCase
{
    private const string DOCUMENT = 'document';

    private const string RESULT = 'result';

    private const int MAX_TIME_EXPIRED = 50;

    private const int WRITE_CONFLICT = 112;

    private const int PRIMARY_STEPPED_DOWN = 189;

    private const int NO_SUCH_TRANSACTION = 251;

    private const int EXCEEDED_TIME_LIMIT = 262;

    private const int SOCKET_EXCEPTION = 9001;

    private const int SOCKET_TIMEOUT = 11601;

    /**
     * Commit errors as utopia-php/mongo 1.5.4 raises them, without error labels, after which the commit may have
     * applied.
     *
     * @return array<string, array{MongoException}>
     */
    public static function unknownResults(): array
    {
        return [
            'primary stepped down (189)' => [self::primarySteppedDown()],
            'max time expired (50)' => [new MongoException('E50 MaxTimeMSExpired: operation exceeded time limit', self::MAX_TIME_EXPIRED)],
            'exceeded time limit (262)' => [new MongoException('E262 ExceededTimeLimit: operation exceeded time limit', self::EXCEEDED_TIME_LIMIT)],
        ];
    }

    /**
     * Commit errors after which utopia-php/mongo 1.5.4 drops the connection with its sessions, so the commit cannot
     * be sent again.
     *
     * @return array<string, array{MongoException, MongoCommitRetryOutcome, list<string>}>
     */
    public static function disconnects(): array
    {
        return [
            'receive timeout (11601) after the commit applied' => [
                new MongoException('Receive timeout: no data received within reasonable time', self::SOCKET_TIMEOUT),
                MongoCommitRetryOutcome::AppliedThenLost,
                [self::DOCUMENT],
            ],
            'send failure (9001) before the commit applied' => [
                new MongoException('Failed to send data to MongoDB after reconnection attempt', self::SOCKET_EXCEPTION),
                MongoCommitRetryOutcome::NotApplied,
                [],
            ],
        ];
    }

    /**
     * Commit errors as utopia-php/mongo 1.5.4 raises them, without error labels, for a transaction the server
     * aborted, so none of its writes are stored.
     *
     * @return array<string, array{MongoException}>
     */
    public static function abortedCommits(): array
    {
        return [
            'no such transaction (251)' => [self::noSuchTransaction()],
            'write conflict (112)' => [new MongoException('E112 WriteConflict: write conflict during plan execution', self::WRITE_CONFLICT)],
        ];
    }

    #[DataProvider('unknownResults')]
    public function testAnUnknownCommitResultThatAppliedIsRetriedWithoutRunningTheCallbackAgain(MongoException $error): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [MongoCommitRetryOutcome::AppliedThenLost, $error],
            [MongoCommitRetryOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * utopia-php/mongo 1.5.4 never sets error labels; this checks a client that does.
     */
    public function testForwardCompatibilityALabelledUnknownCommitResultIsRetriedWithoutRunningTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [MongoCommitRetryOutcome::AppliedThenLost, new MongoException('Commit failed', 0, null, [Client::UNKNOWN_TRANSACTION_COMMIT_RESULT])],
            [MongoCommitRetryOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * @param  list<string>  $expected
     */
    #[DataProvider('disconnects')]
    public function testACommitThatDropsTheConnectionThrowsUnconfirmedWithoutRunningTheCallbackAgain(MongoException $error, MongoCommitRetryOutcome $outcome, array $expected): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [$outcome, $error],
        ]));
        $attempts = 0;

        $thrown = $this->failure(fn (Closure $callback): mixed => $adapter->withTransaction($callback), $this->work($staged, $attempts));

        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame($error, $thrown->getPrevious());
        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame($expected, $stored->getArrayCopy());
        $this->assertFalse($adapter->isRetryable($thrown), 'Running an unconfirmed commit again could store its writes twice');
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * @param  list<string>  $expected
     */
    #[DataProvider('disconnects')]
    public function testADatabaseTransactionWhoseCommitDropsTheConnectionThrowsUnconfirmed(MongoException $error, MongoCommitRetryOutcome $outcome, array $expected): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $database = new Database(new Mongo($this->client($staged, $stored, [
            [$outcome, $error],
        ])), new Cache(new None()));
        $attempts = 0;

        $thrown = $this->failure(fn (Closure $callback): mixed => $database->withTransaction($callback), $this->work($staged, $attempts));

        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame($error, $thrown->getPrevious());
        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame($expected, $stored->getArrayCopy());
    }

    public function testACommitThatStaysUnconfirmedThrowsWithoutRunningTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $unknown = self::primarySteppedDown();
        $outage = new ArrayObject([self::primarySteppedDown()]);
        $adapter = new Mongo($this->client($staged, $stored, [
            [MongoCommitRetryOutcome::AppliedThenLost, $unknown],
        ], $outage));
        $attempts = 0;

        $thrown = $this->failure(fn (Closure $callback): mixed => $adapter->withTransaction($callback), $this->work($staged, $attempts));

        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame($unknown, $thrown->getPrevious());
        $this->assertFalse($adapter->isRetryable($thrown), 'Running an unconfirmed commit again could store its writes twice');
        $this->assertFalse($adapter->inTransaction());

        $outage->exchangeArray([]);
        $attempts = 0;

        $this->assertSame(self::RESULT, $adapter->withTransaction($this->work($staged, $attempts)));
        $this->assertSame(1, $attempts);
        $this->assertSame([self::DOCUMENT, self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    public function testACommitTimeLimitIsRetriedThenThrowsUnconfirmed(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $expired = new MongoException('E50 MaxTimeMSExpired: operation exceeded time limit', self::MAX_TIME_EXPIRED);
        $adapter = new Mongo($this->client($staged, $stored, [], new ArrayObject([$expired])));
        $attempts = 0;

        $thrown = $this->failure(fn (Closure $callback): mixed => $adapter->withTransaction($callback), $this->work($staged, $attempts));

        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([], $stored->getArrayCopy());
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame($expired, $thrown->getPrevious());
        $this->assertFalse($adapter->inTransaction());
    }

    #[DataProvider('abortedCommits')]
    public function testAFirstCommitTheServerReportsAbortedRunsTheCallbackAgain(MongoException $error): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [MongoCommitRetryOutcome::Aborted, $error],
            [MongoCommitRetryOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(2, $attempts, 'An aborted transaction stored nothing, so the callback must run again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    #[DataProvider('abortedCommits')]
    public function testARetriedCommitThatReportsTheTransactionAbortedRunsTheCallbackAgain(MongoException $error): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [MongoCommitRetryOutcome::NotApplied, self::primarySteppedDown()],
            [MongoCommitRetryOutcome::Aborted, $error],
            [MongoCommitRetryOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(2, $attempts, 'An aborted transaction stored nothing, so the callback must run again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    /**
     * utopia-php/mongo 1.5.4 never sets error labels; this checks a client that does.
     */
    public function testForwardCompatibilityALabelledTransientTransactionErrorAtCommitRunsTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [MongoCommitRetryOutcome::NotApplied, new MongoException('Transaction aborted', 0, null, [Client::TRANSIENT_TRANSACTION_ERROR])],
            [MongoCommitRetryOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(2, $attempts);
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    public function testAnUnsentCommitRunsTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [MongoCommitRetryOutcome::NotApplied, new UnsentException('Connection to MongoDB has been lost')],
            [MongoCommitRetryOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(2, $attempts);
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    public function testACommitRetryThatWasNeverSentIsSentAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [MongoCommitRetryOutcome::NotApplied, self::primarySteppedDown()],
            [MongoCommitRetryOutcome::NotApplied, new UnsentException('Failed to connect to MongoDB')],
            [MongoCommitRetryOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    private static function primarySteppedDown(): MongoException
    {
        return new MongoException('E189 PrimarySteppedDown: primary stepped down while waiting for replication', self::PRIMARY_STEPPED_DOWN);
    }

    private static function noSuchTransaction(): MongoException
    {
        return new MongoException('E251 NoSuchTransaction: Transaction with { txnNumber: 1 } has been aborted.', self::NO_SUCH_TRANSACTION);
    }

    /**
     * @return ArrayObject<int, string>
     */
    private function documents(): ArrayObject
    {
        /** @var ArrayObject<int, string> $documents */
        $documents = new ArrayObject();

        return $documents;
    }

    /**
     * @param  ArrayObject<int, string>  $staged
     * @return Closure(): string
     */
    private function work(ArrayObject $staged, int &$attempts): Closure
    {
        return function () use ($staged, &$attempts): string {
            $attempts++;
            $staged->append(self::DOCUMENT);

            return self::RESULT;
        };
    }

    /**
     * @param  Closure(Closure(): string): mixed  $transaction
     * @param  Closure(): string  $callback
     */
    private function failure(Closure $transaction, Closure $callback): Throwable
    {
        try {
            $transaction($callback);
        } catch (Throwable $thrown) {
            return $thrown;
        }

        $this->fail('The transaction was expected to fail');
    }

    /**
     * A replica-set client that behaves as utopia-php/mongo 1.5.4 does: it never sets error labels, and a socket
     * timeout or send failure drops the connection with its sessions and its replica-set state, after which every
     * command that needs the server is unsent and a commit or abort reports an invalid session, until the adapter
     * reconnects. Each commit follows the next scripted outcome; once the script is spent, commits fail with the
     * outage's error while it holds one, and succeed otherwise.
     *
     * @param  ArrayObject<int, string>  $staged
     * @param  ArrayObject<int, string>  $stored
     * @param  list<array{MongoCommitRetryOutcome, MongoException|null}>  $script
     * @param  ArrayObject<int, MongoException>|null  $outage
     */
    private function client(ArrayObject $staged, ArrayObject $stored, array $script, ?ArrayObject $outage = null): Client
    {
        return new class ($staged, $stored, $script, $outage ?? new ArrayObject()) extends Client {
            private const array DISCONNECTING_CODES = [9001, 11601];

            private const int NO_SUCH_TRANSACTION = 251;

            private bool $connected = false;

            private int $sessions = 0;

            /**
             * @param  ArrayObject<int, string>  $staged
             * @param  ArrayObject<int, string>  $stored
             * @param  list<array{MongoCommitRetryOutcome, MongoException|null}>  $script
             * @param  ArrayObject<int, MongoException>  $outage
             */
            public function __construct(
                private readonly ArrayObject $staged,
                private readonly ArrayObject $stored,
                private array $script,
                private readonly ArrayObject $outage,
            ) {
            }

            #[\Override]
            public function connect(): self
            {
                $this->connected = true;

                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            #[\Override]
            public function isReplicaSet(): bool
            {
                $this->ensureConnected();

                return true;
            }

            /**
             * @param  array<mixed>  $options
             * @return array<mixed>
             */
            #[\Override]
            public function startSession(array $options = []): array
            {
                $this->ensureConnected();
                $this->sessions++;

                return ['id' => (object) ['id' => $this->sessions]];
            }

            /**
             * @param  array<mixed>  $session
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function startTransaction(array $session, array $options = []): bool
            {
                $this->staged->exchangeArray([]);

                return true;
            }

            /**
             * @param  array<mixed>  $session
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function commitTransaction(array $session, array $options = []): bool
            {
                if (! $this->connected) {
                    throw new MongoException('Invalid session provided to commitTransaction');
                }

                [$outcome, $error] = \array_shift($this->script) ?? $this->unscripted();

                return match ($outcome) {
                    MongoCommitRetryOutcome::Committed => $this->store(),
                    MongoCommitRetryOutcome::AppliedThenLost => $this->fail($error, applied: true),
                    MongoCommitRetryOutcome::NotApplied => $this->fail($error, applied: false),
                    MongoCommitRetryOutcome::Aborted => $this->abort($error),
                };
            }

            /**
             * @param  array<mixed>  $session
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function abortTransaction(array $session, array $options = []): bool
            {
                if (! $this->connected) {
                    throw new MongoException('Invalid session provided to abortTransaction');
                }

                $this->staged->exchangeArray([]);

                return true;
            }

            /**
             * @param  array<mixed>  $sessions
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function endSessions(array $sessions, array $options = []): bool
            {
                $this->ensureConnected();

                return true;
            }

            private function ensureConnected(): void
            {
                if (! $this->connected) {
                    throw new UnsentException('Client is not connected to MongoDB');
                }
            }

            /**
             * @return array{MongoCommitRetryOutcome, MongoException|null}
             */
            private function unscripted(): array
            {
                $error = $this->outage[0] ?? null;

                return $error === null ? [MongoCommitRetryOutcome::Committed, null] : [MongoCommitRetryOutcome::NotApplied, $error];
            }

            private function store(): true
            {
                foreach ($this->staged as $document) {
                    $this->stored->append($document);
                }
                $this->staged->exchangeArray([]);

                return true;
            }

            private function abort(?MongoException $error): never
            {
                $this->staged->exchangeArray([]);

                throw $error ?? new MongoException('E251 NoSuchTransaction: Transaction with { txnNumber: 1 } has been aborted.', self::NO_SUCH_TRANSACTION);
            }

            private function fail(?MongoException $error, bool $applied): never
            {
                $error ??= new MongoException('Commit failed');

                if ($applied) {
                    $this->store();
                }

                if (\in_array($error->getCode(), self::DISCONNECTING_CODES, true)) {
                    $this->connected = false;
                }

                throw $error;
            }
        };
    }
}
