<?php

namespace Tests\Unit;

use ArrayObject;
use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\CommitOutcome;
use Throwable;
use Utopia\Database\Adapter\Mongo;
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

    private const int PRIMARY_STEPPED_DOWN = 189;

    private const int MAX_TIME_EXPIRED = 50;

    private const int SOCKET_TIMEOUT = 11601;

    private const int SOCKET_EXCEPTION = 9001;

    /**
     * @return array<string, array{MongoException}>
     */
    public static function unknownResults(): array
    {
        return [
            'labelled unknown result' => [self::labelledUnknownResult()],
            'primary stepped down' => [new MongoException('E189 PrimarySteppedDown: primary stepped down while waiting for replication', self::PRIMARY_STEPPED_DOWN)],
        ];
    }

    #[DataProvider('unknownResults')]
    public function testAnUnconfirmedCommitThatAppliedIsRetriedWithoutRunningTheCallbackAgain(MongoException $error): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [CommitOutcome::AppliedThenLost, $error],
            [CommitOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    public function testACommitReceiveTimeoutThrowsUnconfirmedWithoutRunningTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $timeout = new MongoException('Receive timeout: no data received within reasonable time', self::SOCKET_TIMEOUT);
        $adapter = new Mongo($this->client($staged, $stored, [
            [CommitOutcome::AppliedThenLost, $timeout],
        ]));
        $attempts = 0;

        $thrown = $this->failure($adapter, $this->work($staged, $attempts));

        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame($timeout, $thrown->getPrevious());
        $this->assertFalse($adapter->inTransaction());
    }

    public function testACommitSendFailureThrowsUnconfirmedWithoutRunningTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $failure = new MongoException('Failed to send data to MongoDB after reconnection attempt', self::SOCKET_EXCEPTION);
        $adapter = new Mongo($this->client($staged, $stored, [
            [CommitOutcome::NotApplied, $failure],
        ]));
        $attempts = 0;

        $thrown = $this->failure($adapter, $this->work($staged, $attempts));

        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([], $stored->getArrayCopy());
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame($failure, $thrown->getPrevious());
        $this->assertFalse($adapter->inTransaction());
    }

    public function testACommitThatStaysUnconfirmedThrowsWithoutRunningTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $unknown = self::labelledUnknownResult();
        $outage = new ArrayObject([self::labelledUnknownResult()]);
        $adapter = new Mongo($this->client($staged, $stored, [
            [CommitOutcome::AppliedThenLost, $unknown],
        ], $outage));
        $attempts = 0;

        $thrown = $this->failure($adapter, $this->work($staged, $attempts));

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

        $thrown = $this->failure($adapter, $this->work($staged, $attempts));

        $this->assertSame(1, $attempts, 'A commit whose result is unknown must not run the callback again');
        $this->assertSame([], $stored->getArrayCopy());
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
        $this->assertSame($expired, $thrown->getPrevious());
        $this->assertFalse($adapter->inTransaction());
    }

    public function testARetriedCommitThatReportsTheTransactionAbortedRunsTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [CommitOutcome::NotApplied, self::labelledUnknownResult()],
            [CommitOutcome::Aborted, null],
            [CommitOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(2, $attempts, 'An aborted transaction stored nothing, so the callback must run again');
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    public function testALabelledTransientTransactionErrorAtCommitRunsTheCallbackAgain(): void
    {
        $staged = $this->documents();
        $stored = $this->documents();
        $adapter = new Mongo($this->client($staged, $stored, [
            [CommitOutcome::NotApplied, new MongoException('Transaction aborted', 0, null, [Client::TRANSIENT_TRANSACTION_ERROR])],
            [CommitOutcome::Committed, null],
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
            [CommitOutcome::NotApplied, new UnsentException('Connection to MongoDB has been lost')],
            [CommitOutcome::Committed, null],
        ]));
        $attempts = 0;

        $result = $adapter->withTransaction($this->work($staged, $attempts));

        $this->assertSame(self::RESULT, $result);
        $this->assertSame(2, $attempts);
        $this->assertSame([self::DOCUMENT], $stored->getArrayCopy());
        $this->assertFalse($adapter->inTransaction());
    }

    private static function labelledUnknownResult(): MongoException
    {
        return new MongoException('Commit failed', 0, null, [Client::UNKNOWN_TRANSACTION_COMMIT_RESULT]);
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
     * @param  Closure(): string  $callback
     */
    private function failure(Mongo $adapter, Closure $callback): Throwable
    {
        try {
            $adapter->withTransaction($callback);
        } catch (Throwable $thrown) {
            return $thrown;
        }

        $this->fail('The transaction was expected to fail');
    }

    /**
     * A replica-set client that behaves as utopia-php/mongo 1.5.4 does: a socket timeout or send failure drops the
     * connection and its sessions, after which a commit reports an invalid session and ending a session is unsent.
     * Each commit follows the next scripted outcome; once the script is spent, commits fail with the outage's error
     * while it holds one, and succeed otherwise.
     *
     * @param  ArrayObject<int, string>  $staged
     * @param  ArrayObject<int, string>  $stored
     * @param  list<array{CommitOutcome, MongoException|null}>  $script
     * @param  ArrayObject<int, MongoException>|null  $outage
     */
    private function client(ArrayObject $staged, ArrayObject $stored, array $script, ?ArrayObject $outage = null): Client
    {
        return new class ($staged, $stored, $script, $outage ?? new ArrayObject()) extends Client {
            private const array DISCONNECTING_CODES = [9001, 11601];

            private const int NO_SUCH_TRANSACTION = 251;

            private bool $connected = true;

            private int $sessions = 0;

            /**
             * @param  ArrayObject<int, string>  $staged
             * @param  ArrayObject<int, string>  $stored
             * @param  list<array{CommitOutcome, MongoException|null}>  $script
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
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            #[\Override]
            public function isReplicaSet(): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $options
             * @return array<mixed>
             */
            #[\Override]
            public function startSession(array $options = []): array
            {
                $this->connected = true;
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
                    CommitOutcome::Committed => $this->store(),
                    CommitOutcome::AppliedThenLost => $this->fail($error, applied: true),
                    CommitOutcome::NotApplied => $this->fail($error, applied: false),
                    CommitOutcome::Aborted => $this->abort(),
                };
            }

            /**
             * @param  array<mixed>  $session
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function abortTransaction(array $session, array $options = []): bool
            {
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
                if (! $this->connected) {
                    throw new UnsentException('Client is not connected to MongoDB');
                }

                return true;
            }

            /**
             * @return array{CommitOutcome, MongoException|null}
             */
            private function unscripted(): array
            {
                $error = $this->outage[0] ?? null;

                return $error === null ? [CommitOutcome::Committed, null] : [CommitOutcome::NotApplied, $error];
            }

            private function store(): true
            {
                foreach ($this->staged as $document) {
                    $this->stored->append($document);
                }
                $this->staged->exchangeArray([]);

                return true;
            }

            private function abort(): never
            {
                $this->staged->exchangeArray([]);

                throw new MongoException('E251 NoSuchTransaction: Transaction with { txnNumber: 1 } has been aborted.', self::NO_SUCH_TRANSACTION, null, [Client::TRANSIENT_TRANSACTION_ERROR]);
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
