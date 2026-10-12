<?php

namespace Utopia\Database\Trait;

use Closure;
use Throwable;
use Utopia\Console;
use Utopia\Database\Capability;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception\Unconfirmed as UnconfirmedException;
use WeakMap;

trait Transactions
{
    /** @var array<int, array<string, string>> */
    protected array $documentCachePurges = [];

    /** @var array<int, array<string, true>> */
    protected array $transactionWrites = [];

    /** @var array<int, array<string, array<string, Document>>> */
    protected array $transactionDefinitions = [];

    /** @var array<int, array<string, array<string, Closure(): void>>> */
    protected array $definitionRefills = [];

    private bool $definitionFillsFail = false;

    /** @var array<int, list<Closure(): void>> */
    protected array $documentPurgeEvents = [];

    /** @var WeakMap<Throwable, true>|null */
    private ?WeakMap $committedFailures = null;

    /** @var WeakMap<Throwable, true>|null */
    private ?WeakMap $transactionFailures = null;

    /** @var WeakMap<Throwable, true>|null */
    private ?WeakMap $unconfirmedCommits = null;

    /**
     * Run a callback inside a transaction.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     *
     * @throws \Throwable
     */
    public function withTransaction(callable $callback): mixed
    {
        return $this->withInvalidationScope(fn () => $this->withAdapterTransaction($callback));
    }

    /**
     * Run a mutation with mandatory cache invalidation ordered safely around it.
     *
     * A shared tombstone is published after the transaction starts but before
     * the mutation. The outer scope activates a fresh epoch only after commit.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     *
     * @throws \Throwable
     */
    protected function withMutation(Event $event, mixed $data, callable $callback): mixed
    {
        return $this->withInvalidationScope(fn () => $this->withAdapterTransaction(function () use ($event, $data, $callback) {
            $this->blockMutation($event, $data);

            return $callback();
        }));
    }

    /**
     * Run the callback in a savepoint of the open transaction, which the adapter must support, without
     * retrying it. When the callback throws, the savepoint is rolled back and the fallback's result is
     * returned instead, unless the failure is one the transaction retries (see Adapter::isRetryable()), such
     * as a lock conflict whose lock the open transaction holds until it rolls back.
     *
     * @internal
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @param  callable(Throwable): T  $fallback
     * @return T
     *
     * @throws Throwable When the savepoint cannot be started or committed, or the callback's failure when the
     *                   transaction retries it or the savepoint cannot be rolled back, as when the engine rolled
     *                   the whole transaction back
     */
    public function withSavepoint(callable $callback, callable $fallback): mixed
    {
        $context = $this->getEventContext();
        $queued = \count($this->documentPurgeEvents[$context] ?? []);

        $this->adapter->startTransaction();

        try {
            $result = $callback();
        } catch (Throwable $error) {
            try {
                $rolledBack = $this->adapter->rollbackTransaction();
            } catch (Throwable) {
                throw $error;
            }

            if (! $rolledBack || $this->adapter->isRetryable($error)) {
                throw $error;
            }

            if (isset($this->documentPurgeEvents[$context])) {
                \array_splice($this->documentPurgeEvents[$context], $queued);
            }

            return $fallback($error);
        }

        $this->adapter->commitTransaction();

        return $result;
    }

    /**
     * Block the query cache of the collections a mutation writes, once per invalidation scope.
     */
    private function blockMutation(Event $event, mixed $data): void
    {
        $tokens = $this->getInvalidationTokens($event, $data);
        $context = $this->getEventContext();
        $pending = [];
        foreach ($tokens as $key => $token) {
            if (isset($this->queryCacheMutations[$context][$key])) {
                continue;
            }

            $pending[$key] = $token;
        }
        $this->blockInvalidation($pending);
        foreach ($pending as $key => $token) {
            $this->queryCacheMutations[$context][$key] = $token;
        }
    }

    /**
     * Run the callback in an adapter transaction, dropping the document purge events of every attempt the adapter
     * rolls back. Without savepoints a failed nested call is not rolled back, so its events stay queued. The events of
     * the last attempt also stay queued when its commit could not be confirmed.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     *
     * @throws Throwable
     */
    private function withAdapterTransaction(callable $callback): mixed
    {
        $context = $this->getEventContext();
        $queued = \count($this->documentPurgeEvents[$context]);
        $discard = function () use ($context, $queued): void {
            \array_splice($this->documentPurgeEvents[$context], $queued);
        };
        $returned = false;

        try {
            return $this->adapter->withTransaction(function () use ($callback, $discard, &$returned): mixed {
                $returned = false;
                $discard();
                $result = $callback();
                $returned = true;

                return $result;
            });
        } catch (Throwable $error) {
            if ($returned && $error instanceof UnconfirmedException) {
                $this->unconfirmedCommits ??= new WeakMap();
                $this->unconfirmedCommits[$error] = true;
            } elseif ($this->adapter->supports(Capability::TransactionNested)) {
                $discard();
            }

            $this->transactionFailures ??= new WeakMap();
            $this->transactionFailures[$error] = true;

            throw $error;
        }
    }

    /**
     * Fire every queued document purge event, even after one of them fails.
     *
     * @param  list<Closure(): void>  $events
     * @return Throwable|null The first failure
     */
    private function announceDocumentPurges(array $events): ?Throwable
    {
        $failure = null;
        foreach ($events as $announce) {
            try {
                $announce();
            } catch (Throwable $error) {
                $failure ??= $error;
            }
        }

        return $failure;
    }

    /**
     * Keep all nested mutation tombstones blocked, and purge every written document
     * again, once the outer transaction has committed or rolled back. Document purge
     * events queued in the scope fire after a commit, even when the invalidation after it
     * fails, and after a commit that could not be confirmed, whose failure is still the
     * one thrown. A rollback drops them, also when its callback threw Exception\Unconfirmed.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     *
     * @throws Throwable
     */
    private function withInvalidationScope(callable $callback): mixed
    {
        $context = $this->getEventContext();
        $outer = ! isset($this->queryCacheMutations[$context]);
        if ($outer) {
            $this->queryCacheMutations[$context] = [];
            $this->documentCacheMutations[$context] = [];
            $this->documentCachePurges[$context] = [];
            $this->documentPurgeEvents[$context] = [];
            if (! $this->adapter->inTransaction()) {
                $this->transactionWrites[$context] = [];
            }
        }

        try {
            $result = $callback();
        } catch (Throwable $error) {
            if ($outer) {
                $queryTokens = $this->queryCacheMutations[$context];
                $documentTokens = $this->documentCacheMutations[$context];
                $documents = $this->documentCachePurges[$context];
                $purgeEvents = $this->endedInUnconfirmedCommit($error) ? $this->documentPurgeEvents[$context] : [];
                unset(
                    $this->queryCacheMutations[$context],
                    $this->documentCacheMutations[$context],
                    $this->documentCachePurges[$context],
                    $this->transactionWrites[$context],
                    $this->transactionDefinitions[$context],
                    $this->definitionRefills[$context],
                    $this->documentPurgeEvents[$context],
                );
                try {
                    $this->purgeWrittenDocuments($documents);
                } catch (Throwable) {
                    // The transaction's own failure is the one thrown.
                }
                try {
                    $this->activateDocumentInvalidation($documentTokens);
                } catch (Throwable) {
                    // A failed restore leaves the shared tombstone fail-closed.
                }
                try {
                    $this->activateInvalidation($queryTokens);
                } catch (Throwable) {
                    // A failed restore leaves the shared tombstone fail-closed.
                }
                $this->announceDocumentPurges($purgeEvents);
            }

            throw $error;
        }

        if ($outer) {
            $queryTokens = $this->queryCacheMutations[$context];
            $documentTokens = $this->documentCacheMutations[$context];
            $documents = $this->documentCachePurges[$context];
            $purgeEvents = $this->documentPurgeEvents[$context];
            $refills = $this->definitionRefills[$context] ?? [];
            unset(
                $this->queryCacheMutations[$context],
                $this->documentCacheMutations[$context],
                $this->documentCachePurges[$context],
                $this->transactionWrites[$context],
                $this->transactionDefinitions[$context],
                $this->definitionRefills[$context],
                $this->documentPurgeEvents[$context],
            );

            $failure = null;
            try {
                $this->purgeWrittenDocuments($documents);
            } catch (Throwable $error) {
                $failure = $error;
            }
            try {
                $this->activateDocumentInvalidation($documentTokens);
            } catch (Throwable $error) {
                $failure ??= $error;
            }
            try {
                $this->activateInvalidation($queryTokens);
            } catch (Throwable $error) {
                $failure ??= $error;
            }

            $announcement = $this->announceDocumentPurges($purgeEvents);
            $failure ??= $announcement;

            if ($failure !== null) {
                $this->committedFailures ??= new WeakMap();
                $this->committedFailures[$failure] = true;

                throw $failure;
            }

            $this->cacheTransactionDefinitions($refills);
        }

        return $result;
    }

    private function queueDefinitionRefill(string $definitionKey, string $field, string $id): void
    {
        $tenant = $this->adapter->getTenant();
        $filtering = $this->filtering()->get();
        $exclusions = $this->filterExclusions()->get();

        $this->definitionRefills[$this->getEventContext()][$definitionKey][$field] = function () use ($tenant, $filtering, $exclusions, $id): void {
            $this->withTenant($tenant, fn (): Document => $this->filtering()->with(
                $filtering,
                fn (): Document => $this->filterExclusions()->with(
                    $exclusions,
                    fn (): Document => $this->silent(fn (): Document => $this->getDocument(self::METADATA, $id)),
                ),
            ));
        };
    }

    /**
     * @param  array<string, array<string, Closure(): void>>  $refills
     */
    private function cacheTransactionDefinitions(array $refills): void
    {
        foreach ($refills as $fields) {
            foreach ($fields as $refill) {
                if ($this->definitionFillsFail) {
                    return;
                }

                try {
                    $refill();
                } catch (Throwable $error) {
                    Console::warning('Warning: Failed to cache collection definition after commit: '.$error->getMessage());
                }

                if ($this->isReadFromReplica()) {
                    $this->definitionFillsFail = true;
                }
            }
        }
    }

    /**
     * Whether the error was raised after its outermost transaction committed: the writes it
     * reports on are stored, and only the invalidation or the events after the commit failed.
     */
    private function failedAfterCommit(Throwable $error): bool
    {
        return isset($this->committedFailures[$error]);
    }

    /**
     * Whether the error is the Exception\Unconfirmed of a commit, not one its callback threw: the writes of the
     * transaction's last attempt may be stored. It answers once, so a later transaction that rethrows the same error
     * from its callback counts as rolled back.
     */
    private function endedInUnconfirmedCommit(Throwable $error): bool
    {
        if (! isset($this->unconfirmedCommits[$error])) {
            return false;
        }

        unset($this->unconfirmedCommits[$error]);

        return true;
    }

    /**
     * Whether the writes the error reports on may be stored: it was raised after their outermost transaction
     * committed, or the commit could not be confirmed. Undoing what they describe could leave a stored definition
     * without its table, column or index.
     */
    private function mayHaveCommitted(Throwable $error): bool
    {
        return $error instanceof UnconfirmedException || $this->failedAfterCommit($error);
    }

    /**
     * Whether an adapter transaction let the error through although it retries such a failure: it already spent its
     * attempts on it, or left it to the outermost transaction that encloses it. Running that transaction again would
     * multiply its retries.
     */
    private function retriedByTransaction(Throwable $error): bool
    {
        return isset($this->transactionFailures[$error]) && $this->adapter->isRetryable($error);
    }
}
