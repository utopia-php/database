<?php

namespace Utopia\Database\Traits;

use Closure;
use Throwable;
use Utopia\Database\Event;

/**
 * Provides transactional execution support, delegating to the underlying database adapter.
 */
trait Transactions
{
    /** @var array<int, array<string, string>> Collection keys of the documents written in the open invalidation scope, by coroutine id and document key. */
    protected array $documentCachePurges = [];

    /** @var array<int, list<Closure(): void>> Document purge events of the open invalidation scope, by coroutine id, fired once its outermost transaction has committed. */
    protected array $documentPurgeEvents = [];

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

            return $callback();
        }));
    }

    /**
     * Run the callback in an adapter transaction that leaves no document purge event of a
     * rolled-back attempt queued: each attempt starts from the events queued before the
     * transaction, and a transaction that fails drops the events queued inside it.
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

        try {
            return $this->adapter->withTransaction(function () use ($callback, $discard): mixed {
                $discard();

                return $callback();
            });
        } catch (Throwable $error) {
            $discard();

            throw $error;
        }
    }

    /**
     * Keep all nested mutation tombstones blocked, and purge every written document
     * again, once the outer transaction has committed or rolled back. Document purge
     * events queued in the scope fire after a commit, even when the invalidation after it
     * fails, and are dropped with a rollback.
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
        }

        try {
            $result = $callback();
        } catch (Throwable $error) {
            if ($outer) {
                $queryTokens = $this->queryCacheMutations[$context];
                $documentTokens = $this->documentCacheMutations[$context];
                $documents = $this->documentCachePurges[$context];
                unset(
                    $this->queryCacheMutations[$context],
                    $this->documentCacheMutations[$context],
                    $this->documentCachePurges[$context],
                    $this->documentPurgeEvents[$context],
                );
                try {
                    $this->purgeWrittenDocuments($documents);
                } catch (Throwable) {
                    // Rolled back: the cached entries still hold committed rows.
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
            }

            throw $error;
        }

        if ($outer) {
            $queryTokens = $this->queryCacheMutations[$context];
            $documentTokens = $this->documentCacheMutations[$context];
            $documents = $this->documentCachePurges[$context];
            $purgeEvents = $this->documentPurgeEvents[$context];
            unset(
                $this->queryCacheMutations[$context],
                $this->documentCacheMutations[$context],
                $this->documentCachePurges[$context],
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

            foreach ($purgeEvents as $announce) {
                try {
                    $announce();
                } catch (Throwable $error) {
                    $failure ??= $error;
                }
            }

            if ($failure !== null) {
                throw $failure;
            }
        }

        return $result;
    }
}
