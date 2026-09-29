<?php

namespace Utopia\Database\Traits;

use Throwable;
use Utopia\Database\Event;

/**
 * Provides transactional execution support, delegating to the underlying database adapter.
 */
trait Transactions
{
    /** @var array<int, array<string, string>> Collection keys of the documents written in the open invalidation scope, by coroutine id and document key. */
    protected array $documentCachePurges = [];

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
        return $this->withInvalidationScope(fn () => $this->adapter->withTransaction($callback));
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
        return $this->withInvalidationScope(fn () => $this->adapter->withTransaction(function () use ($event, $data, $callback) {
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
     * Keep all nested mutation tombstones blocked, and purge every written document
     * again, once the outer transaction has committed or rolled back.
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
            unset(
                $this->queryCacheMutations[$context],
                $this->documentCacheMutations[$context],
                $this->documentCachePurges[$context],
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

            if ($failure !== null) {
                throw $failure;
            }
        }

        return $result;
    }
}
