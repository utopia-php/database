<?php

namespace Utopia\Database\Traits;

use Throwable;
use Utopia\Database\Event;

/**
 * Provides transactional execution support, delegating to the underlying database adapter.
 */
trait Transactions
{
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
            foreach ($tokens as $collection => $token) {
                if (isset($this->queryCacheMutations[$context][$collection])) {
                    continue;
                }

                $pending[$collection] = $token;
            }
            $this->blockInvalidation($pending);
            foreach ($pending as $collection => $token) {
                $this->queryCacheMutations[$context][$collection] = $token;
            }

            return $callback();
        }));
    }

    /**
     * Keep all nested mutation tombstones blocked until the outer transaction
     * has committed or rolled back.
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
        }

        try {
            $result = $callback();
        } catch (Throwable $error) {
            if ($outer) {
                $tokens = $this->queryCacheMutations[$context];
                unset($this->queryCacheMutations[$context]);
                try {
                    $this->activateInvalidation($tokens);
                } catch (Throwable) {
                    // A failed restore leaves the shared tombstone fail-closed.
                }
            }

            throw $error;
        }

        if ($outer) {
            $tokens = $this->queryCacheMutations[$context];
            unset($this->queryCacheMutations[$context]);
            $this->activateInvalidation($tokens);
        }

        return $result;
    }
}
