<?php

namespace Utopia\Database\Traits;

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
        return $this->adapter->withTransaction($callback);
    }

    /**
     * Run a mutation with mandatory cache invalidation ordered safely around it.
     *
     * Transactional adapters invalidate after the mutation and before commit, so
     * invalidation failures roll the mutation back. Non-transactional adapters
     * invalidate before and after the mutation: the first failure prevents the
     * write, while the second closes any stale-fill window during the write.
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
        return $this->adapter->withTransaction(function () use ($event, $data, $callback) {
            $transactional = $this->adapter->inTransaction();

            if (! $transactional) {
                $this->invalidate($event, $data);
            }

            try {
                $result = $callback();
            } finally {
                if (! $transactional) {
                    $this->invalidate($event, $data);
                }
            }

            if ($transactional) {
                $this->invalidate($event, $data);
            }

            return $result;
        });
    }
}
