<?php

namespace Utopia\Database\Traits;

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
}
