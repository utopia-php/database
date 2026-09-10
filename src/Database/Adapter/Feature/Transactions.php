<?php

namespace Utopia\Database\Adapter\Feature;

/**
 * Defines transaction control operations for a database adapter.
 */
interface Transactions
{
    /**
     * Begin a new database transaction.
     *
     * @return bool True on success.
     */
    public function startTransaction(): bool;

    /**
     * Commit the current database transaction.
     *
     * @return bool True on success.
     */
    public function commitTransaction(): bool;

    /**
     * Roll back the current database transaction.
     *
     * @return bool True on success.
     */
    public function rollbackTransaction(): bool;
}
