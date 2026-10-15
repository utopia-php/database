<?php

namespace Tests\Unit\Documents\Interleaving;

use Utopia\Database\Adapter\Memory as MemoryAdapter;

/**
 * An engine without transactions, as a standalone MongoDB server is: every write is stored as it runs, and nothing
 * is rolled back.
 */
final class Standalone extends MemoryAdapter
{
    use Interleaves;

    #[\Override]
    public function startTransaction(): bool
    {
        return true;
    }

    #[\Override]
    public function commitTransaction(): bool
    {
        return true;
    }

    #[\Override]
    public function rollbackTransaction(): bool
    {
        return true;
    }

    #[\Override]
    public function withTransaction(callable $callback): mixed
    {
        return $callback();
    }
}
