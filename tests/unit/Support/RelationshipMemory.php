<?php

namespace Tests\Unit\Support;

use Utopia\Database\Capability;

/**
 * Memory that can go without savepoints, running a nested transaction inside the open one as MongoDB does.
 */
class RelationshipMemory extends CountingMemory
{
    public function __construct(
        private readonly bool $savepoints = true,
    ) {
        parent::__construct();
    }

    /**
     * @return array<Capability>
     */
    #[\Override]
    public function capabilities(): array
    {
        $capabilities = parent::capabilities();
        if ($this->savepoints) {
            return $capabilities;
        }

        return \array_values(\array_filter(
            $capabilities,
            static fn (Capability $capability): bool => $capability !== Capability::NestedTransactions,
        ));
    }

    /**
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    #[\Override]
    public function withTransaction(callable $callback): mixed
    {
        if (! $this->savepoints && $this->inTransaction()) {
            return $callback();
        }

        return parent::withTransaction($callback);
    }
}
