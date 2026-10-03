<?php

namespace Tests\Unit\Support;

use PDO;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Capability;

/**
 * SQLite that can go without savepoints, running a nested transaction inside the open one as MongoDB does.
 */
class RelationshipSQLite extends SQLite
{
    public function __construct(
        PDO $pdo,
        private readonly bool $savepoints = true,
    ) {
        parent::__construct($pdo);
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
