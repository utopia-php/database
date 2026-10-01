<?php

namespace Tests\Unit\Support;

use PDO;
use RuntimeException;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Capability;
use Utopia\Database\Document;

/**
 * SQLite that can go without savepoints, running a nested transaction inside the open one as MongoDB does, which
 * leaves the relationship hook writing each related document of a create where it would be written on its own,
 * and can refuse to look up sequences, which leaves the hook unable to tell that a create's related documents are
 * new and so relating them one by one, as it always did.
 */
class RelationshipSQLite extends SQLite
{
    public function __construct(
        PDO $pdo,
        private readonly bool $savepoints = true,
        private readonly bool $sequences = true,
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

    /**
     * @param  array<Document>  $documents
     * @return array<Document>
     */
    #[\Override]
    public function getSequences(string $collection, array $documents): array
    {
        if (! $this->sequences) {
            throw new RuntimeException('Sequences are not looked up');
        }

        return parent::getSequences($collection, $documents);
    }
}
