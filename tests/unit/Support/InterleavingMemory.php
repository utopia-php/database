<?php

namespace Tests\Unit\Support;

use Closure;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

/**
 * Counting spy that runs a callback once, right after the next collection definition read returns, so a test can
 * land another writer's commit between a read and the cache fill it leads to.
 */
final class InterleavingMemory extends CountingMemory
{
    private ?Closure $afterDefinitionRead = null;

    /**
     * @param  Closure(): void  $callback
     */
    public function afterNextDefinitionRead(Closure $callback): void
    {
        $this->afterDefinitionRead = $callback;
    }

    /**
     * @param  array<Query>  $queries
     */
    #[\Override]
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $document = parent::getDocument($collection, $id, $queries, $forUpdate);

        $callback = $this->afterDefinitionRead;
        if ($callback !== null && $collection->getId() === Database::METADATA) {
            $this->afterDefinitionRead = null;
            $callback();
        }

        return $document;
    }
}
