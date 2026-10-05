<?php

namespace Tests\Unit\Adapter;

use Tests\Unit\CountingAdapterHooks;
use Utopia\Database\Database;
use Utopia\Database\Document;

trait SQLSchemaMagicReadsAdapter
{
    use CountingAdapterHooks;

    /**
     * @var array<string, Document>
     */
    private array $definitions = [];

    public function define(Document $definition): void
    {
        $this->definitions[$definition->getId()] = $definition;
    }

    /**
     * @param  array<mixed>  $queries
     */
    #[\Override]
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        if ($collection->getId() !== Database::METADATA) {
            return new Document();
        }

        return $this->definitions[$id] ?? new Document();
    }
}
