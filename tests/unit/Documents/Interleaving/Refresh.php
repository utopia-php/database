<?php

namespace Tests\Unit\Documents\Interleaving;

use Utopia\Database\Database;
use Utopia\Database\Document;

/**
 * Updates a document the first time a find() of its collection returns it.
 */
final class Refresh
{
    private bool $done = false;

    /**
     * @param  array<string, mixed>  $changes
     */
    public function __construct(
        private readonly Database $database,
        private readonly string $collection,
        private readonly string $id,
        private readonly array $changes,
    ) {
    }

    /**
     * @param  array<Document>  $documents
     */
    public function __invoke(Document $collection, array $documents): void
    {
        if ($this->done || $collection->getId() !== $this->collection) {
            return;
        }

        foreach ($documents as $document) {
            if ($document->getId() === $this->id) {
                $this->done = true;
                $this->database->updateDocument($this->collection, $this->id, new Document($this->changes));

                return;
            }
        }
    }
}
