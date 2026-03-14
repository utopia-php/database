<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Change;
use Utopia\Database\Document;

/**
 * Defines upsert (insert-or-update) operations for a database adapter.
 */
interface Upserts
{
    /**
     * Upsert multiple documents, inserting or updating based on a unique attribute.
     *
     * @param Document $collection The collection document.
     * @param string $attribute The unique attribute used to determine insert vs update.
     * @param array<Change> $changes The old/new document pairs to upsert.
     * @return array<Document> The resulting documents after upsert.
     */
    public function upsertDocuments(Document $collection, string $attribute, array $changes): array;
}
