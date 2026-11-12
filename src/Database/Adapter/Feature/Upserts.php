<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Change;
use Utopia\Database\Document;

interface Upserts
{
    /**
     * Insert the change's new document, or update the stored one it replaces.
     */
    public function upsertDocument(Document $collection, Change $change): Document;

    /**
     * Insert or update each change's new document. With $increase, an update adds the new document's value of that
     * attribute to the stored one instead of replacing it.
     *
     * @param  array<Change>  $changes
     * @return array<Document> The written documents, in the order of $changes
     */
    public function upsertDocuments(Document $collection, array $changes, ?string $increase = null): array;
}
