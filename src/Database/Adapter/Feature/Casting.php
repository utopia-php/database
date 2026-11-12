<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

/**
 * Implemented by an adapter whose engine stores and returns native types, so it converts documents itself instead
 * of having the library cast them. Reads are converted a page at a time.
 */
interface Casting
{
    /**
     * Convert a document's values to the engine's native types before it is written.
     */
    public function castBefore(Document $collection, Document $document): Document;

    /**
     * Convert documents read from the engine back to the collection's attribute types.
     *
     * @param  array<Document>  $documents
     * @return array<Document> The documents under the keys they were given with
     */
    public function castAfter(Document $collection, array $documents): array;

    /**
     * Convert a datetime string to the value the engine compares stored datetimes with.
     */
    public function castDatetime(string $value): mixed;
}
