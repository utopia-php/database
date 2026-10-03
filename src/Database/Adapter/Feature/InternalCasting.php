<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

/**
 * Defines hooks for casting document values before and after database operations.
 */
interface InternalCasting
{
    /**
     * Cast document attribute values before writing to the database.
     *
     * @param Document $collection The collection document.
     * @param Document $document The document to cast.
     * @return Document The document with cast values.
     */
    public function castingBefore(Document $collection, Document $document): Document;

    /**
     * Cast document attribute values after reading from the database.
     *
     * @param Document $collection The collection document.
     * @param Document $document The document to cast.
     * @return Document The document with cast values.
     */
    public function castingAfter(Document $collection, Document $document): Document;

    /**
     * Cast the attribute values of documents read from the database, as castingAfter() does for each of them.
     *
     * @param Document $collection The collection document.
     * @param array<Document> $documents The documents to cast.
     * @return array<Document> The documents with cast values, under the keys they were given with.
     */
    public function castingAfterDocuments(Document $collection, array $documents): array;
}
