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
}
