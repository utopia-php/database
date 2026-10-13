<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;
use Utopia\Query\Hook;

/**
 * Hook for preparing each nested related document of a write before the write relates it.
 *
 * It is given each related document that createDocument(), createDocuments(), updateDocument(), upsertDocument(),
 * upsertDocuments(), authorizeCreate() or authorizeUpdate() is given as a document (a {@see Document} or an
 * associative array), at every depth the write relates. It runs before the write checks the document's grants and
 * relates it, a parent before its children. Hooks run in the order they were registered, each given the document
 * the previous one returned.
 *
 * It is given a copy, never the caller's document, and the document it returns is the one the write uses. In
 * createDocuments(), upsertDocument() and upsertDocuments() the prepared related documents replace the relationship
 * values of the caller's top-level documents in place, as those writes prepare the documents they are given.
 *
 * It is not given the top-level document, a related document given as an ID, or anything from updateDocuments().
 *
 * It can run more than once for one logical write, as a pre-check and then the write, or a retried transaction, so
 * it must return the same document when given its own output.
 *
 * It runs while the relationship hook is attached and enabled, whether or not events are silenced. Its exceptions
 * abort the write before anything is written.
 */
interface Nested extends Hook
{
    /**
     * Prepare a related document before the write checks and relates it.
     *
     * @param  Document  $collection  The collection the related document belongs to
     * @param  Document  $document  A copy of the related document as the write was given it
     * @return Document The related document the write checks and relates
     */
    public function prepare(Document $collection, Document $document): Document;
}
