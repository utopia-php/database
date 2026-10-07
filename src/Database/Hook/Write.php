<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Change;
use Utopia\Database\Document;
use Utopia\Query\Hook;

/**
 * Intercepts the adapter's document writes: decorates every row written and keeps rows of its own in step with the
 * documents.
 */
interface Write extends Hook
{
    /**
     * Decorate a row written for a document, to its table or to a side table.
     *
     * @param  array<string, mixed>  $row
     * @return array<string, mixed>
     */
    public function decorateRow(array $row, RowMetadata $metadata): array;

    /**
     * @param  array<Document>  $documents
     */
    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void;

    /**
     * @param  string  $id  The id the document was stored under before the update
     */
    public function afterDocumentUpdate(string $collection, string $id, Document $document, WriteContext $context): void;

    /**
     * @param  array<Document>  $documents
     */
    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void;

    /**
     * @param  array<Change>  $changes
     */
    public function afterDocumentUpsert(string $collection, array $changes, WriteContext $context): void;

    /**
     * @param  list<string>  $documentIds
     */
    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void;
}
