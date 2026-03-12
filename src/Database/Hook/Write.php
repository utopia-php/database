<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Change;
use Utopia\Database\Document;
use Utopia\Query\Hook\Write as BaseWrite;

interface Write extends BaseWrite
{
    /**
     * Decorate a row before it's written to any table (document or side table).
     * Database-level adapter calls this with document metadata extracted from Document objects.
     *
     * @param  array<string, mixed>  $row
     * @param  array<string, mixed>  $metadata
     * @return array<string, mixed>
     */
    public function decorateRow(array $row, array $metadata = []): array;

    /**
     * Execute after documents are created (e.g. insert permission rows).
     *
     * @param  array<Document>  $documents
     */
    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void;

    /**
     * Execute after a document is updated (e.g. sync permission rows).
     */
    public function afterDocumentUpdate(string $collection, Document $document, bool $skipPermissions, WriteContext $context): void;

    /**
     * Execute after documents are updated in batch (e.g. sync permission rows).
     *
     * @param  array<Document>  $documents
     */
    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void;

    /**
     * Execute after documents are upserted (e.g. sync permission rows from old→new diffs).
     *
     * @param  array<Change>  $changes
     */
    public function afterDocumentUpsert(string $collection, array $changes, WriteContext $context): void;

    /**
     * Execute after documents are deleted (e.g. clean up permission rows).
     *
     * @param  list<string>  $documentIds
     */
    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void;
}
