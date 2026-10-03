<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;

/**
 * Base class for database write hooks with default no-op implementations.
 *
 * Provides empty defaults for all query-level and document-level write methods.
 * Subclasses override only the methods they need, avoiding boilerplate stubs.
 *
 * Query-level methods (afterCreate, afterUpdate, afterBatchUpdate, afterDelete)
 * exist because the database Write interface extends the query-layer Write
 * interface. Most database hooks only need the document-level methods.
 */
abstract class Interceptor implements Write
{
    public function decorateRow(array $row, array $metadata = []): array
    {
        return $row;
    }

    public function afterCreate(string $table, array $metadata, mixed $context): void
    {
    }

    public function afterUpdate(string $table, array $metadata, mixed $context): void
    {
    }

    public function afterBatchUpdate(string $table, array $updateData, array $metadata, mixed $context): void
    {
    }

    public function afterDelete(string $table, array $ids, mixed $context): void
    {
    }

    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
    }

    public function afterDocumentUpdate(string $collection, Document $document, bool $skipPermissions, WriteContext $context): void
    {
    }

    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void
    {
    }

    public function afterDocumentUpsert(string $collection, array $changes, WriteContext $context): void
    {
    }

    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void
    {
    }
}
