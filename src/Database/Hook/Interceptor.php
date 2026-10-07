<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;

/**
 * A {@see Write} hook that leaves every row and document as it is; subclasses override what they intercept.
 */
abstract class Interceptor implements Write
{
    public function decorateRow(array $row, RowMetadata $metadata): array
    {
        return $row;
    }

    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
    }

    public function afterDocumentUpdate(string $collection, string $id, Document $document, WriteContext $context): void
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
