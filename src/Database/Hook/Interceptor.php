<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;

/**
 * A {@see Write} hook that leaves every row and document as it is; subclasses override what they intercept.
 */
abstract class Interceptor implements Write
{
    #[\Override]
    public function decorateRow(array $row, RowMetadata $metadata): array
    {
        return $row;
    }

    #[\Override]
    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
    }

    #[\Override]
    public function afterDocumentUpdate(string $collection, string $id, Document $document, WriteContext $context): void
    {
    }

    #[\Override]
    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void
    {
    }

    #[\Override]
    public function afterDocumentUpsert(string $collection, array $changes, WriteContext $context): void
    {
    }

    #[\Override]
    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void
    {
    }
}
