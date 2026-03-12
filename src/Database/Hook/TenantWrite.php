<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;

class TenantWrite implements Write
{
    public function __construct(
        private int $tenant,
        private string $column = '_tenant',
    ) {}

    public function decorateRow(array $row, array $metadata = []): array
    {
        $row[$this->column] = $metadata['tenant'] ?? $this->tenant;

        return $row;
    }

    public function afterCreate(string $table, array $metadata, mixed $context): void {}

    public function afterUpdate(string $table, array $metadata, mixed $context): void {}

    public function afterBatchUpdate(string $table, array $updateData, array $metadata, mixed $context): void {}

    public function afterDelete(string $table, array $ids, mixed $context): void {}

    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void {}

    public function afterDocumentUpdate(string $collection, Document $document, bool $skipPermissions, WriteContext $context): void {}

    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void {}

    public function afterDocumentUpsert(string $collection, array $changes, WriteContext $context): void {}

    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void {}
}
