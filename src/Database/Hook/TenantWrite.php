<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;

/**
 * Write hook that injects the tenant identifier into every row written to a shared table.
 */
class TenantWrite implements Write
{
    /**
     * @param int $tenant The current tenant identifier
     * @param string $column The column name used to store the tenant value
     */
    public function __construct(
        private int $tenant,
        private string $column = '_tenant',
    ) {
    }

    /**
     * {@inheritDoc}
     */
    public function decorateRow(array $row, array $metadata = []): array
    {
        $row[$this->column] = $metadata['tenant'] ?? $this->tenant;

        return $row;
    }

    /**
     * {@inheritDoc}
     */
    public function afterCreate(string $table, array $metadata, mixed $context): void
    {
    }

    /**
     * {@inheritDoc}
     */
    public function afterUpdate(string $table, array $metadata, mixed $context): void
    {
    }

    /**
     * {@inheritDoc}
     */
    public function afterBatchUpdate(string $table, array $updateData, array $metadata, mixed $context): void
    {
    }

    /**
     * {@inheritDoc}
     */
    public function afterDelete(string $table, array $ids, mixed $context): void
    {
    }

    /**
     * {@inheritDoc}
     */
    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
    }

    /**
     * {@inheritDoc}
     */
    public function afterDocumentUpdate(string $collection, Document $document, bool $skipPermissions, WriteContext $context): void
    {
    }

    /**
     * {@inheritDoc}
     */
    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void
    {
    }

    /**
     * {@inheritDoc}
     */
    public function afterDocumentUpsert(string $collection, array $changes, WriteContext $context): void
    {
    }

    /**
     * {@inheritDoc}
     */
    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void
    {
    }
}
