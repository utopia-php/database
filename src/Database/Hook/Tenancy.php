<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Storage;

/**
 * Stores the tenant in every row written to a shared table: the document's own, or the adapter's when the document
 * names none. Reads are scoped to the tenant by the adapter's tenant filter.
 */
class Tenancy extends Interceptor
{
    public function __construct(
        private readonly string $column = Storage::TENANT,
    ) {
    }

    #[\Override]
    public function decorateRow(array $row, RowMetadata $metadata): array
    {
        $row[$this->column] = $metadata->tenant;

        return $row;
    }
}
