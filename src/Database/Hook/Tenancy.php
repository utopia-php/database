<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Storage;

/**
 * Tenant hook that handles both read-side query filtering and write-side row decoration.
 *
 * On reads: The SQL adapter generates tenant isolation conditions when this hook is registered.
 * On writes: Injects the tenant identifier into every row written to a shared table.
 */
class Tenancy extends Interceptor
{
    /**
     * @param int|string|null $tenant The ambient tenant identifier, or null when each document carries its own
     * @param string $column The column name used to store the tenant value
     */
    public function __construct(
        private int|string|null $tenant,
        private string $column = Storage::TENANT,
    ) {
    }

    public function getTenant(): int|string|null
    {
        return $this->tenant;
    }

    public function decorateRow(array $row, array $metadata = []): array
    {
        $row[$this->column] = $metadata['tenant'] ?? $this->tenant;

        return $row;
    }
}
