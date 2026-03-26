<?php

namespace Utopia\Database\Hook;

/**
 * Tenant hook that handles both read-side query filtering and write-side row decoration.
 *
 * On reads: The SQL adapter generates tenant isolation conditions when this hook is registered.
 * On writes: Injects the tenant identifier into every row written to a shared table.
 */
class Tenancy extends Interceptor
{
    /**
     * @param int|string $tenant The current tenant identifier
     * @param string $column The column name used to store the tenant value
     */
    public function __construct(
        private int|string $tenant,
        private string $column = '_tenant',
    ) {
    }

    public function getTenant(): int|string
    {
        return $this->tenant;
    }

    public function decorateRow(array $row, array $metadata = []): array
    {
        $row[$this->column] = $metadata['tenant'] ?? $this->tenant;

        return $row;
    }
}
