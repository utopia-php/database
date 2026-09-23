<?php

namespace Utopia\Database\Hook;

use Utopia\Database\PermissionType;
use Utopia\Query\Hook;

/**
 * Read hook interface for MongoDB adapters that narrow a query's filter array by permission.
 *
 * Only reads apply these hooks. Database authorizes every write before it reaches the adapter,
 * so write paths are scoped by tenant alone and never inherit a read filter.
 */
interface Read extends Hook
{
    /**
     * Apply read-side filters to a MongoDB filter array.
     *
     * @param  array<string, mixed>  $filters  The current MongoDB filter array
     * @param  string  $collection  The collection being queried
     * @param  PermissionType  $forPermission  The permission the caller must hold on every matched document
     * @return array<string, mixed> The modified filter array
     */
    public function applyFilters(array $filters, string $collection, PermissionType $forPermission): array;
}
