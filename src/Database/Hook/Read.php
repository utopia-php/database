<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Hook;

/**
 * Read hook interface for MongoDB adapters that apply filters to query filter arrays.
 */
interface Read extends Hook
{
    /**
     * Apply read-side filters to a MongoDB filter array.
     *
     * @param  array<string, mixed>  $filters  The current MongoDB filter array
     * @param  string  $collection  The collection being queried
     * @param  string  $forPermission  The permission type to check (e.g. 'read')
     * @return array<string, mixed> The modified filter array
     */
    public function applyFilters(array $filters, string $collection, string $forPermission = 'read'): array;
}
