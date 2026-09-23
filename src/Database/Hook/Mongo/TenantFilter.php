<?php

namespace Utopia\Database\Hook\Mongo;

use Closure;
use Utopia\Database\Storage;
use Utopia\Query\Hook;

/**
 * MongoDB hook that injects tenant isolation filters into queries and writes for shared-table configurations.
 *
 * Unlike SQL adapters which use separate TenantFilter (read) and Tenant (write) hooks,
 * MongoDB stores the tenant identifier as an embedded `_tenant` field directly on the document.
 * The Mongo adapter sets this field during document creation without a separate write hook,
 * and scopes every read, update and delete filter with this hook.
 */
class TenantFilter implements Hook
{
    /**
     * @param bool $sharedTables Whether shared tables mode is enabled
     * @param Closure(string, array<int|string>=): (int|string|null|array<string, array<int|string|null>>) $getTenantFilters Closure that returns tenant filter values for a collection
     */
    public function __construct(
        private bool $sharedTables,
        private Closure $getTenantFilters,
    ) {
    }

    /**
     * Add a _tenant filter to restrict results to the current tenant.
     *
     * @param array<string, mixed> $filters The current MongoDB filter array
     * @param string $collection The collection being queried
     * @return array<string, mixed> The modified filter array with tenant constraints
     */
    public function applyFilters(array $filters, string $collection): array
    {
        if (! $this->sharedTables) {
            return $filters;
        }

        $filters[Storage::TENANT] = ($this->getTenantFilters)($collection);

        return $filters;
    }
}
