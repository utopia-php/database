<?php

namespace Utopia\Database\Hook;

use Closure;

/**
 * MongoDB read hook that injects tenant isolation filters into queries for shared-table configurations.
 *
 * Unlike SQL adapters which use separate TenantFilter (read) and Tenant (write) hooks,
 * MongoDB stores the tenant identifier as an embedded `_tenant` field directly on the document.
 * The Mongo adapter sets this field during document creation without a separate write hook.
 * Read filtering is sufficient because tenant isolation only requires query-time filtering.
 */
class MongoTenantFilter implements Read
{
    /**
     * @param int|string|null $tenant The current tenant ID
     * @param bool $sharedTables Whether shared tables mode is enabled
     * @param Closure(string, array<int|string>=): (int|string|null|array<string, array<int|string>>) $getTenantFilters Closure that returns tenant filter values for a collection
     */
    public function __construct(
        private int|string|null $tenant,
        private bool $sharedTables,
        private Closure $getTenantFilters,
    ) {
    }

    /**
     * Add a _tenant filter to restrict results to the current tenant.
     *
     * @param array<string, mixed> $filters The current MongoDB filter array
     * @param string $collection The collection being queried
     * @param string $forPermission The permission type (unused in tenant filtering)
     * @return array<string, mixed> The modified filter array with tenant constraints
     */
    public function applyFilters(array $filters, string $collection, string $forPermission = 'read'): array
    {
        if (! $this->sharedTables || $this->tenant === null) {
            return $filters;
        }

        $filters['_tenant'] = ($this->getTenantFilters)($collection);

        return $filters;
    }
}
