<?php

namespace Utopia\Database\Hook;

use Closure;

/**
 * MongoDB read hook that injects tenant isolation filters into queries for shared-table configurations.
 */
class MongoTenantFilter implements Read
{
    /**
     * @param int|null $tenant The current tenant ID
     * @param bool $sharedTables Whether shared tables mode is enabled
     * @param Closure(string, array<int>=): (int|null|array<string, array<int>>) $getTenantFilters Closure that returns tenant filter values for a collection
     */
    public function __construct(
        private ?int $tenant,
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
