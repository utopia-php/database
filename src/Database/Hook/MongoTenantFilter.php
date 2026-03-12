<?php

namespace Utopia\Database\Hook;

class MongoTenantFilter implements Read
{
    /**
     * @param  \Closure(string, array<int>=): (int|null|array<string, array<int>>)  $getTenantFilters
     */
    public function __construct(
        private ?int $tenant,
        private bool $sharedTables,
        private \Closure $getTenantFilters,
    ) {}

    public function applyFilters(array $filters, string $collection, string $forPermission = 'read'): array
    {
        if (! $this->sharedTables || $this->tenant === null) {
            return $filters;
        }

        $filters['_tenant'] = ($this->getTenantFilters)($collection);

        return $filters;
    }
}
