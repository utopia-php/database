<?php

namespace Utopia\Database\Hook\Mongo;

use Closure;
use Utopia\Database\PermissionType;
use Utopia\Database\Storage;

/**
 * Scopes every MongoDB read, update and delete filter to the current tenant when tables are shared. MongoDB keeps
 * the tenant in the embedded `_tenant` field the adapter writes on create, so no write-side hook exists.
 */
final readonly class Tenant implements Read
{
    /**
     * @param  Closure(string, array<int|string>=): (int|string|null|array<string, array<int|string|null>>)  $tenantFilters
     */
    public function __construct(
        private bool $sharedTables,
        private Closure $tenantFilters,
    ) {
    }

    /**
     * @param  array<string, mixed>  $filters
     * @return array<string, mixed>
     */
    public function applyFilters(array $filters, string $collection, PermissionType $forPermission = PermissionType::Read): array
    {
        if (! $this->sharedTables) {
            return $filters;
        }

        $filters[Storage::TENANT] = ($this->tenantFilters)($collection);

        return $filters;
    }
}
