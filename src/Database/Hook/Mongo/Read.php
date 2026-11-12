<?php

namespace Utopia\Database\Hook\Mongo;

use Utopia\Database\PermissionType;
use Utopia\Query\Hook;

/**
 * Narrows a MongoDB filter array before the Mongo adapter runs it.
 *
 * Database authorizes every write before it reaches the adapter, so write paths apply the tenant hook alone and
 * never inherit a permission filter.
 */
interface Read extends Hook
{
    /**
     * @param  array<string, mixed>  $filters
     * @return array<string, mixed>
     */
    public function applyFilters(array $filters, string $collection, PermissionType $forPermission): array;
}
