<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

/**
 * SQL read hook that generates tenant isolation conditions for shared-table configurations.
 */
class TenantFilter implements Filter
{
    /**
     * @param int|string $tenant The current tenant identifier
     * @param string $metadataCollection The metadata collection name; metadata tables allow NULL tenants
     */
    public function __construct(
        private int|string $tenant,
        private string $metadataCollection = ''
    ) {
    }

    /**
     * Generate a SQL condition restricting results to the current tenant.
     *
     * @param string $table The table name being queried
     * @return Condition A condition filtering by the _tenant column
     */
    public function filter(string $table): Condition
    {
        // For metadata tables, also allow NULL tenant
        if (! empty($this->metadataCollection) && str_contains($table, $this->metadataCollection)) {
            return new Condition('(_tenant IN (?) OR _tenant IS NULL)', [$this->tenant]);
        }

        return new Condition('_tenant IN (?)', [$this->tenant]);
    }
}
