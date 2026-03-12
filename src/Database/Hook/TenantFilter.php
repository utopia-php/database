<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

class TenantFilter implements Filter
{
    public function __construct(
        private int|string $tenant,
        private string $metadataCollection = ''
    ) {
    }

    public function filter(string $table): Condition
    {
        // For metadata tables, also allow NULL tenant
        if (! empty($this->metadataCollection) && str_contains($table, $this->metadataCollection)) {
            return new Condition('(_tenant IN (?) OR _tenant IS NULL)', [$this->tenant]);
        }

        return new Condition('_tenant IN (?)', [$this->tenant]);
    }
}
