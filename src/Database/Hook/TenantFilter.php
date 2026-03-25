<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Filter;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * SQL read hook that generates tenant isolation conditions for shared-table configurations.
 */
class TenantFilter implements Filter, JoinFilter
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

    public function filter(string $table): Condition
    {
        // Only qualify with table/alias when it looks like a simple alias (no dots/backticks)
        // This avoids breaking subqueries where $table is a fully-qualified raw table name
        $prefix = (!\str_contains($table, '.') && !\str_contains($table, '`')) ? "{$table}." : '';

        if (! empty($this->metadataCollection) && str_contains($table, $this->metadataCollection)) {
            return new Condition("({$prefix}_tenant IN (?) OR {$prefix}_tenant IS NULL)", [$this->tenant]);
        }

        return new Condition("{$prefix}_tenant IN (?)", [$this->tenant]);
    }

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        $condition = new Condition("{$table}._tenant IN (?)", [$this->tenant]);

        $placement = match ($joinType) {
            JoinType::Left, JoinType::Right => Placement::On,
            default => Placement::Where,
        };

        return new JoinCondition($condition, $placement);
    }
}
