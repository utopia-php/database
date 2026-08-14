<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Storage;
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
     * @param string $collection The actual collection/table name being queried (not the alias)
     * @param string $allowNullColumn When set, unmatched outer-join rows keep a NULL tenant
     */
    public function __construct(
        private int|string $tenant,
        private string $metadataCollection = '',
        private string $collection = '',
        private string $allowNullColumn = '',
        private string $quoteChar = '`',
    ) {
    }

    public function filter(string $table): Condition
    {
        $prefix = (!\str_contains($table, '.') && !\str_contains($table, '`')) ? "{$table}." : '';

        $name = $this->collection !== '' ? $this->collection : $table;

        if (! empty($this->metadataCollection) && $name === $this->metadataCollection) {
            $condition = new Condition("({$prefix}".Storage::TENANT." IN (?) OR {$prefix}".Storage::TENANT." IS NULL)", [$this->tenant]);
        } else {
            $condition = new Condition("{$prefix}".Storage::TENANT." IN (?)", [$this->tenant]);
        }

        if ($this->allowNullColumn === '') {
            return $condition;
        }

        return AllowNullColumn::wrap($condition, $this->allowNullColumn, $this->quoteChar);
    }

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        $condition = new Condition("{$table}.".Storage::TENANT." IN (?)", [$this->tenant]);

        if ($joinType === JoinType::FullOuter) {
            $condition = AllowNullColumn::wrap(
                $condition,
                $table.'.'.Storage::TENANT,
                $this->quoteChar,
            );
        }

        $placement = match ($joinType) {
            JoinType::Left => Placement::On,
            default => Placement::Where,
        };

        return new JoinCondition($condition, $placement);
    }
}
