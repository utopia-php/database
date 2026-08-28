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

        // A metadata row may be tenantless -- a shared pool creates its system
        // collections once, with no tenant, so every tenant on the pool reads
        // the one definition. Its permission rows carry the document's tenant,
        // so they are tenantless too, and the side table has to be recognised
        // as metadata or a write holding a project's tenant filters them out:
        // the rows are matched for neither read nor delete, and revoking a
        // permission on a shared definition silently does nothing.
        $isMetadata = ! empty($this->metadataCollection)
            && ($name === $this->metadataCollection
                || $name === Storage::permissionsTable($this->metadataCollection));

        if ($isMetadata) {
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
