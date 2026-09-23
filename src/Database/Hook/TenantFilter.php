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
     * @var list<int|string|null>
     */
    private array $tenants;

    /**
     * @param int|string|null|list<int|string|null> $tenant The selected tenant, a list of them for a
     *                                                     query that spans tenants, or null when none
     *                                                     is selected: a shared table then matches no
     *                                                     tenant's rows rather than every tenant's
     * @param string $metadataCollection The metadata collection name; metadata tables allow NULL tenants
     * @param string $collection The actual collection/table name being queried (not the alias)
     * @param string $allowNullColumn When set, rows where this column is NULL also pass: the rows an
     *                                outer join produced without a main-table match. It must be a
     *                                NOT NULL column such as `_uid`, never `_tenant`, or a stored row
     *                                that has no tenant would pass as if it were missing
     */
    public function __construct(
        int|string|null|array $tenant,
        private string $metadataCollection = '',
        private string $collection = '',
        private string $allowNullColumn = '',
        private string $quoteChar = '`',
    ) {
        if (! \is_array($tenant)) {
            $tenant = [$tenant];
        }

        $this->tenants = $tenant === [] ? [null] : $tenant;
    }

    private function placeholders(): string
    {
        return \implode(', ', \array_fill(0, \count($this->tenants), '?'));
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

        $placeholders = $this->placeholders();

        if ($isMetadata) {
            $condition = new Condition("({$prefix}".Storage::TENANT." IN ({$placeholders}) OR {$prefix}".Storage::TENANT." IS NULL)", $this->tenants);
        } else {
            $condition = new Condition("{$prefix}".Storage::TENANT." IN ({$placeholders})", $this->tenants);
        }

        if ($this->allowNullColumn === '') {
            return $condition;
        }

        return AllowNullColumn::wrap($condition, $this->allowNullColumn, $this->quoteChar);
    }

    /**
     * A condition in ON only limits what the joined table matches; one in WHERE runs after every
     * join. When the query has a join that keeps unmatched rows - the main table is then relaxed
     * through $allowNullColumn - a table filtered in WHERE may be missing from a row, and only a
     * missing row may pass, never a stored row without a tenant: `_uid` is NOT NULL.
     */
    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        $placement = match ($joinType) {
            JoinType::Left, JoinType::Inner => Placement::On,
            default => Placement::Where,
        };

        $condition = $this->joined($table);

        if ($placement === Placement::Where && ($joinType === JoinType::FullOuter || $this->allowNullColumn !== '')) {
            $condition = AllowNullColumn::wrap(
                $condition,
                $table.'.'.Storage::UID,
                $this->quoteChar,
            );
        }

        return new JoinCondition($condition, $placement);
    }

    /**
     * The tenant condition of a joined table, before an outer join places or relaxes it.
     */
    public function joined(string $table): Condition
    {
        return new Condition("{$table}.".Storage::TENANT." IN ({$this->placeholders()})", $this->tenants);
    }
}
