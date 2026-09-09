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
     * @param string $allowNullColumn When set, unmatched outer-join rows keep a NULL tenant
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

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        $condition = new Condition("{$table}.".Storage::TENANT." IN ({$this->placeholders()})", $this->tenants);

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
