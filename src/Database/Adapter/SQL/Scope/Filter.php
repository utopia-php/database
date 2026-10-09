<?php

namespace Utopia\Database\Adapter\SQL\Scope;

use Closure;
use Utopia\Database\Adapter\SQL\Hook\Tenant;
use Utopia\Database\Builder\Scope;
use Utopia\Database\Database;
use Utopia\Database\Storage;
use Utopia\Query\Builder;
use Utopia\Query\Hook\Attribute\Map as AttributeMap;

/**
 * The scope of the builders the adapter reads and writes documents through: it maps document attributes to
 * columns and, under shared tables, keeps the main table to the tenants with Tenant\Filter, the read naming
 * its joins up front, so that a query made with no tenant selected matches no tenant's rows rather than
 * every tenant's.
 */
final readonly class Filter implements Scope
{
    /**
     * @param  Closure(string): string  $table  The name a collection's table is stored under
     * @param  int|string|null|list<int|string|null>  $tenants  The selected tenant, or the tenants a read spans
     * @param  bool  $allowNullTenant  Whether rows an outer join produced without a main-table match pass
     * @param  list<string>  $unindexed  The read's join aliases unindexedJoins() names
     */
    public function __construct(
        private Closure $table,
        private AttributeMap $attributes,
        private bool $sharedTables,
        private int|string|null|array $tenants,
        private string $quoteCharacter,
        private bool $allowNullTenant = false,
        private array $unindexed = [],
    ) {
    }

    #[\Override]
    public function table(string $collection): string
    {
        return ($this->table)($collection);
    }

    #[\Override]
    public function bind(Builder $builder, string $collection, string $table, string $alias): void
    {
        $builder->addHook($this->attributes);

        if (! $this->sharedTables) {
            return;
        }

        $source = $alias !== '' ? $alias : $collection;
        $filter = new Tenant\Filter(
            $this->tenants,
            Database::METADATA,
            $collection,
            $this->allowNullTenant ? $source.'.'.Storage::UID : '',
            $this->quoteCharacter,
            $this->unindexed,
        );

        $builder
            ->addHook($filter)
            ->addHook(new Tenant\OuterJoin($filter, $source));
    }
}
