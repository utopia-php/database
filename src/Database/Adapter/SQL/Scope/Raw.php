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
 * The scope of a builder SQL::builder() hands out: it maps document attributes to columns and, under
 * shared tables, keeps the statement and every table joined through the builder's join methods to the
 * tenant (Tenant\Raw), which learns the joins as the builder compiles them.
 *
 * Its state is taken when the builder is handed out, so a builder keeps the database, namespace and
 * tenant of the connection it came from, even one a Pool has since lent elsewhere.
 */
final readonly class Raw implements Scope
{
    /**
     * @param  Closure(string): string  $table  The name a collection's table is stored under
     */
    public function __construct(
        private Closure $table,
        private AttributeMap $attributes,
        private bool $sharedTables,
        private int|string|null $tenant,
        private string $quoteCharacter,
    ) {
    }

    #[\Override]
    public function table(string $collection): string
    {
        return ($this->table)($collection);
    }

    #[\Override]
    public function joinTable(string $table): string
    {
        return $this->table($table);
    }

    #[\Override]
    public function bind(Builder $builder, string $collection, string $table, string $alias): array
    {
        $builder->addHook($this->attributes);

        if (! $this->sharedTables) {
            return [];
        }

        $tenants = new Tenant\Raw(
            $this->tenant,
            $alias !== '' ? $alias : $table,
            $table === $this->table(Database::METADATA) || $table === $this->table(Storage::permissionsTable(Database::METADATA)),
            $this->quoteCharacter,
        );

        $builder
            ->addHook($tenants)
            ->addHook(new Tenant\RawOuterJoin($tenants));

        return [$tenants->reset(...)];
    }
}
