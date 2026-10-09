<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder;

/**
 * How a builder's from() reads a collection: the table it is stored under, and the hooks that map its
 * attributes to columns and keep its statements to a tenant.
 */
interface Scope
{
    /**
     * The name the collection's table is stored under.
     */
    public function table(string $collection): string;

    /**
     * The name a builder that read a collection through this scope joins $table under.
     */
    public function joinTable(string $table): string;

    /**
     * Registers the hooks for statements over the collection, stored as $table and named $alias in them when
     * one is given.
     */
    public function bind(Builder $builder, string $collection, string $table, string $alias): void;
}
