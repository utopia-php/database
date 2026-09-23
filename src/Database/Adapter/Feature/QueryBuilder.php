<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Query\Builder;
use Utopia\Query\Schema;

/**
 * Provides access to the query builder and schema for a database adapter.
 */
interface QueryBuilder
{
    /**
     * Get a query builder over the given collection's table, for Database::from().
     *
     * It maps document attributes to columns and, under shared tables, keeps every statement to
     * the selected tenant. It applies no permissions, and its statements bypass the document and
     * query caches, `_perms` upkeep, validation, hooks and events.
     *
     * @param string $collection The collection identifier.
     * @return Builder The query builder.
     */
    public function getBuilder(string $collection): Builder;

    /**
     * Get the query schema for this adapter.
     *
     * @return Schema The query schema.
     */
    public function getSchema(): Schema;
}
