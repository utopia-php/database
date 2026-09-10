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
     * Get a query builder for the given collection.
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
