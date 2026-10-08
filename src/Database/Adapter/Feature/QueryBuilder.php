<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Query\Builder;
use Utopia\Query\Schema;

interface QueryBuilder
{
    /**
     * A query builder over the given collection's table, for Database::from().
     *
     * It maps document attributes to columns and, under shared tables, keeps every statement to
     * the selected tenant. It applies no permissions, and its statements bypass the document and
     * query caches, `_perms` upkeep, validation, hooks and events.
     */
    public function builder(string $collection): Builder;

    /**
     * A schema builder in the adapter's dialect.
     */
    public function schema(): Schema;
}
