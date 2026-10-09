<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Query\Builder;
use Utopia\Query\Schema;

interface QueryBuilder
{
    /**
     * A query builder in the adapter's dialect, for Database::from(). Its from() takes a collection id.
     *
     * Over a collection it maps document attributes to columns and, under shared tables, keeps every
     * statement to the tenant selected when the builder was handed out. It applies no permissions,
     * and its statements bypass the document and query caches, `_perms` upkeep, validation, hooks and
     * events.
     */
    public function builder(): Builder;

    /**
     * A schema builder in the adapter's dialect.
     */
    public function schema(): Schema;
}
