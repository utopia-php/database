<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\Condition;
use Utopia\Query\Query;

/**
 * A builder that compiles filters on their own, as build() writes them into a WHERE clause.
 */
interface Filtering
{
    /**
     * The filters joined by AND, each compiled as build() compiles it, with their bindings in order.
     *
     * @param  list<Query>  $filters
     */
    public function compileFilters(array $filters): Condition;
}
