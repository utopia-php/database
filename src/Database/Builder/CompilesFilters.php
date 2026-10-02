<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\Condition;
use Utopia\Query\Query;

/**
 * Compiles filters the way build() compiles the filters of its WHERE clause, without building the
 * rest of a statement. build() compiles copies of its queries, so the filters given are left as
 * they are.
 */
trait CompilesFilters
{
    /**
     * @param  list<Query>  $filters
     */
    public function compileFilters(array $filters): Condition
    {
        $this->bindings = [];
        $this->resolvedAttributeCache = [];

        $expressions = [];
        foreach ($filters as $filter) {
            $expressions[] = $this->compileFilter(clone $filter);
        }

        return new Condition(\implode(' AND ', $expressions), $this->getBindingValues());
    }
}
