<?php

namespace Utopia\Database\Adapter\SQL;

use Utopia\Query\Builder\JoinType;
use Utopia\Query\Query as BaseQuery;

/**
 * A join every row of a bounded page needs a match from: an inner join, a left join whose rows a condition keeps only
 * when they hold a joined row, or the join such a join reaches its main document through.
 */
final readonly class PageJoin
{
    /**
     * @param  string  $table  The joined table as the statement names it
     * @param  string  $collection  The joined collection
     * @param  list<array{string, string, string}>  $comparisons  The ON comparisons: column, operator, column, each qualified by its alias
     * @param  list<BaseQuery>  $conditions  The read's conditions on this join's attributes
     * @param  list<PageJoin>  $joins  The joins of this kind whose ON compares this join's columns
     */
    public function __construct(
        public string $table,
        public string $collection,
        public string $alias,
        public JoinType $type,
        public array $comparisons,
        public array $conditions,
        public array $joins,
    ) {
    }
}
