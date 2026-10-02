<?php

namespace Utopia\Database\Adapter\SQL;

use Utopia\Database\Query;
use Utopia\Query\OrderDirection;
use Utopia\Query\Query as BaseQuery;

/**
 * The main rows a joined read's page can come from: the first `rows` main documents in the main order of the read
 * that meet its main conditions and its searches. When a main document can give the read no row (an inner join, a
 * condition on a joined attribute), they hold the page only when the read fills it from them or they are every main
 * document the read matches: the read checks that and reads again without them otherwise.
 */
final readonly class BoundedPage
{
    /**
     * @param  non-empty-list<string>  $orderAttributes
     * @param  list<OrderDirection>  $orderTypes
     * @param  list<BaseQuery>  $conditions  The read's conditions on main attributes, its searches included
     * @param  list<Query>  $adapterConditions  The read's searches the adapter compiles itself
     * @param  list<BaseQuery>  $searches  The read's own searches, which only the page applies
     * @param  bool  $exact  Whether every main document the page holds gives the read at least one row
     */
    public function __construct(
        public array $orderAttributes,
        public array $orderTypes,
        public int $rows,
        public array $conditions,
        public array $adapterConditions,
        public array $searches,
        public bool $exact,
    ) {
    }

    /**
     * @template T of BaseQuery
     *
     * @param  array<T>  $queries
     * @return array<T>
     */
    public function withoutSearches(array $queries): array
    {
        if ($this->searches === []) {
            return $queries;
        }

        return \array_values(\array_filter($queries, fn (BaseQuery $query): bool => ! \in_array($query, $this->searches, true)));
    }
}
