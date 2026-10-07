<?php

namespace Utopia\Database;

use Utopia\Query\Builder\ParsedQuery as BaseParsedQuery;
use Utopia\Query\CursorDirection;
use Utopia\Query\OrderDirection;

/**
 * Database queries grouped by kind ({@see Query::groupByType()}), with their orders, which the base grouping leaves to
 * the builder, in the order the queries give them.
 */
final readonly class ParsedQuery extends BaseParsedQuery
{
    /**
     * @param  list<Query>  $filters
     * @param  list<Query>  $selections
     * @param  list<Query>  $aggregations
     * @param  list<string>  $groupBy
     * @param  list<Query>  $having
     * @param  list<Query>  $joins
     * @param  list<Query>  $unions
     * @param  Document|null  $cursor
     * @param  list<array{attribute: string, interval: string}>  $timeBuckets
     * @param  list<string>  $orderAttributes
     * @param  list<OrderDirection>  $orderTypes
     */
    public function __construct(
        public array $filters = [],
        public array $selections = [],
        public array $aggregations = [],
        public array $groupBy = [],
        public array $having = [],
        public bool $distinct = false,
        public array $joins = [],
        public array $unions = [],
        public ?int $limit = null,
        public ?int $offset = null,
        public mixed $cursor = null,
        public ?CursorDirection $cursorDirection = null,
        public array $timeBuckets = [],
        public array $orderAttributes = [],
        public array $orderTypes = [],
    ) {
    }
}
