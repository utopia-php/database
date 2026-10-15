<?php

namespace Utopia\Database\Adapter\SQL;

use Utopia\Database\Query;
use Utopia\Query\Query as BaseQuery;

/**
 * The queries of a find() or a row count, split by how the statement reads them.
 */
final readonly class Plan
{
    /**
     * @param  array<BaseQuery>  $queries  Every query but the vector and adapter filter ones, with the join columns qualified
     * @param  array<Query>  $vectorQueries
     * @param  array<Query>  $adapterFilterQueries
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @param  list<string>  $unindexed  The join aliases a join cannot look its rows up by an index for
     */
    public function __construct(
        public array $queries,
        public array $vectorQueries,
        public array $adapterFilterQueries,
        public array $joinTablePrefixes,
        public array $unindexed,
        public bool $hasAggregation,
        public bool $hasJoins,
        public bool $hasDistinct,
        public bool $hasPreservingOuterJoin,
        public bool $emulatesFullOuterJoin,
    ) {
    }
}
