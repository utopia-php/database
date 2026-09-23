<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * The tenant conditions a right or full outer join of a Database::from() builder needs in its ON
 * (RawTenantFilter::outerJoin()). A join filter contributes one condition, in one placement, per
 * join, and such a join also needs its own table's condition in WHERE, which RawTenantFilter places.
 * Register it after the RawTenantFilter it reads, so that filter has learned the join.
 */
final readonly class RawOuterJoinTenantFilter implements JoinFilter
{
    public function __construct(
        private RawTenantFilter $filter,
    ) {
    }

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        if ($joinType !== JoinType::Right && $joinType !== JoinType::FullOuter) {
            return null;
        }

        return new JoinCondition($this->filter->outerJoin($table, $joinType), Placement::On);
    }
}
