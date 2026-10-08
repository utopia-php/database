<?php

namespace Utopia\Database\Adapter\SQL\Hook\Tenant;

use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * The tenant conditions a right or full outer join of a Database::from() builder needs in its ON
 * (Raw::outerJoin()). A join filter contributes one condition, in one placement, per
 * join, and such a join also needs its own table's condition in WHERE, which Raw places.
 * Register it after the Raw filter it reads, so that filter has learned the join.
 */
final readonly class RawOuterJoin implements JoinFilter
{
    public function __construct(
        private Raw $filter,
    ) {
    }

    #[\Override]
    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        if ($joinType !== JoinType::Right && $joinType !== JoinType::FullOuter) {
            return null;
        }

        return new JoinCondition($this->filter->outerJoin($table, $joinType), Placement::On);
    }
}
