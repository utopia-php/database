<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * Permission check bound to one join alias. Outer joins place it in ON.
 */
final class PermissionJoinFilter implements JoinFilter
{
    public function __construct(
        private readonly PermissionFilter $filter,
        private readonly string $alias,
    ) {
    }

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        if ($table !== $this->alias) {
            return null;
        }

        return new JoinCondition(
            $this->filter->filter($table),
            match ($joinType) {
                JoinType::Left, JoinType::Right => Placement::On,
                default => Placement::Where,
            },
        );
    }
}
