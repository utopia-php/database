<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Storage;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * Permission check bound to one join alias, placed the way TenantFilter places tenant conditions.
 *
 * Inner and left joins check it in ON, so only readable rows are matched. Right, full outer and
 * cross joins check it in WHERE, since ON cannot drop the rows they keep. A condition in WHERE
 * runs after every join, so when the read has a right or full outer join it also lets through the
 * rows an outer join left without this table, recognised by the NOT NULL `_uid`; a full outer join
 * leaves its own table missing from the rows it keeps unmatched.
 */
final readonly class PermissionJoinFilter implements JoinFilter
{
    /**
     * @param bool $preservingOuterJoin Whether the read has a right or full outer join
     */
    public function __construct(
        private PermissionFilter $filter,
        private string $alias,
        private string $quoteChar = '`',
        private bool $preservingOuterJoin = false,
    ) {
    }

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        if ($table !== $this->alias) {
            return null;
        }

        $placement = match ($joinType) {
            JoinType::Left, JoinType::Inner => Placement::On,
            default => Placement::Where,
        };

        $condition = $this->filter->filter($table);
        if ($placement === Placement::Where && ($joinType === JoinType::FullOuter || $this->preservingOuterJoin)) {
            $condition = AllowNullColumn::wrap(
                $condition,
                $this->alias.'.'.Storage::UID,
                $this->quoteChar,
            );
        }

        return new JoinCondition($condition, $placement);
    }
}
