<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Storage;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * Permission check bound to one join alias.
 *
 * Left joins place the check in ON so a failed ACL nulls the join side and
 * keeps the main row. Right, full-outer, inner, and cross joins place it in
 * WHERE so unauthorized join rows are dropped. Full outer keeps unmatched
 * primary rows with `acl OR alias._uid IS NULL`.
 */
final class PermissionJoinFilter implements JoinFilter
{
    public function __construct(
        private readonly PermissionFilter $filter,
        private readonly string $alias,
        private readonly string $quoteChar = '`',
    ) {
    }

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        if ($table !== $this->alias) {
            return null;
        }

        $condition = $this->filter->filter($table);
        if ($joinType === JoinType::FullOuter) {
            $condition = AllowNullColumn::wrap(
                $condition,
                $this->alias.'.'.Storage::UID,
                $this->quoteChar,
            );
        }

        return new JoinCondition(
            $condition,
            match ($joinType) {
                JoinType::Left => Placement::On,
                default => Placement::Where,
            },
        );
    }
}
