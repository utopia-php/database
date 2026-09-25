<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Storage;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * The permission conditions a right or full outer join needs inside ON: the main table's and the
 * joined table's own.
 *
 * These joins pair rows before the WHERE permission checks run, so a row whose only match is
 * unreadable would be paired with it and then dropped: it vanishes instead of coming back
 * unmatched, and the reader learns that an unreadable document exists. PermissionJoinFilter still
 * places each joined table's own condition in WHERE; this is a separate hook because a join filter
 * contributes a single condition, in a single placement, per join. It mirrors OuterJoinTenantFilter.
 */
final readonly class OuterJoinPermissionFilter implements JoinFilter
{
    /**
     * @param string $source The main table's alias
     * @param array<string, Condition> $conditions The permission condition of each table read per
     *                                             document, by alias, the main table's included
     */
    public function __construct(
        private string $source,
        private array $conditions,
        private string $quoteChar = '`',
    ) {
    }

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        if ($joinType !== JoinType::Right && $joinType !== JoinType::FullOuter) {
            return null;
        }

        $conditions = [];
        if (isset($this->conditions[$this->source])) {
            $conditions[] = AllowNullColumn::wrap(
                $this->conditions[$this->source],
                $this->source.'.'.Storage::UID,
                $this->quoteChar,
            );
        }
        if (isset($this->conditions[$table])) {
            $conditions[] = $this->conditions[$table];
        }

        if ($conditions === []) {
            return null;
        }

        $expressions = [];
        $bindings = [];
        foreach ($conditions as $condition) {
            $expressions[] = $condition->expression;
            \array_push($bindings, ...$condition->bindings);
        }

        return new JoinCondition(new Condition(\implode(' AND ', $expressions), $bindings), Placement::On);
    }
}
