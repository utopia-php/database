<?php

namespace Utopia\Database\Adapter\SQL\Hook\Join;

use Utopia\Database\Adapter\SQL\Hook\Column\AllowNull;
use Utopia\Database\Storage;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * The conditions of earlier joined tables that a right or full outer join needs inside ON.
 *
 * A table joined right, full outer or cross keeps its condition in WHERE, which runs after every
 * join. A later right or full outer join would pair its rows with that table's rows outside the
 * condition first, and WHERE would then drop the pairs: its rows would vanish instead of coming
 * back unmatched, so what a reader gets would depend on rows it may not read. Each repeated
 * condition lets through the rows an outer join left without its table, recognised by the NOT
 * NULL `_uid`. The main table's condition and the joined table's own are placed by the hooks
 * that own them, Tenant\OuterJoin and Permission\OuterJoin.
 */
final readonly class OuterChain implements JoinFilter
{
    /**
     * @param array<string, Condition> $conditions The condition each joined table's rows must meet, by alias
     */
    public function __construct(
        private Chain $chain,
        private array $conditions,
        private string $quoteCharacter = '`',
    ) {
    }

    #[\Override]
    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        if ($joinType !== JoinType::Right && $joinType !== JoinType::FullOuter) {
            return null;
        }

        $expressions = [];
        $bindings = [];
        foreach ($this->chain->preceding($table) as $alias) {
            if (! isset($this->conditions[$alias])) {
                continue;
            }

            $condition = AllowNull::wrap($this->conditions[$alias], $alias.'.'.Storage::UID, $this->quoteCharacter);
            $expressions[] = $condition->expression;
            \array_push($bindings, ...$condition->bindings);
        }

        if ($expressions === []) {
            return null;
        }

        return new JoinCondition(new Condition(\implode(' AND ', $expressions), $bindings), Placement::On);
    }
}
