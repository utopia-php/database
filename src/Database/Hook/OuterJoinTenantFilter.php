<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * The tenant conditions a right or full outer join needs inside ON: the main table's and the joined
 * table's own. OuterJoinChainFilter adds those of the tables joined before it.
 *
 * These joins pair rows before the WHERE tenant filters run, so a row whose only match lies outside
 * the tenant would be paired with it and then dropped by WHERE: it vanishes instead of surviving
 * unmatched, and another tenant's rows decide what this tenant reads. TenantFilter still places each
 * joined table's own condition; this is a separate hook because a join filter contributes a single
 * condition, in a single placement, per join.
 */
final readonly class OuterJoinTenantFilter implements JoinFilter
{
    /**
     * @param string $source The main table's alias, the reference the builder hands TenantFilter::filter()
     */
    public function __construct(
        private TenantFilter $filter,
        private string $source,
    ) {
    }

    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        if ($joinType !== JoinType::Right && $joinType !== JoinType::FullOuter) {
            return null;
        }

        $source = $this->filter->filter($this->source);
        $joined = $this->filter->joined($table);

        return new JoinCondition(
            new Condition(
                $source->expression.' AND '.$joined->expression,
                [...$source->bindings, ...$joined->bindings],
            ),
            Placement::On,
        );
    }
}
