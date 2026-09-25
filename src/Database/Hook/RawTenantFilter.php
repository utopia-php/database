<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Storage;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Filter;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * Tenant conditions for a builder Database::from() hands out under shared tables. Its caller adds
 * the joins after the hook is made, so the hook learns them from the builder as it compiles each
 * statement: the builder hands every join to the join filters in order, then the main table to the
 * filters.
 *
 * Each joined table meets its own condition where TenantFilter places it: inner and left joins in
 * ON, the others in WHERE, where a row an outer join left without the table passes, recognised by
 * the NOT NULL `_uid`. The main table's condition runs in WHERE after every join, relaxed the same
 * way only when the statement has a right or full outer join. RawOuterJoinTenantFilter adds what
 * such a join needs in its own ON (outerJoin()).
 *
 * Tables are named quoted with the adapter's identifier quote, as the builder declares them.
 */
final class RawTenantFilter implements Filter, JoinFilter
{
    /**
     * @var array<string, JoinType> The statement's joins so far, by the name the builder hands join filters
     */
    private array $joins = [];

    /**
     * @param string $table The main table as the builder names it until the caller renames it
     * @param bool $metadata Whether the main table holds definitions a shared pool keeps once, with
     *                       no tenant, for every tenant to read
     */
    public function __construct(
        private readonly int|string|null $tenant,
        private readonly string $table,
        private readonly bool $metadata,
        private readonly string $quoteChar,
    ) {
    }

    public function reset(): void
    {
        $this->joins = [];
    }

    public function filterJoin(string $table, JoinType $joinType): JoinCondition
    {
        $this->joins[$table] = $joinType;

        if ($joinType === JoinType::Inner || $joinType === JoinType::Left) {
            return new JoinCondition($this->joined($table), Placement::On);
        }

        return new JoinCondition($this->allowMissing($this->joined($table), $table), Placement::Where);
    }

    /**
     * @throws QueryException When the statement has no table, or renames the main table a right or full outer join pairs with
     */
    public function filter(string $table): Condition
    {
        $preserving = (new JoinChain($this->joins))->hasPreservingOuterJoin();
        $this->reset();

        if ($table === '') {
            throw new QueryException('A query builder statement without a table cannot be kept to the selected tenant under shared tables');
        }

        if ($preserving && $table !== $this->table) {
            throw new QueryException("A right or full outer join needs the main table named '{$this->table}', as Database::from() names it, under shared tables");
        }

        $condition = $this->main($table);

        return $preserving ? $this->allowMissing($condition, $table) : $condition;
    }

    /**
     * What a right or full outer join needs in its ON: the main table's condition, its own, and those
     * of the tables joined before it whose conditions sit in WHERE, each letting through rows an
     * earlier outer join left without its table. The join pairs rows before WHERE runs, so a row
     * whose only match lies in another tenant would be paired with it and then dropped: it would
     * vanish instead of coming back unmatched, and what the tenant reads would depend on another
     * tenant's rows.
     */
    public function outerJoin(string $table, JoinType $joinType): Condition
    {
        $chain = new JoinChain($this->joins);
        $earlier = [];
        foreach ($chain->preceding($table) as $alias) {
            $earlier[$alias] = $this->joined($alias);
        }

        $conditions = [$this->allowMissing($this->main($this->table), $this->table), $this->joined($table)];
        $preceding = (new OuterJoinChainFilter($chain, $earlier, $this->quoteChar))->filterJoin($table, $joinType);
        if ($preceding !== null) {
            $conditions[] = $preceding->condition;
        }

        $expressions = [];
        $bindings = [];
        foreach ($conditions as $condition) {
            $expressions[] = $condition->expression;
            \array_push($bindings, ...$condition->bindings);
        }

        return new Condition(\implode(' AND ', $expressions), $bindings);
    }

    private function main(string $table): Condition
    {
        $column = $this->column($table);

        if ($this->metadata) {
            return new Condition("({$column} IN (?) OR {$column} IS NULL)", [$this->tenant]);
        }

        return new Condition("{$column} IN (?)", [$this->tenant]);
    }

    private function joined(string $table): Condition
    {
        return new Condition("{$this->column($table)} IN (?)", [$this->tenant]);
    }

    private function allowMissing(Condition $condition, string $table): Condition
    {
        return AllowNullColumn::wrap($condition, $table.'.'.Storage::UID, $this->quoteChar);
    }

    private function column(string $table): string
    {
        return AllowNullColumn::quote($table, $this->quoteChar).'.'.Storage::TENANT;
    }
}
