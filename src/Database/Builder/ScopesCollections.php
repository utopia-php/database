<?php

namespace Utopia\Database\Builder;

use Closure;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Method;
use Utopia\Query\Query;

/**
 * from() takes a collection id once the builder has a scope: it reads the table the scope stores the
 * collection under, and binds the scope's hooks to that collection. A bound builder reads that collection
 * only: from() of it again, under another alias or after reset(), keeps the hooks, while from() of another
 * collection, fromTable() and scope() throw, since the hooks would keep naming the first. A bound builder
 * names the tables it joins as its scope joins them: by collection id on the builder SQL::builder() hands out.
 */
trait ScopesCollections
{
    private ?Scope $scope = null;

    private ?string $boundCollection = null;

    #[\Override]
    public function scope(Scope $scope): static
    {
        if ($this->boundCollection !== null) {
            throw new QueryException("The builder reads collection '{$this->boundCollection}' through its scope already");
        }

        $this->scope = $scope;

        return $this;
    }

    #[\Override]
    public function isScoped(): bool
    {
        return $this->scope !== null;
    }

    #[\Override]
    public function from(string $table = '', string $alias = ''): static
    {
        if ($this->scope === null || $table === '') {
            return parent::from($table, $alias);
        }

        if ($this->boundCollection !== null) {
            if ($table !== $this->boundCollection) {
                throw new QueryException("The builder reads collection '{$this->boundCollection}', not '{$table}': start another builder");
            }

            return parent::from($this->scope->table($table), $alias);
        }

        $stored = $this->scope->table($table);
        parent::from($stored, $alias);

        $this->boundCollection = $table;
        $this->scope->bind($this, $table, $stored, $alias);

        return $this;
    }

    #[\Override]
    public function fromTable(string $table, string $alias = ''): static
    {
        if ($this->boundCollection !== null) {
            throw new QueryException("The builder reads collection '{$this->boundCollection}', not table '{$table}': start another builder");
        }

        return parent::from($table, $alias);
    }

    #[\Override]
    public function join(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        return parent::join($this->joinedTable($table), $left, $right, $operator, $alias);
    }

    #[\Override]
    public function leftJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        return parent::leftJoin($this->joinedTable($table), $left, $right, $operator, $alias);
    }

    #[\Override]
    public function rightJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        return parent::rightJoin($this->joinedTable($table), $left, $right, $operator, $alias);
    }

    /**
     * @param  Closure(JoinBuilder): void  $callback
     */
    #[\Override]
    public function joinWhere(string $table, Closure $callback, JoinType $type = JoinType::Inner, string $alias = ''): static
    {
        return parent::joinWhere($this->joinedTable($table), $callback, $type, $alias);
    }

    #[\Override]
    public function crossJoin(string $table, string $alias = ''): static
    {
        return parent::crossJoin($this->joinedTable($table), $alias);
    }

    #[\Override]
    public function naturalJoin(string $table, string $alias = ''): static
    {
        return parent::naturalJoin($this->joinedTable($table), $alias);
    }

    /**
     * @param  array<Query>  $queries
     */
    #[\Override]
    public function filter(array $queries): static
    {
        return parent::filter(\array_map($this->scopeJoin(...), $queries));
    }

    /**
     * @param  array<Query>  $queries
     */
    #[\Override]
    public function queries(array $queries): static
    {
        return parent::queries(\array_map($this->scopeJoin(...), $queries));
    }

    private function joinedTable(string $table): string
    {
        return $this->scope === null || $this->boundCollection === null ? $table : $this->scope->joinTable($table);
    }

    private function scopeJoin(Query $query): Query
    {
        $joins = [Method::Join, Method::LeftJoin, Method::RightJoin, Method::CrossJoin, Method::FullOuterJoin, Method::NaturalJoin];
        if (! \in_array($query->getMethod(), $joins, true)) {
            return $query;
        }

        $table = $this->joinedTable($query->getAttribute());

        return $table === $query->getAttribute() ? $query : new Query($query->getMethod(), $table, $query->getValues(), $query->getAlias());
    }
}
