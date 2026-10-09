<?php

namespace Utopia\Database\Builder;

use Utopia\Database\Exception\Query as QueryException;
use Utopia\Query\Builder\Statement;
use Utopia\Query\Query;

/**
 * from() takes a collection id once the builder has a scope: it reads the table the scope stores the
 * collection under, and binds the scope's hooks to that collection and alias. A bound builder reads that
 * collection only, under that alias: from() of it again keeps the hooks (after reset(), say), while from() of
 * another collection or under another alias, fromTable(), into(), scope() and a dialect's multi-table writes
 * throw, since the hooks would keep naming the first table or not reach the second. Every join of a bound
 * builder names its table as the scope joins it, whenever the join was added: by collection id on the builder
 * SQL::builder() hands out.
 */
trait ScopesCollections
{
    private ?Scope $scope = null;

    private ?string $boundCollection = null;

    private string $boundAlias = '';

    /**
     * @var list<\Closure(): void>
     */
    private array $beforeEachBuild = [];

    #[\Override]
    public function scope(Scope $scope): static
    {
        $this->requireUnbound('take another scope');
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
            if ($table !== $this->boundCollection || $alias !== $this->boundAlias) {
                throw new QueryException("The builder reads collection '{$this->boundCollection}'".($this->boundAlias !== '' ? " as '{$this->boundAlias}'" : '').", not '{$table}'".($alias !== '' ? " as '{$alias}'" : '').': start another builder');
            }

            return parent::from($this->scope->table($table), $alias);
        }

        $stored = $this->scope->table($table);
        parent::from($stored, $alias);

        $this->boundCollection = $table;
        $this->boundAlias = $alias;
        $this->beforeEachBuild = $this->scope->bind($this, $table, $stored, $alias);

        return $this;
    }

    #[\Override]
    public function fromTable(string $table, string $alias = ''): static
    {
        $this->requireUnbound("read table '{$table}'");

        return parent::from($table, $alias);
    }

    #[\Override]
    public function into(string $table): static
    {
        $this->requireUnbound("insert into table '{$table}'");

        return parent::into($table);
    }

    #[\Override]
    public function build(): Statement
    {
        if ($this->scope === null || $this->boundCollection === null) {
            return parent::build();
        }

        foreach ($this->beforeEachBuild as $callback) {
            $callback();
        }

        $written = $this->pendingQueries;
        $this->pendingQueries = \array_map($this->scopeJoin(...), $written);

        try {
            return parent::build();
        } finally {
            $this->pendingQueries = $written;
        }
    }

    /**
     * @throws QueryException Once from() has read a collection through the scope
     */
    private function requireUnbound(string $action): void
    {
        if ($this->boundCollection !== null) {
            throw new QueryException("The builder reads collection '{$this->boundCollection}' and cannot {$action}: start another builder");
        }
    }

    private function scopeJoin(Query $query): Query
    {
        if ($this->scope === null || ! $query->getMethod()->isJoin()) {
            return $query;
        }

        $table = $this->scope->joinTable($query->getAttribute());

        return $table === $query->getAttribute() ? $query : new Query($query->getMethod(), $table, $query->getValues(), $query->getAlias());
    }
}
