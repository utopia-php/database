<?php

namespace Utopia\Database\Builder;

/**
 * from() takes a collection id once the builder has a scope: it reads the table the scope stores the
 * collection under, and the first collection it names binds the scope's hooks. They stay bound, so naming
 * another table, through from() or fromTable(), never drops them, and they name the first table as that
 * from() named it.
 */
trait ScopesCollections
{
    private ?Scope $scope = null;

    private bool $bound = false;

    public function scope(Scope $scope): static
    {
        $this->scope = $scope;

        return $this;
    }

    #[\Override]
    public function from(string $table = '', string $alias = ''): static
    {
        if ($this->scope === null || $table === '') {
            return parent::from($table, $alias);
        }

        $stored = $this->scope->table($table);
        parent::from($stored, $alias);

        if (! $this->bound) {
            $this->bound = true;
            $this->scope->bind($this, $table, $stored, $alias);
        }

        return $this;
    }

    public function fromTable(string $table, string $alias = ''): static
    {
        return parent::from($table, $alias);
    }
}
