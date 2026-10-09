<?php

namespace Utopia\Database\Builder;

/**
 * A builder whose from() takes a collection id and reads the collection through its scope.
 */
interface Scoping
{
    /**
     * @throws \Utopia\Database\Exception\Query Once from() has read a collection through the scope it has
     */
    public function scope(Scope $scope): static;

    public function isScoped(): bool;

    /**
     * Reads the table stored under the name as it is, kept to no tenant. Only a builder that has not read a
     * collection through its scope reads a table this way.
     *
     * @throws \Utopia\Database\Exception\Query Once from() has read a collection
     */
    public function fromTable(string $table, string $alias = ''): static;
}
