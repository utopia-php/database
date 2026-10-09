<?php

namespace Utopia\Database\Builder;

/**
 * A builder whose from() takes a collection id and reads the collection through its scope.
 */
interface Scoping
{
    public function scope(Scope $scope): static;

    /**
     * Reads the table stored under the name as it is, through no scope.
     */
    public function fromTable(string $table, string $alias = ''): static;
}
