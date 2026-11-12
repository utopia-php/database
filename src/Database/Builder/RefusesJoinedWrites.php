<?php

namespace Utopia\Database\Builder;

use Utopia\Database\Exception\Query as QueryException;

/**
 * The MySQL and MariaDB multi-table writes, refused on a builder that read a collection through its scope: its
 * tenant scope keeps the main table to the tenant but does not reach the joined one.
 */
trait RefusesJoinedWrites
{
    abstract private function requireUnbound(string $action): void;

    /**
     * @throws QueryException On a builder that read a collection
     */
    #[\Override]
    public function updateJoin(string $table, string $left, string $right, string $alias = ''): static
    {
        $this->requireUnbound("update joining table '{$table}'");

        return parent::updateJoin($table, $left, $right, $alias);
    }

    /**
     * @throws QueryException On a builder that read a collection
     */
    #[\Override]
    public function deleteJoin(string $alias, string $table, string $left, string $right): static
    {
        $this->requireUnbound("delete joining table '{$table}'");

        return parent::deleteJoin($alias, $table, $left, $right);
    }
}
