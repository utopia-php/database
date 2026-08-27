<?php

namespace Utopia\Database\Repository;

use Utopia\Database\Query;

interface Specification
{
    /**
     * @return array<Query>
     */
    public function toQueries(): array;

    public function and(Specification $other): Specification;

    public function or(Specification $other): Specification;
}
