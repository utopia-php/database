<?php

namespace Utopia\Database\Repository;

use Utopia\Database\Query;

interface Scope
{
    /**
     * @return array<Query>
     */
    public function apply(): array;
}
