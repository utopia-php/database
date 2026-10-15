<?php

namespace Tests\Unit\Support;

use Utopia\Database\Cache\Scope;
use Utopia\Database\Database;

final class ScopedDatabase extends Database
{
    public function getScope(): Scope
    {
        return $this->getQueryCacheScope();
    }
}
