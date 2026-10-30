<?php

namespace Tests\Unit\Documents;

use Closure;
use Utopia\Database\Adapter\SQLite;

final class EvictionSQLite extends SQLite
{
    public ?Closure $beforeCommit = null;

    #[\Override]
    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 1) {
            $callback = $this->beforeCommit;
            $this->beforeCommit = null;
            $callback?->__invoke();
        }

        return parent::commitTransaction();
    }
}
