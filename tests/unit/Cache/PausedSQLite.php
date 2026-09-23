<?php

namespace Tests\Unit\Cache;

use Closure;
use Utopia\Database\Adapter\SQLite;

final class PausedSQLite extends SQLite
{
    private ?Closure $commitCallback = null;

    public function pauseNextCommit(Closure $callback): void
    {
        $this->commitCallback = $callback;
    }

    #[\Override]
    public function commitTransaction(): bool
    {
        $callback = $this->commitCallback;
        $this->commitCallback = null;
        $callback?->__invoke();

        return parent::commitTransaction();
    }
}
