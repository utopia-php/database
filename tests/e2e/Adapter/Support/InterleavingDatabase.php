<?php

namespace Tests\E2E\Adapter\Support;

use Utopia\Database\Database;
use Utopia\Database\Event;

final class InterleavingDatabase extends Database
{
    private ?\Closure $interleave = null;

    public function interleave(callable $callback): void
    {
        $this->interleave = \Closure::fromCallable($callback);
    }

    #[\Override]
    protected function withMutation(Event $event, mixed $data, callable $callback): mixed
    {
        $interleave = $this->interleave;
        $this->interleave = null;
        $interleave?->__invoke();

        return parent::withMutation($event, $data, $callback);
    }
}
