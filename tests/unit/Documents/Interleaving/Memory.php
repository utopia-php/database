<?php

namespace Tests\Unit\Documents\Interleaving;

use Closure;
use Utopia\Database\Adapter\Memory as MemoryAdapter;

final class Memory extends MemoryAdapter
{
    use Interleaves;

    /**
     * Run a write outside the open transaction, as another connection would, so that rolling the transaction back
     * keeps it.
     *
     * @param  Closure(): mixed  $write
     */
    public function detached(Closure $write): void
    {
        $depth = $this->inTransaction;
        $journals = $this->journals;
        $this->inTransaction = 0;
        $this->journals = [];

        try {
            $write();
        } finally {
            $this->inTransaction = $depth;
            $this->journals = $journals;
        }
    }
}
