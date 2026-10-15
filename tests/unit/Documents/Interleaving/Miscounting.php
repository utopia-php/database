<?php

namespace Tests\Unit\Documents\Interleaving;

use Closure;
use Utopia\Database\Adapter\Memory as MemoryAdapter;
use Utopia\Database\Document;

/**
 * An adapter that deletes as asked but reports a count of its own, as a custom adapter may.
 */
final class Miscounting extends MemoryAdapter
{
    /**
     * @param  Closure(int $deleted, array<string> $permissionIds): int  $report
     */
    public function __construct(private readonly Closure $report)
    {
        parent::__construct();
    }

    #[\Override]
    public function deleteDocuments(Document $collection, array $sequences, array $permissionIds): int
    {
        return ($this->report)(parent::deleteDocuments($collection, $sequences, $permissionIds), $permissionIds);
    }
}
