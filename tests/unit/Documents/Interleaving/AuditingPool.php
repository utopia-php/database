<?php

namespace Tests\Unit\Documents\Interleaving;

use Utopia\Database\Adapter\Pool;
use Utopia\Database\Document;

/**
 * A pool whose subclass wraps deleteDocuments(), as an application that audits its deletes does.
 */
final class AuditingPool extends Pool
{
    public int $deletes = 0;

    #[\Override]
    public function deleteDocuments(Document $collection, array $sequences, array $permissionIds): int
    {
        $this->deletes++;

        return parent::deleteDocuments($collection, $sequences, $permissionIds);
    }
}
