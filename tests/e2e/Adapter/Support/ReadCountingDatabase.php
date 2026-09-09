<?php

namespace Tests\E2E\Adapter\Support;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;

final class ReadCountingDatabase extends Database
{
    public int $documentReads = 0;

    public int $collectionReads = 0;

    public function resetReadCounts(): void
    {
        $this->documentReads = 0;
        $this->collectionReads = 0;
    }

    #[\Override]
    public function getDocument(string $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $this->documentReads++;

        return parent::getDocument($collection, $id, $queries, $forUpdate);
    }

    #[\Override]
    public function find(string $collection, array $queries = [], PermissionType $forPermission = PermissionType::Read): array
    {
        $this->collectionReads++;

        return parent::find($collection, $queries, $forPermission);
    }
}
