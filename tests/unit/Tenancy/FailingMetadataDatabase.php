<?php

namespace Tests\Unit\Tenancy;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;

/**
 * A Database whose collection-metadata writes fail while armed, as a lost connection would
 * fail them after the schema change ran.
 */
final class FailingMetadataDatabase extends Database
{
    private bool $failing = false;

    public function failMetadataWrites(bool $failing): void
    {
        $this->failing = $failing;
    }

    #[\Override]
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        if ($this->failing && $collection === self::METADATA) {
            throw new DatabaseException('Metadata write failed');
        }

        return parent::updateDocument($collection, $id, $document);
    }
}
