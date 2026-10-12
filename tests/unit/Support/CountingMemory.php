<?php

namespace Tests\Unit\Support;

use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Query\CursorDirection;
use Utopia\Query\OrderDirection;

/**
 * Passthrough spy over the Memory adapter that tallies reads at the adapter
 * boundary. Every call is delegated to the real implementation, so what the
 * tallies measure is which reads Database actually issued.
 */
class CountingMemory extends Memory
{
    public int $metadataReads = 0;

    public int $documentReads = 0;

    public int $finds = 0;

    /**
     * @param  array<Query>  $queries
     */
    #[\Override]
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        if ($collection->getId() === Database::METADATA) {
            $this->metadataReads++;
        } else {
            $this->documentReads++;
        }

        return parent::getDocument($collection, $id, $queries, $forUpdate);
    }

    /**
     * @param  array<Query>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<Document>
     */
    #[\Override]
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
    {
        if ($collection->getId() === Database::METADATA) {
            $this->metadataReads++;
        } else {
            $this->finds++;
        }

        return parent::find($collection, $queries, $limit, $offset, $orderAttributes, $orderTypes, $cursor, $cursorDirection, $forPermission);
    }

    public function reset(): void
    {
        $this->metadataReads = 0;
        $this->documentReads = 0;
        $this->finds = 0;
    }
}
