<?php

namespace Tests\Unit\Cache;

use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Query\CursorDirection;

final class ObservedSQLite extends SQLite
{
    private ?string $findCollection = null;

    private int $finds = 0;

    public function observeFinds(string $collection): void
    {
        $this->findCollection = $collection;
        $this->finds = 0;
    }

    public function getObservedFinds(): int
    {
        return $this->finds;
    }

    #[\Override]
    public function find(
        Document $collection,
        array $queries = [],
        ?int $limit = 25,
        ?int $offset = null,
        array $orderAttributes = [],
        array $orderTypes = [],
        array $cursor = [],
        CursorDirection $cursorDirection = CursorDirection::After,
        PermissionType $forPermission = PermissionType::Read,
    ): array {
        if ($collection->getId() === $this->findCollection) {
            $this->finds++;
        }

        return parent::find(
            $collection,
            $queries,
            $limit,
            $offset,
            $orderAttributes,
            $orderTypes,
            $cursor,
            $cursorDirection,
            $forPermission,
        );
    }
}
