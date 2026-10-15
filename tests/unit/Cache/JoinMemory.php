<?php

namespace Tests\Unit\Cache;

use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Capability;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Query\CursorDirection;

final class JoinMemory extends DatabaseMemory
{
    private int $finds = 0;

    #[\Override]
    public function capabilities(): array
    {
        return [...parent::capabilities(), Capability::Joins];
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
        if ($collection->getId() === 'parents') {
            $this->finds++;
        }

        $queries = \array_values(\array_filter(
            $queries,
            static fn (Query $query): bool => ! \in_array($query->getMethod(), [
                \Utopia\Query\Method::Join,
                \Utopia\Query\Method::LeftJoin,
                \Utopia\Query\Method::RightJoin,
                \Utopia\Query\Method::CrossJoin,
            ], true),
        ));

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

    public function getJoinFinds(): int
    {
        return $this->finds;
    }
}
