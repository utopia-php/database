<?php

namespace Tests\Unit\Documents\Interleaving;

use Closure;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Query\CursorDirection;

/**
 * Runs a write of the test's choosing right after a find() returns, or right before a bulk delete or an increase
 * reaches the engine, as another connection whose write lands in between would, and counts the finds and bulk
 * deletes.
 */
trait Interleaves
{
    /**
     * @var (Closure(Document, array<Document>): void)|null
     */
    public ?Closure $afterFind = null;

    /**
     * @var (Closure(Document): void)|null
     */
    public ?Closure $beforeDelete = null;

    /**
     * Runs once, before the next increase or decrease reaches the engine.
     *
     * @var (Closure(Document, string): void)|null
     */
    public ?Closure $beforeIncrease = null;

    public int $finds = 0;

    public int $deletes = 0;

    #[\Override]
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
    {
        $this->finds++;

        $documents = parent::find($collection, $queries, $limit, $offset, $orderAttributes, $orderTypes, $cursor, $cursorDirection, $forPermission);

        if ($this->afterFind !== null) {
            ($this->afterFind)($collection, $documents);
        }

        return $documents;
    }

    #[\Override]
    public function deleteDocuments(Document $collection, array $sequences, array $permissionIds): int
    {
        $this->deletes++;

        if ($this->beforeDelete !== null) {
            ($this->beforeDelete)($collection);
        }

        return parent::deleteDocuments($collection, $sequences, $permissionIds);
    }

    #[\Override]
    public function increaseDocumentAttribute(Document $collection, string $id, string $attribute, int|float|string $value, string $updatedAt, int|float|string|null $min = null, int|float|string|null $max = null): bool
    {
        $beforeIncrease = $this->beforeIncrease;
        $this->beforeIncrease = null;
        if ($beforeIncrease !== null) {
            $beforeIncrease($collection, $id);
        }

        return parent::increaseDocumentAttribute($collection, $id, $attribute, $value, $updatedAt, $min, $max);
    }
}
