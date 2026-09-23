<?php

namespace Tests\Unit\Cache;

use Closure;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Query\CursorDirection;

final class ObservedMemory extends DatabaseMemory
{
    private ?Closure $metadataCallback = null;

    private ?Closure $validatorCallback = null;

    private ?string $metadataCollection = null;

    private ?string $findCollection = null;

    private int $metadataReads = 0;

    private int $validators = 0;

    private int $finds = 0;

    public function observeMetadata(string $collection, Closure $callback): void
    {
        $this->metadataCollection = $collection;
        $this->metadataCallback = $callback;
        $this->metadataReads = 0;
    }

    public function observeValidators(Closure $callback): void
    {
        $this->validatorCallback = $callback;
        $this->validators = 0;
    }

    public function observeFinds(string $collection): void
    {
        $this->findCollection = $collection;
        $this->finds = 0;
    }

    public function getObservedMetadataReads(): int
    {
        return $this->metadataReads;
    }

    public function getObservedValidators(): int
    {
        return $this->validators;
    }

    public function getObservedFinds(): int
    {
        return $this->finds;
    }

    #[\Override]
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $document = parent::getDocument($collection, $id, $queries, $forUpdate);

        if ($collection->getId() === Database::METADATA && $id === $this->metadataCollection) {
            $this->metadataReads++;
            $callback = $this->metadataCallback;
            $this->metadataCallback = null;
            $callback?->__invoke();
        }

        return $document;
    }

    #[\Override]
    public function getIdAttributeType(): string
    {
        if ($this->validatorCallback !== null) {
            $this->validators++;
            $callback = $this->validatorCallback;
            $this->validatorCallback = null;
            $callback();
        } elseif ($this->validators > 0) {
            $this->validators++;
        }

        return parent::getIdAttributeType();
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
