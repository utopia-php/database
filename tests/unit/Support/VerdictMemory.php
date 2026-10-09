<?php

namespace Tests\Unit\Support;

use Closure;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipUpdate;

/**
 * A Memory adapter whose schema methods answer with the verdict set for them, or apply the change otherwise.
 */
final class VerdictMemory extends Memory
{
    /**
     * @var array<string, Closure(): bool>
     */
    public array $verdicts = [];

    #[\Override]
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::createAttribute($collection, $attribute);
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    #[\Override]
    public function createAttributes(string $collection, array $attributes): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::createAttributes($collection, $attributes);
    }

    #[\Override]
    public function updateAttribute(string $collection, string $key, Attribute $attribute): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::updateAttribute($collection, $key, $attribute);
    }

    #[\Override]
    public function relaxAttributeRequired(string $collection, string $id): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::relaxAttributeRequired($collection, $id);
    }

    #[\Override]
    public function deleteAttribute(string $collection, string $key): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::deleteAttribute($collection, $key);
    }

    #[\Override]
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::renameAttribute($collection, $old, $new);
    }

    #[\Override]
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::createIndex($collection, $index, $indexAttributeTypes, $collation);
    }

    #[\Override]
    public function deleteIndex(string $collection, string $key): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::deleteIndex($collection, $key);
    }

    #[\Override]
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::renameIndex($collection, $old, $new);
    }

    #[\Override]
    public function createRelationship(string $collection, Relationship $relationship): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::createRelationship($collection, $relationship);
    }

    #[\Override]
    public function updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::updateRelationship($collection, $relationship, $side, $update);
    }

    #[\Override]
    public function deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side): bool
    {
        return $this->verdict(__FUNCTION__) ?? parent::deleteRelationship($collection, $relationship, $side);
    }

    private function verdict(string $method): ?bool
    {
        $verdict = $this->verdicts[$method] ?? null;

        return $verdict === null ? null : $verdict();
    }
}
