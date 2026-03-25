<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Database;

class DiffResult
{
    /**
     * @param  array<SchemaChange>  $changes
     */
    public function __construct(
        public readonly array $changes,
    ) {
    }

    public function hasChanges(): bool
    {
        return $this->changes !== [];
    }

    public function apply(Database $db, string $collectionId): void
    {
        foreach ($this->changes as $change) {
            match ($change->type) {
                SchemaChangeType::AddAttribute => $change->attribute !== null
                    ? $db->createAttribute($collectionId, $change->attribute)
                    : null,
                SchemaChangeType::DropAttribute => $change->attribute !== null
                    ? $db->deleteAttribute($collectionId, $change->attribute->key)
                    : null,
                SchemaChangeType::ModifyAttribute => $change->attribute !== null
                    ? $db->updateAttribute($collectionId, $change->attribute->key, $change->attribute)
                    : null,
                SchemaChangeType::AddIndex => $change->index !== null
                    ? $db->createIndex($collectionId, $change->index)
                    : null,
                SchemaChangeType::DropIndex => $change->index !== null
                    ? $db->deleteIndex($collectionId, $change->index->key)
                    : null,
                default => null,
            };
        }
    }

    /**
     * @return array<SchemaChange>
     */
    public function getAdditions(): array
    {
        return \array_filter($this->changes, fn (SchemaChange $c) => \in_array($c->type, [
            SchemaChangeType::AddAttribute,
            SchemaChangeType::AddIndex,
            SchemaChangeType::AddRelationship,
            SchemaChangeType::CreateCollection,
        ], true));
    }

    /**
     * @return array<SchemaChange>
     */
    public function getRemovals(): array
    {
        return \array_filter($this->changes, fn (SchemaChange $c) => \in_array($c->type, [
            SchemaChangeType::DropAttribute,
            SchemaChangeType::DropIndex,
            SchemaChangeType::DropRelationship,
            SchemaChangeType::DropCollection,
        ], true));
    }

    /**
     * @return array<SchemaChange>
     */
    public function getModifications(): array
    {
        return \array_filter($this->changes, fn (SchemaChange $c) => $c->type === SchemaChangeType::ModifyAttribute);
    }
}
