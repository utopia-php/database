<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Database;

class DiffResult
{
    /**
     * @param  array<Change>  $changes
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
                ChangeType::AddAttribute => $change->attribute !== null
                    ? $db->createAttribute($collectionId, $change->attribute)
                    : null,
                ChangeType::DropAttribute => $change->attribute !== null
                    ? $db->deleteAttribute($collectionId, $change->attribute->key)
                    : null,
                ChangeType::ModifyAttribute => $change->attribute !== null
                    ? $db->updateAttribute(
                        $collectionId,
                        $change->attribute->key,
                        type: $change->attribute->type,
                        size: $change->attribute->size,
                        required: $change->attribute->required,
                        default: $change->attribute->default,
                        signed: $change->attribute->signed,
                        array: $change->attribute->array,
                        format: $change->attribute->format,
                        formatOptions: $change->attribute->formatOptions,
                        filters: $change->attribute->filters,
                    )
                    : null,
                ChangeType::AddIndex => $change->index !== null
                    ? $db->createIndex($collectionId, $change->index)
                    : null,
                ChangeType::DropIndex => $change->index !== null
                    ? $db->deleteIndex($collectionId, $change->index->key)
                    : null,
                default => null,
            };
        }
    }

    /**
     * @return array<Change>
     */
    public function getAdditions(): array
    {
        return \array_filter($this->changes, fn (Change $c) => \in_array($c->type, [
            ChangeType::AddAttribute,
            ChangeType::AddIndex,
            ChangeType::AddRelationship,
            ChangeType::CreateCollection,
        ], true));
    }

    /**
     * @return array<Change>
     */
    public function getRemovals(): array
    {
        return \array_filter($this->changes, fn (Change $c) => \in_array($c->type, [
            ChangeType::DropAttribute,
            ChangeType::DropIndex,
            ChangeType::DropRelationship,
            ChangeType::DropCollection,
        ], true));
    }

    /**
     * @return array<Change>
     */
    public function getModifications(): array
    {
        return \array_filter($this->changes, fn (Change $c) => $c->type === ChangeType::ModifyAttribute);
    }
}
