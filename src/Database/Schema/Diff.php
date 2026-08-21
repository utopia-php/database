<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Index;

class Diff
{
    public function diff(Collection $source, Collection $target): DiffResult
    {
        $changes = [];
        $collectionId = $target->getId() !== '' ? $target->getId() : $source->getId();

        $sourceAttrs = [];
        foreach ($source->attributes as $attr) {
            $sourceAttrs[$attr->key] = $attr;
        }

        $targetAttrs = [];
        foreach ($target->attributes as $attr) {
            $targetAttrs[$attr->key] = $attr;
        }

        foreach ($targetAttrs as $key => $attr) {
            if (! isset($sourceAttrs[$key])) {
                $changes[] = new Change(ChangeType::AddAttribute, attribute: $attr, collectionId: $collectionId);
            } elseif ($this->attributeDiffers($sourceAttrs[$key], $attr)) {
                $changes[] = new Change(
                    ChangeType::ModifyAttribute,
                    attribute: $attr,
                    previousAttribute: $sourceAttrs[$key],
                    collectionId: $collectionId,
                );
            }
        }

        foreach ($sourceAttrs as $key => $attr) {
            if (! isset($targetAttrs[$key])) {
                $changes[] = new Change(ChangeType::DropAttribute, attribute: $attr, collectionId: $collectionId);
            }
        }

        $sourceIndexes = [];
        foreach ($source->indexes as $idx) {
            $sourceIndexes[$idx->key] = $idx;
        }

        $targetIndexes = [];
        foreach ($target->indexes as $idx) {
            $targetIndexes[$idx->key] = $idx;
        }

        foreach ($targetIndexes as $key => $idx) {
            if (! isset($sourceIndexes[$key])) {
                $changes[] = new Change(ChangeType::AddIndex, index: $idx, collectionId: $collectionId);
            } elseif ($this->indexDiffers($sourceIndexes[$key], $idx)) {
                $changes[] = new Change(ChangeType::DropIndex, index: $sourceIndexes[$key], collectionId: $collectionId);
                $changes[] = new Change(ChangeType::AddIndex, index: $idx, collectionId: $collectionId);
            }
        }

        foreach ($sourceIndexes as $key => $idx) {
            if (! isset($targetIndexes[$key])) {
                $changes[] = new Change(ChangeType::DropIndex, index: $idx, collectionId: $collectionId);
            }
        }

        return new DiffResult($changes);
    }

    private function attributeDiffers(Attribute $source, Attribute $target): bool
    {
        return $source->type !== $target->type
            || $source->size !== $target->size
            || $source->required !== $target->required
            || $source->signed !== $target->signed
            || $source->array !== $target->array
            || $source->format !== $target->format
            || $source->default !== $target->default;
    }

    private function indexDiffers(Index $source, Index $target): bool
    {
        return $source->type !== $target->type
            || $source->attributes !== $target->attributes
            || $source->lengths !== $target->lengths
            || $source->ttl !== $target->ttl
            || $source->orders != $target->orders;
    }
}
