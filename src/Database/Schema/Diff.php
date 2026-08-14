<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Attribute;
use Utopia\Database\Collection;

class Diff
{
    public function diff(Collection $source, Collection $target): DiffResult
    {
        $changes = [];

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
                $changes[] = new Change(ChangeType::AddAttribute, attribute: $attr);
            } elseif ($this->attributeDiffers($sourceAttrs[$key], $attr)) {
                $changes[] = new Change(
                    ChangeType::ModifyAttribute,
                    attribute: $attr,
                    previousAttribute: $sourceAttrs[$key],
                );
            }
        }

        foreach ($sourceAttrs as $key => $attr) {
            if (! isset($targetAttrs[$key])) {
                $changes[] = new Change(ChangeType::DropAttribute, attribute: $attr);
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
                $changes[] = new Change(ChangeType::AddIndex, index: $idx);
            }
        }

        foreach ($sourceIndexes as $key => $idx) {
            if (! isset($targetIndexes[$key])) {
                $changes[] = new Change(ChangeType::DropIndex, index: $idx);
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
}
