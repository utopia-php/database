<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Attribute;
use Utopia\Database\Collection;

class SchemaDiff
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
                $changes[] = new SchemaChange(SchemaChangeType::AddAttribute, attribute: $attr);
            } elseif ($this->attributeDiffers($sourceAttrs[$key], $attr)) {
                $changes[] = new SchemaChange(
                    SchemaChangeType::ModifyAttribute,
                    attribute: $attr,
                    previousAttribute: $sourceAttrs[$key],
                );
            }
        }

        foreach ($sourceAttrs as $key => $attr) {
            if (! isset($targetAttrs[$key])) {
                $changes[] = new SchemaChange(SchemaChangeType::DropAttribute, attribute: $attr);
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
                $changes[] = new SchemaChange(SchemaChangeType::AddIndex, index: $idx);
            }
        }

        foreach ($sourceIndexes as $key => $idx) {
            if (! isset($targetIndexes[$key])) {
                $changes[] = new SchemaChange(SchemaChangeType::DropIndex, index: $idx);
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
