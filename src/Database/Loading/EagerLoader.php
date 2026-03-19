<?php

namespace Utopia\Database\Loading;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;

class EagerLoader
{
    /**
     * @param  array<Document>  $documents
     * @param  array<string>  $relations  Relationship keys to eager-load, supports dot-notation (e.g. 'author.profile')
     * @param  Document  $collection  The collection metadata document
     * @return array<Document>
     */
    public function load(array $documents, array $relations, Document $collection, Database $db): array
    {
        if ($documents === [] || $relations === []) {
            return $documents;
        }

        $grouped = $this->groupByDepth($relations);

        foreach ($grouped as $relationKey => $nestedPaths) {
            $this->loadRelation($documents, $relationKey, $nestedPaths, $collection, $db);
        }

        return $documents;
    }

    /**
     * @param  array<string>  $paths
     * @return array<string, array<string>>
     */
    private function groupByDepth(array $paths): array
    {
        $grouped = [];

        foreach ($paths as $path) {
            $parts = \explode('.', $path, 2);
            $key = $parts[0];
            $rest = $parts[1] ?? null;

            if (! isset($grouped[$key])) {
                $grouped[$key] = [];
            }

            if ($rest !== null) {
                $grouped[$key][] = $rest;
            }
        }

        return $grouped;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<string>  $nestedPaths
     */
    private function loadRelation(array &$documents, string $relationKey, array $nestedPaths, Document $collection, Database $db): void
    {
        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        $relationAttr = null;
        foreach ($attributes as $attr) {
            if ($attr->getAttribute('key') === $relationKey
                && $attr->getAttribute('type') === ColumnType::Relationship->value) {
                $relationAttr = $attr;
                break;
            }
        }

        if ($relationAttr === null) {
            return;
        }

        $rel = Relationship::fromDocument($collection->getId(), $relationAttr);

        $foreignKeys = [];
        foreach ($documents as $doc) {
            $value = $doc->getAttribute($relationKey);

            if (\is_string($value) && $value !== '') {
                $foreignKeys[$value] = true;
            } elseif (\is_array($value)) {
                foreach ($value as $item) {
                    if (\is_string($item) && $item !== '') {
                        $foreignKeys[$item] = true;
                    } elseif ($item instanceof Document && $item->getId() !== '') {
                        $foreignKeys[$item->getId()] = true;
                    }
                }
            } elseif ($value instanceof Document && $value->getId() !== '') {
                $foreignKeys[$value->getId()] = true;
            }
        }

        if ($foreignKeys === []) {
            return;
        }

        $ids = \array_keys($foreignKeys);
        $relatedDocs = $db->find($rel->relatedCollection, [
            Query::equal('$id', $ids),
            Query::limit(\count($ids)),
        ]);

        $relatedById = [];
        foreach ($relatedDocs as $relDoc) {
            $relatedById[$relDoc->getId()] = $relDoc;
        }

        if ($nestedPaths !== []) {
            $relCollection = $db->getCollection($rel->relatedCollection);
            $this->load($relatedDocs, $nestedPaths, $relCollection, $db);
        }

        foreach ($documents as $doc) {
            $value = $doc->getAttribute($relationKey);

            if ($rel->type === RelationType::OneToOne || $rel->type === RelationType::ManyToOne) {
                $id = null;
                if (\is_string($value)) {
                    $id = $value;
                } elseif ($value instanceof Document) {
                    $id = $value->getId();
                }

                if ($id !== null && isset($relatedById[$id])) {
                    $doc->setAttribute($relationKey, $relatedById[$id]);
                }
            } else {
                $items = [];
                $rawItems = \is_array($value) ? $value : [];
                foreach ($rawItems as $item) {
                    $id = null;
                    if (\is_string($item)) {
                        $id = $item;
                    } elseif ($item instanceof Document) {
                        $id = $item->getId();
                    }

                    if ($id !== null && isset($relatedById[$id])) {
                        $items[] = $relatedById[$id];
                    }
                }

                $doc->setAttribute($relationKey, $items);
            }
        }
    }
}
