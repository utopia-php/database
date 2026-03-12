<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Query;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;

class RelationshipHandler implements Relationship
{
    private bool $enabled = true;

    private bool $checkExist = true;

    private int $fetchDepth = 0;

    private bool $inBatchPopulation = false;

    /** @var array<string> */
    private array $writeStack = [];

    /** @var array<Document> */
    private array $deleteStack = [];

    public function __construct(
        private Database $db,
    ) {}

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function setEnabled(bool $enabled): void
    {
        $this->enabled = $enabled;
    }

    public function shouldCheckExist(): bool
    {
        return $this->checkExist;
    }

    public function setCheckExist(bool $check): void
    {
        $this->checkExist = $check;
    }

    public function getWriteStackCount(): int
    {
        return \count($this->writeStack);
    }

    public function getFetchDepth(): int
    {
        return $this->fetchDepth;
    }

    public function isInBatchPopulation(): bool
    {
        return $this->inBatchPopulation;
    }

    public function afterDocumentCreate(Document $collection, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);

        $relationships = \array_filter(
            $attributes,
            fn ($attribute) => $attribute['type'] === ColumnType::Relationship->value
        );

        $stackCount = \count($this->writeStack);

        foreach ($relationships as $relationship) {
            $key = $relationship['key'];
            $value = $document->getAttribute($key);
            $relatedCollection = $this->db->getCollection($relationship['options']['relatedCollection']);
            $relationType = $relationship['options']['relationType'];
            $twoWay = $relationship['options']['twoWay'];
            $twoWayKey = $relationship['options']['twoWayKey'];
            $side = $relationship['options']['side'];

            if ($stackCount >= Database::RELATION_MAX_DEPTH - 1 && $this->writeStack[$stackCount - 1] !== $relatedCollection->getId()) {
                $document->removeAttribute($key);

                continue;
            }

            $this->writeStack[] = $collection->getId();

            try {
                switch (\gettype($value)) {
                    case 'array':
                        if (
                            ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Parent->value) ||
                            ($relationType === RelationType::OneToMany->value && $side === RelationSide::Child->value) ||
                            ($relationType === RelationType::OneToOne->value)
                        ) {
                            throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document, array given.');
                        }

                        foreach ($value as $related) {
                            switch (\gettype($related)) {
                                case 'object':
                                    if (! $related instanceof Document) {
                                        throw new RelationshipException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                                    }
                                    $this->relateDocuments(
                                        $collection,
                                        $relatedCollection,
                                        $key,
                                        $document,
                                        $related,
                                        $relationType,
                                        $twoWay,
                                        $twoWayKey,
                                        $side,
                                    );
                                    break;
                                case 'string':
                                    $this->relateDocumentsById(
                                        $collection,
                                        $relatedCollection,
                                        $key,
                                        $document->getId(),
                                        $related,
                                        $relationType,
                                        $twoWay,
                                        $twoWayKey,
                                        $side,
                                    );
                                    break;
                                default:
                                    throw new RelationshipException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                            }
                        }
                        $document->removeAttribute($key);
                        break;

                    case 'object':
                        if (! $value instanceof Document) {
                            throw new RelationshipException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                        }

                        if ($relationType === RelationType::OneToOne->value && ! $twoWay && $side === RelationSide::Child->value) {
                            throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                        }

                        if (
                            ($relationType === RelationType::OneToMany->value && $side === RelationSide::Parent->value) ||
                            ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Child->value) ||
                            ($relationType === RelationType::ManyToMany->value)
                        ) {
                            throw new RelationshipException('Invalid relationship value. Must be either an array of documents or document IDs, document given.');
                        }

                        $relatedId = $this->relateDocuments(
                            $collection,
                            $relatedCollection,
                            $key,
                            $document,
                            $value,
                            $relationType,
                            $twoWay,
                            $twoWayKey,
                            $side,
                        );
                        $document->setAttribute($key, $relatedId);
                        break;

                    case 'string':
                        if ($relationType === RelationType::OneToOne->value && $twoWay === false && $side === RelationSide::Child->value) {
                            throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                        }

                        if (
                            ($relationType === RelationType::OneToMany->value && $side === RelationSide::Parent->value) ||
                            ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Child->value) ||
                            ($relationType === RelationType::ManyToMany->value)
                        ) {
                            throw new RelationshipException('Invalid relationship value. Must be either an array of documents or document IDs, document ID given.');
                        }

                        $this->relateDocumentsById(
                            $collection,
                            $relatedCollection,
                            $key,
                            $document->getId(),
                            $value,
                            $relationType,
                            $twoWay,
                            $twoWayKey,
                            $side,
                        );
                        break;

                    case 'NULL':
                        if (
                            ($relationType === RelationType::OneToMany->value && $side === RelationSide::Child->value) ||
                            ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Parent->value) ||
                            ($relationType === RelationType::OneToOne->value && $side === RelationSide::Parent->value) ||
                            ($relationType === RelationType::OneToOne->value && $side === RelationSide::Child->value && $twoWay === true)
                        ) {
                            break;
                        }

                        $document->removeAttribute($key);
                        break;

                    default:
                        throw new RelationshipException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                }
            } finally {
                \array_pop($this->writeStack);
            }
        }

        return $document;
    }

    public function afterDocumentUpdate(Document $collection, Document $old, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);

        $relationships = \array_filter($attributes, function ($attribute) {
            return $attribute['type'] === ColumnType::Relationship->value;
        });

        $stackCount = \count($this->writeStack);

        foreach ($relationships as $index => $relationship) {
            /** @var string $key */
            $key = $relationship['key'];
            $value = $document->getAttribute($key);
            $oldValue = $old->getAttribute($key);
            $relatedCollection = $this->db->getCollection($relationship['options']['relatedCollection']);
            $relationType = (string) $relationship['options']['relationType'];
            $twoWay = (bool) $relationship['options']['twoWay'];
            $twoWayKey = (string) $relationship['options']['twoWayKey'];
            $side = (string) $relationship['options']['side'];

            if (Operator::isOperator($value)) {
                $operator = $value;
                if ($operator->isArrayOperation()) {
                    $existingIds = [];
                    if (\is_array($oldValue)) {
                        $existingIds = \array_map(function ($item) {
                            if ($item instanceof Document) {
                                return $item->getId();
                            }

                            return $item;
                        }, $oldValue);
                    }

                    $value = $this->applyRelationshipOperator($operator, $existingIds);
                    $document->setAttribute($key, $value);
                }
            }

            if ($oldValue == $value) {
                if (
                    ($relationType === RelationType::OneToOne->value
                        || ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Parent->value)) &&
                    $value instanceof Document
                ) {
                    $document->setAttribute($key, $value->getId());

                    continue;
                }
                $document->removeAttribute($key);

                continue;
            }

            if ($stackCount >= Database::RELATION_MAX_DEPTH - 1 && $this->writeStack[$stackCount - 1] !== $relatedCollection->getId()) {
                $document->removeAttribute($key);

                continue;
            }

            $this->writeStack[] = $collection->getId();

            try {
                switch ($relationType) {
                    case RelationType::OneToOne->value:
                        if (! $twoWay) {
                            if ($side === RelationSide::Child->value) {
                                throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                            }

                            if (\is_string($value)) {
                                $related = $this->db->skipRelationships(fn () => $this->db->getDocument($relatedCollection->getId(), $value, [Query::select(['$id'])]));
                                if ($related->isEmpty()) {
                                    $document->setAttribute($key, null);
                                }
                            } elseif ($value instanceof Document) {
                                $relationId = $this->relateDocuments(
                                    $collection,
                                    $relatedCollection,
                                    $key,
                                    $document,
                                    $value,
                                    $relationType,
                                    false,
                                    $twoWayKey,
                                    $side,
                                );
                                $document->setAttribute($key, $relationId);
                            } elseif (is_array($value)) {
                                throw new RelationshipException('Invalid relationship value. Must be either a document, document ID or null. Array given.');
                            }

                            break;
                        }

                        switch (\gettype($value)) {
                            case 'string':
                                $related = $this->db->skipRelationships(
                                    fn () => $this->db->getDocument($relatedCollection->getId(), $value, [Query::select(['$id'])])
                                );

                                if ($related->isEmpty()) {
                                    $document->setAttribute($key, null);
                                    break;
                                }
                                if (
                                    $oldValue?->getId() !== $value
                                    && ! ($this->db->skipRelationships(fn () => $this->db->findOne($relatedCollection->getId(), [
                                        Query::select(['$id']),
                                        Query::equal($twoWayKey, [$value]),
                                    ]))->isEmpty())
                                ) {
                                    throw new DuplicateException('Document already has a related document');
                                }

                                $this->db->skipRelationships(fn () => $this->db->updateDocument(
                                    $relatedCollection->getId(),
                                    $related->getId(),
                                    $related->setAttribute($twoWayKey, $document->getId())
                                ));
                                break;
                            case 'object':
                                if ($value instanceof Document) {
                                    $related = $this->db->skipRelationships(fn () => $this->db->getDocument($relatedCollection->getId(), $value->getId()));

                                    if (
                                        $oldValue?->getId() !== $value->getId()
                                        && ! ($this->db->skipRelationships(fn () => $this->db->findOne($relatedCollection->getId(), [
                                            Query::select(['$id']),
                                            Query::equal($twoWayKey, [$value->getId()]),
                                        ]))->isEmpty())
                                    ) {
                                        throw new DuplicateException('Document already has a related document');
                                    }

                                    $this->writeStack[] = $relatedCollection->getId();
                                    if ($related->isEmpty()) {
                                        if (! isset($value['$permissions'])) {
                                            $value->setAttribute('$permissions', $document->getAttribute('$permissions'));
                                        }
                                        $related = $this->db->createDocument(
                                            $relatedCollection->getId(),
                                            $value->setAttribute($twoWayKey, $document->getId())
                                        );
                                    } else {
                                        $related = $this->db->updateDocument(
                                            $relatedCollection->getId(),
                                            $related->getId(),
                                            $value->setAttribute($twoWayKey, $document->getId())
                                        );
                                    }
                                    \array_pop($this->writeStack);

                                    $document->setAttribute($key, $related->getId());
                                    break;
                                }
                                // no break
                            case 'NULL':
                                if (! \is_null($oldValue?->getId())) {
                                    $oldRelated = $this->db->skipRelationships(
                                        fn () => $this->db->getDocument($relatedCollection->getId(), $oldValue->getId())
                                    );
                                    $this->db->skipRelationships(fn () => $this->db->updateDocument(
                                        $relatedCollection->getId(),
                                        $oldRelated->getId(),
                                        new Document([$twoWayKey => null])
                                    ));
                                }
                                break;
                            default:
                                throw new RelationshipException('Invalid relationship value. Must be either a document, document ID or null.');
                        }
                        break;
                    case RelationType::OneToMany->value:
                    case RelationType::ManyToOne->value:
                        if (
                            ($relationType === RelationType::OneToMany->value && $side === RelationSide::Parent->value) ||
                            ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Child->value)
                        ) {
                            if (! \is_array($value) || ! \array_is_list($value)) {
                                throw new RelationshipException('Invalid relationship value. Must be either an array of documents or document IDs, '.\gettype($value).' given.');
                            }

                            $oldIds = \array_map(fn ($document) => $document->getId(), $oldValue);

                            $newIds = \array_map(function ($item) {
                                if (\is_string($item)) {
                                    return $item;
                                } elseif ($item instanceof Document) {
                                    return $item->getId();
                                } else {
                                    throw new RelationshipException('Invalid relationship value. Must be either a document or document ID.');
                                }
                            }, $value);

                            $removedDocuments = \array_diff($oldIds, $newIds);

                            foreach ($removedDocuments as $relation) {
                                $this->db->getAuthorization()->skip(fn () => $this->db->skipRelationships(fn () => $this->db->updateDocument(
                                    $relatedCollection->getId(),
                                    $relation,
                                    new Document([$twoWayKey => null])
                                )));
                            }

                            foreach ($value as $relation) {
                                if (\is_string($relation)) {
                                    $related = $this->db->skipRelationships(
                                        fn () => $this->db->getDocument($relatedCollection->getId(), $relation, [Query::select(['$id'])])
                                    );

                                    if ($related->isEmpty()) {
                                        continue;
                                    }

                                    $this->db->skipRelationships(fn () => $this->db->updateDocument(
                                        $relatedCollection->getId(),
                                        $related->getId(),
                                        $related->setAttribute($twoWayKey, $document->getId())
                                    ));
                                } elseif ($relation instanceof Document) {
                                    $related = $this->db->skipRelationships(
                                        fn () => $this->db->getDocument($relatedCollection->getId(), $relation->getId(), [Query::select(['$id'])])
                                    );

                                    if ($related->isEmpty()) {
                                        if (! isset($relation['$permissions'])) {
                                            $relation->setAttribute('$permissions', $document->getAttribute('$permissions'));
                                        }
                                        $this->db->createDocument(
                                            $relatedCollection->getId(),
                                            $relation->setAttribute($twoWayKey, $document->getId())
                                        );
                                    } else {
                                        $this->db->updateDocument(
                                            $relatedCollection->getId(),
                                            $related->getId(),
                                            $relation->setAttribute($twoWayKey, $document->getId())
                                        );
                                    }
                                } else {
                                    throw new RelationshipException('Invalid relationship value.');
                                }
                            }

                            $document->removeAttribute($key);
                            break;
                        }

                        if (\is_string($value)) {
                            $related = $this->db->skipRelationships(
                                fn () => $this->db->getDocument($relatedCollection->getId(), $value, [Query::select(['$id'])])
                            );

                            if ($related->isEmpty()) {
                                $document->setAttribute($key, null);
                            }
                            $this->db->purgeCachedDocument($relatedCollection->getId(), $value);
                        } elseif ($value instanceof Document) {
                            $related = $this->db->skipRelationships(
                                fn () => $this->db->getDocument($relatedCollection->getId(), $value->getId(), [Query::select(['$id'])])
                            );

                            if ($related->isEmpty()) {
                                if (! isset($value['$permissions'])) {
                                    $value->setAttribute('$permissions', $document->getAttribute('$permissions'));
                                }
                                $this->db->createDocument(
                                    $relatedCollection->getId(),
                                    $value
                                );
                            } elseif ($related->getAttributes() != $value->getAttributes()) {
                                $this->db->updateDocument(
                                    $relatedCollection->getId(),
                                    $related->getId(),
                                    $value
                                );
                                $this->db->purgeCachedDocument($relatedCollection->getId(), $related->getId());
                            }

                            $document->setAttribute($key, $value->getId());
                        } elseif (\is_null($value)) {
                            break;
                        } elseif (is_array($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document, array given.');
                        } elseif (empty($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document.');
                        } else {
                            throw new RelationshipException('Invalid relationship value.');
                        }

                        break;
                    case RelationType::ManyToMany->value:
                        if (\is_null($value)) {
                            break;
                        }
                        if (! \is_array($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be an array of documents or document IDs.');
                        }

                        $oldIds = \array_map(fn ($document) => $document->getId(), $oldValue);

                        $newIds = \array_map(function ($item) {
                            if (\is_string($item)) {
                                return $item;
                            } elseif ($item instanceof Document) {
                                return $item->getId();
                            } else {
                                throw new RelationshipException('Invalid relationship value. Must be either a document or document ID.');
                            }
                        }, $value);

                        $removedDocuments = \array_diff($oldIds, $newIds);

                        foreach ($removedDocuments as $relation) {
                            $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                            $junctions = $this->db->find($junction, [
                                Query::equal($key, [$relation]),
                                Query::equal($twoWayKey, [$document->getId()]),
                                Query::limit(PHP_INT_MAX),
                            ]);

                            foreach ($junctions as $junction) {
                                $this->db->getAuthorization()->skip(fn () => $this->db->deleteDocument($junction->getCollection(), $junction->getId()));
                            }
                        }

                        foreach ($value as $relation) {
                            if (\is_string($relation)) {
                                if (\in_array($relation, $oldIds) || $this->db->getDocument($relatedCollection->getId(), $relation, [Query::select(['$id'])])->isEmpty()) {
                                    continue;
                                }
                            } elseif ($relation instanceof Document) {
                                $related = $this->db->getDocument($relatedCollection->getId(), $relation->getId(), [Query::select(['$id'])]);

                                if ($related->isEmpty()) {
                                    if (! isset($value['$permissions'])) {
                                        $relation->setAttribute('$permissions', $document->getAttribute('$permissions'));
                                    }
                                    $related = $this->db->createDocument(
                                        $relatedCollection->getId(),
                                        $relation
                                    );
                                } elseif ($related->getAttributes() != $relation->getAttributes()) {
                                    $related = $this->db->updateDocument(
                                        $relatedCollection->getId(),
                                        $related->getId(),
                                        $relation
                                    );
                                }

                                if (\in_array($relation->getId(), $oldIds)) {
                                    continue;
                                }

                                $relation = $related->getId();
                            } else {
                                throw new RelationshipException('Invalid relationship value. Must be either a document or document ID.');
                            }

                            $this->db->skipRelationships(fn () => $this->db->createDocument(
                                $this->getJunctionCollection($collection, $relatedCollection, $side),
                                new Document([
                                    $key => $relation,
                                    $twoWayKey => $document->getId(),
                                    '$permissions' => [
                                        Permission::read(Role::any()),
                                        Permission::update(Role::any()),
                                        Permission::delete(Role::any()),
                                    ],
                                ])
                            ));
                        }

                        $document->removeAttribute($key);
                        break;
                }
            } finally {
                \array_pop($this->writeStack);
            }
        }

        return $document;
    }

    public function beforeDocumentDelete(Document $collection, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);

        $relationships = \array_filter($attributes, function ($attribute) {
            return $attribute['type'] === ColumnType::Relationship->value;
        });

        foreach ($relationships as $relationship) {
            $key = $relationship['key'];
            $value = $document->getAttribute($key);
            $relatedCollection = $this->db->getCollection($relationship['options']['relatedCollection']);
            $relationType = $relationship['options']['relationType'];
            $twoWay = $relationship['options']['twoWay'];
            $twoWayKey = $relationship['options']['twoWayKey'];
            $onDelete = $relationship['options']['onDelete'];
            $side = $relationship['options']['side'];

            $relationship->setAttribute('collection', $collection->getId());
            $relationship->setAttribute('document', $document->getId());

            switch ($onDelete) {
                case ForeignKeyAction::Restrict->value:
                    $this->deleteRestrict($relatedCollection, $document, $value, $relationType, $twoWay, $twoWayKey, $side);
                    break;
                case ForeignKeyAction::SetNull->value:
                    $this->deleteSetNull($collection, $relatedCollection, $document, $value, $relationType, $twoWay, $twoWayKey, $side);
                    break;
                case ForeignKeyAction::Cascade->value:
                    foreach ($this->deleteStack as $processedRelationship) {
                        $existingKey = $processedRelationship['key'];
                        $existingCollection = $processedRelationship['collection'];
                        $existingRelatedCollection = $processedRelationship['options']['relatedCollection'];
                        $existingTwoWayKey = $processedRelationship['options']['twoWayKey'];
                        $existingSide = $processedRelationship['options']['side'];

                        $reflexive = $processedRelationship == $relationship;

                        $symmetric = $existingKey === $twoWayKey
                            && $existingTwoWayKey === $key
                            && $existingRelatedCollection === $collection->getId()
                            && $existingCollection === $relatedCollection->getId()
                            && $existingSide !== $side;

                        $transitive = (($existingKey === $twoWayKey
                                && $existingCollection === $relatedCollection->getId()
                                && $existingSide !== $side)
                            || ($existingTwoWayKey === $key
                                && $existingRelatedCollection === $collection->getId()
                                && $existingSide !== $side)
                            || ($existingKey === $key
                                && $existingTwoWayKey !== $twoWayKey
                                && $existingRelatedCollection === $relatedCollection->getId()
                                && $existingSide !== $side)
                            || ($existingKey !== $key
                                && $existingTwoWayKey === $twoWayKey
                                && $existingRelatedCollection === $relatedCollection->getId()
                                && $existingSide !== $side));

                        if ($reflexive || $symmetric || $transitive) {
                            break 2;
                        }
                    }
                    $this->deleteCascade($collection, $relatedCollection, $document, $key, $value, $relationType, $twoWayKey, $side, $relationship);
                    break;
            }
        }

        return $document;
    }

    public function populateDocuments(array $documents, Document $collection, int $fetchDepth, array $selects = []): array
    {
        $this->inBatchPopulation = true;

        try {
            $queue = [
                [
                    'documents' => $documents,
                    'collection' => $collection,
                    'depth' => $fetchDepth,
                    'selects' => $selects,
                    'skipKey' => null,
                    'hasExplicitSelects' => ! empty($selects),
                ],
            ];

            $currentDepth = $fetchDepth;

            while (! empty($queue) && $currentDepth < Database::RELATION_MAX_DEPTH) {
                $nextQueue = [];

                foreach ($queue as $item) {
                    $docs = $item['documents'];
                    $coll = $item['collection'];
                    $sels = $item['selects'];
                    $skipKey = $item['skipKey'] ?? null;
                    $parentHasExplicitSelects = $item['hasExplicitSelects'];

                    if (empty($docs)) {
                        continue;
                    }

                    $attributes = $coll->getAttribute('attributes', []);
                    $relationships = [];

                    foreach ($attributes as $attribute) {
                        if ($attribute['type'] === ColumnType::Relationship->value) {
                            if ($attribute['key'] === $skipKey) {
                                continue;
                            }

                            if (! $parentHasExplicitSelects || \array_key_exists($attribute['key'], $sels)) {
                                $relationships[] = $attribute;
                            }
                        }
                    }

                    foreach ($relationships as $relationship) {
                        $key = $relationship['key'];
                        $queries = $sels[$key] ?? [];
                        $relationship->setAttribute('collection', $coll->getId());
                        $isAtMaxDepth = ($currentDepth + 1) >= Database::RELATION_MAX_DEPTH;

                        if ($isAtMaxDepth) {
                            foreach ($docs as $doc) {
                                $doc->removeAttribute($key);
                            }

                            continue;
                        }

                        $relatedDocs = $this->populateSingleRelationshipBatch(
                            $docs,
                            $relationship,
                            $queries
                        );

                        $twoWay = $relationship['options']['twoWay'];
                        $twoWayKey = $relationship['options']['twoWayKey'];

                        $hasNestedSelectsForThisRel = isset($sels[$key]);
                        $shouldQueue = ! empty($relatedDocs) &&
                            ($hasNestedSelectsForThisRel || ! $parentHasExplicitSelects);

                        if ($shouldQueue) {
                            $relatedCollectionId = $relationship['options']['relatedCollection'];
                            $relatedCollection = $this->db->silent(fn () => $this->db->getCollection($relatedCollectionId));

                            if (! $relatedCollection->isEmpty()) {
                                $relationshipQueries = $hasNestedSelectsForThisRel ? $sels[$key] : [];

                                $relatedCollectionRelationships = $relatedCollection->getAttribute('attributes', []);
                                $relatedCollectionRelationships = \array_filter(
                                    $relatedCollectionRelationships,
                                    fn ($attr) => $attr['type'] === ColumnType::Relationship->value
                                );

                                $nextSelects = $this->processQueries($relatedCollectionRelationships, $relationshipQueries);

                                $childHasExplicitSelects = $parentHasExplicitSelects;

                                $nextQueue[] = [
                                    'documents' => $relatedDocs,
                                    'collection' => $relatedCollection,
                                    'depth' => $currentDepth + 1,
                                    'selects' => $nextSelects,
                                    'skipKey' => $twoWay ? $twoWayKey : null,
                                    'hasExplicitSelects' => $childHasExplicitSelects,
                                ];
                            }
                        }

                        if ($twoWay && ! empty($relatedDocs)) {
                            foreach ($relatedDocs as $relatedDoc) {
                                $relatedDoc->removeAttribute($twoWayKey);
                            }
                        }
                    }
                }

                $queue = $nextQueue;
                $currentDepth++;
            }
        } finally {
            $this->inBatchPopulation = false;
        }

        return $documents;
    }

    public function processQueries(array $relationships, array $queries): array
    {
        $nestedSelections = [];

        foreach ($queries as $query) {
            if ($query->getMethod() !== Query::TYPE_SELECT) {
                continue;
            }

            $values = $query->getValues();
            foreach ($values as $valueIndex => $value) {
                if (! \str_contains($value, '.')) {
                    continue;
                }

                $nesting = \explode('.', $value);
                $selectedKey = \array_shift($nesting);

                $relationship = \array_values(\array_filter(
                    $relationships,
                    fn (Document $relationship) => $relationship->getAttribute('key') === $selectedKey,
                ))[0] ?? null;

                if (! $relationship) {
                    continue;
                }

                $nestingPath = \implode('.', $nesting);

                if (empty($nestingPath)) {
                    $nestedSelections[$selectedKey][] = Query::select(['*']);
                } else {
                    $nestedSelections[$selectedKey][] = Query::select([$nestingPath]);
                }

                $type = $relationship->getAttribute('options')['relationType'];
                $side = $relationship->getAttribute('options')['side'];

                switch ($type) {
                    case RelationType::ManyToMany->value:
                        unset($values[$valueIndex]);
                        break;
                    case RelationType::OneToMany->value:
                        if ($side === RelationSide::Parent->value) {
                            unset($values[$valueIndex]);
                        } else {
                            $values[$valueIndex] = $selectedKey;
                        }
                        break;
                    case RelationType::ManyToOne->value:
                        if ($side === RelationSide::Parent->value) {
                            $values[$valueIndex] = $selectedKey;
                        } else {
                            unset($values[$valueIndex]);
                        }
                        break;
                    case RelationType::OneToOne->value:
                        $values[$valueIndex] = $selectedKey;
                        break;
                }
            }

            $finalValues = \array_values($values);
            if ($query->getMethod() === Query::TYPE_SELECT) {
                if (empty($finalValues)) {
                    $finalValues = ['*'];
                }
            }
            $query->setValues($finalValues);
        }

        return $nestedSelections;
    }

    public function convertQueries(array $relationships, array $queries, ?Document $collection = null): ?array
    {
        $hasRelationshipQuery = false;
        foreach ($queries as $query) {
            $attr = $query->getAttribute();
            if (\str_contains($attr, '.') || $query->getMethod() === Query::TYPE_CONTAINS_ALL) {
                $hasRelationshipQuery = true;
                break;
            }
        }

        if (! $hasRelationshipQuery) {
            return $queries;
        }

        $relationshipsByKey = [];
        foreach ($relationships as $relationship) {
            $relationshipsByKey[$relationship->getAttribute('key')] = $relationship;
        }

        $additionalQueries = [];
        $groupedQueries = [];
        $indicesToRemove = [];

        foreach ($queries as $index => $query) {
            if ($query->getMethod() !== Query::TYPE_CONTAINS_ALL) {
                continue;
            }

            $attribute = $query->getAttribute();

            if (! \str_contains($attribute, '.')) {
                continue;
            }

            $parts = \explode('.', $attribute);
            $relationshipKey = \array_shift($parts);
            $nestedAttribute = \implode('.', $parts);
            $relationship = $relationshipsByKey[$relationshipKey] ?? null;

            if (! $relationship) {
                continue;
            }

            $parentIdSets = [];
            $resolvedAttribute = '$id';
            foreach ($query->getValues() as $value) {
                $relatedQuery = Query::equal($nestedAttribute, [$value]);
                $result = $this->resolveRelationshipGroupToIds($relationship, [$relatedQuery], $collection);

                if ($result === null) {
                    return null;
                }

                $resolvedAttribute = $result['attribute'];
                $parentIdSets[] = $result['ids'];
            }

            $ids = \count($parentIdSets) > 1
                ? \array_values(\array_intersect(...$parentIdSets))
                : ($parentIdSets[0] ?? []);

            if (empty($ids)) {
                return null;
            }

            $additionalQueries[] = Query::equal($resolvedAttribute, $ids);
            $indicesToRemove[] = $index;
        }

        foreach ($queries as $index => $query) {
            if ($query->getMethod() === Query::TYPE_SELECT || $query->getMethod() === Query::TYPE_CONTAINS_ALL) {
                continue;
            }

            $attribute = $query->getAttribute();

            if (! \str_contains($attribute, '.')) {
                continue;
            }

            $parts = \explode('.', $attribute);
            $relationshipKey = \array_shift($parts);
            $nestedAttribute = \implode('.', $parts);
            $relationship = $relationshipsByKey[$relationshipKey] ?? null;

            if (! $relationship) {
                continue;
            }

            if (! isset($groupedQueries[$relationshipKey])) {
                $groupedQueries[$relationshipKey] = [
                    'relationship' => $relationship,
                    'queries' => [],
                    'indices' => [],
                ];
            }

            $groupedQueries[$relationshipKey]['queries'][] = [
                'method' => $query->getMethod(),
                'attribute' => $nestedAttribute,
                'values' => $query->getValues(),
            ];

            $groupedQueries[$relationshipKey]['indices'][] = $index;
        }

        foreach ($groupedQueries as $relationshipKey => $group) {
            $relationship = $group['relationship'];

            $equalAttrs = [];
            foreach ($group['queries'] as $queryData) {
                if ($queryData['method'] === Query::TYPE_EQUAL) {
                    $attr = $queryData['attribute'];
                    if (isset($equalAttrs[$attr])) {
                        throw new QueryException("Multiple equal queries on '{$relationshipKey}.{$attr}' will never match a single document. Use Query::containsAll() to match across different related documents.");
                    }
                    $equalAttrs[$attr] = true;
                }
            }

            $relatedQueries = [];
            foreach ($group['queries'] as $queryData) {
                $relatedQueries[] = new Query(
                    $queryData['method'],
                    $queryData['attribute'],
                    $queryData['values']
                );
            }

            try {
                $result = $this->resolveRelationshipGroupToIds($relationship, $relatedQueries, $collection);

                if ($result === null) {
                    return null;
                }

                $additionalQueries[] = Query::equal($result['attribute'], $result['ids']);

                foreach ($group['indices'] as $originalIndex) {
                    $indicesToRemove[] = $originalIndex;
                }
            } catch (QueryException $e) {
                throw $e;
            } catch (\Exception $e) {
                return null;
            }
        }

        foreach ($indicesToRemove as $index) {
            unset($queries[$index]);
        }

        return \array_merge(\array_values($queries), $additionalQueries);
    }

    private function relateDocuments(
        Document $collection,
        Document $relatedCollection,
        string $key,
        Document $document,
        Document $relation,
        string $relationType,
        bool $twoWay,
        string $twoWayKey,
        string $side,
    ): string {
        switch ($relationType) {
            case RelationType::OneToOne->value:
                if ($twoWay) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
            case RelationType::OneToMany->value:
                if ($side === RelationSide::Parent->value) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
            case RelationType::ManyToOne->value:
                if ($side === RelationSide::Child->value) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
        }

        $related = $this->db->getDocument($relatedCollection->getId(), $relation->getId());

        if ($related->isEmpty()) {
            if (! isset($relation['$permissions'])) {
                $relation->setAttribute('$permissions', $document->getPermissions());
            }

            $related = $this->db->createDocument($relatedCollection->getId(), $relation);
        } elseif ($related->getAttributes() != $relation->getAttributes()) {
            foreach ($relation->getAttributes() as $attribute => $value) {
                $related->setAttribute($attribute, $value);
            }

            $related = $this->db->updateDocument($relatedCollection->getId(), $related->getId(), $related);
        }

        if ($relationType === RelationType::ManyToMany->value) {
            $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

            $this->db->createDocument($junction, new Document([
                $key => $related->getId(),
                $twoWayKey => $document->getId(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]));
        }

        return $related->getId();
    }

    private function relateDocumentsById(
        Document $collection,
        Document $relatedCollection,
        string $key,
        string $documentId,
        string $relationId,
        string $relationType,
        bool $twoWay,
        string $twoWayKey,
        string $side,
    ): void {
        $related = $this->db->skipRelationships(fn () => $this->db->getDocument($relatedCollection->getId(), $relationId));

        if ($related->isEmpty() && $this->checkExist) {
            return;
        }

        switch ($relationType) {
            case RelationType::OneToOne->value:
                if ($twoWay) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->db->skipRelationships(fn () => $this->db->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationType::OneToMany->value:
                if ($side === RelationSide::Parent->value) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->db->skipRelationships(fn () => $this->db->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationType::ManyToOne->value:
                if ($side === RelationSide::Child->value) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->db->skipRelationships(fn () => $this->db->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationType::ManyToMany->value:
                $this->db->purgeCachedDocument($relatedCollection->getId(), $relationId);

                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $this->db->skipRelationships(fn () => $this->db->createDocument($junction, new Document([
                    $key => $relationId,
                    $twoWayKey => $documentId,
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                ])));
                break;
        }
    }

    private function getJunctionCollection(Document $collection, Document $relatedCollection, string $side): string
    {
        return $side === RelationSide::Parent->value
            ? '_'.$collection->getSequence().'_'.$relatedCollection->getSequence()
            : '_'.$relatedCollection->getSequence().'_'.$collection->getSequence();
    }

    /**
     * @param  array<string>  $existingIds
     * @return array<string|Document>
     */
    private function applyRelationshipOperator(Operator $operator, array $existingIds): array
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        $valueIds = \array_filter(\array_map(fn ($item) => $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : null), $values));

        switch ($method) {
            case OperatorType::ArrayAppend->value:
                return \array_values(\array_merge($existingIds, $valueIds));

            case OperatorType::ArrayPrepend->value:
                return \array_values(\array_merge($valueIds, $existingIds));

            case OperatorType::ArrayInsert->value:
                $index = $values[0] ?? 0;
                $item = $values[1] ?? null;
                $itemId = $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : null);
                if ($itemId !== null) {
                    \array_splice($existingIds, $index, 0, [$itemId]);
                }

                return \array_values($existingIds);

            case OperatorType::ArrayRemove->value:
                $toRemove = $values[0] ?? null;
                if (\is_array($toRemove)) {
                    $toRemoveIds = \array_filter(\array_map(fn ($item) => $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : null), $toRemove));

                    return \array_values(\array_diff($existingIds, $toRemoveIds));
                }
                $toRemoveId = $toRemove instanceof Document ? $toRemove->getId() : (\is_string($toRemove) ? $toRemove : null);
                if ($toRemoveId !== null) {
                    return \array_values(\array_diff($existingIds, [$toRemoveId]));
                }

                return $existingIds;

            case OperatorType::ArrayUnique->value:
                return \array_values(\array_unique($existingIds));

            case OperatorType::ArrayIntersect->value:
                return \array_values(\array_intersect($existingIds, $valueIds));

            case OperatorType::ArrayDiff->value:
                return \array_values(\array_diff($existingIds, $valueIds));

            default:
                return $existingIds;
        }
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateSingleRelationshipBatch(array $documents, Document $relationship, array $queries): array
    {
        return match ($relationship['options']['relationType']) {
            RelationType::OneToOne->value => $this->populateOneToOneRelationshipsBatch($documents, $relationship, $queries),
            RelationType::OneToMany->value => $this->populateOneToManyRelationshipsBatch($documents, $relationship, $queries),
            RelationType::ManyToOne->value => $this->populateManyToOneRelationshipsBatch($documents, $relationship, $queries),
            RelationType::ManyToMany->value => $this->populateManyToManyRelationshipsBatch($documents, $relationship, $queries),
            default => [],
        };
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateOneToOneRelationshipsBatch(array $documents, Document $relationship, array $queries): array
    {
        $key = $relationship['key'];
        $relatedCollection = $this->db->getCollection($relationship['options']['relatedCollection']);

        $relatedIds = [];
        $documentsByRelatedId = [];

        foreach ($documents as $document) {
            $value = $document->getAttribute($key);
            if (! \is_null($value)) {
                if ($value instanceof Document) {
                    continue;
                }

                $relatedIds[] = $value;
                if (! isset($documentsByRelatedId[$value])) {
                    $documentsByRelatedId[$value] = [];
                }
                $documentsByRelatedId[$value][] = $document;
            }
        }

        if (empty($relatedIds)) {
            return [];
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $uniqueRelatedIds = \array_unique($relatedIds);
        $relatedDocuments = [];

        foreach (\array_chunk($uniqueRelatedIds, Database::RELATION_QUERY_CHUNK_SIZE) as $chunk) {
            $chunkDocs = $this->db->find($relatedCollection->getId(), [
                Query::equal('$id', $chunk),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]);
            \array_push($relatedDocuments, ...$chunkDocs);
        }

        $relatedById = [];
        foreach ($relatedDocuments as $related) {
            $relatedById[$related->getId()] = $related;
        }

        $this->db->applySelectFiltersToDocuments($relatedDocuments, $selectQueries);

        foreach ($documentsByRelatedId as $relatedId => $docs) {
            if (isset($relatedById[$relatedId])) {
                foreach ($docs as $document) {
                    $document->setAttribute($key, $relatedById[$relatedId]);
                }
            } else {
                foreach ($docs as $document) {
                    $document->setAttribute($key, new Document);
                }
            }
        }

        return $relatedDocuments;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateOneToManyRelationshipsBatch(array $documents, Document $relationship, array $queries): array
    {
        $key = $relationship['key'];
        $twoWay = $relationship['options']['twoWay'];
        $twoWayKey = $relationship['options']['twoWayKey'];
        $side = $relationship['options']['side'];
        $relatedCollection = $this->db->getCollection($relationship['options']['relatedCollection']);

        if ($side === RelationSide::Child->value) {
            if (! $twoWay) {
                foreach ($documents as $document) {
                    $document->removeAttribute($key);
                }

                return [];
            }

            return $this->populateOneToOneRelationshipsBatch($documents, $relationship, $queries);
        }

        $parentIds = [];
        foreach ($documents as $document) {
            $parentId = $document->getId();
            $parentIds[] = $parentId;
        }

        $parentIds = \array_unique($parentIds);

        if (empty($parentIds)) {
            return [];
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $relatedDocuments = [];

        foreach (\array_chunk($parentIds, Database::RELATION_QUERY_CHUNK_SIZE) as $chunk) {
            $chunkDocs = $this->db->find($relatedCollection->getId(), [
                Query::equal($twoWayKey, $chunk),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]);
            \array_push($relatedDocuments, ...$chunkDocs);
        }

        $relatedByParentId = [];
        foreach ($relatedDocuments as $related) {
            $parentId = $related->getAttribute($twoWayKey);
            if (! \is_null($parentId)) {
                $parentKey = $parentId instanceof Document
                    ? $parentId->getId()
                    : $parentId;

                if (! isset($relatedByParentId[$parentKey])) {
                    $relatedByParentId[$parentKey] = [];
                }
                $relatedByParentId[$parentKey][] = $related;
            }
        }

        $this->db->applySelectFiltersToDocuments($relatedDocuments, $selectQueries);

        foreach ($documents as $document) {
            $parentId = $document->getId();
            $relatedDocs = $relatedByParentId[$parentId] ?? [];
            $document->setAttribute($key, $relatedDocs);
        }

        return $relatedDocuments;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateManyToOneRelationshipsBatch(array $documents, Document $relationship, array $queries): array
    {
        $key = $relationship['key'];
        $twoWay = $relationship['options']['twoWay'];
        $twoWayKey = $relationship['options']['twoWayKey'];
        $side = $relationship['options']['side'];
        $relatedCollection = $this->db->getCollection($relationship['options']['relatedCollection']);

        if ($side === RelationSide::Parent->value) {
            return $this->populateOneToOneRelationshipsBatch($documents, $relationship, $queries);
        }

        if (! $twoWay) {
            foreach ($documents as $document) {
                $document->removeAttribute($key);
            }

            return [];
        }

        $childIds = [];
        foreach ($documents as $document) {
            $childId = $document->getId();
            $childIds[] = $childId;
        }

        $childIds = array_unique($childIds);

        if (empty($childIds)) {
            return [];
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $relatedDocuments = [];

        foreach (\array_chunk($childIds, Database::RELATION_QUERY_CHUNK_SIZE) as $chunk) {
            $chunkDocs = $this->db->find($relatedCollection->getId(), [
                Query::equal($twoWayKey, $chunk),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]);
            \array_push($relatedDocuments, ...$chunkDocs);
        }

        $relatedByChildId = [];
        foreach ($relatedDocuments as $related) {
            $childId = $related->getAttribute($twoWayKey);
            if (! \is_null($childId)) {
                $childKey = $childId instanceof Document
                    ? $childId->getId()
                    : $childId;

                if (! isset($relatedByChildId[$childKey])) {
                    $relatedByChildId[$childKey] = [];
                }
                $relatedByChildId[$childKey][] = $related;
            }
        }

        $this->db->applySelectFiltersToDocuments($relatedDocuments, $selectQueries);

        foreach ($documents as $document) {
            $childId = $document->getId();
            $document->setAttribute($key, $relatedByChildId[$childId] ?? []);
        }

        return $relatedDocuments;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateManyToManyRelationshipsBatch(array $documents, Document $relationship, array $queries): array
    {
        $key = $relationship['key'];
        $twoWay = $relationship['options']['twoWay'];
        $twoWayKey = $relationship['options']['twoWayKey'];
        $side = $relationship['options']['side'];
        $relatedCollection = $this->db->getCollection($relationship['options']['relatedCollection']);
        $collection = $this->db->getCollection($relationship->getAttribute('collection'));

        if (! $twoWay && $side === RelationSide::Child->value) {
            return [];
        }

        $documentIds = [];
        foreach ($documents as $document) {
            $documentId = $document->getId();
            $documentIds[] = $documentId;
        }

        $documentIds = array_unique($documentIds);

        if (empty($documentIds)) {
            return [];
        }

        $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

        $junctions = [];

        foreach (\array_chunk($documentIds, Database::RELATION_QUERY_CHUNK_SIZE) as $chunk) {
            $chunkJunctions = $this->db->skipRelationships(fn () => $this->db->find($junction, [
                Query::equal($twoWayKey, $chunk),
                Query::limit(PHP_INT_MAX),
            ]));
            \array_push($junctions, ...$chunkJunctions);
        }

        $relatedIds = [];
        $junctionsByDocumentId = [];

        foreach ($junctions as $junctionDoc) {
            $documentId = $junctionDoc->getAttribute($twoWayKey);
            $relatedId = $junctionDoc->getAttribute($key);

            if (! \is_null($documentId) && ! \is_null($relatedId)) {
                if (! isset($junctionsByDocumentId[$documentId])) {
                    $junctionsByDocumentId[$documentId] = [];
                }
                $junctionsByDocumentId[$documentId][] = $relatedId;
                $relatedIds[] = $relatedId;
            }
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $related = [];
        $allRelatedDocs = [];
        if (! empty($relatedIds)) {
            $uniqueRelatedIds = array_unique($relatedIds);
            $foundRelated = [];

            foreach (\array_chunk($uniqueRelatedIds, Database::RELATION_QUERY_CHUNK_SIZE) as $chunk) {
                $chunkDocs = $this->db->find($relatedCollection->getId(), [
                    Query::equal('$id', $chunk),
                    Query::limit(PHP_INT_MAX),
                    ...$otherQueries,
                ]);
                \array_push($foundRelated, ...$chunkDocs);
            }

            $allRelatedDocs = $foundRelated;

            $relatedById = [];
            foreach ($foundRelated as $doc) {
                $relatedById[$doc->getId()] = $doc;
            }

            $this->db->applySelectFiltersToDocuments($allRelatedDocs, $selectQueries);

            foreach ($junctionsByDocumentId as $documentId => $relatedDocIds) {
                $documentRelated = [];
                foreach ($relatedDocIds as $relatedId) {
                    if (isset($relatedById[$relatedId])) {
                        $documentRelated[] = $relatedById[$relatedId];
                    }
                }
                $related[$documentId] = $documentRelated;
            }
        }

        foreach ($documents as $document) {
            $documentId = $document->getId();
            $document->setAttribute($key, $related[$documentId] ?? []);
        }

        return $allRelatedDocs;
    }

    private function deleteRestrict(
        Document $relatedCollection,
        Document $document,
        mixed $value,
        string $relationType,
        bool $twoWay,
        string $twoWayKey,
        string $side
    ): void {
        if ($value instanceof Document && $value->isEmpty()) {
            $value = null;
        }

        if (
            ! empty($value)
            && $relationType !== RelationType::ManyToOne->value
            && $side === RelationSide::Parent->value
        ) {
            throw new RestrictedException('Cannot delete document because it has at least one related document.');
        }

        if (
            $relationType === RelationType::OneToOne->value
            && $side === RelationSide::Child->value
            && ! $twoWay
        ) {
            $this->db->getAuthorization()->skip(function () use ($document, $relatedCollection, $twoWayKey) {
                $related = $this->db->findOne($relatedCollection->getId(), [
                    Query::select(['$id']),
                    Query::equal($twoWayKey, [$document->getId()]),
                ]);

                if ($related->isEmpty()) {
                    return;
                }

                $this->db->skipRelationships(fn () => $this->db->updateDocument(
                    $relatedCollection->getId(),
                    $related->getId(),
                    new Document([
                        $twoWayKey => null,
                    ])
                ));
            });
        }

        if (
            $relationType === RelationType::ManyToOne->value
            && $side === RelationSide::Child->value
        ) {
            $related = $this->db->getAuthorization()->skip(fn () => $this->db->findOne($relatedCollection->getId(), [
                Query::select(['$id']),
                Query::equal($twoWayKey, [$document->getId()]),
            ]));

            if (! $related->isEmpty()) {
                throw new RestrictedException('Cannot delete document because it has at least one related document.');
            }
        }
    }

    private function deleteSetNull(Document $collection, Document $relatedCollection, Document $document, mixed $value, string $relationType, bool $twoWay, string $twoWayKey, string $side): void
    {
        switch ($relationType) {
            case RelationType::OneToOne->value:
                if (! $twoWay && $side === RelationSide::Parent->value) {
                    break;
                }

                $this->db->getAuthorization()->skip(function () use ($document, $value, $relatedCollection, $twoWay, $twoWayKey, $side) {
                    if (! $twoWay && $side === RelationSide::Child->value) {
                        $related = $this->db->findOne($relatedCollection->getId(), [
                            Query::select(['$id']),
                            Query::equal($twoWayKey, [$document->getId()]),
                        ]);
                    } else {
                        if (empty($value)) {
                            return;
                        }
                        $related = $this->db->getDocument($relatedCollection->getId(), $value->getId(), [Query::select(['$id'])]);
                    }

                    if ($related->isEmpty()) {
                        return;
                    }

                    $this->db->skipRelationships(fn () => $this->db->updateDocument(
                        $relatedCollection->getId(),
                        $related->getId(),
                        new Document([
                            $twoWayKey => null,
                        ])
                    ));
                });
                break;

            case RelationType::OneToMany->value:
                if ($side === RelationSide::Child->value) {
                    break;
                }
                foreach ($value as $relation) {
                    $this->db->getAuthorization()->skip(function () use ($relatedCollection, $twoWayKey, $relation) {
                        $this->db->skipRelationships(fn () => $this->db->updateDocument(
                            $relatedCollection->getId(),
                            $relation->getId(),
                            new Document([
                                $twoWayKey => null,
                            ]),
                        ));
                    });
                }
                break;

            case RelationType::ManyToOne->value:
                if ($side === RelationSide::Parent->value) {
                    break;
                }

                if (! $twoWay) {
                    $value = $this->db->find($relatedCollection->getId(), [
                        Query::select(['$id']),
                        Query::equal($twoWayKey, [$document->getId()]),
                        Query::limit(PHP_INT_MAX),
                    ]);
                }

                foreach ($value as $relation) {
                    $this->db->getAuthorization()->skip(function () use ($relatedCollection, $twoWayKey, $relation) {
                        $this->db->skipRelationships(fn () => $this->db->updateDocument(
                            $relatedCollection->getId(),
                            $relation->getId(),
                            new Document([
                                $twoWayKey => null,
                            ])
                        ));
                    });
                }
                break;

            case RelationType::ManyToMany->value:
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $junctions = $this->db->find($junction, [
                    Query::select(['$id']),
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX),
                ]);

                foreach ($junctions as $document) {
                    $this->db->skipRelationships(fn () => $this->db->deleteDocument(
                        $junction,
                        $document->getId()
                    ));
                }
                break;
        }
    }

    private function deleteCascade(Document $collection, Document $relatedCollection, Document $document, string $key, mixed $value, string $relationType, string $twoWayKey, string $side, Document $relationship): void
    {
        switch ($relationType) {
            case RelationType::OneToOne->value:
                if ($value !== null) {
                    $this->deleteStack[] = $relationship;

                    $this->db->deleteDocument(
                        $relatedCollection->getId(),
                        ($value instanceof Document) ? $value->getId() : $value
                    );

                    \array_pop($this->deleteStack);
                }
                break;
            case RelationType::OneToMany->value:
                if ($side === RelationSide::Child->value) {
                    break;
                }

                $this->deleteStack[] = $relationship;

                foreach ($value as $relation) {
                    $this->db->deleteDocument(
                        $relatedCollection->getId(),
                        $relation->getId()
                    );
                }

                \array_pop($this->deleteStack);

                break;
            case RelationType::ManyToOne->value:
                if ($side === RelationSide::Parent->value) {
                    break;
                }

                $value = $this->db->find($relatedCollection->getId(), [
                    Query::select(['$id']),
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX),
                ]);

                $this->deleteStack[] = $relationship;

                foreach ($value as $relation) {
                    $this->db->deleteDocument(
                        $relatedCollection->getId(),
                        $relation->getId()
                    );
                }

                \array_pop($this->deleteStack);

                break;
            case RelationType::ManyToMany->value:
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $junctions = $this->db->skipRelationships(fn () => $this->db->find($junction, [
                    Query::select(['$id', $key]),
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX),
                ]));

                $this->deleteStack[] = $relationship;

                foreach ($junctions as $document) {
                    if ($side === RelationSide::Parent->value) {
                        $this->db->deleteDocument(
                            $relatedCollection->getId(),
                            $document->getAttribute($key)
                        );
                    }
                    $this->db->deleteDocument(
                        $junction,
                        $document->getId()
                    );
                }

                \array_pop($this->deleteStack);
                break;
        }
    }

    /**
     * @param  array<Query>  $queries
     * @return array<string>|null
     */
    private function processNestedRelationshipPath(string $startCollection, array $queries): ?array
    {
        $pathGroups = [];
        foreach ($queries as $query) {
            $attribute = $query->getAttribute();
            if (\str_contains($attribute, '.')) {
                $parts = \explode('.', $attribute);
                $pathKey = \implode('.', \array_slice($parts, 0, -1));
                if (! isset($pathGroups[$pathKey])) {
                    $pathGroups[$pathKey] = [];
                }
                $pathGroups[$pathKey][] = [
                    'method' => $query->getMethod(),
                    'attribute' => \end($parts),
                    'values' => $query->getValues(),
                ];
            }
        }

        $allMatchingIds = [];
        foreach ($pathGroups as $path => $queryGroup) {
            $pathParts = \explode('.', $path);
            $currentCollection = $startCollection;
            $relationshipChain = [];

            foreach ($pathParts as $relationshipKey) {
                $collectionDoc = $this->db->silent(fn () => $this->db->getCollection($currentCollection));
                $relationships = \array_filter(
                    $collectionDoc->getAttribute('attributes', []),
                    fn ($attr) => $attr['type'] === ColumnType::Relationship->value
                );

                $relationship = null;
                foreach ($relationships as $rel) {
                    if ($rel['key'] === $relationshipKey) {
                        $relationship = $rel;
                        break;
                    }
                }

                if (! $relationship) {
                    return null;
                }

                $relationshipChain[] = [
                    'key' => $relationshipKey,
                    'fromCollection' => $currentCollection,
                    'toCollection' => $relationship['options']['relatedCollection'],
                    'relationType' => $relationship['options']['relationType'],
                    'side' => $relationship['options']['side'],
                    'twoWayKey' => $relationship['options']['twoWayKey'],
                ];

                $currentCollection = $relationship['options']['relatedCollection'];
            }

            $leafQueries = [];
            foreach ($queryGroup as $q) {
                $leafQueries[] = new Query($q['method'], $q['attribute'], $q['values']);
            }

            $matchingDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                $currentCollection,
                \array_merge($leafQueries, [
                    Query::select(['$id']),
                    Query::limit(PHP_INT_MAX),
                ])
            )));

            $matchingIds = \array_map(fn ($doc) => $doc->getId(), $matchingDocs);

            if (empty($matchingIds)) {
                return null;
            }

            for ($i = \count($relationshipChain) - 1; $i >= 0; $i--) {
                $link = $relationshipChain[$i];
                $relationType = $link['relationType'];
                $side = $link['side'];

                $needsReverseLookup = (
                    ($relationType === RelationType::OneToMany->value && $side === RelationSide::Parent->value) ||
                    ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Child->value) ||
                    ($relationType === RelationType::ManyToMany->value)
                );

                if ($needsReverseLookup) {
                    if ($relationType === RelationType::ManyToMany->value) {
                        $fromCollectionDoc = $this->db->silent(fn () => $this->db->getCollection($link['fromCollection']));
                        $toCollectionDoc = $this->db->silent(fn () => $this->db->getCollection($link['toCollection']));
                        $junction = $this->getJunctionCollection($fromCollectionDoc, $toCollectionDoc, $link['side']);

                        $junctionDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find($junction, [
                            Query::equal($link['key'], $matchingIds),
                            Query::limit(PHP_INT_MAX),
                        ])));

                        $parentIds = [];
                        foreach ($junctionDocs as $jDoc) {
                            $pId = $jDoc->getAttribute($link['twoWayKey']);
                            if ($pId && ! \in_array($pId, $parentIds)) {
                                $parentIds[] = $pId;
                            }
                        }
                    } else {
                        $childDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                            $link['toCollection'],
                            [
                                Query::equal('$id', $matchingIds),
                                Query::select(['$id', $link['twoWayKey']]),
                                Query::limit(PHP_INT_MAX),
                            ]
                        )));

                        $parentIds = [];
                        foreach ($childDocs as $doc) {
                            $parentValue = $doc->getAttribute($link['twoWayKey']);
                            if (\is_array($parentValue)) {
                                foreach ($parentValue as $pId) {
                                    if ($pId instanceof Document) {
                                        $pId = $pId->getId();
                                    }
                                    if ($pId && ! \in_array($pId, $parentIds)) {
                                        $parentIds[] = $pId;
                                    }
                                }
                            } else {
                                if ($parentValue instanceof Document) {
                                    $parentValue = $parentValue->getId();
                                }
                                if ($parentValue && ! \in_array($parentValue, $parentIds)) {
                                    $parentIds[] = $parentValue;
                                }
                            }
                        }
                    }
                    $matchingIds = $parentIds;
                } else {
                    $parentDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                        $link['fromCollection'],
                        [
                            Query::equal($link['key'], $matchingIds),
                            Query::select(['$id']),
                            Query::limit(PHP_INT_MAX),
                        ]
                    )));
                    $matchingIds = \array_map(fn ($doc) => $doc->getId(), $parentDocs);
                }

                if (empty($matchingIds)) {
                    return null;
                }
            }

            $allMatchingIds = \array_merge($allMatchingIds, $matchingIds);
        }

        return \array_unique($allMatchingIds);
    }

    /**
     * @param  array<Query>  $relatedQueries
     * @return array{attribute: string, ids: string[]}|null
     */
    private function resolveRelationshipGroupToIds(
        Document $relationship,
        array $relatedQueries,
        ?Document $collection = null,
    ): ?array {
        $relatedCollection = $relationship->getAttribute('options')['relatedCollection'];
        $relationType = $relationship->getAttribute('options')['relationType'];
        $side = $relationship->getAttribute('options')['side'];
        $relationshipKey = $relationship->getAttribute('key');

        $hasNestedPaths = false;
        foreach ($relatedQueries as $relatedQuery) {
            if (\str_contains($relatedQuery->getAttribute(), '.')) {
                $hasNestedPaths = true;
                break;
            }
        }

        if ($hasNestedPaths) {
            $matchingIds = $this->processNestedRelationshipPath(
                $relatedCollection,
                $relatedQueries
            );

            if ($matchingIds === null || empty($matchingIds)) {
                return null;
            }

            $relatedQueries = \array_values(\array_merge(
                \array_filter($relatedQueries, fn (Query $q) => ! \str_contains($q->getAttribute(), '.')),
                [Query::equal('$id', $matchingIds)]
            ));
        }

        $needsParentResolution = (
            ($relationType === RelationType::OneToMany->value && $side === RelationSide::Parent->value) ||
            ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Child->value) ||
            ($relationType === RelationType::ManyToMany->value)
        );

        if ($relationType === RelationType::ManyToMany->value && $needsParentResolution && $collection !== null) {
            $matchingDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                $relatedCollection,
                \array_merge($relatedQueries, [
                    Query::select(['$id']),
                    Query::limit(PHP_INT_MAX),
                ])
            )));

            $matchingIds = \array_map(fn ($doc) => $doc->getId(), $matchingDocs);

            if (empty($matchingIds)) {
                return null;
            }

            $twoWayKey = $relationship->getAttribute('options')['twoWayKey'];
            $relatedCollectionDoc = $this->db->silent(fn () => $this->db->getCollection($relatedCollection));
            $junction = $this->getJunctionCollection($collection, $relatedCollectionDoc, $side);

            $junctionDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find($junction, [
                Query::equal($relationshipKey, $matchingIds),
                Query::limit(PHP_INT_MAX),
            ])));

            $parentIds = [];
            foreach ($junctionDocs as $jDoc) {
                $pId = $jDoc->getAttribute($twoWayKey);
                if ($pId && ! \in_array($pId, $parentIds)) {
                    $parentIds[] = $pId;
                }
            }

            return empty($parentIds) ? null : ['attribute' => '$id', 'ids' => $parentIds];
        } elseif ($needsParentResolution) {
            $matchingDocs = $this->db->silent(fn () => $this->db->find(
                $relatedCollection,
                \array_merge($relatedQueries, [
                    Query::limit(PHP_INT_MAX),
                ])
            ));

            $twoWayKey = $relationship->getAttribute('options')['twoWayKey'];
            $parentIds = [];

            foreach ($matchingDocs as $doc) {
                $parentId = $doc->getAttribute($twoWayKey);

                if (\is_array($parentId)) {
                    foreach ($parentId as $id) {
                        if ($id instanceof Document) {
                            $id = $id->getId();
                        }
                        if ($id && ! \in_array($id, $parentIds)) {
                            $parentIds[] = $id;
                        }
                    }
                } else {
                    if ($parentId instanceof Document) {
                        $parentId = $parentId->getId();
                    }
                    if ($parentId && ! \in_array($parentId, $parentIds)) {
                        $parentIds[] = $parentId;
                    }
                }
            }

            return empty($parentIds) ? null : ['attribute' => '$id', 'ids' => $parentIds];
        } else {
            $matchingDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                $relatedCollection,
                \array_merge($relatedQueries, [
                    Query::select(['$id']),
                    Query::limit(PHP_INT_MAX),
                ])
            )));

            $matchingIds = \array_map(fn ($doc) => $doc->getId(), $matchingDocs);

            return empty($matchingIds) ? null : ['attribute' => $relationshipKey, 'ids' => $matchingIds];
        }
    }
}
