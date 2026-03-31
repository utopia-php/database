<?php

namespace Utopia\Database\Hook;

use Exception;
use Utopia\Async\Promise;
use Utopia\Database\Attribute;
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
use Utopia\Database\Relationship as RelationshipVO;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Hook;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;

/**
 * Handles relationship side effects for document CRUD, populates nested relationships
 * on read, and converts relationship filter queries into adapter-compatible subqueries.
 */
class Relationships implements Hook
{
    private bool $enabled = true;

    private bool $checkExist = true;

    private int $fetchDepth = 0;

    private bool $inBatchPopulation = false;

    /** @var array<string> */
    private array $writeStack = [];

    /** @var array<Document> */
    private array $deleteStack = [];

    /**
     * @param Database $db The database instance used for relationship operations
     */
    public function __construct(
        private Database $db,
    ) {
    }

    /**
     * {@inheritDoc}
     */
    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    /**
     * {@inheritDoc}
     */
    public function setEnabled(bool $enabled): void
    {
        $this->enabled = $enabled;
    }

    /**
     * {@inheritDoc}
     */
    public function shouldCheckExist(): bool
    {
        return $this->checkExist;
    }

    /**
     * {@inheritDoc}
     */
    public function setCheckExist(bool $check): void
    {
        $this->checkExist = $check;
    }

    /**
     * {@inheritDoc}
     */
    public function getWriteStackCount(): int
    {
        return \count($this->writeStack);
    }

    /**
     * {@inheritDoc}
     */
    public function getFetchDepth(): int
    {
        return $this->fetchDepth;
    }

    /**
     * {@inheritDoc}
     */
    public function isInBatchPopulation(): bool
    {
        return $this->inBatchPopulation;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DuplicateException If a related document already exists
     * @throws RelationshipException If a relationship constraint is violated
     */
    public function afterDocumentCreate(Document $collection, Document $document): Document
    {
        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        /** @var array<Document> $relationships */
        $relationships = \array_filter(
            $attributes,
            fn (Document $attribute): bool => Attribute::fromDocument($attribute)->type === ColumnType::Relationship
        );

        $stackCount = \count($this->writeStack);

        foreach ($relationships as $relationship) {
            $typedRelAttr = Attribute::fromDocument($relationship);
            $key = $typedRelAttr->key;
            $value = $document->getAttribute($key);
            $rel = RelationshipVO::fromDocument($collection->getId(), $relationship);
            $relatedCollection = $this->db->getCollection($rel->relatedCollection);
            $relationType = $rel->type;
            $twoWay = $rel->twoWay;
            $twoWayKey = $rel->twoWayKey;
            $side = $rel->side;

            if ($stackCount >= Database::RELATION_MAX_DEPTH - 1 && $this->writeStack[$stackCount - 1] !== $relatedCollection->getId()) {
                $document->removeAttribute($key);

                continue;
            }

            $this->writeStack[] = $collection->getId();

            try {
                if (\is_array($value) && ! \array_is_list($value)) {
                    /** @var array<string, mixed> $value */
                    $value = new Document($value);
                    $document->setAttribute($key, $value);
                }

                if (\is_array($value)) {
                    if (
                        ($relationType === RelationType::ManyToOne && $side === RelationSide::Parent) ||
                        ($relationType === RelationType::OneToMany && $side === RelationSide::Child) ||
                        ($relationType === RelationType::OneToOne)
                    ) {
                        throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document, array given.');
                    }

                    foreach ($value as $related) {
                        if ($related instanceof Document) {
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
                        } elseif (\is_string($related)) {
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
                        } else {
                            throw new RelationshipException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                        }
                    }
                    $document->removeAttribute($key);
                } elseif ($value instanceof Document) {
                    if ($relationType === RelationType::OneToOne && ! $twoWay && $side === RelationSide::Child) {
                        throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                    }

                    if (
                        ($relationType === RelationType::OneToMany && $side === RelationSide::Parent) ||
                        ($relationType === RelationType::ManyToOne && $side === RelationSide::Child) ||
                        ($relationType === RelationType::ManyToMany)
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
                } elseif (\is_string($value)) {
                    if ($relationType === RelationType::OneToOne && $twoWay === false && $side === RelationSide::Child) {
                        throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                    }

                    if (
                        ($relationType === RelationType::OneToMany && $side === RelationSide::Parent) ||
                        ($relationType === RelationType::ManyToOne && $side === RelationSide::Child) ||
                        ($relationType === RelationType::ManyToMany)
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
                } elseif ($value === null) {
                    if (
                        !(($relationType === RelationType::OneToMany && $side === RelationSide::Child) ||
                        ($relationType === RelationType::ManyToOne && $side === RelationSide::Parent) ||
                        ($relationType === RelationType::OneToOne && $side === RelationSide::Parent) ||
                        ($relationType === RelationType::OneToOne && $twoWay === true))
                    ) {
                        $document->removeAttribute($key);
                    }
                } else {
                    throw new RelationshipException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                }
            } finally {
                \array_pop($this->writeStack);
            }
        }

        return $document;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DuplicateException If a related document already exists
     * @throws RelationshipException If a relationship constraint is violated
     * @throws RestrictedException If a restricted relationship is violated
     */
    public function afterDocumentUpdate(Document $collection, Document $old, Document $document): Document
    {
        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        /** @var array<Document> $relationships */
        $relationships = \array_filter(
            $attributes,
            fn (Document $attribute): bool => Attribute::fromDocument($attribute)->type === ColumnType::Relationship
        );

        $stackCount = \count($this->writeStack);

        foreach ($relationships as $index => $relationship) {
            $typedRelAttr = Attribute::fromDocument($relationship);
            $key = $typedRelAttr->key;
            $value = $document->getAttribute($key);

            if (\is_array($value) && ! \array_is_list($value)) {
                /** @var array<string, mixed> $value */
                $value = new Document($value);
                $document->setAttribute($key, $value);
            }

            $oldValue = $old->getAttribute($key);
            $rel = RelationshipVO::fromDocument($collection->getId(), $relationship);
            $relatedCollection = $this->db->getCollection($rel->relatedCollection);
            $relationType = $rel->type;
            $twoWay = $rel->twoWay;
            $twoWayKey = $rel->twoWayKey;
            $side = $rel->side;

            if (Operator::isOperator($value)) {
                /** @var Operator $operator */
                $operator = $value;
                if ($operator->isArrayOperation()) {
                    $existingIds = [];
                    if (\is_array($oldValue)) {
                        /** @var array<Document|string> $oldValue */
                        $existingIds = \array_map(fn ($item) => $item instanceof Document ? $item->getId() : (string) $item, $oldValue);
                    }

                    $value = $this->applyRelationshipOperator($operator, $existingIds);
                    $document->setAttribute($key, $value);
                }
            }

            if ($oldValue == $value) {
                if (
                    ($relationType === RelationType::OneToOne
                        || ($relationType === RelationType::ManyToOne && $side === RelationSide::Parent)) &&
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
                    case RelationType::OneToOne:
                        if (! $twoWay) {
                            if ($side === RelationSide::Child) {
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

                        if (\is_string($value)) {
                            $related = $this->db->skipRelationships(
                                fn () => $this->db->getDocument($relatedCollection->getId(), $value, [Query::select(['$id'])])
                            );

                            if ($related->isEmpty()) {
                                $document->setAttribute($key, null);
                            } else {
                                /** @var Document|null $oldValueDoc */
                                $oldValueDoc = $oldValue instanceof Document ? $oldValue : null;
                                if (
                                    $oldValueDoc?->getId() !== $value
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
                            }
                        } elseif ($value instanceof Document) {
                            $related = $this->db->skipRelationships(fn () => $this->db->getDocument($relatedCollection->getId(), $value->getId()));

                            /** @var Document|null $oldValueDoc2 */
                            $oldValueDoc2 = $oldValue instanceof Document ? $oldValue : null;
                            if (
                                $oldValueDoc2?->getId() !== $value->getId()
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
                        } elseif ($value === null) {
                            /** @var Document|null $oldValueDocNull */
                            $oldValueDocNull = $oldValue instanceof Document ? $oldValue : null;
                            if ($oldValueDocNull?->getId() !== null) {
                                $oldRelated = $this->db->skipRelationships(
                                    fn () => $this->db->getDocument($relatedCollection->getId(), $oldValueDocNull->getId())
                                );
                                $this->db->skipRelationships(fn () => $this->db->updateDocument(
                                    $relatedCollection->getId(),
                                    $oldRelated->getId(),
                                    new Document([$twoWayKey => null])
                                ));
                            }
                        } else {
                            throw new RelationshipException('Invalid relationship value. Must be either a document, document ID or null.');
                        }
                        break;
                    case RelationType::OneToMany:
                    case RelationType::ManyToOne:
                        if (
                            ($relationType === RelationType::OneToMany && $side === RelationSide::Parent) ||
                            ($relationType === RelationType::ManyToOne && $side === RelationSide::Child)
                        ) {
                            if (! \is_array($value) || ! \array_is_list($value)) {
                                throw new RelationshipException('Invalid relationship value. Must be either an array of documents or document IDs, '.\gettype($value).' given.');
                            }

                            /** @var array<Document> $oldValueArr */
                            $oldValueArr = \is_array($oldValue) ? $oldValue : [];
                            $oldIds = \array_map(fn (Document $document) => $document->getId(), $oldValueArr);

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
                        } elseif ($value === null) {
                            break;
                        } elseif (is_array($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document, array given.');
                        } elseif (empty($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document.');
                        } else {
                            throw new RelationshipException('Invalid relationship value.');
                        }

                        break;
                    case RelationType::ManyToMany:
                        if ($value === null) {
                            break;
                        }
                        if (! \is_array($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be an array of documents or document IDs.');
                        }

                        /** @var array<Document> $oldValueArrM2M */
                        $oldValueArrM2M = \is_array($oldValue) ? $oldValue : [];
                        $oldIds = \array_map(fn (Document $document) => $document->getId(), $oldValueArrM2M);

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

    /**
     * {@inheritDoc}
     *
     * @throws RestrictedException If a restricted relationship prevents deletion
     */
    public function beforeDocumentDelete(Document $collection, Document $document): Document
    {
        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        /** @var array<Document> $relationships */
        $relationships = \array_filter(
            $attributes,
            fn (Document $attribute): bool => Attribute::fromDocument($attribute)->type === ColumnType::Relationship
        );

        foreach ($relationships as $relationship) {
            $typedRelAttr = Attribute::fromDocument($relationship);
            $key = $typedRelAttr->key;
            $value = $document->getAttribute($key);
            $rel = RelationshipVO::fromDocument($collection->getId(), $relationship);
            $relatedCollection = $this->db->getCollection($rel->relatedCollection);
            $relationType = $rel->type;
            $twoWay = $rel->twoWay;
            $twoWayKey = $rel->twoWayKey;
            $onDelete = $rel->onDelete;
            $side = $rel->side;

            $relationship->setAttribute('collection', $collection->getId());
            $relationship->setAttribute('document', $document->getId());

            switch ($onDelete) {
                case ForeignKeyAction::Restrict:
                    $this->deleteRestrict($relatedCollection, $document, $value, $relationType, $twoWay, $twoWayKey, $side);
                    break;
                case ForeignKeyAction::SetNull:
                    $this->deleteSetNull($collection, $relatedCollection, $document, $value, $relationType, $twoWay, $twoWayKey, $side);
                    break;
                case ForeignKeyAction::Cascade:
                    foreach ($this->deleteStack as $processedRelationship) {
                        /** @var string $existingKey */
                        $existingKey = $processedRelationship['key'];
                        /** @var string $existingCollection */
                        $existingCollection = $processedRelationship['collection'];
                        $existingRel = RelationshipVO::fromDocument($existingCollection, $processedRelationship);
                        $existingRelatedCollection = $existingRel->relatedCollection;
                        $existingTwoWayKey = $existingRel->twoWayKey;
                        $existingSide = $existingRel->side;

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

    /**
     * {@inheritDoc}
     */
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

                    /** @var array<Document> $popAttributes */
                    $popAttributes = $coll->getAttribute('attributes', []);
                    /** @var array<Document> $relationships */
                    $relationships = [];

                    foreach ($popAttributes as $attribute) {
                        $typedPopAttr = Attribute::fromDocument($attribute);
                        if ($typedPopAttr->type === ColumnType::Relationship) {
                            if ($typedPopAttr->key === $skipKey) {
                                continue;
                            }

                            if (! $parentHasExplicitSelects || \array_key_exists($typedPopAttr->key, $sels)) {
                                $relationships[] = $attribute;
                            }
                        }
                    }

                    foreach ($relationships as $relationship) {
                        $typedRelAttr = Attribute::fromDocument($relationship);
                        $key = $typedRelAttr->key;
                        $queries = $sels[$key] ?? [];
                        $relationship->setAttribute('collection', $coll->getId());
                        $isAtMaxDepth = ($currentDepth + 1) >= Database::RELATION_MAX_DEPTH;

                        if ($isAtMaxDepth) {
                            foreach ($docs as $doc) {
                                $doc->removeAttribute($key);
                            }

                            continue;
                        }

                        $relVO = RelationshipVO::fromDocument($coll->getId(), $relationship);

                        $relatedDocs = $this->populateSingleRelationshipBatch(
                            $docs,
                            $relVO,
                            $queries
                        );

                        $twoWay = $relVO->twoWay;
                        $twoWayKey = $relVO->twoWayKey;

                        $hasNestedSelectsForThisRel = isset($sels[$key]);
                        $shouldQueue = ! empty($relatedDocs) &&
                            ($hasNestedSelectsForThisRel || ! $parentHasExplicitSelects);

                        if ($shouldQueue) {
                            $relatedCollectionId = $relVO->relatedCollection;
                            $relatedCollection = $this->db->silent(fn () => $this->db->getCollection($relatedCollectionId));

                            if (! $relatedCollection->isEmpty()) {
                                $relationshipQueries = $hasNestedSelectsForThisRel ? $sels[$key] : [];

                                /** @var array<Document> $relatedCollectionRelationships */
                                $relatedCollectionRelationships = $relatedCollection->getAttribute('attributes', []);
                                /** @var array<Document> $relatedCollectionRelationships */
                                $relatedCollectionRelationships = \array_filter(
                                    $relatedCollectionRelationships,
                                    fn (Document $attr): bool => Attribute::fromDocument($attr)->type === ColumnType::Relationship
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

    /**
     * {@inheritDoc}
     */
    public function processQueries(array $relationships, array $queries): array
    {
        $nestedSelections = [];

        foreach ($queries as $query) {
            if ($query->getMethod() !== Method::Select) {
                continue;
            }

            $values = $query->getValues();
            foreach ($values as $valueIndex => $value) {
                /** @var string $value */
                if (! \str_contains($value, '.')) {
                    continue;
                }

                $nesting = \explode('.', $value);
                $selectedKey = \array_shift($nesting);

                $relationship = \array_values(\array_filter(
                    $relationships,
                    fn (Document $relationship) => Attribute::fromDocument($relationship)->key === $selectedKey,
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

                $relVO = RelationshipVO::fromDocument('', $relationship);

                switch ($relVO->type) {
                    case RelationType::ManyToMany:
                        unset($values[$valueIndex]);
                        break;
                    case RelationType::OneToMany:
                        if ($relVO->side === RelationSide::Parent) {
                            unset($values[$valueIndex]);
                        } else {
                            $values[$valueIndex] = $selectedKey;
                        }
                        break;
                    case RelationType::ManyToOne:
                        if ($relVO->side === RelationSide::Parent) {
                            $values[$valueIndex] = $selectedKey;
                        } else {
                            unset($values[$valueIndex]);
                        }
                        break;
                    case RelationType::OneToOne:
                        $values[$valueIndex] = $selectedKey;
                        break;
                }
            }

            $finalValues = \array_values($values);
            if (empty($finalValues)) {
                $finalValues = ['*'];
            }
            $query->setValues($finalValues);
        }

        return $nestedSelections;
    }

    /**
     * {@inheritDoc}
     *
     * @throws QueryException If a relationship query references an invalid attribute
     */
    public function convertQueries(array $relationships, array $queries, ?Document $collection = null): ?array
    {
        $hasRelationshipQuery = false;
        foreach ($queries as $query) {
            $attr = $query->getAttribute();
            if (\str_contains($attr, '.') || $query->getMethod() === Method::ContainsAll) {
                $hasRelationshipQuery = true;
                break;
            }
        }

        if (! $hasRelationshipQuery) {
            return $queries;
        }

        $collectionId = $collection?->getId() ?? '';

        /** @var array<string, RelationshipVO> $relationshipsByKey */
        $relationshipsByKey = [];
        foreach ($relationships as $relationship) {
            $relVO = RelationshipVO::fromDocument($collectionId, $relationship);
            $relationshipsByKey[$relVO->key] = $relVO;
        }

        $additionalQueries = [];
        $groupedQueries = [];
        $indicesToRemove = [];

        foreach ($queries as $index => $query) {
            if ($query->getMethod() !== Method::ContainsAll) {
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
                /** @var string|int|float|bool|null $value */
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
            if ($query->getMethod() === Method::Select || $query->getMethod() === Method::ContainsAll) {
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
                if ($queryData['method'] === Method::Equal) {
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
            } catch (Exception $e) {
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
        RelationType $relationType,
        bool $twoWay,
        string $twoWayKey,
        RelationSide $side,
    ): string {
        switch ($relationType) {
            case RelationType::OneToOne:
                if ($twoWay) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Child) {
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

        if ($relationType === RelationType::ManyToMany) {
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
        RelationType $relationType,
        bool $twoWay,
        string $twoWayKey,
        RelationSide $side,
    ): void {
        $related = $this->db->skipRelationships(fn () => $this->db->getDocument($relatedCollection->getId(), $relationId));

        if ($related->isEmpty() && $this->checkExist) {
            return;
        }

        switch ($relationType) {
            case RelationType::OneToOne:
                if ($twoWay) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->db->skipRelationships(fn () => $this->db->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->db->skipRelationships(fn () => $this->db->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Child) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->db->skipRelationships(fn () => $this->db->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationType::ManyToMany:
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

    private function getJunctionCollection(Document $collection, Document $relatedCollection, RelationSide $side): string
    {
        return $side === RelationSide::Parent
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
            case OperatorType::ArrayAppend:
                return \array_values(\array_merge($existingIds, $valueIds));

            case OperatorType::ArrayPrepend:
                return \array_values(\array_merge($valueIds, $existingIds));

            case OperatorType::ArrayInsert:
                /** @var int $index */
                $index = $values[0] ?? 0;
                $item = $values[1] ?? null;
                $itemId = $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : null);
                if ($itemId !== null) {
                    \array_splice($existingIds, (int) $index, 0, [$itemId]);
                }

                return \array_values($existingIds);

            case OperatorType::ArrayRemove:
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

            case OperatorType::ArrayUnique:
                return \array_values(\array_unique($existingIds));

            case OperatorType::ArrayIntersect:
                return \array_values(\array_intersect($existingIds, $valueIds));

            case OperatorType::ArrayDiff:
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
    private function populateSingleRelationshipBatch(array $documents, RelationshipVO $relationship, array $queries): array
    {
        return match ($relationship->type) {
            RelationType::OneToOne => $this->populateOneToOneRelationshipsBatch($documents, $relationship, $queries),
            RelationType::OneToMany => $this->populateOneToManyRelationshipsBatch($documents, $relationship, $queries),
            RelationType::ManyToOne => $this->populateManyToOneRelationshipsBatch($documents, $relationship, $queries),
            RelationType::ManyToMany => $this->populateManyToManyRelationshipsBatch($documents, $relationship, $queries),
        };
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateOneToOneRelationshipsBatch(array $documents, RelationshipVO $relationship, array $queries): array
    {
        $key = $relationship->key;
        $relatedCollection = $this->db->getCollection($relationship->relatedCollection);

        $relatedIds = [];
        $documentsByRelatedId = [];

        foreach ($documents as $document) {
            $value = $document->getAttribute($key);
            if ($value !== null) {
                if ($value instanceof Document) {
                    continue;
                }

                /** @var string $relId */
                $relId = $value;
                $relatedIds[] = $relId;
                if (! isset($documentsByRelatedId[$relId])) {
                    $documentsByRelatedId[$relId] = [];
                }
                $documentsByRelatedId[$relId][] = $document;
            }
        }

        if (empty($relatedIds)) {
            return [];
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        /** @var array<string> $uniqueRelatedIds */
        $uniqueRelatedIds = \array_unique($relatedIds);
        $relatedDocuments = [];

        $chunks = \array_chunk($uniqueRelatedIds, Database::RELATION_QUERY_CHUNK_SIZE);

        if (\count($chunks) > 1) {
            $collectionId = $relatedCollection->getId();
            $tasks = \array_map(
                fn (array $chunk) => fn () => $this->db->find($collectionId, [
                    Query::equal('$id', $chunk),
                    Query::limit(PHP_INT_MAX),
                    ...$otherQueries,
                ]),
                $chunks
            );

            /** @var array<array<Document>> $chunkResults */
            $chunkResults = Promise::map($tasks)->await();

            foreach ($chunkResults as $chunkDocs) {
                \array_push($relatedDocuments, ...$chunkDocs);
            }
        } elseif (\count($chunks) === 1) {
            $relatedDocuments = $this->db->find($relatedCollection->getId(), [
                Query::equal('$id', $chunks[0]),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]);
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
                    $document->setAttribute($key, new Document());
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
    private function populateOneToManyRelationshipsBatch(array $documents, RelationshipVO $relationship, array $queries): array
    {
        $key = $relationship->key;
        $twoWay = $relationship->twoWay;
        $twoWayKey = $relationship->twoWayKey;
        $side = $relationship->side;
        $relatedCollection = $this->db->getCollection($relationship->relatedCollection);

        if ($side === RelationSide::Child) {
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
            if ($query->getMethod() === Method::Select) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $relatedDocuments = [];

        $chunks = \array_chunk($parentIds, Database::RELATION_QUERY_CHUNK_SIZE);

        if (\count($chunks) > 1) {
            $collectionId = $relatedCollection->getId();
            $tasks = \array_map(
                fn (array $chunk) => fn () => $this->db->find($collectionId, [
                    Query::equal($twoWayKey, $chunk),
                    Query::limit(PHP_INT_MAX),
                    ...$otherQueries,
                ]),
                $chunks
            );

            /** @var array<array<Document>> $chunkResults */
            $chunkResults = Promise::map($tasks)->await();

            foreach ($chunkResults as $chunkDocs) {
                \array_push($relatedDocuments, ...$chunkDocs);
            }
        } elseif (\count($chunks) === 1) {
            $relatedDocuments = $this->db->find($relatedCollection->getId(), [
                Query::equal($twoWayKey, $chunks[0]),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]);
        }

        $relatedByParentId = [];
        foreach ($relatedDocuments as $related) {
            $parentId = $related->getAttribute($twoWayKey);
            if ($parentId instanceof Document) {
                $parentKey = $parentId->getId();
            } elseif (\is_string($parentId)) {
                $parentKey = $parentId;
            } else {
                continue;
            }

            if (! isset($relatedByParentId[$parentKey])) {
                $relatedByParentId[$parentKey] = [];
            }
            $relatedByParentId[$parentKey][] = $related;
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
    private function populateManyToOneRelationshipsBatch(array $documents, RelationshipVO $relationship, array $queries): array
    {
        $key = $relationship->key;
        $twoWay = $relationship->twoWay;
        $twoWayKey = $relationship->twoWayKey;
        $side = $relationship->side;
        $relatedCollection = $this->db->getCollection($relationship->relatedCollection);

        if ($side === RelationSide::Parent) {
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
            if ($query->getMethod() === Method::Select) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $relatedDocuments = [];

        $chunks = \array_chunk($childIds, Database::RELATION_QUERY_CHUNK_SIZE);

        if (\count($chunks) > 1) {
            $collectionId = $relatedCollection->getId();
            $tasks = \array_map(
                fn (array $chunk) => fn () => $this->db->find($collectionId, [
                    Query::equal($twoWayKey, $chunk),
                    Query::limit(PHP_INT_MAX),
                    ...$otherQueries,
                ]),
                $chunks
            );

            /** @var array<array<Document>> $chunkResults */
            $chunkResults = Promise::map($tasks)->await();

            foreach ($chunkResults as $chunkDocs) {
                \array_push($relatedDocuments, ...$chunkDocs);
            }
        } elseif (\count($chunks) === 1) {
            $relatedDocuments = $this->db->find($relatedCollection->getId(), [
                Query::equal($twoWayKey, $chunks[0]),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]);
        }

        $relatedByChildId = [];
        foreach ($relatedDocuments as $related) {
            $childId = $related->getAttribute($twoWayKey);
            if ($childId instanceof Document) {
                $childKey = $childId->getId();
            } elseif (\is_string($childId)) {
                $childKey = $childId;
            } else {
                continue;
            }

            if (! isset($relatedByChildId[$childKey])) {
                $relatedByChildId[$childKey] = [];
            }
            $relatedByChildId[$childKey][] = $related;
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
    private function populateManyToManyRelationshipsBatch(array $documents, RelationshipVO $relationship, array $queries): array
    {
        $key = $relationship->key;
        $twoWay = $relationship->twoWay;
        $twoWayKey = $relationship->twoWayKey;
        $side = $relationship->side;
        $relatedCollection = $this->db->getCollection($relationship->relatedCollection);
        $collection = $this->db->getCollection($relationship->collection);

        if (! $twoWay && $side === RelationSide::Child) {
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

        $junctionChunks = \array_chunk($documentIds, Database::RELATION_QUERY_CHUNK_SIZE);

        if (\count($junctionChunks) > 1) {
            $tasks = \array_map(
                fn (array $chunk) => fn () => $this->db->skipRelationships(fn () => $this->db->find($junction, [
                    Query::equal($twoWayKey, $chunk),
                    Query::limit(PHP_INT_MAX),
                ])),
                $junctionChunks
            );

            /** @var array<array<Document>> $junctionChunkResults */
            $junctionChunkResults = Promise::map($tasks)->await();

            foreach ($junctionChunkResults as $chunkJunctions) {
                \array_push($junctions, ...$chunkJunctions);
            }
        } elseif (\count($junctionChunks) === 1) {
            $junctions = $this->db->skipRelationships(fn () => $this->db->find($junction, [
                Query::equal($twoWayKey, $junctionChunks[0]),
                Query::limit(PHP_INT_MAX),
            ]));
        }

        /** @var array<string> $relatedIds */
        $relatedIds = [];
        /** @var array<string, array<string>> $junctionsByDocumentId */
        $junctionsByDocumentId = [];

        foreach ($junctions as $junctionDoc) {
            $documentId = $junctionDoc->getAttribute($twoWayKey);
            $relatedId = $junctionDoc->getAttribute($key);

            if ($documentId !== null && $relatedId !== null) {
                $documentIdStr = $documentId instanceof Document ? $documentId->getId() : (\is_string($documentId) ? $documentId : null);
                $relatedIdStr = $relatedId instanceof Document ? $relatedId->getId() : (\is_string($relatedId) ? $relatedId : null);
                if ($documentIdStr === null || $relatedIdStr === null) {
                    continue;
                }
                if (! isset($junctionsByDocumentId[$documentIdStr])) {
                    $junctionsByDocumentId[$documentIdStr] = [];
                }
                $junctionsByDocumentId[$documentIdStr][] = $relatedIdStr;
                $relatedIds[] = $relatedIdStr;
            }
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select) {
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

            $relatedChunks = \array_chunk($uniqueRelatedIds, Database::RELATION_QUERY_CHUNK_SIZE);

            if (\count($relatedChunks) > 1) {
                $relatedCollectionId = $relatedCollection->getId();
                $tasks = \array_map(
                    fn (array $chunk) => fn () => $this->db->find($relatedCollectionId, [
                        Query::equal('$id', $chunk),
                        Query::limit(PHP_INT_MAX),
                        ...$otherQueries,
                    ]),
                    $relatedChunks
                );

                /** @var array<array<Document>> $relatedChunkResults */
                $relatedChunkResults = Promise::map($tasks)->await();

                foreach ($relatedChunkResults as $chunkDocs) {
                    \array_push($foundRelated, ...$chunkDocs);
                }
            } elseif (\count($relatedChunks) === 1) {
                $foundRelated = $this->db->find($relatedCollection->getId(), [
                    Query::equal('$id', $relatedChunks[0]),
                    Query::limit(PHP_INT_MAX),
                    ...$otherQueries,
                ]);
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
        RelationType $relationType,
        bool $twoWay,
        string $twoWayKey,
        RelationSide $side
    ): void {
        if ($value instanceof Document && $value->isEmpty()) {
            $value = null;
        }

        if (
            ! empty($value)
            && $relationType !== RelationType::ManyToOne
            && $side === RelationSide::Parent
        ) {
            throw new RestrictedException('Cannot delete document because it has at least one related document.');
        }

        if (
            $relationType === RelationType::OneToOne
            && $side === RelationSide::Child
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
            $relationType === RelationType::ManyToOne
            && $side === RelationSide::Child
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

    private function deleteSetNull(Document $collection, Document $relatedCollection, Document $document, mixed $value, RelationType $relationType, bool $twoWay, string $twoWayKey, RelationSide $side): void
    {
        switch ($relationType) {
            case RelationType::OneToOne:
                if (! $twoWay && $side === RelationSide::Parent) {
                    break;
                }

                $this->db->getAuthorization()->skip(function () use ($document, $value, $relatedCollection, $twoWay, $twoWayKey) {
                    if (! $twoWay) {
                        $related = $this->db->findOne($relatedCollection->getId(), [
                            Query::select(['$id']),
                            Query::equal($twoWayKey, [$document->getId()]),
                        ]);
                    } else {
                        if (empty($value)) {
                            return;
                        }
                        /** @var Document $value */
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

            case RelationType::OneToMany:
                if ($side === RelationSide::Child) {
                    break;
                }
                /** @var array<Document> $value */
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

            case RelationType::ManyToOne:
                if ($side === RelationSide::Parent) {
                    break;
                }

                if (! $twoWay) {
                    $value = $this->db->find($relatedCollection->getId(), [
                        Query::select(['$id']),
                        Query::equal($twoWayKey, [$document->getId()]),
                        Query::limit(PHP_INT_MAX),
                    ]);
                }

                /** @var array<Document> $value */
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

            case RelationType::ManyToMany:
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

    private function deleteCascade(Document $collection, Document $relatedCollection, Document $document, string $key, mixed $value, RelationType $relationType, string $twoWayKey, RelationSide $side, Document $relationship): void
    {
        switch ($relationType) {
            case RelationType::OneToOne:
                if ($value !== null) {
                    $this->deleteStack[] = $relationship;

                    $deleteId = ($value instanceof Document) ? $value->getId() : (\is_string($value) ? $value : null);
                    if ($deleteId !== null) {
                        $this->db->deleteDocument(
                            $relatedCollection->getId(),
                            $deleteId
                        );
                    }

                    \array_pop($this->deleteStack);
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Child) {
                    break;
                }

                $this->deleteStack[] = $relationship;

                /** @var array<Document> $value */
                foreach ($value as $relation) {
                    $this->db->deleteDocument(
                        $relatedCollection->getId(),
                        $relation->getId()
                    );
                }

                \array_pop($this->deleteStack);

                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Parent) {
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
            case RelationType::ManyToMany:
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $junctions = $this->db->skipRelationships(fn () => $this->db->find($junction, [
                    Query::select(['$id', $key]),
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX),
                ]));

                $this->deleteStack[] = $relationship;

                foreach ($junctions as $document) {
                    if ($side === RelationSide::Parent) {
                        $relatedAttr = $document->getAttribute($key);
                        $relatedId = $relatedAttr instanceof Document ? $relatedAttr->getId() : (\is_string($relatedAttr) ? $relatedAttr : null);
                        if ($relatedId !== null) {
                            $this->db->deleteDocument(
                                $relatedCollection->getId(),
                                $relatedId
                            );
                        }
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

        /** @var array<string> $allMatchingIds */
        $allMatchingIds = [];
        foreach ($pathGroups as $path => $queryGroup) {
            $pathParts = \explode('.', $path);
            $currentCollection = $startCollection;
            /** @var list<array{key: string, fromCollection: string, toCollection: string, relationType: RelationType, side: RelationSide, twoWayKey: string}> $relationshipChain */
            $relationshipChain = [];

            foreach ($pathParts as $relationshipKey) {
                $collectionDoc = $this->db->silent(fn () => $this->db->getCollection($currentCollection));
                /** @var array<Document|array<string, mixed>> $attributes */
                $attributes = $collectionDoc->getAttribute('attributes', []);
                $relationships = \array_filter(
                    $attributes,
                    function (mixed $attr): bool {
                        if ($attr instanceof Document) {
                            $type = $attr->getAttribute('type', '');
                        } else {
                            $type = $attr['type'] ?? '';
                        }
                        return \is_string($type) && ColumnType::tryFrom($type) === ColumnType::Relationship;
                    }
                );

                /** @var array<string, mixed>|null $relationship */
                $relationship = null;
                foreach ($relationships as $rel) {
                    /** @var array<string, mixed> $rel */
                    if ($rel['key'] === $relationshipKey) {
                        $relationship = $rel;
                        break;
                    }
                }

                if (! $relationship) {
                    return null;
                }

                /** @var Document $relationship */
                $nestedRel = RelationshipVO::fromDocument($currentCollection, $relationship);
                $relationshipChain[] = [
                    'key' => $relationshipKey,
                    'fromCollection' => $currentCollection,
                    'toCollection' => $nestedRel->relatedCollection,
                    'relationType' => $nestedRel->type,
                    'side' => $nestedRel->side,
                    'twoWayKey' => $nestedRel->twoWayKey,
                ];

                $currentCollection = $nestedRel->relatedCollection;
            }

            $leafQueries = [];
            foreach ($queryGroup as $q) {
                $leafQueries[] = new Query($q['method'], $q['attribute'], $q['values']);
            }

            /** @var array<Document> $matchingDocs */
            $matchingDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                $currentCollection,
                \array_merge($leafQueries, [
                    Query::select(['$id']),
                    Query::limit(PHP_INT_MAX),
                ])
            )));

            /** @var array<string> $matchingIds */
            $matchingIds = \array_map(fn (Document $doc) => $doc->getId(), $matchingDocs);

            if (empty($matchingIds)) {
                return null;
            }

            for ($i = \count($relationshipChain) - 1; $i >= 0; $i--) {
                $link = $relationshipChain[$i];
                $relationType = $link['relationType'];
                $side = $link['side'];
                $linkKey = $link['key'];
                $linkFromCollection = $link['fromCollection'];
                $linkToCollection = $link['toCollection'];
                $linkTwoWayKey = $link['twoWayKey'];

                $needsReverseLookup = (
                    ($relationType === RelationType::OneToMany && $side === RelationSide::Parent) ||
                    ($relationType === RelationType::ManyToOne && $side === RelationSide::Child) ||
                    ($relationType === RelationType::ManyToMany)
                );

                if ($needsReverseLookup) {
                    if ($relationType === RelationType::ManyToMany) {
                        $fromCollectionDoc = $this->db->silent(fn () => $this->db->getCollection($linkFromCollection));
                        $toCollectionDoc = $this->db->silent(fn () => $this->db->getCollection($linkToCollection));
                        $junction = $this->getJunctionCollection($fromCollectionDoc, $toCollectionDoc, $side);

                        /** @var array<Document> $junctionDocs */
                        $junctionDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find($junction, [
                            Query::equal($linkKey, $matchingIds),
                            Query::limit(PHP_INT_MAX),
                        ])));

                        /** @var array<string> $parentIds */
                        $parentIds = [];
                        foreach ($junctionDocs as $jDoc) {
                            $pIdRaw = $jDoc->getAttribute($linkTwoWayKey);
                            $pId = $pIdRaw instanceof Document ? $pIdRaw->getId() : (\is_string($pIdRaw) ? $pIdRaw : null);
                            if ($pId && ! \in_array($pId, $parentIds)) {
                                $parentIds[] = $pId;
                            }
                        }
                    } else {
                        /** @var array<Document> $childDocs */
                        $childDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                            $linkToCollection,
                            [
                                Query::equal('$id', $matchingIds),
                                Query::select(['$id', $linkTwoWayKey]),
                                Query::limit(PHP_INT_MAX),
                            ]
                        )));

                        /** @var array<string> $parentIds */
                        $parentIds = [];
                        foreach ($childDocs as $doc) {
                            $parentValue = $doc->getAttribute($linkTwoWayKey);
                            if (\is_array($parentValue)) {
                                foreach ($parentValue as $pId) {
                                    if ($pId instanceof Document) {
                                        $pId = $pId->getId();
                                    }
                                    if (\is_string($pId) && $pId && ! \in_array($pId, $parentIds)) {
                                        $parentIds[] = $pId;
                                    }
                                }
                            } else {
                                if ($parentValue instanceof Document) {
                                    $parentValue = $parentValue->getId();
                                }
                                if (\is_string($parentValue) && $parentValue && ! \in_array($parentValue, $parentIds)) {
                                    $parentIds[] = $parentValue;
                                }
                            }
                        }
                    }
                    $matchingIds = $parentIds;
                } else {
                    /** @var array<Document> $parentDocs */
                    $parentDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                        $linkFromCollection,
                        [
                            Query::equal($linkKey, $matchingIds),
                            Query::select(['$id']),
                            Query::limit(PHP_INT_MAX),
                        ]
                    )));
                    $matchingIds = \array_map(fn (Document $doc) => $doc->getId(), $parentDocs);
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
        RelationshipVO $relationship,
        array $relatedQueries,
        ?Document $collection = null,
    ): ?array {
        $relatedCollection = $relationship->relatedCollection;
        $relationType = $relationship->type;
        $side = $relationship->side;
        $twoWayKey = $relationship->twoWayKey;
        $relationshipKey = $relationship->key;

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
            ($relationType === RelationType::OneToMany && $side === RelationSide::Parent) ||
            ($relationType === RelationType::ManyToOne && $side === RelationSide::Child) ||
            ($relationType === RelationType::ManyToMany)
        );

        if ($relationType === RelationType::ManyToMany && $needsParentResolution && $collection !== null) {
            /** @var array<Document> $matchingDocs */
            $matchingDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                $relatedCollection,
                \array_merge($relatedQueries, [
                    Query::select(['$id']),
                    Query::limit(PHP_INT_MAX),
                ])
            )));

            $matchingIds = \array_map(fn (Document $doc) => $doc->getId(), $matchingDocs);

            if (empty($matchingIds)) {
                return null;
            }

            /** @var Document $relatedCollectionDoc */
            $relatedCollectionDoc = $this->db->silent(fn () => $this->db->getCollection($relatedCollection));
            $junction = $this->getJunctionCollection($collection, $relatedCollectionDoc, $side);

            /** @var array<Document> $junctionDocs */
            $junctionDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find($junction, [
                Query::equal($relationshipKey, $matchingIds),
                Query::limit(PHP_INT_MAX),
            ])));

            /** @var array<string> $parentIds */
            $parentIds = [];
            foreach ($junctionDocs as $jDoc) {
                $pIdRaw = $jDoc->getAttribute($twoWayKey);
                $pId = $pIdRaw instanceof Document ? $pIdRaw->getId() : (\is_string($pIdRaw) ? $pIdRaw : null);
                if ($pId && ! \in_array($pId, $parentIds)) {
                    $parentIds[] = $pId;
                }
            }

            return empty($parentIds) ? null : ['attribute' => '$id', 'ids' => $parentIds];
        } elseif ($needsParentResolution) {
            /** @var array<Document> $matchingDocs */
            $matchingDocs = $this->db->silent(fn () => $this->db->find(
                $relatedCollection,
                \array_merge($relatedQueries, [
                    Query::limit(PHP_INT_MAX),
                ])
            ));

            /** @var array<string> $parentIds */
            $parentIds = [];

            foreach ($matchingDocs as $doc) {
                $parentId = $doc->getAttribute($twoWayKey);

                if (\is_array($parentId)) {
                    foreach ($parentId as $id) {
                        if ($id instanceof Document) {
                            $id = $id->getId();
                        }
                        if (\is_string($id) && $id && ! \in_array($id, $parentIds)) {
                            $parentIds[] = $id;
                        }
                    }
                } else {
                    if ($parentId instanceof Document) {
                        $parentId = $parentId->getId();
                    }
                    if (\is_string($parentId) && $parentId && ! \in_array($parentId, $parentIds)) {
                        $parentIds[] = $parentId;
                    }
                }
            }

            return empty($parentIds) ? null : ['attribute' => '$id', 'ids' => $parentIds];
        } else {
            /** @var array<Document> $matchingDocs */
            $matchingDocs = $this->db->silent(fn () => $this->db->skipRelationships(fn () => $this->db->find(
                $relatedCollection,
                \array_merge($relatedQueries, [
                    Query::select(['$id']),
                    Query::limit(PHP_INT_MAX),
                ])
            )));

            /** @var array<string> $matchingIds */
            $matchingIds = \array_map(fn (Document $doc) => $doc->getId(), $matchingDocs);

            return empty($matchingIds) ? null : ['attribute' => $relationshipKey, 'ids' => $matchingIds];
        }
    }
}
