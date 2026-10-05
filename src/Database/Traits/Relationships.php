<?php

namespace Utopia\Database\Traits;

use Throwable;
use Utopia\Console;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\SetType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;

/**
 * Provides relationship attribute management including creation, update, deletion, and traversal control.
 */
trait Relationships
{
    /**
     * Skip relationships for all the calls inside the callback
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function skipRelationships(callable $callback): mixed
    {
        if ($this->relationshipHook === null) {
            return $callback();
        }

        return $this->relationshipHook->withEnabled(false, $callback);
    }

    /**
     * Skip relationship existence checks for all calls inside the callback.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function skipRelationshipsExistCheck(callable $callback): mixed
    {
        if ($this->relationshipHook === null) {
            return $callback();
        }

        return $this->relationshipHook->withCheckExist(false, $callback);
    }

    /**
     * Cleanup a relationship on failure
     *
     * @param  string  $collectionId  The collection ID
     * @param  string  $relatedCollectionId  The related collection ID
     * @param  RelationType  $type  The relationship type
     * @param  bool  $twoWay  Whether the relationship is two-way
     * @param  string  $key  The relationship key
     * @param  string  $twoWayKey  The two-way relationship key
     * @param  RelationSide  $side  The relationship side
     * @param  int  $maxAttempts  Maximum retry attempts
     *
     * @throws DatabaseException If cleanup fails after all retries
     */
    private function cleanupRelationship(
        string $collectionId,
        string $relatedCollectionId,
        RelationType $type,
        bool $twoWay,
        string $key,
        string $twoWayKey,
        RelationSide $side = RelationSide::Parent,
        int $maxAttempts = 3
    ): void {
        $adapter = $this->adapter;
        if (! $adapter->hasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }

        $relationshipModel = new Relationship(
            collection: $collectionId,
            relatedCollection: $relatedCollectionId,
            type: $type,
            twoWay: $twoWay,
            key: $key,
            twoWayKey: $twoWayKey,
            side: $side,
        );
        $this->cleanup(
            fn () => $adapter->deleteRelationship($relationshipModel),
            'relationship',
            $key,
            $maxAttempts
        );
    }

    /**
     * Create a relationship attribute between two collections.
     *
     * @param  Relationship  $relationship  The relationship definition
     * @return bool True if the relationship was created successfully
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     */
    public function createRelationship(
        Relationship $relationship
    ): bool {
        if (! $this->adapter->hasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }

        $collection = $this->silent(fn () => $this->getCollection($relationship->getSourceCollection()));
        $relatedCollection = $this->silent(fn () => $this->getCollection($relationship->getRelatedCollection()));

        /** @var Document $collection */
        /** @var Document $relatedCollection */
        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }
        if ($relatedCollection->isEmpty()) {
            throw new NotFoundException('Related collection not found');
        }

        $type = $relationship->getType();
        $twoWay = $relationship->isTwoWay();
        $id = ! empty($relationship->getKey()) ? $relationship->getKey() : $this->adapter->filter($relatedCollection->getId());
        $twoWayKey = ! empty($relationship->getTwoWayKey()) ? $relationship->getTwoWayKey() : $this->adapter->filter($collection->getId());
        $onDelete = $relationship->getOnDelete();

        /** @var array<Attribute> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        $collectionId = $collection->getId();
        $relatedCollectionId = $relatedCollection->getId();
        foreach ($attributes as $attribute) {
            if (\strtolower($attribute->getKey()) === \strtolower($id)) {
                throw new DuplicateException('Attribute already exists');
            }

            if ($attribute->getType() === ColumnType::Relationship) {
                $existingRelationship = Relationship::fromArray(['collection' => $collectionId] + $attribute->getArrayCopy());
                if (
                    \strtolower($existingRelationship->getTwoWayKey()) === \strtolower($twoWayKey)
                    && $existingRelationship->getRelatedCollection() === $relatedCollectionId
                ) {
                    throw new DuplicateException('Related attribute already exists');
                }
            }
        }

        $relationship = Attribute::relationship(
            key: $id,
            options: [
                'relatedCollection' => $relatedCollection->getId(),
                'relationType' => $type->value,
                'twoWay' => $twoWay,
                'twoWayKey' => $twoWayKey,
                'onDelete' => $onDelete->value,
                'side' => RelationSide::Parent->value,
            ],
        );

        $twoWayRelationship = Attribute::relationship(
            key: $twoWayKey,
            options: [
                'relatedCollection' => $collection->getId(),
                'relationType' => $type->value,
                'twoWay' => $twoWay,
                'twoWayKey' => $id,
                'onDelete' => $onDelete->value,
                'side' => RelationSide::Child->value,
            ],
        );

        $this->checkAttribute($collection, $relationship);
        $this->checkAttribute($relatedCollection, $twoWayRelationship);

        /** @var ?string $junctionCollection */
        $junctionCollection = null;
        if ($type === RelationType::ManyToMany) {
            $junctionCollection = '_'.$collection->getSequence().'_'.$relatedCollection->getSequence();
            $junctionAttributes = [
                Attribute::string(key: $id, required: true),
                Attribute::string(key: $twoWayKey, required: true),
            ];
            $junctionIndexes = [
                Index::key(key: '_index_'.$id, attributes: [$id]),
                Index::key(key: '_index_'.$twoWayKey, attributes: [$twoWayKey]),
            ];
            try {
                $this->silent(fn () => $this->createCollection(new Collection(id: $junctionCollection, attributes: $junctionAttributes, indexes: $junctionIndexes)));
            } catch (DuplicateException) {
                // Junction metadata already exists from a prior partial failure.
                // Ensure the physical schema also exists.
                try {
                    $this->adapter->createCollection($junctionCollection, $junctionAttributes, $junctionIndexes);
                } catch (DuplicateException) {
                    // Schema already exists — ignore
                }
            }
        }

        $created = false;

        $adapterRelationship = new Relationship(
            collection: $collection->getId(),
            relatedCollection: $relatedCollection->getId(),
            type: $type,
            twoWay: $twoWay,
            key: $id,
            twoWayKey: $twoWayKey,
            onDelete: $onDelete,
            side: RelationSide::Parent,
        );

        try {
            $created = $this->adapter->createRelationship($adapterRelationship);

            if (! $created) {
                if ($junctionCollection !== null) {
                    try {
                        $this->silent(fn () => $this->cleanupCollection($junctionCollection));
                    } catch (Throwable $error) {
                        Console::error("Failed to cleanup junction collection '{$junctionCollection}': ".$error->getMessage());
                    }
                }
                throw new DatabaseException('Failed to create relationship');
            }
        } catch (DuplicateException) {
            // Metadata checks (above) already verified relationship is absent
            // from metadata. A DuplicateException from the adapter means the
            // relationship exists only in physical schema — an orphan from a
            // prior partial failure. Skip creation and proceed to metadata update.
        }

        $collection->setAttribute('attributes', $relationship, SetType::Append);
        $relatedCollection->setAttribute('attributes', $twoWayRelationship, SetType::Append);

        $this->silent(function () use ($collection, $relatedCollection, $type, $twoWay, $id, $twoWayKey, $junctionCollection, $created) {
            $committedFailure = null;
            try {
                $this->withRetries(function () use ($collection, $relatedCollection) {
                    $this->withTransaction(function () use ($collection, $relatedCollection) {
                        $this->updateDocument(self::METADATA, $collection->getId(), $collection);
                        $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);
                    });
                });
            } catch (Throwable $error) {
                if (! $this->mayHaveCommitted($error)) {
                    $this->rollbackAttributeMetadata($collection, [$id]);
                    $this->rollbackAttributeMetadata($relatedCollection, [$twoWayKey]);

                    if ($created) {
                        try {
                            $this->cleanupRelationship(
                                $collection->getId(),
                                $relatedCollection->getId(),
                                $type,
                                $twoWay,
                                $id,
                                $twoWayKey,
                                RelationSide::Parent
                            );
                        } catch (Throwable $cleanupError) {
                            Console::error("Failed to cleanup relationship '{$id}': ".$cleanupError->getMessage());
                        }

                        if ($junctionCollection !== null) {
                            try {
                                $this->cleanupCollection($junctionCollection);
                            } catch (Throwable $cleanupError) {
                                Console::error("Failed to cleanup junction collection '{$junctionCollection}': ".$cleanupError->getMessage());
                            }
                        }
                    }

                    throw new DatabaseException('Failed to create relationship: '.$error->getMessage(), previous: $error);
                }

                $committedFailure = $error;
            }

            $indexKey = '_index_'.$id;
            $twoWayIndexKey = '_index_'.$twoWayKey;
            $indexes = match ($type) {
                RelationType::OneToOne => $twoWay
                    ? [
                        [$collection->getId(), Index::unique(key: $indexKey, attributes: [$id])],
                        [$relatedCollection->getId(), Index::unique(key: $twoWayIndexKey, attributes: [$twoWayKey])],
                    ]
                    : [[$collection->getId(), Index::unique(key: $indexKey, attributes: [$id])]],
                RelationType::OneToMany => [[$relatedCollection->getId(), Index::key(key: $twoWayIndexKey, attributes: [$twoWayKey])]],
                RelationType::ManyToOne => [[$collection->getId(), Index::key(key: $indexKey, attributes: [$id])]],
                RelationType::ManyToMany => [],
            };
            $indexesCreated = [];

            try {
                foreach ($indexes as [$indexCollection, $index]) {
                    try {
                        $this->createIndex($indexCollection, $index);
                    } catch (Throwable $error) {
                        if (! $this->mayHaveCommitted($error)) {
                            throw $error;
                        }

                        $committedFailure ??= $error;
                    }
                    $indexesCreated[] = ['collection' => $indexCollection, 'index' => $index->getKey()];
                }
            } catch (Throwable $error) {
                foreach ($indexesCreated as $indexInfo) {
                    try {
                        $this->deleteIndex($indexInfo['collection'], $indexInfo['index']);
                    } catch (Throwable $cleanupError) {
                        Console::error("Failed to cleanup index '{$indexInfo['index']}': ".$cleanupError->getMessage());
                    }
                }

                $definitionsRemoved = true;
                try {
                    $this->withTransaction(function () use ($collection, $relatedCollection, $id, $twoWayKey) {
                        /** @var array<Attribute> $attributes */
                        $attributes = $collection->getAttribute('attributes', []);
                        $collection->setAttribute('attributes', array_filter($attributes, fn (Attribute $existing) => $existing->getId() !== $id));
                        $this->updateDocument(self::METADATA, $collection->getId(), $collection);

                        /** @var array<Attribute> $relatedAttributes */
                        $relatedAttributes = $relatedCollection->getAttribute('attributes', []);
                        $relatedCollection->setAttribute('attributes', array_filter($relatedAttributes, fn (Attribute $existing) => $existing->getId() !== $twoWayKey));
                        $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);
                    });
                } catch (Throwable $cleanupError) {
                    $definitionsRemoved = $this->failedAfterCommit($cleanupError);
                    Console::error("Failed to cleanup metadata for relationship '{$id}': ".$cleanupError->getMessage());
                }

                if ($definitionsRemoved) {
                    try {
                        $this->cleanupRelationship(
                            $collection->getId(),
                            $relatedCollection->getId(),
                            $type,
                            $twoWay,
                            $id,
                            $twoWayKey,
                            RelationSide::Parent
                        );
                    } catch (Throwable $cleanupError) {
                        Console::error("Failed to cleanup relationship '{$id}': ".$cleanupError->getMessage());
                    }

                    if ($junctionCollection !== null) {
                        try {
                            $this->cleanupCollection($junctionCollection);
                        } catch (Throwable $cleanupError) {
                            Console::error("Failed to cleanup junction collection '{$junctionCollection}': ".$cleanupError->getMessage());
                        }
                    }
                }

                throw new DatabaseException('Failed to create relationship indexes: '.$error->getMessage(), previous: $error);
            }

            if ($committedFailure !== null) {
                throw $committedFailure;
            }
        });

        $this->triggerHooks(
            Event::AttributeCreate,
            (clone $relationship)->setAttribute(Document::COLLECTION, $collection->getId()),
        );

        return true;
    }

    /**
     * Update a relationship attribute's keys, two-way status, or on-delete behavior.
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $id  The relationship attribute identifier
     * @param  string|null  $newKey  New key for the relationship attribute
     * @param  string|null  $newTwoWayKey  New key for the two-way relationship attribute
     * @param  bool|null  $twoWay  Whether the relationship should be two-way
     * @param  ForeignKeyAction|null  $onDelete  Action to take on related document deletion
     * @return bool True if the relationship was updated successfully
     *
     * @throws ConflictException
     * @throws DatabaseException
     */
    public function updateRelationship(
        string $collection,
        string $id,
        ?string $newKey = null,
        ?string $newTwoWayKey = null,
        ?bool $twoWay = null,
        ?ForeignKeyAction $onDelete = null
    ): bool {
        if (! $this->adapter->hasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }

        if (
            $newKey === null
            && $newTwoWayKey === null
            && $twoWay === null
            && $onDelete === null
        ) {
            return true;
        }

        $collection = $this->getCollection($collection);
        /** @var array<Attribute> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        if (
            $newKey !== null
            && \in_array($newKey, \array_map(fn (Attribute $attribute) => $attribute->getKey(), $attributes), true)
        ) {
            throw new DuplicateException('Relationship already exists');
        }

        $attributeIndex = array_search($id, array_map(fn (Attribute $attribute) => $attribute->getKey(), $attributes), true);

        if ($attributeIndex === false) {
            throw new NotFoundException('Relationship not found');
        }

        $attribute = $attributes[$attributeIndex];
        $oldRelationship = Relationship::fromArray(['collection' => $collection->getId()] + $attribute->getArrayCopy());

        $relatedCollectionId = $oldRelationship->getRelatedCollection();
        $relatedCollection = $this->getCollection($relatedCollectionId);

        $oldTwoWayKey = $oldRelationship->getTwoWayKey();
        $altering = ($newKey !== null && $newKey !== $id)
            || ($newTwoWayKey !== null && $newTwoWayKey !== $oldTwoWayKey);

        /** @var array<Attribute> $relatedCollectionAttributes */
        $relatedCollectionAttributes = $relatedCollection->getAttribute('attributes', []);
        if (
            $newTwoWayKey !== null
            && \in_array($newTwoWayKey, \array_map(fn (Attribute $attribute) => $attribute->getKey(), $relatedCollectionAttributes), true)
        ) {
            throw new DuplicateException('Related attribute already exists');
        }

        $actualNewKey = $newKey ?? $id;
        $actualNewTwoWayKey = $newTwoWayKey ?? $oldTwoWayKey;
        $actualTwoWay = $twoWay ?? $oldRelationship->isTwoWay();
        $actualOnDelete = $onDelete ?? $oldRelationship->getOnDelete();

        $adapterUpdated = false;
        if ($altering) {
            try {
                $current = new Relationship(
                    collection: $collection->getId(),
                    relatedCollection: $relatedCollection->getId(),
                    type: $oldRelationship->getType(),
                    twoWay: $actualTwoWay,
                    key: $id,
                    twoWayKey: $oldTwoWayKey,
                    onDelete: $actualOnDelete,
                    side: $oldRelationship->getSide(),
                );
                $adapterUpdated = $this->adapter->updateRelationship(
                    $current,
                    $actualNewKey,
                    $actualNewTwoWayKey
                );

                if (! $adapterUpdated) {
                    throw new DatabaseException('Failed to update relationship');
                }
            } catch (Throwable $error) {
                // Check if the rename already happened in schema (orphan from prior
                // partial failure where adapter succeeded but metadata+rollback failed).
                // If the new column names already exist, the prior rename completed.
                if ($this->adapter->hasFeature(Feature\SchemaAttributes::class)) {
                    $schemaAttributes = $this->getSchemaAttributes($collection->getId());
                    $filteredNewKey = $this->adapter->filter($actualNewKey);
                    $newKeyExists = false;
                    foreach ($schemaAttributes as $schemaAttribute) {
                        if (\strtolower($schemaAttribute->getId()) === \strtolower($filteredNewKey)) {
                            $newKeyExists = true;
                            break;
                        }
                    }
                    if ($newKeyExists) {
                        $adapterUpdated = true;
                    } else {
                        throw new DatabaseException("Failed to update relationship '{$id}': ".$error->getMessage(), previous: $error);
                    }
                } else {
                    throw new DatabaseException("Failed to update relationship '{$id}': ".$error->getMessage(), previous: $error);
                }
            }
        }

        $updatedAttributes = [];

        try {
            $updatedAttributes[] = [$collection->getId(), $this->updateAttributeMeta($collection->getId(), $id, function ($attribute) use ($actualNewKey, $actualNewTwoWayKey, $actualTwoWay, $actualOnDelete, $relatedCollection, $oldRelationship) {
                $attribute->setAttribute(Document::ID, $actualNewKey);
                $attribute->setAttribute('key', $actualNewKey);
                $attribute->setAttribute('options', [
                    'relatedCollection' => $relatedCollection->getId(),
                    'relationType' => $oldRelationship->getType()->value,
                    'twoWay' => $actualTwoWay,
                    'twoWayKey' => $actualNewTwoWayKey,
                    'onDelete' => $actualOnDelete->value,
                    'side' => $oldRelationship->getSide()->value,
                ]);
            }, triggerEvent: false)];

            $updatedAttributes[] = [$relatedCollection->getId(), $this->updateAttributeMeta($relatedCollection->getId(), $oldTwoWayKey, function (Document $twoWayAttribute) use ($actualNewKey, $actualNewTwoWayKey, $actualTwoWay, $actualOnDelete) {
                /** @var array<string, mixed> $options */
                $options = $twoWayAttribute->getAttribute('options', []);
                $options['twoWayKey'] = $actualNewKey;
                $options['twoWay'] = $actualTwoWay;
                $options['onDelete'] = $actualOnDelete->value;

                $twoWayAttribute->setAttribute(Document::ID, $actualNewTwoWayKey);
                $twoWayAttribute->setAttribute('key', $actualNewTwoWayKey);
                $twoWayAttribute->setAttribute('options', $options);
            }, triggerEvent: false)];

            if ($oldRelationship->getType() === RelationType::ManyToMany) {
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $oldRelationship->getSide());

                $updatedAttributes[] = [$junction, $this->updateAttributeMeta($junction, $id, function ($junctionAttribute) use ($actualNewKey) {
                    $junctionAttribute->setAttribute(Document::ID, $actualNewKey);
                    $junctionAttribute->setAttribute('key', $actualNewKey);
                }, triggerEvent: false)];
                $updatedAttributes[] = [$junction, $this->updateAttributeMeta($junction, $oldTwoWayKey, function ($junctionAttribute) use ($actualNewTwoWayKey) {
                    $junctionAttribute->setAttribute(Document::ID, $actualNewTwoWayKey);
                    $junctionAttribute->setAttribute('key', $actualNewTwoWayKey);
                }, triggerEvent: false)];

                $this->withRetries(fn () => $this->purgeCachedCollection($junction));
            }
        } catch (Throwable $error) {
            $restores = [
                fn () => $this->updateAttributeMeta($collection->getId(), $actualNewKey, function ($attribute) use ($id, $oldRelationship) {
                    $attribute->setAttribute(Document::ID, $id);
                    $attribute->setAttribute('key', $id);
                    $attribute->setAttribute('options', $oldRelationship->toDocument()->getArrayCopy());
                }, triggerEvent: false),
                fn () => $this->updateAttributeMeta($relatedCollection->getId(), $actualNewTwoWayKey, function (Document $twoWayAttribute) use ($oldTwoWayKey, $id, $oldRelationship) {
                    /** @var array<string, mixed> $options */
                    $options = $twoWayAttribute->getAttribute('options', []);
                    $options['twoWayKey'] = $id;
                    $options['twoWay'] = $oldRelationship->isTwoWay();
                    $options['onDelete'] = $oldRelationship->getOnDelete()->value;
                    $twoWayAttribute->setAttribute(Document::ID, $oldTwoWayKey);
                    $twoWayAttribute->setAttribute('key', $oldTwoWayKey);
                    $twoWayAttribute->setAttribute('options', $options);
                }, triggerEvent: false),
                fn () => $this->updateAttributeMeta($this->getJunctionCollection($collection, $relatedCollection, $oldRelationship->getSide()), $actualNewKey, function ($junctionAttribute) use ($id) {
                    $junctionAttribute->setAttribute(Document::ID, $id);
                    $junctionAttribute->setAttribute('key', $id);
                }, triggerEvent: false),
                fn () => $this->updateAttributeMeta($this->getJunctionCollection($collection, $relatedCollection, $oldRelationship->getSide()), $actualNewTwoWayKey, function ($junctionAttribute) use ($oldTwoWayKey) {
                    $junctionAttribute->setAttribute(Document::ID, $oldTwoWayKey);
                    $junctionAttribute->setAttribute('key', $oldTwoWayKey);
                }, triggerEvent: false),
            ];
            foreach (\array_slice($restores, 0, \count($updatedAttributes)) as $restore) {
                try {
                    $restore();
                } catch (Throwable) {
                    // Best effort
                }
            }

            if ($adapterUpdated && $this->adapter->hasFeature(Feature\Relationships::class)) {
                try {
                    $renamed = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: $oldRelationship->getType(),
                        twoWay: $actualTwoWay,
                        key: $actualNewKey,
                        twoWayKey: $actualNewTwoWayKey,
                        onDelete: $actualOnDelete,
                        side: $oldRelationship->getSide(),
                    );
                    $this->adapter->updateRelationship(
                        $renamed,
                        $id,
                        $oldTwoWayKey
                    );
                } catch (Throwable) {
                    // Ignore
                }
            }
            throw $error;
        }

        $renameIndex = function (string $collection, string $key, string $newKey) {
            $this->updateIndexMeta(
                $collection,
                '_index_'.$key,
                function ($index) use ($newKey) {
                    $index->setAttribute('attributes', [$newKey]);
                }
            );
            $this->silent(
                fn () => $this->renameIndex($collection, '_index_'.$key, '_index_'.$newKey)
            );
        };

        $indexRenamesCompleted = [];

        try {
            switch ($oldRelationship->getType()) {
                case RelationType::OneToOne:
                    if ($id !== $actualNewKey) {
                        $renameIndex($collection->getId(), $id, $actualNewKey);
                        $indexRenamesCompleted[] = [$collection->getId(), $actualNewKey, $id];
                    }
                    if ($actualTwoWay && $oldTwoWayKey !== $actualNewTwoWayKey) {
                        $renameIndex($relatedCollection->getId(), $oldTwoWayKey, $actualNewTwoWayKey);
                        $indexRenamesCompleted[] = [$relatedCollection->getId(), $actualNewTwoWayKey, $oldTwoWayKey];
                    }
                    break;
                case RelationType::OneToMany:
                    if ($oldRelationship->getSide() === RelationSide::Parent) {
                        if ($oldTwoWayKey !== $actualNewTwoWayKey) {
                            $renameIndex($relatedCollection->getId(), $oldTwoWayKey, $actualNewTwoWayKey);
                            $indexRenamesCompleted[] = [$relatedCollection->getId(), $actualNewTwoWayKey, $oldTwoWayKey];
                        }
                    } else {
                        if ($id !== $actualNewKey) {
                            $renameIndex($collection->getId(), $id, $actualNewKey);
                            $indexRenamesCompleted[] = [$collection->getId(), $actualNewKey, $id];
                        }
                    }
                    break;
                case RelationType::ManyToOne:
                    if ($oldRelationship->getSide() === RelationSide::Parent) {
                        if ($id !== $actualNewKey) {
                            $renameIndex($collection->getId(), $id, $actualNewKey);
                            $indexRenamesCompleted[] = [$collection->getId(), $actualNewKey, $id];
                        }
                    } else {
                        if ($oldTwoWayKey !== $actualNewTwoWayKey) {
                            $renameIndex($relatedCollection->getId(), $oldTwoWayKey, $actualNewTwoWayKey);
                            $indexRenamesCompleted[] = [$relatedCollection->getId(), $actualNewTwoWayKey, $oldTwoWayKey];
                        }
                    }
                    break;
                case RelationType::ManyToMany:
                    $junction = $this->getJunctionCollection($collection, $relatedCollection, $oldRelationship->getSide());

                    if ($id !== $actualNewKey) {
                        $renameIndex($junction, $id, $actualNewKey);
                        $indexRenamesCompleted[] = [$junction, $actualNewKey, $id];
                    }
                    if ($oldTwoWayKey !== $actualNewTwoWayKey) {
                        $renameIndex($junction, $oldTwoWayKey, $actualNewTwoWayKey);
                        $indexRenamesCompleted[] = [$junction, $actualNewTwoWayKey, $oldTwoWayKey];
                    }
                    break;
                default:
                    throw new RelationshipException('Invalid relationship type.');
            }
        } catch (Throwable $error) {
            if ($adapterUpdated && $this->adapter->hasFeature(Feature\Relationships::class)) {
                try {
                    $renamed = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: $oldRelationship->getType(),
                        twoWay: $oldRelationship->isTwoWay(),
                        key: $actualNewKey,
                        twoWayKey: $actualNewTwoWayKey,
                        onDelete: $oldRelationship->getOnDelete(),
                        side: $oldRelationship->getSide(),
                    );
                    $this->adapter->updateRelationship(
                        $renamed,
                        $id,
                        $oldTwoWayKey
                    );
                } catch (Throwable) {
                    // Best effort
                }
            }

            foreach (\array_reverse($indexRenamesCompleted) as [$indexedCollection, $from, $to]) {
                try {
                    $renameIndex($indexedCollection, $from, $to);
                } catch (Throwable) {
                    // Best effort
                }
            }

            try {
                $this->updateAttributeMeta($collection->getId(), $actualNewKey, function ($attribute) use ($id, $oldRelationship) {
                    $attribute->setAttribute(Document::ID, $id);
                    $attribute->setAttribute('key', $id);
                    $attribute->setAttribute('options', $oldRelationship->toDocument()->getArrayCopy());
                }, triggerEvent: false);
            } catch (Throwable) {
                // Best effort
            }

            try {
                $this->updateAttributeMeta($relatedCollection->getId(), $actualNewTwoWayKey, function (Document $twoWayAttribute) use ($oldTwoWayKey, $id, $oldRelationship) {
                    /** @var array<string, mixed> $options */
                    $options = $twoWayAttribute->getAttribute('options', []);
                    $options['twoWayKey'] = $id;
                    $options['twoWay'] = $oldRelationship->isTwoWay();
                    $options['onDelete'] = $oldRelationship->getOnDelete()->value;
                    $twoWayAttribute->setAttribute(Document::ID, $oldTwoWayKey);
                    $twoWayAttribute->setAttribute('key', $oldTwoWayKey);
                    $twoWayAttribute->setAttribute('options', $options);
                }, triggerEvent: false);
            } catch (Throwable) {
                // Best effort
            }

            if ($oldRelationship->getType() === RelationType::ManyToMany) {
                $junctionId = $this->getJunctionCollection($collection, $relatedCollection, $oldRelationship->getSide());
                try {
                    $this->updateAttributeMeta($junctionId, $actualNewKey, function ($junctionAttribute) use ($id) {
                        $junctionAttribute->setAttribute(Document::ID, $id);
                        $junctionAttribute->setAttribute('key', $id);
                    }, triggerEvent: false);
                } catch (Throwable) {
                    // Best effort
                }
                try {
                    $this->updateAttributeMeta($junctionId, $actualNewTwoWayKey, function ($junctionAttribute) use ($oldTwoWayKey) {
                        $junctionAttribute->setAttribute(Document::ID, $oldTwoWayKey);
                        $junctionAttribute->setAttribute('key', $oldTwoWayKey);
                    }, triggerEvent: false);
                } catch (Throwable) {
                    // Best effort
                }
            }

            throw new DatabaseException("Failed to update relationship indexes for '{$id}': ".$error->getMessage(), previous: $error);
        }

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));
        $this->withRetries(fn () => $this->purgeCachedCollection($relatedCollection->getId()));

        foreach ($updatedAttributes as [$updatedCollection, $attribute]) {
            $this->triggerHooks(
                Event::AttributeUpdate,
                $attribute->toDocument()->setAttribute(Document::COLLECTION, $updatedCollection),
            );
        }

        return true;
    }

    /**
     * Delete a relationship attribute and its inverse from both collections.
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $id  The relationship attribute identifier
     * @return bool True if the relationship was deleted successfully
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function deleteRelationship(string $collection, string $id): bool
    {
        if (! $this->adapter->hasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        /** @var array<int|string, Attribute> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        $relationship = null;

        foreach ($attributes as $name => $attribute) {
            if ($attribute->getKey() === $id) {
                $relationship = $attribute;
                unset($attributes[$name]);
                break;
            }
        }

        if ($relationship === null) {
            throw new NotFoundException('Relationship not found');
        }

        $collection->setAttribute('attributes', \array_values($attributes));

        $definition = Relationship::fromArray(['collection' => $collection->getId()] + $relationship->getArrayCopy());

        $relatedCollection = $this->silent(fn () => $this->getCollection($definition->getRelatedCollection()));
        /** @var array<int|string, Attribute> $relatedAttributes */
        $relatedAttributes = $relatedCollection->getAttribute('attributes', []);

        $twoWayKey = $definition->getTwoWayKey();
        foreach ($relatedAttributes as $name => $attribute) {
            if ($attribute->getKey() === $twoWayKey) {
                unset($relatedAttributes[$name]);
                break;
            }
        }

        $relatedCollection->setAttribute('attributes', \array_values($relatedAttributes));

        $collectionAttributes = $collection->getAttribute('attributes');
        $relatedCollectionAttributes = $relatedCollection->getAttribute('attributes');

        // Delete indexes BEFORE dropping columns to avoid referencing non-existent columns
        $deletedIndexes = [];
        $deletedJunction = null;

        $this->silent(function () use ($collection, $relatedCollection, $definition, $id, &$deletedIndexes, &$deletedJunction) {
            $indexKey = '_index_'.$id;
            $twoWayIndexKey = '_index_'.$definition->getTwoWayKey();

            switch ($definition->getType()) {
                case RelationType::OneToOne:
                    if ($definition->getSide() === RelationSide::Parent) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Unique, 'attributes' => [$id]];
                        if ($definition->isTwoWay()) {
                            $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                            $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Unique, 'attributes' => [$definition->getTwoWayKey()]];
                        }
                    }
                    if ($definition->getSide() === RelationSide::Child) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Unique, 'attributes' => [$definition->getTwoWayKey()]];
                        if ($definition->isTwoWay()) {
                            $this->deleteIndex($collection->getId(), $indexKey);
                            $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Unique, 'attributes' => [$id]];
                        }
                    }
                    break;
                case RelationType::OneToMany:
                    if ($definition->getSide() === RelationSide::Parent) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Key, 'attributes' => [$definition->getTwoWayKey()]];
                    } else {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Key, 'attributes' => [$id]];
                    }
                    break;
                case RelationType::ManyToOne:
                    if ($definition->getSide() === RelationSide::Parent) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Key, 'attributes' => [$id]];
                    } else {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Key, 'attributes' => [$definition->getTwoWayKey()]];
                    }
                    break;
                case RelationType::ManyToMany:
                    $junction = $this->getJunctionCollection(
                        $collection,
                        $relatedCollection,
                        $definition->getSide()
                    );

                    $deletedJunction = $this->silent(fn () => $this->getDocument(self::METADATA, $junction));
                    $this->deleteDocument(self::METADATA, $junction);
                    break;
                default:
                    throw new RelationshipException('Invalid relationship type.');
            }
        });

        $collection = $this->silent(fn () => $this->getCollection($collection->getId()));
        $relatedCollection = $this->silent(fn () => $this->getCollection($relatedCollection->getId()));
        $collection->setAttribute('attributes', $collectionAttributes);
        $relatedCollection->setAttribute('attributes', $relatedCollectionAttributes);

        $dropped = new Relationship(
            collection: $collection->getId(),
            relatedCollection: $relatedCollection->getId(),
            type: $definition->getType(),
            twoWay: $definition->isTwoWay(),
            key: $id,
            twoWayKey: $definition->getTwoWayKey(),
            side: $definition->getSide(),
        );

        $shouldRollback = false;
        try {
            $deleted = $this->adapter->deleteRelationship($dropped);

            if (! $deleted) {
                throw new DatabaseException('Failed to delete relationship');
            }
            $shouldRollback = true;
        } catch (NotFoundException) {
            // Ignore — relationship already absent from schema
        }

        try {
            $this->withRetries(function () use ($collection, $relatedCollection) {
                $this->silent(function () use ($collection, $relatedCollection) {
                    $this->withTransaction(function () use ($collection, $relatedCollection) {
                        $this->updateDocument(self::METADATA, $collection->getId(), $collection);
                        $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);
                    });
                });
            });
        } catch (Throwable $error) {
            if ($shouldRollback) {
                try {
                    $restored = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: $definition->getType(),
                        twoWay: $definition->isTwoWay(),
                        key: $id,
                        twoWayKey: $definition->getTwoWayKey(),
                        onDelete: $definition->getOnDelete(),
                        side: RelationSide::Parent,
                    );
                    $this->adapter->createRelationship($restored);
                } catch (Throwable) {
                    // Silent rollback — best effort to restore consistency
                }
            }

            foreach ($deletedIndexes as $indexInfo) {
                try {
                    $this->createIndex(
                        $indexInfo['collection'],
                        new Index(
                            key: $indexInfo['key'],
                            type: $indexInfo['type'],
                            attributes: $indexInfo['attributes']
                        )
                    );
                } catch (Throwable) {
                    // Silent rollback — best effort
                }
            }

            if ($deletedJunction !== null && ! $deletedJunction->isEmpty()) {
                try {
                    $this->silent(fn () => $this->createDocument(self::METADATA, $deletedJunction));
                } catch (Throwable) {
                    // Silent rollback — best effort
                }
            }

            throw new DatabaseException(
                "Failed to persist metadata after retries for relationship deletion '{$id}': ".$error->getMessage(),
                previous: $error
            );
        }

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));
        $this->withRetries(fn () => $this->purgeCachedCollection($relatedCollection->getId()));

        $this->triggerHooks(
            Event::AttributeDelete,
            (clone $relationship)->setAttribute(Document::COLLECTION, $collection->getId()),
        );

        return true;
    }

    private function getJunctionCollection(Document $collection, Document $relatedCollection, RelationSide $side): string
    {
        return $side === RelationSide::Parent
            ? '_'.$collection->getSequence().'_'.$relatedCollection->getSequence()
            : '_'.$relatedCollection->getSequence().'_'.$collection->getSequence();
    }
}
