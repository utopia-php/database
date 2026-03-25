<?php

namespace Utopia\Database\Traits;

use Throwable;
use Utopia\Console;
use Utopia\Database\Attribute;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Database;
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

        $previous = $this->relationshipHook->isEnabled();
        $this->relationshipHook->setEnabled(false);

        try {
            return $callback();
        } finally {
            $this->relationshipHook->setEnabled($previous);
        }
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

        $previous = $this->relationshipHook->shouldCheckExist();
        $this->relationshipHook->setCheckExist(false);

        try {
            return $callback();
        } finally {
            $this->relationshipHook->setCheckExist($previous);
        }
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
            fn () => $this->adapter->deleteRelationship($relationshipModel),
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
        $collection = $this->silent(fn () => $this->getCollection($relationship->collection));
        $relatedCollection = $this->silent(fn () => $this->getCollection($relationship->relatedCollection));

        /** @var Document $collection */
        /** @var Document $relatedCollection */
        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }
        if ($relatedCollection->isEmpty()) {
            throw new NotFoundException('Related collection not found');
        }

        $type = $relationship->type;
        $twoWay = $relationship->twoWay;
        $id = ! empty($relationship->key) ? $relationship->key : $this->adapter->filter($relatedCollection->getId());
        $twoWayKey = ! empty($relationship->twoWayKey) ? $relationship->twoWayKey : $this->adapter->filter($collection->getId());
        $onDelete = $relationship->onDelete;

        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            $typedAttr = Attribute::fromDocument($attribute);
            if (\strtolower($typedAttr->key) === \strtolower($id)) {
                throw new DuplicateException('Attribute already exists');
            }

            if ($typedAttr->type === ColumnType::Relationship) {
                $existingRel = Relationship::fromDocument($collection->getId(), $attribute);
                if (
                    \strtolower($existingRel->twoWayKey) === \strtolower($twoWayKey)
                    && $existingRel->relatedCollection === $relatedCollection->getId()
                ) {
                    throw new DuplicateException('Related attribute already exists');
                }
            }
        }

        $relationship = new Document([
            '$id' => ID::custom($id),
            'key' => $id,
            'type' => ColumnType::Relationship->value,
            'required' => false,
            'default' => null,
            'options' => [
                'relatedCollection' => $relatedCollection->getId(),
                'relationType' => $type,
                'twoWay' => $twoWay,
                'twoWayKey' => $twoWayKey,
                'onDelete' => $onDelete,
                'side' => RelationSide::Parent,
            ],
        ]);

        $twoWayRelationship = new Document([
            '$id' => ID::custom($twoWayKey),
            'key' => $twoWayKey,
            'type' => ColumnType::Relationship->value,
            'required' => false,
            'default' => null,
            'options' => [
                'relatedCollection' => $collection->getId(),
                'relationType' => $type,
                'twoWay' => $twoWay,
                'twoWayKey' => $id,
                'onDelete' => $onDelete,
                'side' => RelationSide::Child,
            ],
        ]);

        $this->checkAttribute($collection, $relationship);
        $this->checkAttribute($relatedCollection, $twoWayRelationship);

        $junctionCollection = null;
        if ($type === RelationType::ManyToMany) {
            $junctionCollection = '_'.$collection->getSequence().'_'.$relatedCollection->getSequence();
            $junctionAttributes = [
                new Attribute(
                    key: $id,
                    type: ColumnType::String,
                    size: Database::LENGTH_KEY,
                    required: true,
                ),
                new Attribute(
                    key: $twoWayKey,
                    type: ColumnType::String,
                    size: Database::LENGTH_KEY,
                    required: true,
                ),
            ];
            $junctionIndexes = [
                new Index(
                    key: '_index_'.$id,
                    type: IndexType::Key,
                    attributes: [$id],
                ),
                new Index(
                    key: '_index_'.$twoWayKey,
                    type: IndexType::Key,
                    attributes: [$twoWayKey],
                ),
            ];
            try {
                $this->silent(fn () => $this->createCollection($junctionCollection, $junctionAttributes, $junctionIndexes));
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
                    } catch (Throwable $e) {
                        Console::error("Failed to cleanup junction collection '{$junctionCollection}': ".$e->getMessage());
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
            $indexesCreated = [];
            try {
                $this->withRetries(function () use ($collection, $relatedCollection) {
                    $this->withTransaction(function () use ($collection, $relatedCollection) {
                        $this->updateDocument(self::METADATA, $collection->getId(), $collection);
                        $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);
                    });
                });
            } catch (Throwable $e) {
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
                    } catch (Throwable $e) {
                        Console::error("Failed to cleanup relationship '{$id}': ".$e->getMessage());
                    }

                    if ($junctionCollection !== null) {
                        try {
                            $this->cleanupCollection($junctionCollection);
                        } catch (Throwable $e) {
                            Console::error("Failed to cleanup junction collection '{$junctionCollection}': ".$e->getMessage());
                        }
                    }
                }

                throw new DatabaseException('Failed to create relationship: '.$e->getMessage());
            }

            $indexKey = '_index_'.$id;
            $twoWayIndexKey = '_index_'.$twoWayKey;
            $indexesCreated = [];

            try {
                switch ($type) {
                    case RelationType::OneToOne:
                        $this->createIndex($collection->getId(), new Index(key: $indexKey, type: IndexType::Unique, attributes: [$id]));
                        $indexesCreated[] = ['collection' => $collection->getId(), 'index' => $indexKey];
                        if ($twoWay) {
                            $this->createIndex($relatedCollection->getId(), new Index(key: $twoWayIndexKey, type: IndexType::Unique, attributes: [$twoWayKey]));
                            $indexesCreated[] = ['collection' => $relatedCollection->getId(), 'index' => $twoWayIndexKey];
                        }
                        break;
                    case RelationType::OneToMany:
                        $this->createIndex($relatedCollection->getId(), new Index(key: $twoWayIndexKey, type: IndexType::Key, attributes: [$twoWayKey]));
                        $indexesCreated[] = ['collection' => $relatedCollection->getId(), 'index' => $twoWayIndexKey];
                        break;
                    case RelationType::ManyToOne:
                        $this->createIndex($collection->getId(), new Index(key: $indexKey, type: IndexType::Key, attributes: [$id]));
                        $indexesCreated[] = ['collection' => $collection->getId(), 'index' => $indexKey];
                        break;
                    case RelationType::ManyToMany:
                        // Indexes created on junction collection creation
                        break;
                    default:
                        throw new RelationshipException('Invalid relationship type.');
                }
            } catch (Throwable $e) {
                foreach ($indexesCreated as $indexInfo) {
                    try {
                        $this->deleteIndex($indexInfo['collection'], $indexInfo['index']);
                    } catch (Throwable $cleanupError) {
                        Console::error("Failed to cleanup index '{$indexInfo['index']}': ".$cleanupError->getMessage());
                    }
                }

                try {
                    $this->withTransaction(function () use ($collection, $relatedCollection, $id, $twoWayKey) {
                        /** @var array<Document> $attributes */
                        $attributes = $collection->getAttribute('attributes', []);
                        $collection->setAttribute('attributes', array_filter($attributes, fn (Document $attr) => $attr->getId() !== $id));
                        $this->updateDocument(self::METADATA, $collection->getId(), $collection);

                        /** @var array<Document> $relatedAttributes */
                        $relatedAttributes = $relatedCollection->getAttribute('attributes', []);
                        $relatedCollection->setAttribute('attributes', array_filter($relatedAttributes, fn (Document $attr) => $attr->getId() !== $twoWayKey));
                        $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);
                    });
                } catch (Throwable $cleanupError) {
                    Console::error("Failed to cleanup metadata for relationship '{$id}': ".$cleanupError->getMessage());
                }

                // Cleanup relationship
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

                throw new DatabaseException('Failed to create relationship indexes: '.$e->getMessage());
            }
        });

        $this->trigger(Event::AttributeCreate, $relationship);

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
        if (
            $newKey === null
            && $newTwoWayKey === null
            && $twoWay === null
            && $onDelete === null
        ) {
            return true;
        }

        $collection = $this->getCollection($collection);
        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        if (
            $newKey !== null
            && \in_array($newKey, \array_map(fn (Document $attribute) => Attribute::fromDocument($attribute)->key, $attributes))
        ) {
            throw new DuplicateException('Relationship already exists');
        }

        $attributeIndex = array_search($id, array_map(fn (Document $attribute) => Attribute::fromDocument($attribute)->key, $attributes));

        if ($attributeIndex === false) {
            throw new NotFoundException('Relationship not found');
        }

        /** @var Document $attribute */
        $attribute = $attributes[$attributeIndex];
        $oldRel = Relationship::fromDocument($collection->getId(), $attribute);

        $relatedCollectionId = $oldRel->relatedCollection;
        $relatedCollection = $this->getCollection($relatedCollectionId);

        // Determine if we need to alter the database (rename columns/indexes)
        $oldTwoWayKey = $oldRel->twoWayKey;
        $altering = ($newKey !== null && $newKey !== $id)
            || ($newTwoWayKey !== null && $newTwoWayKey !== $oldTwoWayKey);

        // Validate new keys don't already exist
        /** @var array<Document> $relatedAttrs */
        $relatedAttrs = $relatedCollection->getAttribute('attributes', []);
        if (
            $newTwoWayKey !== null
            && \in_array($newTwoWayKey, \array_map(fn (Document $attribute) => Attribute::fromDocument($attribute)->key, $relatedAttrs))
        ) {
            throw new DuplicateException('Related attribute already exists');
        }

        $actualNewKey = $newKey ?? $id;
        $actualNewTwoWayKey = $newTwoWayKey ?? $oldTwoWayKey;
        $actualTwoWay = $twoWay ?? $oldRel->twoWay;
        $actualOnDelete = $onDelete ?? $oldRel->onDelete;

        $adapterUpdated = false;
        if ($altering) {
            try {
                $updateRelModel = new Relationship(
                    collection: $collection->getId(),
                    relatedCollection: $relatedCollection->getId(),
                    type: $oldRel->type,
                    twoWay: $actualTwoWay,
                    key: $id,
                    twoWayKey: $oldTwoWayKey,
                    onDelete: $actualOnDelete,
                    side: $oldRel->side,
                );
                $adapterUpdated = $this->adapter->updateRelationship(
                    $updateRelModel,
                    $actualNewKey,
                    $actualNewTwoWayKey
                );

                if (! $adapterUpdated) {
                    throw new DatabaseException('Failed to update relationship');
                }
            } catch (Throwable $e) {
                // Check if the rename already happened in schema (orphan from prior
                // partial failure where adapter succeeded but metadata+rollback failed).
                // If the new column names already exist, the prior rename completed.
                if ($this->adapter instanceof Feature\SchemaAttributes) {
                    $schemaAttributes = $this->getSchemaAttributes($collection->getId());
                    $filteredNewKey = $this->adapter->filter($actualNewKey);
                    $newKeyExists = false;
                    foreach ($schemaAttributes as $schemaAttr) {
                        if (\strtolower($schemaAttr->getId()) === \strtolower($filteredNewKey)) {
                            $newKeyExists = true;
                            break;
                        }
                    }
                    if ($newKeyExists) {
                        $adapterUpdated = true;
                    } else {
                        throw new DatabaseException("Failed to update relationship '{$id}': ".$e->getMessage(), previous: $e);
                    }
                } else {
                    throw new DatabaseException("Failed to update relationship '{$id}': ".$e->getMessage(), previous: $e);
                }
            }
        }

        try {
            $this->updateAttributeMeta($collection->getId(), $id, function ($attribute) use ($actualNewKey, $actualNewTwoWayKey, $actualTwoWay, $actualOnDelete, $relatedCollection, $oldRel) {
                $attribute->setAttribute('$id', $actualNewKey);
                $attribute->setAttribute('key', $actualNewKey);
                $attribute->setAttribute('options', [
                    'relatedCollection' => $relatedCollection->getId(),
                    'relationType' => $oldRel->type,
                    'twoWay' => $actualTwoWay,
                    'twoWayKey' => $actualNewTwoWayKey,
                    'onDelete' => $actualOnDelete,
                    'side' => $oldRel->side,
                ]);
            });

            $this->updateAttributeMeta($relatedCollection->getId(), $oldTwoWayKey, function (Document $twoWayAttribute) use ($actualNewKey, $actualNewTwoWayKey, $actualTwoWay, $actualOnDelete) {
                /** @var array<string, mixed> $options */
                $options = $twoWayAttribute->getAttribute('options', []);
                $options['twoWayKey'] = $actualNewKey;
                $options['twoWay'] = $actualTwoWay;
                $options['onDelete'] = $actualOnDelete;

                $twoWayAttribute->setAttribute('$id', $actualNewTwoWayKey);
                $twoWayAttribute->setAttribute('key', $actualNewTwoWayKey);
                $twoWayAttribute->setAttribute('options', $options);
            });

            if ($oldRel->type === RelationType::ManyToMany) {
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $oldRel->side);

                $this->updateAttributeMeta($junction, $id, function ($junctionAttribute) use ($actualNewKey) {
                    $junctionAttribute->setAttribute('$id', $actualNewKey);
                    $junctionAttribute->setAttribute('key', $actualNewKey);
                });
                $this->updateAttributeMeta($junction, $oldTwoWayKey, function ($junctionAttribute) use ($actualNewTwoWayKey) {
                    $junctionAttribute->setAttribute('$id', $actualNewTwoWayKey);
                    $junctionAttribute->setAttribute('key', $actualNewTwoWayKey);
                });

                $this->withRetries(fn () => $this->purgeCachedCollection($junction));
            }
        } catch (Throwable $e) {
            if ($adapterUpdated) {
                try {
                    $reverseRelModel = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: $oldRel->type,
                        twoWay: $actualTwoWay,
                        key: $actualNewKey,
                        twoWayKey: $actualNewTwoWayKey,
                        onDelete: $actualOnDelete,
                        side: $oldRel->side,
                    );
                    $this->adapter->updateRelationship(
                        $reverseRelModel,
                        $id,
                        $oldTwoWayKey
                    );
                } catch (Throwable $e) {
                    // Ignore
                }
            }
            throw $e;
        }

        // Update Indexes — wrapped in rollback for consistency with metadata
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
            switch ($oldRel->type) {
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
                    if ($oldRel->side === RelationSide::Parent) {
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
                    if ($oldRel->side === RelationSide::Parent) {
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
                    $junction = $this->getJunctionCollection($collection, $relatedCollection, $oldRel->side);

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
        } catch (Throwable $e) {
            // Reverse completed index renames
            foreach (\array_reverse($indexRenamesCompleted) as [$coll, $from, $to]) {
                try {
                    $renameIndex($coll, $from, $to);
                } catch (Throwable) {
                    // Best effort
                }
            }

            // Reverse attribute metadata
            try {
                $this->updateAttributeMeta($collection->getId(), $actualNewKey, function ($attribute) use ($id, $oldRel) {
                    $attribute->setAttribute('$id', $id);
                    $attribute->setAttribute('key', $id);
                    $attribute->setAttribute('options', $oldRel->toDocument()->getArrayCopy());
                });
            } catch (Throwable) {
                // Best effort
            }

            try {
                $this->updateAttributeMeta($relatedCollection->getId(), $actualNewTwoWayKey, function (Document $twoWayAttribute) use ($oldTwoWayKey, $id, $oldRel) {
                    /** @var array<string, mixed> $options */
                    $options = $twoWayAttribute->getAttribute('options', []);
                    $options['twoWayKey'] = $id;
                    $options['twoWay'] = $oldRel->twoWay;
                    $options['onDelete'] = $oldRel->onDelete;
                    $twoWayAttribute->setAttribute('$id', $oldTwoWayKey);
                    $twoWayAttribute->setAttribute('key', $oldTwoWayKey);
                    $twoWayAttribute->setAttribute('options', $options);
                });
            } catch (Throwable) {
                // Best effort
            }

            if ($oldRel->type === RelationType::ManyToMany) {
                $junctionId = $this->getJunctionCollection($collection, $relatedCollection, $oldRel->side);
                try {
                    $this->updateAttributeMeta($junctionId, $actualNewKey, function ($attr) use ($id) {
                        $attr->setAttribute('$id', $id);
                        $attr->setAttribute('key', $id);
                    });
                } catch (Throwable) {
                    // Best effort
                }
                try {
                    $this->updateAttributeMeta($junctionId, $actualNewTwoWayKey, function ($attr) use ($oldTwoWayKey) {
                        $attr->setAttribute('$id', $oldTwoWayKey);
                        $attr->setAttribute('key', $oldTwoWayKey);
                    });
                } catch (Throwable) {
                    // Best effort
                }
            }

            // Reverse adapter update
            if ($adapterUpdated) {
                try {
                    $reverseRelModel2 = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: $oldRel->type,
                        twoWay: $oldRel->twoWay,
                        key: $actualNewKey,
                        twoWayKey: $actualNewTwoWayKey,
                        onDelete: $oldRel->onDelete,
                        side: $oldRel->side,
                    );
                    $this->adapter->updateRelationship(
                        $reverseRelModel2,
                        $id,
                        $oldTwoWayKey
                    );
                } catch (Throwable) {
                    // Best effort
                }
            }

            throw new DatabaseException("Failed to update relationship indexes for '{$id}': ".$e->getMessage(), previous: $e);
        }

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));
        $this->withRetries(fn () => $this->purgeCachedCollection($relatedCollection->getId()));

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
        $collection = $this->silent(fn () => $this->getCollection($collection));
        /** @var array<int|string, Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        $relationship = null;

        foreach ($attributes as $name => $attribute) {
            $typedAttr = Attribute::fromDocument($attribute);
            if ($typedAttr->key === $id) {
                $relationship = $attribute;
                unset($attributes[$name]);
                break;
            }
        }

        if ($relationship === null) {
            throw new NotFoundException('Relationship not found');
        }

        $collection->setAttribute('attributes', \array_values($attributes));

        $rel = Relationship::fromDocument($collection->getId(), $relationship);

        $relatedCollection = $this->silent(fn () => $this->getCollection($rel->relatedCollection));
        /** @var array<int|string, Document> $relatedAttributes */
        $relatedAttributes = $relatedCollection->getAttribute('attributes', []);

        foreach ($relatedAttributes as $name => $attribute) {
            $typedRelAttr = Attribute::fromDocument($attribute);
            if ($typedRelAttr->key === $rel->twoWayKey) {
                unset($relatedAttributes[$name]);
                break;
            }
        }

        $relatedCollection->setAttribute('attributes', \array_values($relatedAttributes));

        $collectionAttributes = $collection->getAttribute('attributes');
        $relatedCollectionAttributes = $relatedCollection->getAttribute('attributes');

        // Delete indexes BEFORE dropping columns to avoid referencing non-existent columns
        // Track deleted indexes for rollback
        $deletedIndexes = [];
        $deletedJunction = null;

        $this->silent(function () use ($collection, $relatedCollection, $rel, $id, &$deletedIndexes, &$deletedJunction) {
            $indexKey = '_index_'.$id;
            $twoWayIndexKey = '_index_'.$rel->twoWayKey;

            switch ($rel->type) {
                case RelationType::OneToOne:
                    if ($rel->side === RelationSide::Parent) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Unique, 'attributes' => [$id]];
                        if ($rel->twoWay) {
                            $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                            $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Unique, 'attributes' => [$rel->twoWayKey]];
                        }
                    }
                    if ($rel->side === RelationSide::Child) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Unique, 'attributes' => [$rel->twoWayKey]];
                        if ($rel->twoWay) {
                            $this->deleteIndex($collection->getId(), $indexKey);
                            $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Unique, 'attributes' => [$id]];
                        }
                    }
                    break;
                case RelationType::OneToMany:
                    if ($rel->side === RelationSide::Parent) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Key, 'attributes' => [$rel->twoWayKey]];
                    } else {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Key, 'attributes' => [$id]];
                    }
                    break;
                case RelationType::ManyToOne:
                    if ($rel->side === RelationSide::Parent) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Key, 'attributes' => [$id]];
                    } else {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Key, 'attributes' => [$rel->twoWayKey]];
                    }
                    break;
                case RelationType::ManyToMany:
                    $junction = $this->getJunctionCollection(
                        $collection,
                        $relatedCollection,
                        $rel->side
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

        $deleteRelModel = new Relationship(
            collection: $collection->getId(),
            relatedCollection: $relatedCollection->getId(),
            type: $rel->type,
            twoWay: $rel->twoWay,
            key: $id,
            twoWayKey: $rel->twoWayKey,
            side: $rel->side,
        );

        $shouldRollback = false;
        try {
            $deleted = $this->adapter->deleteRelationship($deleteRelModel);

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
        } catch (Throwable $e) {
            if ($shouldRollback) {
                // Recreate relationship columns
                try {
                    $recreateRelModel = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: $rel->type,
                        twoWay: $rel->twoWay,
                        key: $id,
                        twoWayKey: $rel->twoWayKey,
                        onDelete: $rel->onDelete,
                        side: RelationSide::Parent,
                    );
                    $this->adapter->createRelationship($recreateRelModel);
                } catch (Throwable) {
                    // Silent rollback — best effort to restore consistency
                }
            }

            // Restore deleted indexes
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

            // Restore junction collection metadata for M2M
            if ($deletedJunction !== null && ! $deletedJunction->isEmpty()) {
                try {
                    $this->silent(fn () => $this->createDocument(self::METADATA, $deletedJunction));
                } catch (Throwable) {
                    // Silent rollback — best effort
                }
            }

            throw new DatabaseException(
                "Failed to persist metadata after retries for relationship deletion '{$id}': ".$e->getMessage(),
                previous: $e
            );
        }

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));
        $this->withRetries(fn () => $this->purgeCachedCollection($relatedCollection->getId()));

        $this->trigger(Event::AttributeDelete, $relationship);

        return true;
    }

    private function getJunctionCollection(Document $collection, Document $relatedCollection, RelationSide $side): string
    {
        return $side === RelationSide::Parent
            ? '_'.$collection->getSequence().'_'.$relatedCollection->getSequence()
            : '_'.$relatedCollection->getSequence().'_'.$collection->getSequence();
    }
}
