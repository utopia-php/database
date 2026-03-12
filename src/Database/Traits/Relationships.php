<?php

namespace Utopia\Database\Traits;

use Utopia\CLI\Console;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
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
     * Create a relationship attribute
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

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $relatedCollection = $this->silent(fn () => $this->getCollection($relationship->relatedCollection));

        if ($relatedCollection->isEmpty()) {
            throw new NotFoundException('Related collection not found');
        }

        $type = $relationship->type;
        $twoWay = $relationship->twoWay;
        $id = ! empty($relationship->key) ? $relationship->key : $this->adapter->filter($relatedCollection->getId());
        $twoWayKey = ! empty($relationship->twoWayKey) ? $relationship->twoWayKey : $this->adapter->filter($collection->getId());
        $onDelete = $relationship->onDelete;

        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $attributes */
        foreach ($attributes as $attribute) {
            if (\strtolower($attribute->getId()) === \strtolower($id)) {
                throw new DuplicateException('Attribute already exists');
            }

            if (
                $attribute->getAttribute('type') === ColumnType::Relationship->value
                && \strtolower($attribute->getAttribute('options')['twoWayKey']) === \strtolower($twoWayKey)
                && $attribute->getAttribute('options')['relatedCollection'] === $relatedCollection->getId()
            ) {
                throw new DuplicateException('Related attribute already exists');
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
                'relationType' => $type->value,
                'twoWay' => $twoWay,
                'twoWayKey' => $twoWayKey,
                'onDelete' => $onDelete->value,
                'side' => RelationSide::Parent->value,
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
                'relationType' => $type->value,
                'twoWay' => $twoWay,
                'twoWayKey' => $id,
                'onDelete' => $onDelete->value,
                'side' => RelationSide::Child->value,
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
                    } catch (\Throwable $e) {
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
            } catch (\Throwable $e) {
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
                    } catch (\Throwable $e) {
                        Console::error("Failed to cleanup relationship '{$id}': ".$e->getMessage());
                    }

                    if ($junctionCollection !== null) {
                        try {
                            $this->cleanupCollection($junctionCollection);
                        } catch (\Throwable $e) {
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
            } catch (\Throwable $e) {
                foreach ($indexesCreated as $indexInfo) {
                    try {
                        $this->deleteIndex($indexInfo['collection'], $indexInfo['index']);
                    } catch (\Throwable $cleanupError) {
                        Console::error("Failed to cleanup index '{$indexInfo['index']}': ".$cleanupError->getMessage());
                    }
                }

                try {
                    $this->withTransaction(function () use ($collection, $relatedCollection, $id, $twoWayKey) {
                        $attributes = $collection->getAttribute('attributes', []);
                        $collection->setAttribute('attributes', array_filter($attributes, fn ($attr) => $attr->getId() !== $id));
                        $this->updateDocument(self::METADATA, $collection->getId(), $collection);

                        $relatedAttributes = $relatedCollection->getAttribute('attributes', []);
                        $relatedCollection->setAttribute('attributes', array_filter($relatedAttributes, fn ($attr) => $attr->getId() !== $twoWayKey));
                        $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);
                    });
                } catch (\Throwable $cleanupError) {
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
                } catch (\Throwable $cleanupError) {
                    Console::error("Failed to cleanup relationship '{$id}': ".$cleanupError->getMessage());
                }

                if ($junctionCollection !== null) {
                    try {
                        $this->cleanupCollection($junctionCollection);
                    } catch (\Throwable $cleanupError) {
                        Console::error("Failed to cleanup junction collection '{$junctionCollection}': ".$cleanupError->getMessage());
                    }
                }

                throw new DatabaseException('Failed to create relationship indexes: '.$e->getMessage());
            }
        });

        try {
            $this->trigger(self::EVENT_ATTRIBUTE_CREATE, $relationship);
        } catch (\Throwable $e) {
            // Ignore
        }

        return true;
    }

    /**
     * Update a relationship attribute
     *
     * @param  string|null  $onDelete
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
            \is_null($newKey)
            && \is_null($newTwoWayKey)
            && \is_null($twoWay)
            && \is_null($onDelete)
        ) {
            return true;
        }

        $collection = $this->getCollection($collection);
        $attributes = $collection->getAttribute('attributes', []);

        if (
            ! \is_null($newKey)
            && \in_array($newKey, \array_map(fn ($attribute) => $attribute['key'], $attributes))
        ) {
            throw new DuplicateException('Relationship already exists');
        }

        $attributeIndex = array_search($id, array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($attributeIndex === false) {
            throw new NotFoundException('Relationship not found');
        }

        $attribute = $attributes[$attributeIndex];
        $type = $attribute['options']['relationType'];
        $side = $attribute['options']['side'];

        $relatedCollectionId = $attribute['options']['relatedCollection'];
        $relatedCollection = $this->getCollection($relatedCollectionId);

        // Determine if we need to alter the database (rename columns/indexes)
        $oldAttribute = $attributes[$attributeIndex];
        $oldTwoWayKey = $oldAttribute['options']['twoWayKey'];
        $altering = (! \is_null($newKey) && $newKey !== $id)
            || (! \is_null($newTwoWayKey) && $newTwoWayKey !== $oldTwoWayKey);

        // Validate new keys don't already exist
        if (
            ! \is_null($newTwoWayKey)
            && \in_array($newTwoWayKey, \array_map(fn ($attribute) => $attribute['key'], $relatedCollection->getAttribute('attributes', [])))
        ) {
            throw new DuplicateException('Related attribute already exists');
        }

        $actualNewKey = $newKey ?? $id;
        $actualNewTwoWayKey = $newTwoWayKey ?? $oldTwoWayKey;
        $actualTwoWay = $twoWay ?? $oldAttribute['options']['twoWay'];
        $actualOnDelete = $onDelete ?? ForeignKeyAction::from($oldAttribute['options']['onDelete']);

        $adapterUpdated = false;
        if ($altering) {
            try {
                $updateRelModel = new Relationship(
                    collection: $collection->getId(),
                    relatedCollection: $relatedCollection->getId(),
                    type: RelationType::from($type),
                    twoWay: $actualTwoWay,
                    key: $id,
                    twoWayKey: $oldTwoWayKey,
                    onDelete: $actualOnDelete,
                    side: RelationSide::from($side),
                );
                $adapterUpdated = $this->adapter->updateRelationship(
                    $updateRelModel,
                    $actualNewKey,
                    $actualNewTwoWayKey
                );

                if (! $adapterUpdated) {
                    throw new DatabaseException('Failed to update relationship');
                }
            } catch (\Throwable $e) {
                // Check if the rename already happened in schema (orphan from prior
                // partial failure where adapter succeeded but metadata+rollback failed).
                // If the new column names already exist, the prior rename completed.
                if ($this->adapter->supports(Capability::SchemaAttributes)) {
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
            $this->updateAttributeMeta($collection->getId(), $id, function ($attribute) use ($actualNewKey, $actualNewTwoWayKey, $actualTwoWay, $actualOnDelete, $relatedCollection, $type, $side) {
                $attribute->setAttribute('$id', $actualNewKey);
                $attribute->setAttribute('key', $actualNewKey);
                $attribute->setAttribute('options', [
                    'relatedCollection' => $relatedCollection->getId(),
                    'relationType' => $type,
                    'twoWay' => $actualTwoWay,
                    'twoWayKey' => $actualNewTwoWayKey,
                    'onDelete' => $actualOnDelete->value,
                    'side' => $side,
                ]);
            });

            $this->updateAttributeMeta($relatedCollection->getId(), $oldTwoWayKey, function ($twoWayAttribute) use ($actualNewKey, $actualNewTwoWayKey, $actualTwoWay, $actualOnDelete) {
                $options = $twoWayAttribute->getAttribute('options', []);
                $options['twoWayKey'] = $actualNewKey;
                $options['twoWay'] = $actualTwoWay;
                $options['onDelete'] = $actualOnDelete->value;

                $twoWayAttribute->setAttribute('$id', $actualNewTwoWayKey);
                $twoWayAttribute->setAttribute('key', $actualNewTwoWayKey);
                $twoWayAttribute->setAttribute('options', $options);
            });

            if ($type === RelationType::ManyToMany->value) {
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

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
        } catch (\Throwable $e) {
            if ($adapterUpdated) {
                try {
                    $reverseRelModel = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: RelationType::from($type),
                        twoWay: $actualTwoWay,
                        key: $actualNewKey,
                        twoWayKey: $actualNewTwoWayKey,
                        onDelete: $actualOnDelete,
                        side: RelationSide::from($side),
                    );
                    $this->adapter->updateRelationship(
                        $reverseRelModel,
                        $id,
                        $oldTwoWayKey
                    );
                } catch (\Throwable $e) {
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
            switch ($type) {
                case RelationType::OneToOne->value:
                    if ($id !== $actualNewKey) {
                        $renameIndex($collection->getId(), $id, $actualNewKey);
                        $indexRenamesCompleted[] = [$collection->getId(), $actualNewKey, $id];
                    }
                    if ($actualTwoWay && $oldTwoWayKey !== $actualNewTwoWayKey) {
                        $renameIndex($relatedCollection->getId(), $oldTwoWayKey, $actualNewTwoWayKey);
                        $indexRenamesCompleted[] = [$relatedCollection->getId(), $actualNewTwoWayKey, $oldTwoWayKey];
                    }
                    break;
                case RelationType::OneToMany->value:
                    if ($side === RelationSide::Parent->value) {
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
                case RelationType::ManyToOne->value:
                    if ($side === RelationSide::Parent->value) {
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
                case RelationType::ManyToMany->value:
                    $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

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
        } catch (\Throwable $e) {
            // Reverse completed index renames
            foreach (\array_reverse($indexRenamesCompleted) as [$coll, $from, $to]) {
                try {
                    $renameIndex($coll, $from, $to);
                } catch (\Throwable) {
                    // Best effort
                }
            }

            // Reverse attribute metadata
            try {
                $this->updateAttributeMeta($collection->getId(), $actualNewKey, function ($attribute) use ($id, $oldAttribute) {
                    $attribute->setAttribute('$id', $id);
                    $attribute->setAttribute('key', $id);
                    $attribute->setAttribute('options', $oldAttribute['options']);
                });
            } catch (\Throwable) {
                // Best effort
            }

            try {
                $this->updateAttributeMeta($relatedCollection->getId(), $actualNewTwoWayKey, function ($twoWayAttribute) use ($oldTwoWayKey, $id, $oldAttribute) {
                    $options = $twoWayAttribute->getAttribute('options', []);
                    $options['twoWayKey'] = $id;
                    $options['twoWay'] = $oldAttribute['options']['twoWay'];
                    $options['onDelete'] = $oldAttribute['options']['onDelete'];
                    $twoWayAttribute->setAttribute('$id', $oldTwoWayKey);
                    $twoWayAttribute->setAttribute('key', $oldTwoWayKey);
                    $twoWayAttribute->setAttribute('options', $options);
                });
            } catch (\Throwable) {
                // Best effort
            }

            if ($type === RelationType::ManyToMany->value) {
                $junctionId = $this->getJunctionCollection($collection, $relatedCollection, $side);
                try {
                    $this->updateAttributeMeta($junctionId, $actualNewKey, function ($attr) use ($id) {
                        $attr->setAttribute('$id', $id);
                        $attr->setAttribute('key', $id);
                    });
                } catch (\Throwable) {
                    // Best effort
                }
                try {
                    $this->updateAttributeMeta($junctionId, $actualNewTwoWayKey, function ($attr) use ($oldTwoWayKey) {
                        $attr->setAttribute('$id', $oldTwoWayKey);
                        $attr->setAttribute('key', $oldTwoWayKey);
                    });
                } catch (\Throwable) {
                    // Best effort
                }
            }

            // Reverse adapter update
            if ($adapterUpdated) {
                try {
                    $reverseRelModel2 = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: RelationType::from($type),
                        twoWay: $oldAttribute['options']['twoWay'],
                        key: $actualNewKey,
                        twoWayKey: $actualNewTwoWayKey,
                        onDelete: ForeignKeyAction::from($oldAttribute['options']['onDelete'] ?? ForeignKeyAction::Restrict->value),
                        side: RelationSide::from($side),
                    );
                    $this->adapter->updateRelationship(
                        $reverseRelModel2,
                        $id,
                        $oldTwoWayKey
                    );
                } catch (\Throwable) {
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
     * Delete a relationship attribute
     *
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function deleteRelationship(string $collection, string $id): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $relationship = null;

        foreach ($attributes as $name => $attribute) {
            if ($attribute['$id'] === $id) {
                $relationship = $attribute;
                unset($attributes[$name]);
                break;
            }
        }

        if (\is_null($relationship)) {
            throw new NotFoundException('Relationship not found');
        }

        $collection->setAttribute('attributes', \array_values($attributes));

        $relatedCollection = $relationship['options']['relatedCollection'];
        $type = $relationship['options']['relationType'];
        $twoWay = $relationship['options']['twoWay'];
        $twoWayKey = $relationship['options']['twoWayKey'];
        $onDelete = $relationship['options']['onDelete'] ?? ForeignKeyAction::Restrict->value;
        $side = $relationship['options']['side'];

        $relatedCollection = $this->silent(fn () => $this->getCollection($relatedCollection));
        $relatedAttributes = $relatedCollection->getAttribute('attributes', []);

        foreach ($relatedAttributes as $name => $attribute) {
            if ($attribute['$id'] === $twoWayKey) {
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

        $this->silent(function () use ($collection, $relatedCollection, $type, $twoWay, $id, $twoWayKey, $side, &$deletedIndexes, &$deletedJunction) {
            $indexKey = '_index_'.$id;
            $twoWayIndexKey = '_index_'.$twoWayKey;

            switch ($type) {
                case RelationType::OneToOne->value:
                    if ($side === RelationSide::Parent->value) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Unique, 'attributes' => [$id]];
                        if ($twoWay) {
                            $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                            $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Unique, 'attributes' => [$twoWayKey]];
                        }
                    }
                    if ($side === RelationSide::Child->value) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Unique, 'attributes' => [$twoWayKey]];
                        if ($twoWay) {
                            $this->deleteIndex($collection->getId(), $indexKey);
                            $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Unique, 'attributes' => [$id]];
                        }
                    }
                    break;
                case RelationType::OneToMany->value:
                    if ($side === RelationSide::Parent->value) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Key, 'attributes' => [$twoWayKey]];
                    } else {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Key, 'attributes' => [$id]];
                    }
                    break;
                case RelationType::ManyToOne->value:
                    if ($side === RelationSide::Parent->value) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        $deletedIndexes[] = ['collection' => $collection->getId(), 'key' => $indexKey, 'type' => IndexType::Key, 'attributes' => [$id]];
                    } else {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        $deletedIndexes[] = ['collection' => $relatedCollection->getId(), 'key' => $twoWayIndexKey, 'type' => IndexType::Key, 'attributes' => [$twoWayKey]];
                    }
                    break;
                case RelationType::ManyToMany->value:
                    $junction = $this->getJunctionCollection(
                        $collection,
                        $relatedCollection,
                        $side
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
            type: RelationType::from($type),
            twoWay: $twoWay,
            key: $id,
            twoWayKey: $twoWayKey,
            side: RelationSide::from($side),
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
        } catch (\Throwable $e) {
            if ($shouldRollback) {
                // Recreate relationship columns
                try {
                    $recreateRelModel = new Relationship(
                        collection: $collection->getId(),
                        relatedCollection: $relatedCollection->getId(),
                        type: RelationType::from($type),
                        twoWay: $twoWay,
                        key: $id,
                        twoWayKey: $twoWayKey,
                        onDelete: ForeignKeyAction::from($onDelete),
                        side: RelationSide::Parent,
                    );
                    $this->adapter->createRelationship($recreateRelModel);
                } catch (\Throwable) {
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
                } catch (\Throwable) {
                    // Silent rollback — best effort
                }
            }

            // Restore junction collection metadata for M2M
            if ($deletedJunction !== null && ! $deletedJunction->isEmpty()) {
                try {
                    $this->silent(fn () => $this->createDocument(self::METADATA, $deletedJunction));
                } catch (\Throwable) {
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

        try {
            $this->trigger(self::EVENT_ATTRIBUTE_DELETE, $relationship);
        } catch (\Throwable $e) {
            // Ignore
        }

        return true;
    }

    private function getJunctionCollection(Document $collection, Document $relatedCollection, string $side): string
    {
        return $side === RelationSide::Parent->value
            ? '_'.$collection->getSequence().'_'.$relatedCollection->getSequence()
            : '_'.$relatedCollection->getSequence().'_'.$collection->getSequence();
    }
}
