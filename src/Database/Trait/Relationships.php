<?php

namespace Utopia\Database\Trait;

use Throwable;
use Utopia\Console;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Refused as RefusedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\SetType;

trait Relationships
{
    /**
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
     * Drop the storage of a relationship created by createRelationship() whose metadata could not be written.
     *
     * @throws DatabaseException If cleanup fails after all retries
     */
    private function cleanupRelationship(string $collection, Relationship $relationship, int $maxAttempts = 3): void
    {
        if (! $this->adapterHasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }
        $adapter = $this->adapter;

        $this->cleanup(
            fn () => $adapter->deleteRelationship($collection, $relationship, RelationshipSide::Parent),
            'relationship',
            $relationship->key ?? '',
            $maxAttempts
        );
    }

    /**
     * Create a relationship from $collection, its parent side, to the related collection.
     *
     * A key left null is derived from the id of the collection on the other side, and stored resolved.
     *
     * @return Relationship The stored relationship, from the parent side, with both keys resolved
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws NotFoundException
     * @throws RefusedException When the adapter does not create the relationship
     * @throws StructureException
     */
    public function createRelationship(string $collection, Relationship $relationship): Relationship
    {
        if (! $this->adapterHasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }
        $adapter = $this->adapter;

        $collection = $this->silent(fn () => $this->findCollection($collection))
            ?? throw new NotFoundException('Collection not found');
        $relatedCollection = $this->silent(fn () => $this->findCollection($relationship->relatedCollection))
            ?? throw new NotFoundException('Related collection not found');

        $collectionId = $collection->getId();
        $relatedCollectionId = $relatedCollection->getId();
        $key = $relationship->key ?: $adapter->filter($relatedCollectionId);
        $twoWayKey = $relationship->twoWayKey ?: $adapter->filter($collectionId);
        $relationship = $relationship->apply(new RelationshipUpdate(key: $key, twoWayKey: $twoWayKey));
        $type = $relationship->type;

        foreach ($collection->attributes() as $attribute) {
            if (\strtolower($attribute->key) === \strtolower($key)) {
                throw new DuplicateException('Attribute already exists');
            }

            $existing = $attribute->relationship;
            if (
                $existing !== null
                && \strtolower($existing->twoWayKey ?? '') === \strtolower($twoWayKey)
                && $existing->relatedCollection === $relatedCollectionId
            ) {
                throw new DuplicateException('Related attribute already exists');
            }
        }

        $parent = Attribute::relationship($key, $relationship, RelationshipSide::Parent);
        $child = Attribute::relationship($twoWayKey, $relationship->inverse($collectionId), RelationshipSide::Child);

        $this->checkAttribute($collectionId, $parent);
        $this->checkAttribute($relatedCollectionId, $child);

        $junctionCollection = null;
        if ($type === RelationshipType::ManyToMany) {
            $junctionCollection = '_'.$collection->getSequence().'_'.$relatedCollection->getSequence();
            $junctionAttributes = [
                Attribute::string(key: $key, required: true),
                Attribute::string(key: $twoWayKey, required: true),
            ];
            $junctionIndexes = [
                Index::key(key: '_index_'.$key, attributes: [$key]),
                Index::key(key: '_index_'.$twoWayKey, attributes: [$twoWayKey]),
            ];
            try {
                $this->silent(fn () => $this->createCollection(Collection::create($junctionCollection, attributes: $junctionAttributes, indexes: $junctionIndexes)));
            } catch (DuplicateException) {
                try {
                    $adapter->createCollection($junctionCollection, $junctionAttributes, $junctionIndexes);
                } catch (DuplicateException) {
                    // The junction's metadata and schema both survive a prior partial failure.
                }
            }
        }

        $created = $this->createRelationshipInSchema($collectionId, $relationship, $junctionCollection);

        $collection->setAttribute(self::COLLECTION_ATTRIBUTES, $parent->toDocument(), SetType::Append);
        $relatedCollection->setAttribute(self::COLLECTION_ATTRIBUTES, $child->toDocument(), SetType::Append);

        $this->silent(function () use ($collection, $relatedCollection, $relationship, $key, $twoWayKey, $junctionCollection, $created) {
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
                    $this->forgetAttribute($collection, $key);
                    $this->forgetAttribute($relatedCollection, $twoWayKey);

                    if ($created) {
                        $this->cleanupCreatedRelationship($collection->getId(), $relationship, $junctionCollection);
                    }

                    throw new DatabaseException('Failed to create relationship: '.$error->getMessage(), previous: $error);
                }

                $committedFailure = $error;
            }

            $indexKey = '_index_'.$key;
            $twoWayIndexKey = '_index_'.$twoWayKey;
            $indexes = match ($relationship->type) {
                RelationshipType::OneToOne => $relationship->twoWay
                    ? [
                        [$collection->getId(), Index::unique(key: $indexKey, attributes: [$key])],
                        [$relatedCollection->getId(), Index::unique(key: $twoWayIndexKey, attributes: [$twoWayKey])],
                    ]
                    : [[$collection->getId(), Index::unique(key: $indexKey, attributes: [$key])]],
                RelationshipType::OneToMany => [[$relatedCollection->getId(), Index::key(key: $twoWayIndexKey, attributes: [$twoWayKey])]],
                RelationshipType::ManyToOne => [[$collection->getId(), Index::key(key: $indexKey, attributes: [$key])]],
                RelationshipType::ManyToMany => [],
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
                    $indexesCreated[] = [$indexCollection, $index->key];
                }
            } catch (Throwable $error) {
                foreach ($indexesCreated as [$createdCollection, $createdKey]) {
                    try {
                        $this->deleteIndex($createdCollection, $createdKey);
                    } catch (Throwable $cleanupError) {
                        Console::error("Failed to cleanup index '{$createdKey}': ".$cleanupError->getMessage());
                    }
                }

                $definitionsRemoved = true;
                try {
                    $this->withTransaction(function () use ($collection, $relatedCollection, $key, $twoWayKey) {
                        $this->forgetAttribute($collection, $key);
                        $this->updateDocument(self::METADATA, $collection->getId(), $collection);

                        $this->forgetAttribute($relatedCollection, $twoWayKey);
                        $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);
                    });
                } catch (Throwable $cleanupError) {
                    $definitionsRemoved = $this->failedAfterCommit($cleanupError);
                    Console::error("Failed to cleanup metadata for relationship '{$key}': ".$cleanupError->getMessage());
                }

                if ($definitionsRemoved) {
                    $this->cleanupCreatedRelationship($collection->getId(), $relationship, $junctionCollection);
                }

                throw new DatabaseException('Failed to create relationship indexes: '.$error->getMessage(), previous: $error);
            }

            if ($committedFailure !== null) {
                throw $committedFailure;
            }
        });

        $listeners = $this->listens(Event::AttributeCreate);
        if ($listeners !== []) {
            $this->dispatch(new Event\Attribute\Created($collectionId, $parent), $listeners);
        }

        return $relationship;
    }

    /**
     * Update the relationship stored under $key on $collection, from either of its sides.
     *
     * The update is read from that side: its key renames $key, its twoWayKey the key on the related collection.
     *
     * @return Relationship The stored relationship, from the side of $collection
     *
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws NotFoundException
     * @throws RefusedException When the adapter does not rename the relationship's columns
     */
    public function updateRelationship(string $collection, string $key, RelationshipUpdate $update): Relationship
    {
        if (! $this->adapterHasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }
        $adapter = $this->adapter;

        $collection = $this->getCollection($collection);
        $attributes = $collection->attributes();

        if ($update->key !== null && \in_array($update->key, self::attributeKeys($attributes), true)) {
            throw new DuplicateException('Relationship already exists');
        }

        $attribute = self::relationshipAttribute($attributes, $key);
        $current = $attribute?->relationship;
        $side = $attribute?->side;
        if ($attribute === null || $current === null || $side === null) {
            throw new NotFoundException('Relationship not found');
        }

        if ($update->key === null && $update->twoWayKey === null && $update->twoWay === null && $update->onDelete === null) {
            return $current;
        }

        $collectionId = $collection->getId();
        $relatedCollection = $this->getCollection($current->relatedCollection);
        $relatedCollectionId = $relatedCollection->getId();
        $relatedAttributes = $relatedCollection->attributes();
        $oldTwoWayKey = $current->twoWayKey ?? '';

        if ($update->twoWayKey !== null && \in_array($update->twoWayKey, self::attributeKeys($relatedAttributes), true)) {
            throw new DuplicateException('Related attribute already exists');
        }

        $inverse = self::relationshipAttribute($relatedAttributes, $oldTwoWayKey);
        if ($inverse === null || $inverse->side === null) {
            throw new NotFoundException('Attribute not found');
        }

        $updated = $current->apply($update);
        $newKey = $update->key ?? $key;
        $newTwoWayKey = $update->twoWayKey ?? $oldTwoWayKey;
        $altering = $newKey !== $key || $newTwoWayKey !== $oldTwoWayKey;
        $renamed = $updated->apply(new RelationshipUpdate(key: $newKey, twoWayKey: $newTwoWayKey));

        $adapterUpdated = false;
        if ($altering) {
            try {
                $adapterUpdated = $adapter->updateRelationship(
                    $collectionId,
                    $current,
                    $side,
                    new RelationshipUpdate(key: $newKey, twoWayKey: $newTwoWayKey, twoWay: $updated->twoWay),
                );
            } catch (Throwable $error) {
                if (! $this->adapter->supports(Capability::SchemaIntrospection) || ! $this->hasSchemaColumn($collectionId, $newKey)) {
                    if ($error instanceof DuplicateException || $error instanceof NotFoundException) {
                        throw $error;
                    }

                    throw new DatabaseException("Failed to update relationship '{$key}': ".$error->getMessage(), previous: $error);
                }

                $adapterUpdated = true;
            }

            if (! $adapterUpdated) {
                throw new RefusedException("Failed to update relationship '{$key}'");
            }
        }

        $parentAfter = Attribute::relationship($newKey, $renamed, $side);
        $inverseAfter = Attribute::relationship($newTwoWayKey, $renamed->inverse($collectionId), $inverse->side);
        $junction = $current->type === RelationshipType::ManyToMany
            ? $this->getJunctionCollection($collection, $relatedCollection, $side)
            : null;

        /** @var list<array{string, Attribute}> $updatedAttributes */
        $updatedAttributes = [];
        /** @var list<callable(): mixed> $restores */
        $restores = [];

        try {
            $this->replaceAttribute($collectionId, $key, $parentAfter);
            $updatedAttributes[] = [$collectionId, $parentAfter];
            $restores[] = fn () => $this->replaceAttribute($collectionId, $newKey, $attribute);

            $this->replaceAttribute($relatedCollectionId, $oldTwoWayKey, $inverseAfter);
            $updatedAttributes[] = [$relatedCollectionId, $inverseAfter];
            $restores[] = fn () => $this->replaceAttribute($relatedCollectionId, $newTwoWayKey, $inverse);

            if ($junction !== null) {
                $updatedAttributes[] = [$junction, $this->renameStoredAttribute($junction, $key, $newKey)];
                $restores[] = fn () => $this->renameStoredAttribute($junction, $newKey, $key);
                $updatedAttributes[] = [$junction, $this->renameStoredAttribute($junction, $oldTwoWayKey, $newTwoWayKey)];
                $restores[] = fn () => $this->renameStoredAttribute($junction, $newTwoWayKey, $oldTwoWayKey);

                $this->withRetries(fn () => $this->purgeCachedCollection($junction));
            }
        } catch (Throwable $error) {
            self::bestEffort($restores);

            if ($adapterUpdated) {
                self::bestEffort([fn () => $adapter->updateRelationship(
                    $collectionId,
                    $renamed,
                    $side,
                    new RelationshipUpdate(key: $key, twoWayKey: $oldTwoWayKey, twoWay: $updated->twoWay),
                )]);
            }

            throw $error;
        }

        $indexRenamesCompleted = [];

        try {
            foreach (self::relationshipIndexRenames($current->type, $side, $updated->twoWay, $collectionId, $relatedCollectionId, $junction, $key, $newKey, $oldTwoWayKey, $newTwoWayKey) as [$indexedCollection, $from, $to]) {
                $this->renameRelationshipIndex($indexedCollection, $from, $to);
                $indexRenamesCompleted[] = [$indexedCollection, $to, $from];
            }
        } catch (Throwable $error) {
            if ($adapterUpdated) {
                self::bestEffort([fn () => $adapter->updateRelationship(
                    $collectionId,
                    $renamed,
                    $side,
                    new RelationshipUpdate(key: $key, twoWayKey: $oldTwoWayKey, twoWay: $current->twoWay),
                )]);
            }

            self::bestEffort(\array_map(
                fn (array $rename): callable => fn () => $this->renameRelationshipIndex(...$rename),
                \array_reverse($indexRenamesCompleted),
            ));
            self::bestEffort($restores);

            throw new DatabaseException("Failed to update relationship indexes for '{$key}': ".$error->getMessage(), previous: $error);
        }

        $this->withRetries(fn () => $this->purgeCachedCollection($collectionId));
        $this->withRetries(fn () => $this->purgeCachedCollection($relatedCollectionId));

        $listeners = $this->listens(Event::AttributeUpdate);
        if ($listeners !== []) {
            foreach ($updatedAttributes as [$updatedCollection, $updatedAttribute]) {
                $this->dispatch(new Event\Attribute\Updated($updatedCollection, $updatedAttribute), $listeners);
            }
        }

        return $renamed;
    }

    /**
     * Delete the relationship stored under $key on $collection, from either of its sides, and its inverse.
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws NotFoundException
     * @throws RefusedException When the adapter does not drop the relationship
     * @throws StructureException
     */
    public function deleteRelationship(string $collection, string $key): void
    {
        if (! $this->adapterHasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }
        $adapter = $this->adapter;

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attribute = self::relationshipAttribute($collection->attributes(), $key);
        $relationship = $attribute?->relationship;
        $side = $attribute?->side;
        if ($attribute === null || $relationship === null || $side === null) {
            throw new NotFoundException('Relationship not found');
        }

        $relatedCollection = $this->silent(fn () => $this->getCollection($relationship->relatedCollection));
        $twoWayKey = $relationship->twoWayKey ?? '';

        $collectionAttributes = self::attributeDocuments($collection->attributes(), $key);
        $relatedCollectionAttributes = self::attributeDocuments($relatedCollection->attributes(), $twoWayKey);

        /** @var list<array{string, Index}> $deletedIndexes */
        $deletedIndexes = [];
        $deletedJunction = null;

        $this->silent(function () use ($collection, $relatedCollection, $relationship, $side, $key, $twoWayKey, &$deletedIndexes, &$deletedJunction) {
            if ($relationship->type === RelationshipType::ManyToMany) {
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $deletedJunction = $this->silent(fn () => $this->getDocument(self::METADATA, $junction));
                $this->deleteDocument(self::METADATA, $junction);

                return;
            }

            foreach (self::relationshipIndexes($relationship->type, $side, $relationship->twoWay, $collection->getId(), $relatedCollection->getId(), $key, $twoWayKey) as [$indexCollection, $index]) {
                $this->deleteIndex($indexCollection, $index->key);
                $deletedIndexes[] = [$indexCollection, $index];
            }
        });

        $collection = $this->silent(fn () => $this->getCollection($collection->getId()));
        $relatedCollection = $this->silent(fn () => $this->getCollection($relatedCollection->getId()));
        $collection->setAttribute(self::COLLECTION_ATTRIBUTES, $collectionAttributes);
        $relatedCollection->setAttribute(self::COLLECTION_ATTRIBUTES, $relatedCollectionAttributes);

        try {
            $shouldRollback = $this->deleteRelationshipFromSchema($collection->getId(), $relationship, $side);
        } catch (Throwable $error) {
            self::bestEffort($this->relationshipDefinitionRestores($deletedIndexes, $deletedJunction));

            throw $error;
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
            $rollbacks = [];
            if ($shouldRollback) {
                $rollbacks[] = $side === RelationshipSide::Parent
                    ? fn () => $adapter->createRelationship($collection->getId(), $relationship)
                    : fn () => $adapter->createRelationship($relatedCollection->getId(), $relationship->inverse($collection->getId()));
            }

            self::bestEffort([...$rollbacks, ...$this->relationshipDefinitionRestores($deletedIndexes, $deletedJunction)]);

            throw new DatabaseException(
                "Failed to persist metadata after retries for relationship deletion '{$key}': ".$error->getMessage(),
                previous: $error
            );
        }

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));
        $this->withRetries(fn () => $this->purgeCachedCollection($relatedCollection->getId()));

        $listeners = $this->listens(Event::AttributeDelete);
        if ($listeners !== []) {
            $this->dispatch(new Event\Attribute\Deleted($collection->getId(), $attribute), $listeners);
        }
    }

    /**
     * The steps that put back what deleteRelationship() removed before dropping the relationship: its indexes, or
     * the definition of its junction collection.
     *
     * @param  list<array{string, Index}>  $deletedIndexes
     * @return list<callable(): mixed>
     */
    private function relationshipDefinitionRestores(array $deletedIndexes, ?Document $deletedJunction): array
    {
        $restores = [];
        foreach ($deletedIndexes as [$indexCollection, $index]) {
            $restores[] = fn () => $this->createIndex($indexCollection, $index);
        }

        if ($deletedJunction !== null && ! $deletedJunction->isEmpty()) {
            $restores[] = fn () => $this->silent(fn () => $this->createDocument(self::METADATA, $deletedJunction));
        }

        return $restores;
    }

    /**
     * @return bool True when this call created the relationship, false when the schema already held it
     *
     * @throws DatabaseException When the adapter does not support relationships
     * @throws RefusedException When the adapter does not create the relationship; its junction collection is dropped
     */
    private function createRelationshipInSchema(string $collection, Relationship $relationship, ?string $junctionCollection): bool
    {
        if (! $this->adapterHasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }
        $adapter = $this->adapter;

        try {
            $created = $adapter->createRelationship($collection, $relationship);
        } catch (DuplicateException) {
            // The metadata checks found no such relationship, so the schema holds an orphan of a prior partial
            // failure: keep it and write the metadata.
            return false;
        }

        if ($created) {
            return true;
        }

        if ($junctionCollection !== null) {
            try {
                $this->silent(fn () => $this->cleanupCollection($junctionCollection));
            } catch (Throwable $error) {
                Console::error("Failed to cleanup junction collection '{$junctionCollection}': ".$error->getMessage());
            }
        }

        throw new RefusedException('Failed to create relationship');
    }

    /**
     * @return bool True when this call dropped the relationship, false when the schema no longer held it
     *
     * @throws DatabaseException When the adapter does not support relationships
     * @throws RefusedException When the adapter does not drop the relationship
     */
    private function deleteRelationshipFromSchema(string $collection, Relationship $relationship, RelationshipSide $side): bool
    {
        if (! $this->adapterHasFeature(Feature\Relationships::class)) {
            throw new DatabaseException('Adapter does not support relationships');
        }
        $adapter = $this->adapter;

        try {
            $deleted = $adapter->deleteRelationship($collection, $relationship, $side);
        } catch (NotFoundException) {
            // The relationship is already absent from the schema.
            return false;
        }

        if (! $deleted) {
            throw new RefusedException('Failed to delete relationship');
        }

        return true;
    }

    private function getJunctionCollection(Document $collection, Document $relatedCollection, RelationshipSide $side): string
    {
        return $side === RelationshipSide::Parent
            ? '_'.$collection->getSequence().'_'.$relatedCollection->getSequence()
            : '_'.$relatedCollection->getSequence().'_'.$collection->getSequence();
    }

    /**
     * Drop what createRelationship() created for a relationship whose metadata it then had to remove.
     */
    private function cleanupCreatedRelationship(string $collection, Relationship $relationship, ?string $junctionCollection): void
    {
        try {
            $this->cleanupRelationship($collection, $relationship);
        } catch (Throwable $cleanupError) {
            Console::error("Failed to cleanup relationship '{$relationship->key}': ".$cleanupError->getMessage());
        }

        if ($junctionCollection === null) {
            return;
        }

        try {
            $this->cleanupCollection($junctionCollection);
        } catch (Throwable $cleanupError) {
            Console::error("Failed to cleanup junction collection '{$junctionCollection}': ".$cleanupError->getMessage());
        }
    }

    /**
     * The indexes createRelationship() creates for a relationship, by the collection each is on, seen from $side.
     *
     * @return list<array{string, Index}>
     */
    private static function relationshipIndexes(
        RelationshipType $type,
        RelationshipSide $side,
        bool $twoWay,
        string $collection,
        string $relatedCollection,
        string $key,
        string $twoWayKey,
    ): array {
        $parent = $side === RelationshipSide::Parent;
        $own = [$collection, '_index_'.$key, $key];
        $other = [$relatedCollection, '_index_'.$twoWayKey, $twoWayKey];

        [$unique, $keyed] = match ($type) {
            RelationshipType::OneToOne => [$twoWay ? ($parent ? [$own, $other] : [$other, $own]) : [$parent ? $own : $other], []],
            RelationshipType::OneToMany => [[], [$parent ? $other : $own]],
            RelationshipType::ManyToOne => [[], [$parent ? $own : $other]],
            RelationshipType::ManyToMany => [[], []],
        };

        $indexes = [];
        foreach ($unique as [$indexCollection, $indexKey, $indexed]) {
            $indexes[] = [$indexCollection, Index::unique(key: $indexKey, attributes: [$indexed])];
        }
        foreach ($keyed as [$indexCollection, $indexKey, $indexed]) {
            $indexes[] = [$indexCollection, Index::key(key: $indexKey, attributes: [$indexed])];
        }

        return $indexes;
    }

    /**
     * The relationship indexes a rename of its keys renames, as [collection, from key, to key].
     *
     * @return list<array{string, string, string}>
     */
    private static function relationshipIndexRenames(
        RelationshipType $type,
        RelationshipSide $side,
        bool $twoWay,
        string $collection,
        string $relatedCollection,
        ?string $junction,
        string $key,
        string $newKey,
        string $twoWayKey,
        string $newTwoWayKey,
    ): array {
        $own = $key !== $newKey ? [[$collection, $key, $newKey]] : [];
        $other = $twoWayKey !== $newTwoWayKey ? [[$relatedCollection, $twoWayKey, $newTwoWayKey]] : [];

        return match ($type) {
            RelationshipType::OneToOne => match (true) {
                $twoWay => [...$own, ...$other],
                $side === RelationshipSide::Parent => $own,
                default => $other,
            },
            RelationshipType::OneToMany => $side === RelationshipSide::Parent ? $other : $own,
            RelationshipType::ManyToOne => $side === RelationshipSide::Parent ? $own : $other,
            RelationshipType::ManyToMany => $junction === null ? [] : [
                ...($key !== $newKey ? [[$junction, $key, $newKey]] : []),
                ...($twoWayKey !== $newTwoWayKey ? [[$junction, $twoWayKey, $newTwoWayKey]] : []),
            ],
        };
    }

    /**
     * Point the relationship index on $key at $newKey and rename it after it.
     */
    private function renameRelationshipIndex(string $collection, string $key, string $newKey): void
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));
        $indexKey = '_index_'.$key;

        $indexes = $definition->indexes();
        $found = false;
        foreach ($indexes as $position => $index) {
            if ($index->key === $indexKey) {
                $indexes[$position] = Index::fromDocument($index->toDocument()->setAttribute(self::INDEX_ATTRIBUTES, [$newKey]));
                $found = true;
                break;
            }
        }

        if (! $found) {
            throw new NotFoundException('Index not found');
        }

        $definition->setAttribute(self::COLLECTION_INDEXES, \array_map(static fn (Index $index): Document => $index->toDocument(), $indexes));
        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: null,
            shouldRollback: false,
            operationDescription: "index metadata update '{$indexKey}'"
        );
        $this->withRetries(fn () => $this->purgeCachedCollection($collection));

        $this->silent(fn () => $this->renameIndex($collection, $indexKey, '_index_'.$newKey));
    }

    /**
     * Store $attribute in place of the attribute stored under $key on $collection.
     *
     * @throws NotFoundException
     */
    private function replaceAttribute(string $collection, string $key, Attribute $attribute): Attribute
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));

        $attributes = $definition->attributes();
        $found = false;
        foreach ($attributes as $position => $stored) {
            if ($stored->key === $key) {
                $attributes[$position] = $attribute;
                $found = true;
                break;
            }
        }

        if (! $found) {
            throw new NotFoundException('Attribute not found');
        }

        $definition->setAttribute(self::COLLECTION_ATTRIBUTES, \array_map(static fn (Attribute $stored): Document => $stored->toDocument(), $attributes));
        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: null,
            shouldRollback: false,
            operationDescription: "attribute metadata update '{$key}'"
        );
        $this->withRetries(fn () => $this->purgeCachedCollection($collection));

        return $attribute;
    }

    /**
     * Rename the attribute stored under $key on $collection, a junction collection, to $newKey.
     *
     * @throws NotFoundException
     */
    private function renameStoredAttribute(string $collection, string $key, string $newKey): Attribute
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));
        foreach ($definition->attributes() as $stored) {
            if ($stored->key === $key) {
                return $this->replaceAttribute($collection, $key, $stored->apply(new AttributeUpdate(key: $newKey)));
            }
        }

        throw new NotFoundException('Attribute not found');
    }

    /**
     * Remove the attribute stored under $key from the collection's metadata, without writing it.
     */
    private function forgetAttribute(Collection $collection, string $key): void
    {
        $collection->setAttribute(self::COLLECTION_ATTRIBUTES, self::attributeDocuments($collection->attributes(), $key));
    }

    /**
     * The stored form of $attributes, the first one under $without left out.
     *
     * @param  list<Attribute>  $attributes
     * @return list<Document>
     */
    private static function attributeDocuments(array $attributes, string $without): array
    {
        $documents = [];
        $removed = false;
        foreach ($attributes as $attribute) {
            if (! $removed && $attribute->key === $without) {
                $removed = true;

                continue;
            }
            $documents[] = $attribute->toDocument();
        }

        return $documents;
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private static function relationshipAttribute(array $attributes, string $key): ?Attribute
    {
        foreach ($attributes as $attribute) {
            if ($attribute->key === $key) {
                return $attribute->relationship === null ? null : $attribute;
            }
        }

        return null;
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<string>
     */
    private static function attributeKeys(array $attributes): array
    {
        return \array_map(static fn (Attribute $attribute): string => $attribute->key, $attributes);
    }

    /**
     * Whether the engine's schema of $collection already holds a column named $key, as a rename that completed
     * before a prior partial failure leaves it.
     */
    private function hasSchemaColumn(string $collection, string $key): bool
    {
        $filtered = \strtolower($this->adapter->filter($key));
        foreach ($this->getSchemaAttributes($collection) as $column) {
            if (\strtolower($column->name) === $filtered) {
                return true;
            }
        }

        return false;
    }

    /**
     * Run each step of a rollback, carrying on past a step that fails.
     *
     * @param  list<callable(): mixed>  $steps
     */
    private static function bestEffort(array $steps): void
    {
        foreach ($steps as $step) {
            try {
                $step();
            } catch (Throwable) {
                // A rollback step that fails leaves the rest to run.
            }
        }
    }
}
