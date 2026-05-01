<?php

namespace Utopia\Database\Traits;

use Exception;
use Throwable;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Index;
use Utopia\Database\SetType;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Query\Schema\ColumnType;

/**
 * Provides CRUD operations for collection indexes including creation, renaming, and deletion.
 */
trait Indexes
{
    /**
     * Create Index
     *
     * @param  string  $collection  The collection identifier
     * @param  Index  $index  The index definition to create
     * @return bool True if the index was created successfully
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     * @throws Exception
     */
    public function createIndex(string $collection, Index $index): bool
    {
        $id = $index->key;
        $type = $index->type;
        $attributes = $index->attributes;
        $lengths = $index->lengths;
        $orders = $index->orders;
        $ttl = $index->ttl;

        if (empty($attributes)) {
            throw new DatabaseException('Missing attributes');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        // index IDs are case-insensitive
        $indexes = $collection->getAttribute('indexes', []);

        /** @var array<Document> $indexes */
        foreach ($indexes as $existingIndex) {
            if (\strtolower($existingIndex->getId()) === \strtolower($id)) {
                throw new DuplicateException('Index already exists');
            }
        }

        if ($this->adapter->getCountOfIndexes($collection) >= $this->adapter->getLimitForIndexes()) {
            throw new LimitException('Index limit reached. Cannot create new index.');
        }

        /** @var array<Document> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);
        $typedCollectionAttributes = array_map(fn (Document $doc) => Attribute::fromDocument($doc), $collectionAttributes);
        $indexAttributesWithTypes = [];
        foreach ($attributes as $i => $attr) {
            // Support nested paths on object attributes using dot notation:
            // attribute.key.nestedKey -> base attribute "attribute"
            $baseAttr = $attr;
            if (\str_contains($attr, '.')) {
                $baseAttr = \explode('.', $attr, 2)[0];
            }

            foreach ($typedCollectionAttributes as $typedAttr) {
                if ($typedAttr->key === $baseAttr) {

                    $indexAttributesWithTypes[$attr] = $typedAttr->type->value;

                    /**
                     * mysql does not save length in collection when length = attributes size
                     */
                    if ($typedAttr->type === ColumnType::String) {
                        if (! empty($lengths[$i]) && $lengths[$i] === $typedAttr->size && $this->adapter->getMaxIndexLength() > 0) {
                            $lengths[$i] = null;
                        }
                    }

                    if ($typedAttr->array) {
                        if ($this->adapter->getMaxIndexLength() > 0) {
                            $lengths[$i] = self::MAX_ARRAY_INDEX_LENGTH;
                        }
                        $orders[$i] = null;
                    }
                    break;
                }
            }
        }

        // Update the index model with potentially modified lengths/orders
        $index = new Index(
            key: $id,
            type: $type,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl
        );

        $indexDoc = $index->toDocument();

        if ($this->validate) {
            /** @var array<Document> $collectionAttrsForValidation */
            $collectionAttrsForValidation = $collection->getAttribute('attributes', []);
            /** @var array<Document> $collectionIdxsForValidation */
            $collectionIdxsForValidation = $collection->getAttribute('indexes', []);

            $typedAttrsForValidation = array_map(fn (Document $doc) => Attribute::fromDocument($doc), $collectionAttrsForValidation);
            $typedIdxsForValidation = array_map(fn (Document $doc) => Index::fromDocument($doc), $collectionIdxsForValidation);

            $validator = new IndexValidator(
                $typedAttrsForValidation,
                $typedIdxsForValidation,
                $this->adapter->getMaxIndexLength(),
                $this->adapter->getInternalIndexesKeys(),
                $this->adapter->supports(Capability::IndexArray),
                $this->adapter->supports(Capability::SpatialIndexNull),
                $this->adapter->supports(Capability::SpatialIndexOrder),
                $this->adapter->supports(Capability::Vectors),
                $this->adapter->supports(Capability::DefinedAttributes),
                $this->adapter->supports(Capability::MultipleFulltextIndexes),
                $this->adapter->supports(Capability::IdenticalIndexes),
                $this->adapter->supports(Capability::ObjectIndexes),
                $this->adapter->supports(Capability::TrigramIndex),
                $this->adapter instanceof Feature\Spatial,
                $this->adapter->supports(Capability::Index),
                $this->adapter->supports(Capability::UniqueIndex),
                $this->adapter->supports(Capability::Fulltext),
                $this->adapter->supports(Capability::TTLIndexes),
                $this->adapter->supports(Capability::Objects)
            );
            if (! $validator->isValid($index)) {
                throw new IndexException($validator->getDescription());
            }
        }

        $created = false;

        try {
            $created = $this->adapter->createIndex($collection->getId(), $index, $indexAttributesWithTypes);

            if (! $created) {
                throw new DatabaseException('Failed to create index');
            }
        } catch (DuplicateException $e) {
            // Metadata check (lines above) already verified index is absent
            // from metadata. A DuplicateException from the adapter means the
            // index exists only in physical schema — an orphan from a prior
            // partial failure. Skip creation and proceed to metadata update.
        }

        $collection->setAttribute('indexes', $indexDoc, SetType::Append);

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: fn () => $this->cleanupIndex($collection->getId(), $id),
            shouldRollback: $created,
            operationDescription: "index creation '{$id}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));

        $this->trigger(Event::IndexCreate, $indexDoc);

        return true;
    }

    /**
     * Rename Index
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $old  Current index ID
     * @param  string  $new  New index ID
     * @return bool True if the index was renamed successfully
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        $index = \in_array($old, \array_map(fn ($idx) => $idx['$id'], $indexes));

        if ($index === false) {
            throw new NotFoundException('Index not found');
        }

        $indexNewExists = \in_array($new, \array_map(fn ($idx) => $idx['$id'], $indexes));

        if ($indexNewExists !== false) {
            throw new DuplicateException('Index name already used');
        }

        /** @var Document|null $indexNew */
        $indexNew = null;
        foreach ($indexes as $key => $value) {
            if ($value->getId() === $old) {
                $value->setAttribute('key', $new);
                $value->setAttribute('$id', $new);
                $indexNew = $value;
                $indexes[$key] = $value;
                break;
            }
        }

        $collection->setAttribute('indexes', $indexes);

        $renamed = false;
        try {
            $renamed = $this->adapter->renameIndex($collection->getId(), $old, $new);
            if (! $renamed) {
                throw new DatabaseException('Failed to rename index');
            }
        } catch (Throwable $e) {
            // Check if the rename already happened in schema (orphan from prior
            // partial failure where rename succeeded but metadata update and
            // rollback both failed). Verify by attempting a reverse rename — if
            // $new exists in schema, the reverse succeeds confirming a prior rename.
            try {
                $this->adapter->renameIndex($collection->getId(), $new, $old);
                // Reverse succeeded — index was at $new. Re-rename to complete.
                $renamed = $this->adapter->renameIndex($collection->getId(), $old, $new);
            } catch (Throwable) {
                // Reverse also failed — genuine error
                throw new DatabaseException("Failed to rename index '{$old}' to '{$new}': ".$e->getMessage(), previous: $e);
            }
        }

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: fn () => $this->adapter->renameIndex($collection->getId(), $new, $old),
            shouldRollback: $renamed,
            operationDescription: "index rename '{$old}' to '{$new}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));

        $this->trigger(Event::IndexRename, $indexNew);

        return true;
    }

    /**
     * Delete Index
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $id  The index identifier to delete
     * @return bool True if the index was deleted successfully
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        /** @var Document|null $indexDeleted */
        $indexDeleted = null;
        foreach ($indexes as $key => $value) {
            if ($value->getId() === $id) {
                $indexDeleted = $value;
                unset($indexes[$key]);
            }
        }

        if (\is_null($indexDeleted)) {
            throw new NotFoundException('Index not found');
        }

        $shouldRollback = false;
        $deleted = false;
        try {
            $deleted = $this->adapter->deleteIndex($collection->getId(), $id);

            if (! $deleted) {
                throw new DatabaseException('Failed to delete index');
            }
            $shouldRollback = true;
        } catch (NotFoundException) {
            // Index already absent from schema; treat as deleted
            $deleted = true;
        }

        $collection->setAttribute('indexes', \array_values($indexes));

        // Build indexAttributeTypes from collection attributes for rollback
        /** @var array<Document> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);
        $typedDeletedIndex = Index::fromDocument($indexDeleted);
        /** @var array<string, string> $indexAttributeTypes */
        $indexAttributeTypes = [];
        foreach ($typedDeletedIndex->attributes as $attr) {
            $baseAttr = \str_contains($attr, '.') ? \explode('.', $attr, 2)[0] : $attr;
            foreach ($collectionAttributes as $collectionAttribute) {
                $typedCollAttr = Attribute::fromDocument($collectionAttribute);
                if ($typedCollAttr->key === $baseAttr) {
                    $indexAttributeTypes[$attr] = $typedCollAttr->type->value;
                    break;
                }
            }
        }

        $rollbackIndex = new Index(
            key: $id,
            type: $typedDeletedIndex->type,
            attributes: $typedDeletedIndex->attributes,
            lengths: $typedDeletedIndex->lengths,
            orders: $typedDeletedIndex->orders,
            ttl: $typedDeletedIndex->ttl
        );
        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: fn () => $this->adapter->createIndex(
                $collection->getId(),
                $rollbackIndex,
                $indexAttributeTypes,
            ),
            shouldRollback: $shouldRollback,
            operationDescription: "index deletion '{$id}'",
            silentRollback: true
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));

        $this->trigger(Event::IndexDelete, $indexDeleted);

        return $deleted;
    }

    /**
     * Update index metadata. Utility method for update index methods.
     *
     * @param  callable(Document, Document, int|string): void  $updateCallback  method that receives document, and returns it with changes applied
     *
     * @throws ConflictException
     * @throws DatabaseException
     */
    protected function updateIndexMeta(string $collection, string $id, callable $updateCallback): Document
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() === self::METADATA) {
            throw new DatabaseException('Cannot update metadata indexes');
        }

        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);
        $index = \array_search($id, \array_map(fn ($idx) => $idx['$id'], $indexes));

        if ($index === false) {
            throw new NotFoundException('Index not found');
        }

        /** @var Document $indexDoc */
        $indexDoc = $indexes[$index];

        // Execute update from callback
        $updateCallback($indexDoc, $collection, $index);
        $indexes[$index] = $indexDoc;

        $collection->setAttribute('indexes', $indexes);

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: null,
            shouldRollback: false,
            operationDescription: "index metadata update '{$id}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));

        return $indexDoc;
    }

    /**
     * Cleanup an index that was created in the adapter but whose metadata
     * persistence failed.
     *
     * @param  string  $collectionId  The collection ID
     * @param  string  $indexId  The index ID
     * @param  int  $maxAttempts  Maximum retry attempts
     *
     * @throws DatabaseException If cleanup fails after all retries
     */
    private function cleanupIndex(
        string $collectionId,
        string $indexId,
        int $maxAttempts = 3
    ): void {
        $this->cleanup(
            fn () => $this->adapter->deleteIndex($collectionId, $indexId),
            'index',
            $indexId,
            $maxAttempts
        );
    }
}
