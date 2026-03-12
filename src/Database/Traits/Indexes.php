<?php

namespace Utopia\Database\Traits;

use Exception;
use Utopia\Database\Capability;
use Utopia\Database\Document;
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
use Utopia\Query\Schema\IndexType;

trait Indexes
{
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

        $indexes = $collection->getAttribute('indexes', []);
        $index = \array_search($id, \array_map(fn ($index) => $index['$id'], $indexes));

        if ($index === false) {
            throw new NotFoundException('Index not found');
        }

        // Execute update from callback
        $updateCallback($indexes[$index], $collection, $index);

        $collection->setAttribute('indexes', $indexes);

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: null,
            shouldRollback: false,
            operationDescription: "index metadata update '{$id}'"
        );

        return $indexes[$index];
    }

    /**
     * Rename Index
     *
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

        $indexes = $collection->getAttribute('indexes', []);

        $index = \in_array($old, \array_map(fn ($index) => $index['$id'], $indexes));

        if ($index === false) {
            throw new NotFoundException('Index not found');
        }

        $indexNew = \in_array($new, \array_map(fn ($index) => $index['$id'], $indexes));

        if ($indexNew !== false) {
            throw new DuplicateException('Index name already used');
        }

        foreach ($indexes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $old) {
                $indexes[$key]['key'] = $new;
                $indexes[$key]['$id'] = $new;
                $indexNew = $indexes[$key];
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
        } catch (\Throwable $e) {
            // Check if the rename already happened in schema (orphan from prior
            // partial failure where rename succeeded but metadata update and
            // rollback both failed). Verify by attempting a reverse rename — if
            // $new exists in schema, the reverse succeeds confirming a prior rename.
            try {
                $this->adapter->renameIndex($collection->getId(), $new, $old);
                // Reverse succeeded — index was at $new. Re-rename to complete.
                $renamed = $this->adapter->renameIndex($collection->getId(), $old, $new);
            } catch (\Throwable) {
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

        try {
            $this->trigger(self::EVENT_INDEX_RENAME, $indexNew);
        } catch (\Throwable $e) {
            // Ignore
        }

        return true;
    }

    /**
     * Create Index
     *
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
        $indexAttributesWithTypes = [];
        foreach ($attributes as $i => $attr) {
            // Support nested paths on object attributes using dot notation:
            // attribute.key.nestedKey -> base attribute "attribute"
            $baseAttr = $attr;
            if (\str_contains($attr, '.')) {
                $baseAttr = \explode('.', $attr, 2)[0] ?? $attr;
            }

            foreach ($collectionAttributes as $collectionAttribute) {
                if ($collectionAttribute->getAttribute('key') === $baseAttr) {

                    $attributeType = $collectionAttribute->getAttribute('type');
                    $indexAttributesWithTypes[$attr] = $attributeType;

                    /**
                     * mysql does not save length in collection when length = attributes size
                     */
                    if ($attributeType === ColumnType::String->value) {
                        if (! empty($lengths[$i]) && $lengths[$i] === $collectionAttribute->getAttribute('size') && $this->adapter->getMaxIndexLength() > 0) {
                            $lengths[$i] = null;
                        }
                    }

                    $isArray = $collectionAttribute->getAttribute('array', false);
                    if ($isArray) {
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

            $validator = new IndexValidator(
                $collection->getAttribute('attributes', []),
                $collection->getAttribute('indexes', []),
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
                $this->adapter->supports(Capability::Spatial),
                $this->adapter->supports(Capability::Index),
                $this->adapter->supports(Capability::UniqueIndex),
                $this->adapter->supports(Capability::Fulltext),
                $this->adapter->supports(Capability::TTLIndexes),
                $this->adapter->supports(Capability::Objects)
            );
            if (! $validator->isValid($indexDoc)) {
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

        $this->trigger(self::EVENT_INDEX_CREATE, $indexDoc);

        return true;
    }

    /**
     * Delete Index
     *
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        $indexes = $collection->getAttribute('indexes', []);

        $indexDeleted = null;
        foreach ($indexes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $id) {
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
        $indexAttributeTypes = [];
        foreach ($indexDeleted->getAttribute('attributes', []) as $attr) {
            $baseAttr = \str_contains($attr, '.') ? \explode('.', $attr, 2)[0] : $attr;
            foreach ($collectionAttributes as $collectionAttribute) {
                if ($collectionAttribute->getAttribute('key') === $baseAttr) {
                    $indexAttributeTypes[$attr] = $collectionAttribute->getAttribute('type');
                    break;
                }
            }
        }

        $rollbackIndex = new Index(
            key: $id,
            type: IndexType::from($indexDeleted->getAttribute('type')),
            attributes: $indexDeleted->getAttribute('attributes', []),
            lengths: $indexDeleted->getAttribute('lengths', []),
            orders: $indexDeleted->getAttribute('orders', []),
            ttl: $indexDeleted->getAttribute('ttl', 1)
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

        try {
            $this->trigger(self::EVENT_INDEX_DELETE, $indexDeleted);
        } catch (\Throwable $e) {
            // Ignore
        }

        return $deleted;
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
