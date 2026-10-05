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
use Utopia\Database\Storage;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

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
        $id = $index->getKey();
        $type = $index->getType();
        $attributes = $index->getIndexedAttributes();
        $lengths = $index->getLengths();
        $orders = $index->getOrders();
        $ttl = $index->getTtl();

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

        /** @var array<Attribute> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);
        $indexAttributesWithTypes = [];
        foreach ($attributes as $position => $attribute) {
            // Support nested paths on object attributes using dot notation:
            // attribute.key.nestedKey -> base attribute "attribute"
            $baseAttribute = $attribute;
            if (\str_contains($attribute, '.')) {
                $baseAttribute = \explode('.', $attribute, 2)[0];
            }

            foreach ($collectionAttributes as $typedAttribute) {
                if ($typedAttribute->getKey() === $baseAttribute) {

                    $indexAttributesWithTypes[$attribute] = $typedAttribute->getType()->value;

                    /**
                     * mysql does not save length in collection when length = attributes size
                     */
                    if ($typedAttribute->getType() === ColumnType::String) {
                        if (! empty($lengths[$position]) && $lengths[$position] === $typedAttribute->getSize() && $this->adapter->getMaxIndexLength() > 0) {
                            $lengths[$position] = null;
                        }
                    }

                    if ($typedAttribute->isArray()) {
                        if ($this->adapter->getMaxIndexLength() > 0) {
                            $lengths[$position] = self::MAX_ARRAY_INDEX_LENGTH;
                        }
                        $orders[$position] = null;
                    }
                    break;
                }
            }
        }

        $index = new Index(
            key: $id,
            type: $type,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl
        );

        if ($this->validation()->get()) {
            /** @var array<Index> $collectionIndexes */
            $collectionIndexes = $collection->getAttribute('indexes', []);

            $validator = new IndexValidator(
                $collectionAttributes,
                $collectionIndexes,
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
                $this->adapter->hasFeature(Feature\Spatial::class),
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

        if (! $this->reconcileSchemaOnlyIndex($collection->getId(), $index)) {
            try {
                $created = $this->adapter->createIndex($collection->getId(), $index, $indexAttributesWithTypes);

                if (! $created) {
                    throw new DatabaseException('Failed to create index');
                }
            } catch (DuplicateException) {
                // Metadata check (lines above) already verified index is absent
                // from metadata. A DuplicateException from the adapter means the
                // index exists only in physical schema — an orphan from a prior
                // partial failure. Skip creation and proceed to metadata update.
            }
        }

        $collection->setAttribute('indexes', $index, SetType::Append);

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: fn () => $this->cleanupIndex($collection->getId(), $id),
            shouldRollback: $created,
            operationDescription: "index creation '{$id}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));

        $this->triggerHooks(
            Event::IndexCreate,
            $index->toDocument()->setAttribute(Document::COLLECTION, $collection->getId()),
        );

        return true;
    }

    /**
     * An index in the schema but not in this collection's metadata is reused when its
     * definition matches the request, and dropped to be recreated otherwise. Under shared
     * tables it serves another tenant's collection, so a mismatch is refused instead.
     *
     * @return bool True when the existing index is reused
     *
     * @throws DuplicateException
     */
    private function reconcileSchemaOnlyIndex(string $collection, Index $index): bool
    {
        if (! $this->adapter->hasFeature(Feature\SchemaIndexes::class)
            || ($this->getSharedTables() && $this->isMigrating())) {
            return false;
        }

        $id = \strtolower($this->adapter->filter($index->getKey()));
        foreach ($this->adapter->getInternalIndexesKeys() as $internal) {
            if (\strtolower($this->adapter->filter($internal)) === $id) {
                return false;
            }
        }

        foreach ($this->getSchemaIndexes($collection) as $schemaIndex) {
            if (\strtolower($schemaIndex->getId()) !== $id) {
                continue;
            }

            if ($this->schemaIndexMatches($schemaIndex, $index)) {
                return true;
            }

            if ($this->getSharedTables()) {
                throw new DuplicateException('Index exists in the shared table with another definition');
            }

            try {
                $this->adapter->deleteIndex($collection, $index->getKey());
            } catch (NotFoundException) {
                // Already absent from the schema
            }

            return false;
        }

        return false;
    }

    private function schemaIndexMatches(Document $schemaIndex, Index $index): bool
    {
        $rawColumns = $schemaIndex->getAttribute('columns', []);
        $rawLengths = $schemaIndex->getAttribute('lengths', []);
        $schemaLengths = \is_array($rawLengths) ? \array_values($rawLengths) : [];

        $columns = [];
        $lengths = [];
        foreach (\is_array($rawColumns) ? \array_values($rawColumns) : [] as $position => $column) {
            $length = $schemaLengths[$position] ?? null;
            $columns[] = \is_string($column) ? \strtolower($column) : '';
            $lengths[] = \is_numeric($length) ? (int) $length : 0;
        }

        if ($this->getSharedTables() && ($columns[0] ?? '') === Storage::TENANT) {
            \array_shift($columns);
            \array_shift($lengths);
        }

        $indexedAttributes = $index->getIndexedAttributes();
        if (\count($columns) !== \count($indexedAttributes)) {
            return false;
        }

        $indexLengths = $index->getLengths();
        foreach (\array_values($indexedAttributes) as $position => $attribute) {
            if ($columns[$position] === '') {
                continue;
            }
            if ($columns[$position] !== \strtolower($this->adapter->filter(Storage::column($attribute)))) {
                return false;
            }
            if ($lengths[$position] !== (int) ($indexLengths[$position] ?? 0)) {
                return false;
            }
        }

        $indexType = $schemaIndex->getAttribute('indexType', '');
        $nonUnique = $schemaIndex->getAttribute('nonUnique', 1);
        $schemaType = match (\is_string($indexType) ? \strtoupper($indexType) : '') {
            'FULLTEXT' => IndexType::Fulltext,
            'SPATIAL' => IndexType::Spatial,
            default => \is_numeric($nonUnique) && (int) $nonUnique === 0 ? IndexType::Unique : IndexType::Key,
        };
        $requestedType = $index->getType() === IndexType::Index ? IndexType::Key : $index->getType();

        return $schemaType === $requestedType;
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

        $index = \in_array($old, \array_map(fn ($idx) => $idx[Document::ID], $indexes));

        if ($index === false) {
            throw new NotFoundException('Index not found');
        }

        $indexNewExists = \in_array($new, \array_map(fn ($idx) => $idx[Document::ID], $indexes));

        if ($indexNewExists !== false) {
            throw new DuplicateException('Index name already used');
        }

        /** @var Document|null $indexNew */
        $indexNew = null;
        foreach ($indexes as $key => $value) {
            if ($value->getId() === $old) {
                $value->setAttribute('key', $new);
                $value->setAttribute(Document::ID, $new);
                $indexNew = $value;
                $indexes[$key] = $value;
                break;
            }
        }

        if ($indexNew === null) {
            throw new NotFoundException('Index not found');
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
                if (! $this->adapter->renameIndex($collection->getId(), $new, $old)) {
                    throw new DatabaseException('Failed to rename index');
                }
                // Reverse succeeded — index was at $new. Re-rename to complete.
                $renamed = $this->adapter->renameIndex($collection->getId(), $old, $new);
                if (! $renamed) {
                    throw new DatabaseException('Failed to rename index');
                }
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

        $this->triggerHooks(
            Event::IndexRename,
            (clone $indexNew)->setAttribute(Document::COLLECTION, $collection->getId()),
        );

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

        /** @var array<Index> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        /** @var Index|null $indexDeleted */
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

        /** @var array<Attribute> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);
        $typedDeletedIndex = $indexDeleted;
        /** @var array<string, string> $indexAttributeTypes */
        $indexAttributeTypes = [];
        foreach ($typedDeletedIndex->getIndexedAttributes() as $attribute) {
            $baseAttribute = \str_contains($attribute, '.') ? \explode('.', $attribute, 2)[0] : $attribute;
            foreach ($collectionAttributes as $collectionAttribute) {
                if ($collectionAttribute->getKey() === $baseAttribute) {
                    $indexAttributeTypes[$attribute] = $collectionAttribute->getType()->value;
                    break;
                }
            }
        }

        $rollbackIndex = new Index(
            key: $id,
            type: $typedDeletedIndex->getType(),
            attributes: $typedDeletedIndex->getIndexedAttributes(),
            lengths: $typedDeletedIndex->getLengths(),
            orders: $typedDeletedIndex->getOrders(),
            ttl: $typedDeletedIndex->getTtl()
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

        $this->triggerHooks(
            Event::IndexDelete,
            $indexDeleted->toDocument()->setAttribute(Document::COLLECTION, $collection->getId()),
        );

        return $deleted;
    }

    /**
     * Update index metadata. Utility method for update index methods.
     *
     * @param  callable(Index, Document, int|string): void  $updateCallback
     *
     * @throws ConflictException
     * @throws DatabaseException
     */
    protected function updateIndexMeta(string $collection, string $id, callable $updateCallback): Index
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() === self::METADATA) {
            throw new DatabaseException('Cannot update metadata indexes');
        }

        /** @var array<Index> $indexes */
        $indexes = $collection->getAttribute('indexes', []);
        $index = \array_search($id, \array_map(fn (Index $candidate) => $candidate->getKey(), $indexes), true);

        if ($index === false) {
            throw new NotFoundException('Index not found');
        }

        $indexModel = $indexes[$index];

        $updateCallback($indexModel, $collection, $index);
        $indexes[$index] = $indexModel;

        $collection->setAttribute('indexes', $indexes);

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: null,
            shouldRollback: false,
            operationDescription: "index metadata update '{$id}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));

        return $indexModel;
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
