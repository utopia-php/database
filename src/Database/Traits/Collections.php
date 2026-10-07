<?php

namespace Utopia\Database\Traits;

use Exception;
use Throwable;
use Utopia\Console;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Database\Validator\Permissions;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * Provides CRUD operations for database collections including creation, listing, sizing, and deletion.
 */
trait Collections
{
    /** The metadata collection's model, built once per process; every read gets a deep clone. */
    private static ?Collection $metadataCollection = null;

    /**
     * Create Collection
     *
     * @param  Collection  $collection  Collection to create
     * @return Collection The created collection metadata document
     *
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     */
    public function createCollection(Collection $collection): Collection
    {
        $id = $collection->getId();
        $name = $collection->getName();
        if ($name === '') {
            $name = $id;
        }
        $attributes = \array_map(static fn (Attribute $attribute): Attribute => clone $attribute, $collection->getDeclaredAttributes());
        $indexes = \array_map(static fn (Index $index): Index => clone $index, $collection->getIndexes());
        $permissions = $collection->getDeclaredPermissions() ?? [Permission::create(Role::any())];
        $documentSecurity = $collection->hasDocumentSecurity();
        $metadata = $collection->metadata;

        foreach ($attributes as $attribute) {
            if (in_array($attribute->getType(), Database::ATTRIBUTE_FILTER_COLUMN_TYPES, true)) {
                $existingFilters = $attribute->getFilters();
                $attribute->setFilters(array_values(
                    array_unique(array_merge($existingFilters, [$attribute->getType()->value]))
                ));
            }
        }

        $typeValidator = $this->typeValidator();
        foreach ($attributes as $attribute) {
            $typeValidator->checkType($attribute);
        }

        if ($this->validation()->get()) {
            $validator = new Permissions();
            if (! $validator->isValid($permissions)) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        $collection = $this->silent(fn () => $this->getCollection($id));

        if (! $collection->isEmpty() && $id !== self::METADATA) {
            throw new DuplicateException('Collection '.$id.' already exists');
        }

        if ($this->validation()->get() && $this->adapter->supports(Capability::TTLIndexes)) {
            $ttlIndexes = array_filter($indexes, fn (Index $index) => $index->getType() === IndexType::Ttl);
            if (count($ttlIndexes) > 1) {
                throw new IndexException('There can be only one TTL index in a collection');
            }
        }

        foreach ($indexes as $key => $index) {
            $lengths = $index->getLengths();
            $orders = $index->getOrders();

            foreach ($index->getIndexedAttributes() as $position => $attributeKey) {
                foreach ($attributes as $collectionAttribute) {
                    if ($collectionAttribute->getKey() === $attributeKey) {
                        /**
                         * mysql does not save length in collection when length = attributes size
                         */
                        if ($collectionAttribute->getType() === ColumnType::String) {
                            if (! empty($lengths[$position]) && $lengths[$position] === $collectionAttribute->getSize() && $this->adapter->getMaxIndexLength() > 0) {
                                $lengths[$position] = null;
                            }
                        }

                        $isArray = $collectionAttribute->isArray();
                        if ($isArray) {
                            if ($this->adapter->getMaxIndexLength() > 0) {
                                $lengths[$position] = self::MAX_ARRAY_INDEX_LENGTH;
                            }
                            $orders[$position] = null;
                        }
                        break;
                    }
                }
            }

            $index->setLengths($lengths);
            $index->setOrders($orders);
            $indexes[$key] = $index;
        }

        $collection = new Document(\array_merge([
            Document::ID => ID::custom($id),
            Document::PERMISSIONS => $permissions,
            'name' => $name,
            'attributes' => $attributes,
            'indexes' => $indexes,
            'documentSecurity' => $documentSecurity,
        ], $metadata));

        if ($this->validation()->get()) {
            $validator = new IndexValidator(
                $attributes,
                [],
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
            foreach ($indexes as $index) {
                if (! $validator->isValid($index)) {
                    throw new IndexException($validator->getDescription());
                }
            }
        }

        if ($indexes && $this->adapter->getCountOfIndexes($collection) > $this->adapter->getLimitForIndexes()) {
            throw new LimitException('Index limit of '.$this->adapter->getLimitForIndexes().' exceeded. Cannot create collection.');
        }

        if ($attributes) {
            if (
                $this->adapter->getLimitForAttributes() > 0 &&
                $this->adapter->getCountOfAttributes($collection) > $this->adapter->getLimitForAttributes()
            ) {
                throw new LimitException('Attribute limit of '.$this->adapter->getLimitForAttributes().' exceeded. Cannot create collection.');
            }

            if (
                $this->adapter->getDocumentSizeLimit() > 0 &&
                $this->adapter->getAttributeWidth($collection) > $this->adapter->getDocumentSizeLimit()
            ) {
                throw new LimitException('Document size limit of '.$this->adapter->getDocumentSizeLimit().' exceeded. Cannot create collection.');
            }
        }

        $created = false;

        try {
            $this->adapter->createCollection($id, $attributes, $indexes);
            $created = true;
        } catch (DuplicateException $error) {
            if ($id === self::METADATA
                || ($this->adapter->getSharedTables()
                    && $this->adapter->exists($this->adapter->getDatabase(), $id))) {
                // The metadata table must never be dropped during reconciliation.
                // In shared-tables mode the physical table is reused across
                // tenants. A DuplicateException simply means the table already
                // exists for another tenant — not an orphan.
            } else {
                // The table exists and this process did not create it. It may
                // belong to a peer that has not committed metadata yet, or it
                // may be an orphan. Dropping it destroyed live collections
                // during concurrent boot; attaching this caller's metadata to
                // an unknown physical schema can invent columns that are not
                // there. Leave the table and report Duplicate. Claiming the
                // metadata row first is #939.
                try {
                    $this->purgeCachedDocument(self::METADATA, $id);
                } catch (Throwable $cacheError) {
                    Console::warning('Warning: Failed to purge stale collection cache: '.$cacheError->getMessage());
                }
                throw new DuplicateException('Collection '.$id.' already exists', previous: $error);
            }
        }

        if ($id === self::METADATA) {
            return $this->hydrateCollectionModels(Collection::fromArray(self::collectionMeta()));
        }

        try {
            $createdCollection = $this->silent(fn () => $this->createDocument(self::METADATA, $collection));
        } catch (DuplicateException $error) {
            // A concurrent creator committed the metadata for this id first, so
            // the physical table is the one its metadata describes. Rolling back
            // here would drop a live collection out from under it.
            try {
                $this->purgeCachedDocument(self::METADATA, $id);
            } catch (Throwable $cacheError) {
                Console::warning('Warning: Failed to purge stale collection cache: '.$cacheError->getMessage());
            }
            throw new DuplicateException('Collection '.$id.' already exists', previous: $error);
        } catch (Throwable $error) {
            if ($this->mayHaveCommitted($error)) {
                throw $error;
            }

            if ($created) {
                try {
                    $this->cleanupCollection($id);
                } catch (Throwable $cleanupError) {
                    Console::error("Failed to rollback collection '{$id}': ".$cleanupError->getMessage());
                }
            }
            throw new DatabaseException("Failed to create collection metadata for '{$id}': ".$error->getMessage(), previous: $error);
        }

        $this->triggerHooks(Event::CollectionCreate, $createdCollection);

        return $this->hydrateCollectionModels($createdCollection);
    }

    /**
     * Update Collections Permissions.
     *
     * @param  string  $id  The collection identifier
     * @param  array<string>  $permissions  New permission strings
     * @param  bool  $documentSecurity  Whether to enable document-level security
     * @return Document The updated collection metadata document
     *
     * @throws ConflictException
     * @throws DatabaseException
     */
    public function updateCollection(string $id, array $permissions, bool $documentSecurity): Document
    {
        if ($this->validation()->get()) {
            $validator = new Permissions();
            if (! $validator->isValid($permissions)) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        $collection = $this->silent(fn () => $this->getCollection($id));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        if (
            $this->adapter->getSharedTables()
            && $collection->getTenant() !== $this->adapter->getTenant()
        ) {
            throw new NotFoundException('Collection not found');
        }

        $collection
            ->setAttribute(Document::PERMISSIONS, $permissions)
            ->setAttribute('documentSecurity', $documentSecurity);

        $collection = $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));

        $this->triggerHooks(Event::CollectionUpdate, $collection);

        return $collection;
    }

    /**
     * Get Collection
     *
     * @param  string  $id  The collection identifier
     * @return Collection The collection metadata document, or an empty Collection if not found
     *
     * @throws DatabaseException
     */
    public function getCollection(string $id): Collection
    {
        if ($id === self::METADATA) {
            $collection = clone (self::$metadataCollection ??= $this->hydrateCollectionModels(new Document(self::collectionMeta())));
            $this->trigger(Event::CollectionRead, $collection);

            return $collection;
        }

        $definition = $this->silent(fn () => $this->getDocument(self::METADATA, $id));

        if (
            $id !== self::METADATA
            && $this->adapter->getSharedTables()
            && $definition->getTenant() !== null
            && $definition->getTenant() !== $this->adapter->getTenant()
        ) {
            return new Collection();
        }

        $collection = $this->hydrateCollectionModels($definition);
        $this->attachCollectionCacheEpoch($collection, $this->getCollectionCacheEpoch($definition));

        $this->trigger(Event::CollectionRead, $collection);

        return $collection;
    }

    private function hydrateCollectionModels(Document $collection): Collection
    {
        if ($collection->isEmpty()) {
            return $collection instanceof Collection ? $collection : new Collection();
        }

        if (! $collection instanceof Collection) {
            $collection = Collection::fromArray($collection->getArrayCopy());
        }

        $attributes = $collection->getAttribute('attributes', []);
        if (\is_array($attributes)) {
            $hydrated = [];
            $changed = false;
            foreach ($attributes as $attr) {
                if ($attr instanceof Attribute) {
                    $hydrated[] = $attr;
                    continue;
                }
                if (! \is_array($attr)) {
                    throw new DatabaseException('Collection attributes must be Attribute models');
                }
                $changed = true;
                $typed = [];
                foreach ($attr as $name => $item) {
                    if (\is_string($name)) {
                        $typed[$name] = $item;
                    }
                }
                $hydrated[] = Attribute::fromArray($typed);
            }
            if ($changed) {
                $collection->setAttribute('attributes', $hydrated);
            }
        }

        $indexes = $collection->getAttribute('indexes', []);
        if (\is_array($indexes)) {
            $hydrated = [];
            $changed = false;
            foreach ($indexes as $idx) {
                if ($idx instanceof Index) {
                    $hydrated[] = $idx;
                    continue;
                }
                if (! \is_array($idx)) {
                    throw new DatabaseException('Collection indexes must be Index models');
                }
                $changed = true;
                $typed = [];
                foreach ($idx as $name => $item) {
                    if (\is_string($name)) {
                        $typed[$name] = $item;
                    }
                }
                $hydrated[] = Index::fromArray($typed);
            }
            if ($changed) {
                $collection->setAttribute('indexes', $hydrated);
            }
        }

        return $collection;
    }

    /**
     * List Collections
     *
     * @param  int  $limit  Maximum number of collections to return
     * @param  int  $offset  Number of collections to skip
     * @return array<Collection>
     *
     * @throws Exception
     */
    public function listCollections(int $limit = 25, int $offset = 0): array
    {
        $result = $this->silent(fn () => $this->find(self::METADATA, [
            Query::limit($limit),
            Query::offset($offset),
        ]));

        foreach ($result as $i => $listed) {
            $result[$i] = $this->hydrateCollectionModels($listed);
        }

        $this->trigger(Event::CollectionList, $result);

        return $result;
    }

    /**
     * Get Collection Size
     *
     * @param  string  $collection  The collection identifier
     * @return int The number of documents in the collection
     *
     * @throws Exception
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        if ($this->adapter->getSharedTables() && $collection->getTenant() !== $this->adapter->getTenant()) {
            throw new NotFoundException('Collection not found');
        }

        return $this->adapter->getSizeOfCollection($collection->getId());
    }

    /**
     * Get Collection Size on disk
     *
     * @param  string  $collection  The collection identifier
     * @return int The collection size in bytes on disk
     *
     * @throws DatabaseException
     * @throws NotFoundException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        if ($this->adapter->getSharedTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        if ($this->adapter->getSharedTables() && $collection->getTenant() !== $this->adapter->getTenant()) {
            throw new NotFoundException('Collection not found');
        }

        return $this->adapter->getSizeOfCollectionOnDisk($collection->getId());
    }

    /**
     * Analyze a collection updating its metadata on the database engine.
     *
     * @param  string  $collection  The collection identifier
     * @return bool True if the analysis completed successfully
     */
    public function analyzeCollection(string $collection): bool
    {
        return $this->adapter->analyzeCollection($collection);
    }

    /**
     * Delete Collection
     *
     * @param  string  $id  The collection identifier
     * @return bool True if the collection was successfully deleted
     *
     * @throws DatabaseException
     */
    public function deleteCollection(string $id): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($id));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        if ($this->adapter->getSharedTables() && $collection->getTenant() !== $this->adapter->getTenant()) {
            throw new NotFoundException('Collection not found');
        }

        /** @var array<Attribute> $allAttributes */
        $allAttributes = $collection->getAttribute('attributes', []);
        $relationships = \array_filter(
            $allAttributes,
            fn (Attribute $attribute) => $attribute->getType() === ColumnType::Relationship
        );

        $collectionId = $collection->getId();
        foreach ($relationships as $relationship) {
            $this->deleteRelationship($collectionId, $relationship->getKey());
        }

        // Re-fetch collection to get current state after relationship deletions
        $currentCollection = $this->silent(fn () => $this->getCollection($id));
        /** @var array<Attribute> $currentAttributes */
        $currentAttributes = $currentCollection->isEmpty() ? [] : $currentCollection->getAttribute('attributes', []);
        /** @var array<Index> $currentIndexes */
        $currentIndexes = $currentCollection->isEmpty() ? [] : $currentCollection->getAttribute('indexes', []);

        if ($id === self::METADATA) {
            $this->purgeCachedCollection($id);
        }

        $schemaDeleted = false;
        try {
            $this->adapter->deleteCollection($id);
            $schemaDeleted = true;
        } catch (NotFoundException) {
            // Ignore — collection already absent from schema
        }

        if ($id === self::METADATA) {
            $deleted = true;
        } else {
            try {
                $deleted = $this->silent(fn () => $this->deleteDocument(self::METADATA, $id));
            } catch (Throwable $error) {
                if ($schemaDeleted) {
                    try {
                        $this->adapter->createCollection($id, $currentAttributes, $currentIndexes);
                    } catch (Throwable) {
                        // Silent rollback — best effort to restore consistency
                    }
                }
                throw new DatabaseException(
                    "Failed to persist metadata for collection deletion '{$id}': ".$error->getMessage(),
                    previous: $error
                );
            }
        }

        if ($id !== self::METADATA) {
            $this->purgeCachedCollection($id);
        }

        if ($deleted) {
            $this->triggerHooks(Event::CollectionDelete, $collection);
        }

        return $deleted;
    }

    /**
     * Cleanup (delete) a collection with retry logic
     *
     * @param  string  $collectionId  The collection ID
     * @param  int  $maxAttempts  Maximum retry attempts
     *
     * @throws DatabaseException If cleanup fails after all retries
     */
    private function cleanupCollection(
        string $collectionId,
        int $maxAttempts = 3
    ): void {
        $this->cleanup(
            fn () => $this->adapter->deleteCollection($collectionId),
            'collection',
            $collectionId,
            $maxAttempts
        );
    }
}
