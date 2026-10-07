<?php

namespace Utopia\Database\Traits;

use Exception;
use Throwable;
use Utopia\Console;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\IndexDefinition;
use Utopia\Database\Validator\Permissions;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * Provides CRUD operations for database collections including creation, listing, sizing, and deletion.
 */
trait Collections
{
    /**
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws IndexException
     * @throws LimitException
     */
    public function createCollection(Collection $collection): Collection
    {
        $id = $collection->getId();
        $attributes = \array_map(self::normalise(...), $collection->attributes());
        $permissions = $collection->declaredPermissions() ?? [Permission::create(Role::any())];

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

        if ($id !== self::METADATA && $this->silent(fn () => $this->findCollection($id)) !== null) {
            throw new DuplicateException('Collection '.$id.' already exists');
        }

        $indexes = $collection->indexes();

        if ($this->validation()->get() && $this->adapter->supports(Capability::TTLIndexes)) {
            $ttlIndexes = \array_filter($indexes, static fn (Index $index): bool => $index->type === IndexType::Ttl);
            if (\count($ttlIndexes) > 1) {
                throw new IndexException('There can be only one TTL index in a collection');
            }
        }

        $indexes = \array_map(fn (Index $index): Index => $this->fitIndexToAttributes($index, $attributes), $indexes);

        $definition = Collection::create(
            id: $id,
            name: $collection->name(),
            attributes: $attributes,
            indexes: $indexes,
            permissions: $permissions,
            documentSecurity: $collection->documentSecurity(),
            metadata: \array_diff_key($collection->getArrayCopy(), self::COLLECTION_RESERVED_KEYS),
        );

        if ($this->validation()->get()) {
            $validator = new IndexDefinition(
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

        if ($indexes !== [] && $this->adapter->getCountOfIndexes($definition) > $this->adapter->getLimitForIndexes()) {
            throw new LimitException('Index limit of '.$this->adapter->getLimitForIndexes().' exceeded. Cannot create collection.');
        }

        if ($attributes !== []) {
            if (
                $this->adapter->getLimitForAttributes() > 0 &&
                $this->adapter->getCountOfAttributes($definition) > $this->adapter->getLimitForAttributes()
            ) {
                throw new LimitException('Attribute limit of '.$this->adapter->getLimitForAttributes().' exceeded. Cannot create collection.');
            }

            if (
                $this->adapter->getDocumentSizeLimit() > 0 &&
                $this->adapter->getAttributeWidth($definition) > $this->adapter->getDocumentSizeLimit()
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
                $this->purgeStaleCollectionCache($id);
                throw new DuplicateException('Collection '.$id.' already exists', previous: $error);
            }
        }

        if ($id === self::METADATA) {
            return self::collectionDefinition();
        }

        try {
            $stored = $this->silent(fn () => $this->createDocument(self::METADATA, $definition));
        } catch (DuplicateException $error) {
            // A concurrent creator committed the metadata for this id first, so
            // the physical table is the one its metadata describes. Rolling back
            // here would drop a live collection out from under it.
            $this->purgeStaleCollectionCache($id);
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

        $stored = $this->toCollection($stored);

        $this->triggerHooks(Event::CollectionCreate, $stored);

        return $stored;
    }

    /**
     * @throws ConflictException
     * @throws DatabaseException
     * @throws NotFoundException
     */
    public function updateCollection(string $collection, CollectionUpdate $update): Collection
    {
        if ($update->permissions !== null && $this->validation()->get()) {
            $validator = new Permissions();
            if (! $validator->isValid($update->permissions)) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        $definition = $this->silent(fn () => $this->getOwnCollection($collection));

        if ($update->permissions === null && $update->documentSecurity === null) {
            return $definition;
        }

        if ($update->permissions !== null) {
            $definition->setAttribute(Document::PERMISSIONS, $update->permissions);
        }

        if ($update->documentSecurity !== null) {
            $definition->setAttribute(self::COLLECTION_DOCUMENT_SECURITY, $update->documentSecurity);
        }

        $updated = $this->toCollection(
            $this->silent(fn () => $this->updateDocument(self::METADATA, $definition->getId(), $definition)),
        );

        $this->triggerHooks(Event::CollectionUpdate, $updated);

        return $updated;
    }

    /**
     * @throws DatabaseException
     * @throws NotFoundException when the collection does not exist, or belongs to another tenant under shared tables
     */
    public function getCollection(string $collection): Collection
    {
        return $this->findCollection($collection)
            ?? throw new NotFoundException('Collection not found');
    }

    /**
     * @return Collection|null null when the collection does not exist, or belongs to another tenant under shared tables
     *
     * @throws DatabaseException
     */
    public function findCollection(string $collection): ?Collection
    {
        if ($collection === self::METADATA) {
            $definition = self::collectionDefinition();
            $this->trigger(Event::CollectionRead, $definition);

            return $definition;
        }

        $stored = $this->silent(fn () => $this->getDocument(self::METADATA, $collection));

        if ($stored->getId() === '') {
            return null;
        }

        if (
            $this->adapter->getSharedTables()
            && $stored->getTenant() !== null
            && $stored->getTenant() !== $this->adapter->getTenant()
        ) {
            return null;
        }

        $definition = $this->toCollection($stored);

        $this->trigger(Event::CollectionRead, $definition);

        return $definition;
    }

    /**
     * @return list<Collection>
     *
     * @throws Exception
     */
    public function listCollections(int $limit = 25, int $offset = 0): array
    {
        $result = $this->silent(fn () => $this->find(self::METADATA, [
            Query::limit($limit),
            Query::offset($offset),
        ]));

        $collections = \array_map($this->toCollection(...), \array_values($result));

        $this->trigger(Event::CollectionList, $collections);

        return $collections;
    }

    /**
     * @throws Exception
     */
    public function getSizeOfCollection(string $collection): int
    {
        $definition = $this->silent(fn () => $this->getOwnCollection($collection));

        return $this->adapter->getSizeOfCollection($definition->getId());
    }

    /**
     * @throws DatabaseException
     * @throws NotFoundException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        if ($this->adapter->getSharedTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $definition = $this->silent(fn () => $this->getOwnCollection($collection));

        return $this->adapter->getSizeOfCollectionOnDisk($definition->getId());
    }

    public function analyzeCollection(string $collection): bool
    {
        return $this->adapter->analyzeCollection($collection);
    }

    /**
     * @throws DatabaseException
     * @throws NotFoundException
     */
    public function deleteCollection(string $collection): void
    {
        $definition = $this->silent(fn () => $this->getOwnCollection($collection));

        foreach ($definition->attributes() as $attribute) {
            if ($attribute->type === ColumnType::Relationship) {
                $this->deleteRelationship($collection, $attribute->key);
            }
        }

        $current = $this->silent(fn () => $this->findCollection($collection));
        $currentAttributes = $current?->attributes() ?? [];
        $currentIndexes = $current?->indexes() ?? [];

        if ($collection === self::METADATA) {
            $this->purgeCachedCollection($collection);
        }

        $schemaDeleted = false;
        try {
            $this->adapter->deleteCollection($collection);
            $schemaDeleted = true;
        } catch (NotFoundException) {
            // Already absent from the schema; the metadata is still removed below.
        }

        $deleted = true;
        if ($collection !== self::METADATA) {
            try {
                $deleted = $this->silent(fn () => $this->deleteDocument(self::METADATA, $collection));
            } catch (Throwable $error) {
                if ($schemaDeleted) {
                    try {
                        $this->adapter->createCollection($collection, $currentAttributes, $currentIndexes);
                    } catch (Throwable) {
                        // Best-effort restore; the metadata error below is what the caller needs.
                    }
                }
                throw new DatabaseException(
                    "Failed to persist metadata for collection deletion '{$collection}': ".$error->getMessage(),
                    previous: $error
                );
            }

            $this->purgeCachedCollection($collection);
        }

        if ($deleted) {
            $this->triggerHooks(Event::CollectionDelete, $definition);
        }
    }

    /**
     * The collection, refused when it belongs to another tenant under shared tables.
     *
     * @throws NotFoundException
     */
    private function getOwnCollection(string $collection): Collection
    {
        $definition = $this->getCollection($collection);

        if ($this->adapter->getSharedTables() && $definition->getTenant() !== $this->adapter->getTenant()) {
            throw new NotFoundException('Collection not found');
        }

        return $definition;
    }

    private function toCollection(Document $definition): Collection
    {
        if ($definition instanceof Collection) {
            return $definition;
        }

        $collection = Collection::fromArray($definition->getArrayCopy());
        $this->attachCollectionCacheEpoch($collection, $this->getCollectionCacheEpoch($definition));

        return $collection;
    }

    /**
     * Engines that keep index lengths do not store one equal to a string's size, and index arrays
     * by a fixed prefix in no order.
     *
     * @param  list<Attribute>  $attributes
     *
     * @throws IndexException
     */
    private function fitIndexToAttributes(Index $index, array $attributes): Index
    {
        $maxIndexLength = $this->adapter->getMaxIndexLength();
        $lengths = $index->lengths;
        $orders = $index->orders;

        foreach ($index->attributes as $position => $key) {
            foreach ($attributes as $attribute) {
                if ($attribute->key !== $key) {
                    continue;
                }

                $length = $lengths[$position] ?? null;
                if ($attribute->type === ColumnType::String && ! empty($length) && $length === $attribute->size && $maxIndexLength > 0) {
                    $lengths = self::withPosition($lengths, $position, null);
                }

                if ($attribute->array) {
                    if ($maxIndexLength > 0 && $index->type !== IndexType::Fulltext) {
                        $lengths = self::withPosition($lengths, $position, self::MAX_ARRAY_INDEX_LENGTH);
                    }
                    if (($orders[$position] ?? null) !== null) {
                        $orders = self::withPosition($orders, $position, null);
                    }
                }
                break;
            }
        }

        if ($lengths !== $index->lengths) {
            $index = $index->withLengths($lengths);
        }

        if ($orders !== $index->orders) {
            $index = $index->withOrders($orders);
        }

        return $index;
    }

    /**
     * @template T
     *
     * @param  list<T|null>  $list
     * @param  T|null  $value
     * @return list<T|null>
     */
    private static function withPosition(array $list, int $position, mixed $value): array
    {
        $list = \array_pad($list, $position + 1, null);
        $list[$position] = $value;

        return \array_values($list);
    }

    private function purgeStaleCollectionCache(string $collection): void
    {
        try {
            $this->purgeCachedDocument(self::METADATA, $collection);
        } catch (Throwable $cacheError) {
            Console::warning('Warning: Failed to purge stale collection cache: '.$cacheError->getMessage());
        }
    }

    /**
     * @throws DatabaseException If cleanup fails after all retries
     */
    private function cleanupCollection(string $collection, int $maxAttempts = 3): void
    {
        $this->cleanup(
            fn () => $this->adapter->deleteCollection($collection),
            'collection',
            $collection,
            $maxAttempts
        );
    }
}
