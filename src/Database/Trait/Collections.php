<?php

namespace Utopia\Database\Trait;

use Exception;
use Throwable;
use Utopia\Console;
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
use Utopia\Database\Exception\Refused as RefusedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\IndexDefinition;
use Utopia\Database\Validator\Permissions;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

trait Collections
{
    /**
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws IndexException
     * @throws LimitException
     * @throws RefusedException When the adapter does not create the table
     * @throws StructureException when the collection carries a key the metadata collection does not store
     */
    public function createCollection(Collection $collection): Collection
    {
        $unknown = \array_diff_key($collection->getArrayCopy(), self::COLLECTION_RESERVED_KEYS);
        if ($unknown !== []) {
            throw new StructureException('Unknown collection keys: '.\implode(', ', \array_keys($unknown)));
        }

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

        if ($this->validation()->get() && $this->adapter->supports(Capability::IndexTtl)) {
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
        );

        if ($this->validation()->get()) {
            $validator = new IndexDefinition($attributes, [], $this->profile());
            foreach ($indexes as $index) {
                if (! $validator->isValid($index)) {
                    throw new IndexException($validator->getDescription());
                }
            }
        }

        if ($indexes !== [] && $this->adapter->getCountOfIndexes($definition) > $this->adapter->limits()->indexes) {
            throw new LimitException('Index limit of '.$this->adapter->limits()->indexes.' exceeded. Cannot create collection.');
        }

        if ($attributes !== []) {
            if (
                $this->adapter->limits()->attributes > 0 &&
                $this->adapter->getCountOfAttributes($definition) > $this->adapter->limits()->attributes
            ) {
                throw new LimitException('Attribute limit of '.$this->adapter->limits()->attributes.' exceeded. Cannot create collection.');
            }

            if (
                $this->adapter->limits()->documentSize > 0 &&
                $this->adapter->getAttributeWidth($definition) > $this->adapter->limits()->documentSize
            ) {
                throw new LimitException('Document size limit of '.$this->adapter->limits()->documentSize.' exceeded. Cannot create collection.');
            }
        }

        $created = $this->createCollectionInSchema($id, $attributes, $indexes);

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

        $listeners = $this->listens(Event::CollectionCreate);
        if ($listeners !== []) {
            $this->dispatch(new Event\Collection\Created($stored->getId(), $stored), $listeners);
        }

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

        $listeners = $this->listens(Event::CollectionUpdate);
        if ($listeners !== []) {
            $this->dispatch(new Event\Collection\Updated($updated->getId(), $updated), $listeners);
        }

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
            $listeners = $this->listens(Event::CollectionRead);
            if ($listeners !== []) {
                $this->dispatch(new Event\Collection\Read($definition->getId(), $definition), $listeners);
            }

            return $definition;
        }

        $stored = $this->silent(fn () => $this->getDocument(self::METADATA, $collection));

        if ($stored->getId() === '') {
            return null;
        }

        if (
            $this->adapter->hasSharedTables()
            && $stored->getTenant() !== null
            && $stored->getTenant() !== $this->adapter->getTenant()
        ) {
            return null;
        }

        $definition = $this->toCollection($stored);

        $listeners = $this->listens(Event::CollectionRead);
        if ($listeners !== []) {
            $this->dispatch(new Event\Collection\Read($definition->getId(), $definition), $listeners);
        }

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

        $listeners = $this->listens(Event::CollectionList);
        if ($listeners !== []) {
            $this->dispatch(new Event\Collection\Listed($collections), $listeners);
        }

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
        if ($this->adapter->hasSharedTables() && empty($this->adapter->getTenant())) {
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
     * @throws RefusedException When the adapter does not drop the table
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

        $schemaDeleted = $this->deleteCollectionFromSchema($collection);

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

        $listeners = $deleted ? $this->listens(Event::CollectionDelete) : [];
        if ($listeners !== []) {
            $this->dispatch(new Event\Collection\Deleted($definition->getId(), $definition), $listeners);
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

        if ($this->adapter->hasSharedTables() && $definition->getTenant() !== $this->adapter->getTenant()) {
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
        $maxIndexLength = $this->adapter->limits()->indexLength;
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
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     * @return bool True when this call created the table, false when it reuses the metadata table or a table shared
     *              with other tenants
     *
     * @throws DuplicateException When the table exists and is neither
     * @throws RefusedException When the adapter does not create the table
     */
    private function createCollectionInSchema(string $id, array $attributes, array $indexes): bool
    {
        try {
            $created = $this->adapter->createCollection($id, $attributes, $indexes);
        } catch (DuplicateException $error) {
            if ($id === self::METADATA
                || ($this->adapter->hasSharedTables()
                    && $this->adapter->collectionExists($this->adapter->getDatabase(), $id))) {
                // The metadata table must never be dropped during reconciliation.
                // In shared-tables mode the physical table is reused across
                // tenants. A DuplicateException simply means the table already
                // exists for another tenant — not an orphan.
                return false;
            }

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

        if (! $created) {
            throw new RefusedException('Failed to create collection');
        }

        return true;
    }

    /**
     * @return bool True when this call dropped the table, false when the schema no longer held it
     *
     * @throws RefusedException When the adapter does not drop the table
     */
    private function deleteCollectionFromSchema(string $collection): bool
    {
        try {
            $deleted = $this->adapter->deleteCollection($collection);
        } catch (NotFoundException) {
            // Already absent from the schema; the metadata is still removed.
            return false;
        }

        if (! $deleted) {
            throw new RefusedException('Failed to delete collection');
        }

        return true;
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
