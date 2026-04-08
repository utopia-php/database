<?php

namespace Utopia\Database\Traits;

use Exception;
use Throwable;
use Utopia\Console;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
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
    /**
     * Create Collection
     *
     * @param  string  $id  The collection identifier
     * @param  array<Attribute|Document>  $attributes  Initial attributes for the collection
     * @param  array<Index|Document>  $indexes  Initial indexes for the collection
     * @param  array<string>|null  $permissions  Permission strings, defaults to allow any create
     * @param  bool  $documentSecurity  Whether to enable document-level security
     * @param  array<string, mixed>  $metadata  Additional metadata attributes to merge into the collection document
     * @return Document The created collection metadata document
     *
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     */
    public function createCollection(string $id, array $attributes = [], array $indexes = [], ?array $permissions = null, bool $documentSecurity = true, array $metadata = []): Document
    {
        $attributes = array_map(fn ($attr): Attribute => $attr instanceof Attribute ? $attr : Attribute::fromDocument($attr), $attributes);
        $indexes = array_map(fn ($idx): Index => $idx instanceof Index ? $idx : Index::fromDocument($idx), $indexes);

        foreach ($attributes as $attribute) {
            if (in_array($attribute->type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon, ColumnType::Vector, ColumnType::Object], true)) {
                $existingFilters = $attribute->filters;
                $attribute->filters = array_values(
                    array_unique(array_merge($existingFilters, [$attribute->type->value]))
                );
            }
        }

        $permissions ??= [
            Permission::create(Role::any()),
        ];

        if ($this->validate) {
            $validator = new Permissions();
            if (! $validator->isValid($permissions)) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        $collection = $this->silent(fn () => $this->getCollection($id));

        if (! $collection->isEmpty() && $id !== self::METADATA) {
            throw new DuplicateException('Collection '.$id.' already exists');
        }

        // Enforce single TTL index per collection
        if ($this->validate && $this->adapter->supports(Capability::TTLIndexes)) {
            $ttlIndexes = array_filter($indexes, fn (Index $idx) => $idx->type === IndexType::Ttl);
            if (count($ttlIndexes) > 1) {
                throw new IndexException('There can be only one TTL index in a collection');
            }
        }

        /**
         * Fix metadata index length & orders
         */
        foreach ($indexes as $key => $index) {
            $lengths = $index->lengths;
            $orders = $index->orders;

            foreach ($index->attributes as $i => $attr) {
                foreach ($attributes as $collectionAttribute) {
                    if ($collectionAttribute->key === $attr) {
                        /**
                         * mysql does not save length in collection when length = attributes size
                         */
                        if ($collectionAttribute->type === ColumnType::String) {
                            if (! empty($lengths[$i]) && $lengths[$i] === $collectionAttribute->size && $this->adapter->getMaxIndexLength() > 0) {
                                $lengths[$i] = null;
                            }
                        }

                        $isArray = $collectionAttribute->array;
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

            $index->lengths = $lengths;
            $index->orders = $orders;
            $indexes[$key] = $index;
        }

        // Convert models to Documents for collection metadata
        $attributeDocs = array_map(fn (Attribute $attr) => $attr->toDocument(), $attributes);
        $indexDocs = array_map(fn (Index $idx) => $idx->toDocument(), $indexes);

        $collection = new Document(\array_merge([
            '$id' => ID::custom($id),
            '$permissions' => $permissions,
            'name' => $id,
            'attributes' => $attributeDocs,
            'indexes' => $indexDocs,
            'documentSecurity' => $documentSecurity,
        ], $metadata));

        if ($this->validate) {
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
                $this->adapter instanceof Feature\Spatial,
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

        // Check index limits, if given
        if ($indexes && $this->adapter->getCountOfIndexes($collection) > $this->adapter->getLimitForIndexes()) {
            throw new LimitException('Index limit of '.$this->adapter->getLimitForIndexes().' exceeded. Cannot create collection.');
        }

        // Check attribute limits, if given
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
        } catch (DuplicateException $e) {
            // Metadata check (above) already verified collection is absent
            // from metadata. A DuplicateException from the adapter means the
            // collection exists only in physical schema — an orphan from a prior
            // partial failure. Skip creation and proceed to metadata creation.
        }

        if ($id === self::METADATA) {
            return new Document(self::collectionMeta());
        }

        try {
            $createdCollection = $this->silent(fn () => $this->createDocument(self::METADATA, $collection));
        } catch (Throwable $e) {
            if ($created) {
                try {
                    $this->cleanupCollection($id);
                } catch (Throwable $e) {
                    Console::error("Failed to rollback collection '{$id}': ".$e->getMessage());
                }
            }
            throw new DatabaseException("Failed to create collection metadata for '{$id}': ".$e->getMessage(), previous: $e);
        }

        $this->trigger(Event::CollectionCreate, $createdCollection);

        return $createdCollection;
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
        if ($this->validate) {
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
            ->setAttribute('$permissions', $permissions)
            ->setAttribute('documentSecurity', $documentSecurity);

        $collection = $this->skipValidation(fn () => $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection)));

        $this->trigger(Event::CollectionUpdate, $collection);

        return $collection;
    }

    /**
     * Get Collection
     *
     * @param  string  $id  The collection identifier
     * @return Document The collection metadata document, or an empty Document if not found
     *
     * @throws DatabaseException
     */
    public function getCollection(string $id): Document
    {
        $collection = $this->silent(fn () => $this->getDocument(self::METADATA, $id));

        if (
            $id !== self::METADATA
            && $this->adapter->getSharedTables()
            && $collection->getTenant() !== null
            && $collection->getTenant() !== $this->adapter->getTenant()
        ) {
            return new Document();
        }

        $this->trigger(Event::CollectionRead, $collection);

        return $collection;
    }

    /**
     * List Collections
     *
     * @param  int  $limit  Maximum number of collections to return
     * @param  int  $offset  Number of collections to skip
     * @return array<Document>
     *
     * @throws Exception
     */
    public function listCollections(int $limit = 25, int $offset = 0): array
    {
        $result = $this->silent(fn () => $this->find(self::METADATA, [
            Query::limit($limit),
            Query::offset($offset),
        ]));

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
        $collection = $this->silent(fn () => $this->getDocument(self::METADATA, $id));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        if ($this->adapter->getSharedTables() && $collection->getTenant() !== $this->adapter->getTenant()) {
            throw new NotFoundException('Collection not found');
        }

        /** @var array<Document> $allAttributes */
        $allAttributes = $collection->getAttribute('attributes', []);
        $relationships = \array_filter(
            $allAttributes,
            fn (Document $attribute) => Attribute::fromDocument($attribute)->type === ColumnType::Relationship
        );

        foreach ($relationships as $relationship) {
            $this->deleteRelationship($collection->getId(), $relationship->getId());
        }

        // Re-fetch collection to get current state after relationship deletions
        $currentCollection = $this->silent(fn () => $this->getDocument(self::METADATA, $id));
        /** @var array<Document> $currentAttrDocs */
        $currentAttrDocs = $currentCollection->isEmpty() ? [] : $currentCollection->getAttribute('attributes', []);
        /** @var array<Document> $currentIdxDocs */
        $currentIdxDocs = $currentCollection->isEmpty() ? [] : $currentCollection->getAttribute('indexes', []);
        $currentAttributes = array_map(fn (Document $d) => Attribute::fromDocument($d), $currentAttrDocs);
        $currentIndexes = array_map(fn (Document $d) => Index::fromDocument($d), $currentIdxDocs);

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
            } catch (Throwable $e) {
                if ($schemaDeleted) {
                    try {
                        $this->adapter->createCollection($id, $currentAttributes, $currentIndexes);
                    } catch (Throwable) {
                        // Silent rollback — best effort to restore consistency
                    }
                }
                throw new DatabaseException(
                    "Failed to persist metadata for collection deletion '{$id}': ".$e->getMessage(),
                    previous: $e
                );
            }
        }

        if ($deleted) {
            $this->trigger(Event::CollectionDelete, $collection);
        }

        $this->purgeCachedCollection($id);

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
