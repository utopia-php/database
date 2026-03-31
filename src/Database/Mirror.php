<?php

namespace Utopia\Database;

use DateTime;
use Throwable;
use Utopia\Async\Promise;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Relationship as RelationshipHook;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Mirroring\Filter;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;

/**
 * Wraps a source Database and replicates write operations to an optional destination Database.
 */
class Mirror extends Database
{
    protected Database $source;

    protected ?Database $destination;

    /**
     * Filters to apply to documents before writing to the destination database
     *
     * @var array<Filter>
     */
    protected array $writeFilters = [];

    /**
     * Callbacks to run when an error occurs on the destination database
     *
     * @var array<callable(string, Throwable): void>
     */
    protected array $errorCallbacks = [];

    /**
     * Collections that should only be present in the source database
     */
    protected const SOURCE_ONLY_COLLECTIONS = [
        'upgrades',
    ];

    /**
     * @param  array<Filter>  $filters
     */
    public function __construct(
        Database $source,
        ?Database $destination = null,
        array $filters = [],
    ) {
        $this->source = $source;
        $this->destination = $destination;
        $this->writeFilters = $filters;
        parent::__construct(
            $source->getAdapter(),
            $source->getCache()
        );
    }

    /**
     * Get the source database instance.
     *
     * @return Database
     */
    public function getSource(): Database
    {
        return $this->source;
    }

    /**
     * Get the destination database instance, if configured.
     *
     * @return Database|null
     */
    public function getDestination(): ?Database
    {
        return $this->destination;
    }

    /**
     * @return array<Filter>
     */
    public function getWriteFilters(): array
    {
        return $this->writeFilters;
    }

    /**
     * @param  callable(string, Throwable): void  $callback
     */
    public function onError(callable $callback): void
    {
        $this->errorCallbacks[] = $callback;
    }

    /**
     * @param  array<mixed>  $args
     */
    protected function delegate(string $method, array $args = []): mixed
    {
        if ($this->destination === null) {
            return $this->source->{$method}(...$args);
        }

        $sourceResult = $this->source->{$method}(...$args);

        try {
            $this->destination->{$method}(...$args);
        } catch (Throwable $err) {
            $this->logError($method, $err);
        }

        return $sourceResult;
    }

    /**
     * {@inheritdoc}
     */
    public function setDatabase(string $name): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setNamespace(string $namespace): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setSharedTables(bool $sharedTables): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setTenant(int|string|null $tenant): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setPreserveDates(bool $preserve): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        $this->preserveDates = $preserve;

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setPreserveSequence(bool $preserve): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        $this->preserveSequence = $preserve;

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function enableValidation(): static
    {
        $this->delegate(__FUNCTION__);

        $this->validate = true;

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function disableValidation(): static
    {
        $this->delegate(__FUNCTION__);

        $this->validate = false;

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function addLifecycleHook(Lifecycle $hook): static
    {
        $this->source->addHook($hook);

        return $this;
    }

    protected function trigger(Event $event, mixed $data = null): void
    {
        $this->source->trigger($event, $data);
    }

    /**
     * {@inheritdoc}
     */
    public function silent(callable $callback): mixed
    {
        return $this->source->silent($callback);
    }

    /**
     * {@inheritdoc}
     */
    public function withRequestTimestamp(?DateTime $requestTimestamp, callable $callback): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * {@inheritdoc}
     */
    public function exists(?string $database = null, ?string $collection = null): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function create(?string $database = null): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function delete(?string $database = null): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createCollection(string $id, array $attributes = [], array $indexes = [], ?array $permissions = null, bool $documentSecurity = true): Document
    {
        $result = $this->source->createCollection(
            $id,
            $attributes,
            $indexes,
            $permissions,
            $documentSecurity
        );

        if ($this->destination === null) {
            return $result;
        }

        try {
            foreach ($this->writeFilters as $filter) {
                $filtered = $filter->beforeCreateCollection(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $id,
                    collection: $result,
                );
                if ($filtered !== null) {
                    $result = $filtered;
                }
            }

            $this->destination->createCollection(
                $id,
                $attributes,
                $indexes,
                $permissions,
                $documentSecurity
            );

            $this->silent(function () use ($id) {
                $this->createUpgrades();

                $this->source->createDocument('upgrades', new Document([
                    '$id' => $id,
                    'collectionId' => $id,
                    'status' => 'upgraded',
                ]));
            });
        } catch (Throwable $err) {
            $this->logError('createCollection', $err);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateCollection(string $id, array $permissions, bool $documentSecurity): Document
    {
        $result = $this->source->updateCollection($id, $permissions, $documentSecurity);

        if ($this->destination === null) {
            return $result;
        }

        try {
            foreach ($this->writeFilters as $filter) {
                $filtered = $filter->beforeUpdateCollection(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $id,
                    collection: $result,
                );
                if ($filtered !== null) {
                    $result = $filtered;
                }
            }

            $this->destination->updateCollection($id, $permissions, $documentSecurity);
        } catch (Throwable $err) {
            $this->logError('updateCollection', $err);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteCollection(string $id): bool
    {
        $result = $this->source->deleteCollection($id);

        if ($this->destination === null) {
            return $result;
        }

        try {
            $this->destination->deleteCollection($id);

            foreach ($this->writeFilters as $filter) {
                $filter->beforeDeleteCollection(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $id,
                );
            }
        } catch (Throwable $err) {
            $this->logError('deleteCollection', $err);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        $result = $this->source->createAttribute($collection, $attribute);

        if ($this->destination === null) {
            return $result;
        }

        try {
            // Round-trip through Document is required: Filter interface accepts/returns Document,
            // so we must serialize to Document for filter processing, then deserialize back.
            $document = $attribute->toDocument();

            foreach ($this->writeFilters as $filter) {
                $document = $filter->beforeCreateAttribute(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    attributeId: $attribute->key,
                    attribute: $document,
                );
                if ($document === null) {
                    break;
                }
            }

            if ($document !== null) {
                $filteredAttribute = Attribute::fromDocument($document);
                $result = $this->destination->createAttribute($collection, $filteredAttribute);
            }
        } catch (Throwable $err) {
            $this->logError('createAttribute', $err);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        $result = $this->source->createAttributes($collection, $attributes);

        if ($this->destination === null) {
            return $result;
        }

        try {
            $filteredAttributes = [];
            foreach ($attributes as $attribute) {
                // Round-trip through Document is required: Filter interface accepts/returns Document,
                // so we must serialize to Document for filter processing, then deserialize back.
                $document = $attribute->toDocument();

                foreach ($this->writeFilters as $filter) {
                    $document = $filter->beforeCreateAttribute(
                        source: $this->source,
                        destination: $this->destination,
                        collectionId: $collection,
                        attributeId: $attribute->key,
                        attribute: $document,
                    );
                    if ($document === null) {
                        break;
                    }
                }

                if ($document !== null) {
                    $filteredAttributes[] = Attribute::fromDocument($document);
                }
            }

            if ($filteredAttributes !== []) {
                $result = $this->destination->createAttributes(
                    $collection,
                    $filteredAttributes,
                );
            }
        } catch (Throwable $err) {
            $this->logError('createAttributes', $err);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateAttribute(string $collection, string $id, ColumnType|string|null $type = null, ?int $size = null, ?bool $required = null, mixed $default = null, ?bool $signed = null, ?bool $array = null, ?string $format = null, ?array $formatOptions = null, ?array $filters = null, ?string $newKey = null): Document
    {
        $document = $this->source->updateAttribute(
            $collection,
            $id,
            $type,
            $size,
            $required,
            $default,
            $signed,
            $array,
            $format,
            $formatOptions,
            $filters,
            $newKey,
        );

        if ($this->destination === null) {
            return $document;
        }

        try {
            foreach ($this->writeFilters as $filter) {
                $filtered = $filter->beforeUpdateAttribute(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    attributeId: $id,
                    attribute: $document,
                );
                if ($filtered !== null) {
                    $document = $filtered;
                }
            }

            $typedAttr = Attribute::fromDocument($document);

            $this->destination->updateAttribute(
                $collection,
                $id,
                $typedAttr->type,
                $typedAttr->size,
                $typedAttr->required,
                $typedAttr->default,
                $typedAttr->signed,
                $typedAttr->array,
                $typedAttr->format ?: null,
                $typedAttr->formatOptions ?: null,
                $typedAttr->filters ?: null,
                $newKey,
            );
        } catch (Throwable $err) {
            $this->logError('updateAttribute', $err);
        }

        return $document;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $result = $this->source->deleteAttribute($collection, $id);

        if ($this->destination === null) {
            return $result;
        }

        try {
            foreach ($this->writeFilters as $filter) {
                $filter->beforeDeleteAttribute(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    attributeId: $id,
                );
            }

            $this->destination->deleteAttribute($collection, $id);
        } catch (Throwable $err) {
            $this->logError('deleteAttribute', $err);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createIndex(string $collection, Index $index): bool
    {
        $result = $this->source->createIndex($collection, $index);

        if ($this->destination === null) {
            return $result;
        }

        try {
            // Round-trip through Document is required: Filter interface accepts/returns Document,
            // so we must serialize to Document for filter processing, then deserialize back.
            $document = $index->toDocument();

            foreach ($this->writeFilters as $filter) {
                $filtered = $filter->beforeCreateIndex(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    indexId: $index->key,
                    index: $document,
                );
                if ($filtered !== null) {
                    $document = $filtered;
                }
            }

            $filteredIndex = Index::fromDocument($document);
            $result = $this->destination->createIndex($collection, $filteredIndex);
        } catch (Throwable $err) {
            $this->logError('createIndex', $err);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $result = $this->source->deleteIndex($collection, $id);

        if ($this->destination === null) {
            return $result;
        }

        try {
            $this->destination->deleteIndex($collection, $id);

            foreach ($this->writeFilters as $filter) {
                $filter->beforeDeleteIndex(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    indexId: $id,
                );
            }
        } catch (Throwable $err) {
            $this->logError('deleteIndex', $err);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $document = $this->source->createDocument($collection, $document);

        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $this->destination === null
        ) {
            return $document;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $document;
        }

        try {
            $clone = clone $document;

            foreach ($this->writeFilters as $filter) {
                $clone = $filter->beforeCreateDocument(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    document: $clone,
                );
            }

            $this->destination->setPreserveDates(true);
            $document = $this->destination->createDocument($collection, $clone);
            $this->destination->setPreserveDates(false);

            foreach ($this->writeFilters as $filter) {
                $filter->afterCreateDocument(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    document: $clone,
                );
            }
        } catch (Throwable $err) {
            $this->logError('createDocument', $err);
        }

        return $document;
    }

    /**
     * {@inheritdoc}
     */
    public function createDocuments(
        string $collection,
        array $documents,
        int $batchSize = self::INSERT_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        $modified = $this->source->createDocuments(
            $collection,
            $documents,
            $batchSize,
            $onNext,
            $onError,
        );

        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $this->destination === null
        ) {
            return $modified;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $modified;
        }

        $clones = [];
        $destination = $this->destination;

        foreach ($documents as $document) {
            $clone = clone $document;

            foreach ($this->writeFilters as $filter) {
                $clone = $filter->beforeCreateDocument(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    document: $clone,
                );
            }

            $clones[] = $clone;
        }

        Promise::async(function () use ($destination, $collection, $clones, $batchSize) {
            try {
                $destination->withPreserveDates(
                    fn () => $destination->createDocuments(
                        $collection,
                        $clones,
                        $batchSize,
                    )
                );

                foreach ($clones as $clone) {
                    foreach ($this->writeFilters as $filter) {
                        $filter->afterCreateDocument(
                            source: $this->source,
                            destination: $destination,
                            collectionId: $collection,
                            document: $clone,
                        );
                    }
                }
            } catch (Throwable $err) {
                $this->logError('createDocuments', $err);
            }
        });

        return $modified;
    }

    /**
     * {@inheritdoc}
     */
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        $document = $this->source->updateDocument($collection, $id, $document);

        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $this->destination === null
        ) {
            return $document;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));

        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $document;
        }

        try {
            $clone = clone $document;

            foreach ($this->writeFilters as $filter) {
                $clone = $filter->beforeUpdateDocument(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    document: $clone,
                );
            }

            $this->destination->setPreserveDates(true);
            $this->destination->updateDocument($collection, $id, $clone);
            $this->destination->setPreserveDates(false);

            foreach ($this->writeFilters as $filter) {
                $filter->afterUpdateDocument(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    document: $clone,
                );
            }
        } catch (Throwable $err) {
            $this->logError('updateDocument', $err);
        }

        return $document;
    }

    /**
     * {@inheritdoc}
     */
    public function updateDocuments(
        string $collection,
        Document $updates,
        array $queries = [],
        int $batchSize = self::INSERT_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        $modified = $this->source->updateDocuments(
            $collection,
            $updates,
            $queries,
            $batchSize,
            $onNext,
            $onError,
        );

        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $this->destination === null
        ) {
            return $modified;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $modified;
        }

        $clone = clone $updates;
        $destination = $this->destination;

        foreach ($this->writeFilters as $filter) {
            $clone = $filter->beforeUpdateDocuments(
                source: $this->source,
                destination: $destination,
                collectionId: $collection,
                updates: $clone,
                queries: $queries,
            );
        }

        Promise::async(function () use ($destination, $collection, $clone, $queries, $batchSize) {
            try {
                $destination->withPreserveDates(
                    fn () => $destination->updateDocuments(
                        $collection,
                        $clone,
                        $queries,
                        $batchSize,
                    )
                );

                foreach ($this->writeFilters as $filter) {
                    $filter->afterUpdateDocuments(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        updates: $clone,
                        queries: $queries,
                    );
                }
            } catch (Throwable $err) {
                $this->logError('updateDocuments', $err);
            }
        });

        return $modified;
    }

    /**
     * {@inheritdoc}
     */
    public function upsertDocuments(
        string $collection,
        array $documents,
        int $batchSize = Database::INSERT_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        $modified = $this->source->upsertDocuments(
            $collection,
            $documents,
            $batchSize,
            $onNext,
            $onError,
        );

        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $this->destination === null
        ) {
            return $modified;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $modified;
        }

        $clones = [];
        $destination = $this->destination;

        foreach ($documents as $document) {
            $clone = clone $document;

            foreach ($this->writeFilters as $filter) {
                $clone = $filter->beforeCreateOrUpdateDocument(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    document: $clone,
                );
            }

            $clones[] = $clone;
        }

        Promise::async(function () use ($destination, $collection, $clones, $batchSize) {
            try {
                $destination->withPreserveDates(
                    fn () => $destination->upsertDocuments(
                        $collection,
                        $clones,
                        $batchSize,
                    )
                );

                foreach ($clones as $clone) {
                    foreach ($this->writeFilters as $filter) {
                        $filter->afterCreateOrUpdateDocument(
                            source: $this->source,
                            destination: $destination,
                            collectionId: $collection,
                            document: $clone,
                        );
                    }
                }
            } catch (Throwable $err) {
                $this->logError('upsertDocuments', $err);
            }
        });

        return $modified;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $result = $this->source->deleteDocument($collection, $id);

        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $this->destination === null
        ) {
            return $result;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $result;
        }

        foreach ($this->writeFilters as $filter) {
            $filter->beforeDeleteDocument(
                source: $this->source,
                destination: $this->destination,
                collectionId: $collection,
                documentId: $id,
            );
        }

        $destination = $this->destination;
        Promise::async(function () use ($destination, $collection, $id) {
            try {
                $destination->deleteDocument($collection, $id);

                foreach ($this->writeFilters as $filter) {
                    $filter->afterDeleteDocument(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        documentId: $id,
                    );
                }
            } catch (Throwable $err) {
                $this->logError('deleteDocument', $err);
            }
        });

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteDocuments(
        string $collection,
        array $queries = [],
        int $batchSize = self::DELETE_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        $modified = $this->source->deleteDocuments(
            $collection,
            $queries,
            $batchSize,
            $onNext,
            $onError,
        );

        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $this->destination === null
        ) {
            return $modified;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $modified;
        }

        foreach ($this->writeFilters as $filter) {
            $filter->beforeDeleteDocuments(
                source: $this->source,
                destination: $this->destination,
                collectionId: $collection,
                queries: $queries,
            );
        }

        $destination = $this->destination;
        Promise::async(function () use ($destination, $collection, $queries, $batchSize) {
            try {
                $destination->deleteDocuments(
                    $collection,
                    $queries,
                    $batchSize,
                );

                foreach ($this->writeFilters as $filter) {
                    $filter->afterDeleteDocuments(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        queries: $queries,
                    );
                }
            } catch (Throwable $err) {
                $this->logError('deleteDocuments', $err);
            }
        });

        return $modified;
    }

    /**
     * {@inheritdoc}
     */
    public function updateAttributeRequired(string $collection, string $id, bool $required): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateAttributeFormat(string $collection, string $id, string $format): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateAttributeFormatOptions(string $collection, string $id, array $formatOptions): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, [$collection, $id, $formatOptions]);
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateAttributeFilters(string $collection, string $id, array $filters): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateAttributeDefault(string $collection, string $id, mixed $default = null): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createRelationship(Relationship $relationship): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, [$relationship]);
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateRelationship(
        string $collection,
        string $id,
        ?string $newKey = null,
        ?string $newTwoWayKey = null,
        ?bool $twoWay = null,
        ?ForeignKeyAction $onDelete = null
    ): bool {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteRelationship(string $collection, string $id): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value = 1, int|float|null $max = null): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function decreaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value = 1, int|float|null $min = null): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * Create the upgrades tracking collection in the source database if it does not exist.
     *
     * @return void
     * @throws Limit
     * @throws DuplicateException
     * @throws Exception
     */
    public function createUpgrades(): void
    {
        $collection = $this->source->getCollection('upgrades');

        if (! $collection->isEmpty()) {
            return;
        }

        $this->source->createCollection(
            id: 'upgrades',
            attributes: [
                new Attribute(
                    key: 'collectionId',
                    type: ColumnType::String,
                    size: Database::LENGTH_KEY,
                    required: true,
                ),
                new Attribute(
                    key: 'status',
                    type: ColumnType::String,
                    size: Database::LENGTH_KEY,
                    required: false,
                ),
            ],
            indexes: [
                new Index(
                    key: '_unique_collection',
                    type: IndexType::Unique,
                    attributes: ['collectionId'],
                    lengths: [Database::LENGTH_KEY],
                ),
                new Index(
                    key: '_status_index',
                    type: IndexType::Key,
                    attributes: ['status'],
                    lengths: [Database::LENGTH_KEY],
                    orders: [OrderDirection::Asc->value],
                ),
            ],
        );
    }

    /**
     * @throws Exception
     */
    protected function getUpgradeStatus(string $collection): ?Document
    {
        if ($collection === 'upgrades' || $collection === Database::METADATA) {
            return new Document();
        }

        return $this->getSource()->getAuthorization()->skip(function () use ($collection) {
            try {
                return $this->source->getDocument('upgrades', $collection);
            } catch (Throwable) {
                return;
            }
        });
    }

    protected function logError(string $action, Throwable $err): void
    {
        foreach ($this->errorCallbacks as $callback) {
            $callback($action, $err);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function setAuthorization(Authorization $authorization): self
    {

        parent::setAuthorization($authorization);

        $this->source->setAuthorization($authorization);

        if ($this->destination !== null) {
            $this->destination->setAuthorization($authorization);
        }

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function addHook(\Utopia\Query\Hook $hook): static
    {
        parent::addHook($hook);

        if ($hook instanceof RelationshipHook) {
            $this->source->addHook(new Relationships($this->source));
            $this->destination?->addHook(new Relationships($this->destination));
        }

        return $this;
    }

    /**
     * Set custom document class for a collection
     *
     * @param  string  $collection  Collection ID
     * @param  class-string<Document>  $className  Fully qualified class name that extends Document
     */
    public function setDocumentType(string $collection, string $className): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());
        $this->documentTypes[$collection] = $className;

        return $this;
    }

    /**
     * Clear document type mapping for a collection
     *
     * @param  string  $collection  Collection ID
     */
    public function clearDocumentType(string $collection): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());
        unset($this->documentTypes[$collection]);

        return $this;
    }

    /**
     * Clear all document type mappings
     */
    public function clearAllDocumentTypes(): static
    {
        $this->delegate(__FUNCTION__);
        $this->documentTypes = [];

        return $this;
    }
}
