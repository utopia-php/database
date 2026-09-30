<?php

namespace Utopia\Database;

use Closure;
use DateTime;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Throwable;
use Utopia\Async\Promise;
use Utopia\Cache\Cache;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Hook\Write;
use Utopia\Database\Mirroring\Filter;
use Utopia\Database\Type\TypeRegistry;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\Order;

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
     * The last queued replication of each document, by collection and document id, until it finishes
     *
     * @var array<array-key, array<array-key, Channel>>
     */
    protected array $documentReplications = [];

    /**
     * The last queued replication that can reach every document of a collection, by collection, until it finishes
     *
     * @var array<array-key, Channel>
     */
    protected array $collectionReplications = [];

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
     * Delegate validator caching to the source database so Mirror reuses the
     * source's cache and inherits its invalidation. Mirror's own mutator
     * overrides bypass the inherited traits, so its local
     * `documentsValidatorCache` would otherwise go stale on schema changes.
     *
     * @param  array<Document>  $joinedCollections
     */
    protected function getDocumentsValidator(Document $collection, array $joinedCollections = []): Validator\Queries\Documents
    {
        return $this->source->getDocumentsValidator($collection, $joinedCollections);
    }

    /**
     * Delegate metadata reads to the source database. Mirror's schema mutator
     * overrides forward writes to source and destination directly, so routing
     * reads through the source keeps its view of attributes and relationships
     * in lockstep with the authoritative database.
     */
    public function getCollection(string $id): Collection
    {
        return $this->source->getCollection($id);
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
        parent::setDatabase($name);
        $this->source->setDatabase($name);
        $this->destination?->setDatabase($name);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setNamespace(string $namespace): static
    {
        parent::setNamespace($namespace);
        $this->source->setNamespace($namespace);
        $this->destination?->setNamespace($namespace);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setSharedTables(bool $sharedTables): static
    {
        parent::setSharedTables($sharedTables);
        $this->source->setSharedTables($sharedTables);
        $this->destination?->setSharedTables($sharedTables);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setTenant(int|string|null $tenant): static
    {
        parent::setTenant($tenant);
        $this->source->setTenant($tenant);
        $this->destination?->setTenant($tenant);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setMaxQueryValues(int $max): self
    {
        parent::setMaxQueryValues($max);
        $this->source->setMaxQueryValues($max);
        $this->destination?->setMaxQueryValues($max);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setCache(Cache $cache): static
    {
        parent::setCache($cache);
        $this->source->setCache($cache);
        $this->destination?->setCache($cache);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setQueryCache(?QueryCache $queryCache): static
    {
        parent::setQueryCache($queryCache);
        $this->source->setQueryCache($queryCache);
        $this->destination?->setQueryCache($queryCache);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setCacheName(string $name): static
    {
        parent::setCacheName($name);
        $this->source->setCacheName($name);
        $this->destination?->setCacheName($name);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setCacheWriterTimeout(int $seconds): static
    {
        parent::setCacheWriterTimeout($seconds);
        $this->source->setCacheWriterTimeout($seconds);
        $this->destination?->setCacheWriterTimeout($seconds);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setTenantPerDocument(bool $enabled): static
    {
        parent::setTenantPerDocument($enabled);
        $this->source->setTenantPerDocument($enabled);
        $this->destination?->setTenantPerDocument($enabled);

        return $this;
    }

    /**
     * A destination that cannot apply the timeout is reported through onError(), like a
     * failed destination write: MariaDB applies it on the destination's connection.
     *
     * {@inheritdoc}
     */
    public function setTimeout(int $milliseconds, Event $event = Event::All): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function clearTimeout(Event $event = Event::All): void
    {
        $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * {@inheritdoc}
     */
    public function setGlobalCollections(array $collections): static
    {
        parent::setGlobalCollections($collections);
        $this->source->setGlobalCollections($collections);
        $this->destination?->setGlobalCollections($collections);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function resetGlobalCollections(): void
    {
        parent::resetGlobalCollections();
        $this->source->resetGlobalCollections();
        $this->destination?->resetGlobalCollections();
    }

    /**
     * {@inheritdoc}
     */
    public function setMetadata(string $key, mixed $value): static
    {
        parent::setMetadata($key, $value);
        $this->source->setMetadata($key, $value);
        $this->destination?->setMetadata($key, $value);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function resetMetadata(): void
    {
        parent::resetMetadata();
        $this->source->resetMetadata();
        $this->destination?->resetMetadata();
    }

    /**
     * {@inheritdoc}
     */
    public function setMigrating(bool $migrating): self
    {
        parent::setMigrating($migrating);
        $this->source->setMigrating($migrating);
        $this->destination?->setMigrating($migrating);

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setTypeRegistry(?TypeRegistry $typeRegistry): static
    {
        parent::setTypeRegistry($typeRegistry);
        $this->source->setTypeRegistry($typeRegistry);
        $this->destination?->setTypeRegistry($typeRegistry);

        return $this;
    }

    /**
     * A destination that cannot apply the setting is reported through onError(), like setTimeout().
     *
     * {@inheritdoc}
     */
    public function enableLocks(bool $enabled): static
    {
        parent::enableLocks($enabled);
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function enableFilters(): static
    {
        parent::enableFilters();
        $this->source->enableFilters();
        $this->destination?->enableFilters();

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function disableFilters(): static
    {
        parent::disableFilters();
        $this->source->disableFilters();
        $this->destination?->disableFilters();

        return $this;
    }

    /**
     * Opens the scope on the mirror, its source and its destination.
     *
     * {@inheritdoc}
     */
    public function skipFilters(callable $callback, ?array $filters = null): mixed
    {
        $skip = fn (): mixed => parent::skipFilters($callback, $filters);
        $destination = $this->destination;

        return $this->source->skipFilters(
            fn (): mixed => $destination === null ? $skip() : $destination->skipFilters($skip, $filters),
            $filters,
        );
    }

    /**
     * The mirror queries through its source's adapter, so it reports to the source's profiler.
     *
     * {@inheritdoc}
     */
    public function enableProfiling(): static
    {
        $this->source->enableProfiling();
        $this->destination?->enableProfiling();
        $this->profiler = $this->source->getProfiler();

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function disableProfiling(): static
    {
        $this->source->disableProfiling();
        $this->destination?->disableProfiling();

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setDropUnknownAttributes(bool $drop): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        $this->dropUnknownAttributes = $drop;

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
    public function skipValidation(callable $callback): mixed
    {
        return parent::skipValidation(function () use ($callback) {
            if ($this->destination === null) {
                return $this->source->skipValidation($callback);
            }

            $destination = $this->destination;

            return $this->source->skipValidation(
                fn () => $destination->skipValidation($callback)
            );
        });
    }

    /**
     * Opens the scope on the mirror, its source and its destination, so the writes the mirror replicates before
     * returning use the tenant too; the replications it queues carry it (see replicate()).
     *
     * {@inheritdoc}
     */
    public function withTenant(int|string|null $tenant, callable $callback): mixed
    {
        $destination = $this->destination;
        $scoped = $destination === null ? $callback : fn (): mixed => $destination->withTenant($tenant, $callback);

        return parent::withTenant($tenant, fn (): mixed => $this->source->withTenant($tenant, $scoped));
    }

    /**
     * Opens the scope on the mirror, its source and its destination.
     *
     * {@inheritdoc}
     */
    public function withPreserveDates(callable $callback): mixed
    {
        $destination = $this->destination;
        $scoped = $destination === null ? $callback : fn (): mixed => $destination->withPreserveDates($callback);

        return parent::withPreserveDates(fn (): mixed => $this->source->withPreserveDates($scoped));
    }

    /**
     * Opens the scope on the mirror, its source and its destination.
     *
     * {@inheritdoc}
     */
    public function withPreserveSequence(callable $callback): mixed
    {
        $destination = $this->destination;
        $scoped = $destination === null ? $callback : fn (): mixed => $destination->withPreserveSequence($callback);

        return parent::withPreserveSequence(fn (): mixed => $this->source->withPreserveSequence($scoped));
    }

    /**
     * {@inheritdoc}
     */
    public function skipRelationships(callable $callback): mixed
    {
        return parent::skipRelationships(fn (): mixed => $this->source->skipRelationships($callback));
    }

    /**
     * {@inheritdoc}
     */
    public function skipRelationshipsExistCheck(callable $callback): mixed
    {
        return parent::skipRelationshipsExistCheck(fn (): mixed => $this->source->skipRelationshipsExistCheck($callback));
    }

    /**
     * {@inheritdoc}
     */
    public function addLifecycleHook(Lifecycle $hook): static
    {
        if ($hook instanceof Invalidator) {
            parent::addHook($hook);
        }

        $this->source->addHook($hook);

        return $this;
    }

    /**
     * Invalidates the mirror's own query cache, then lets the source invalidate its own and run
     * the lifecycle hooks, which are registered there (see addLifecycleHook()).
     */
    protected function trigger(Event $event, mixed $data = null): void
    {
        parent::trigger($event, $data);
        $this->source->trigger($event, $data);
    }

    /**
     * Also invalidates a query cache the source holds that is not the mirror's own.
     */
    protected function invalidate(Event $event, mixed $data = null): void
    {
        parent::invalidate($event, $data);

        if ($this->source->getQueryCache() !== $this->queryCache) {
            $this->source->invalidate($event, $data);
        }
    }

    /**
     * Lifecycle hooks are registered on the source (see addLifecycleHook()).
     */
    protected function triggerPropagatingHooks(Event $event, mixed $data = null): void
    {
        $this->source->triggerPropagatingHooks($event, $data);
    }

    /**
     * Silences the source, where lifecycle hooks are registered, and the mirror itself,
     * where decorators are.
     *
     * {@inheritdoc}
     */
    public function silent(callable $callback, ?array $listeners = null): mixed
    {
        return parent::silent(fn () => $this->source->silent($callback, $listeners), $listeners);
    }

    /**
     * Scoped to the mirror and its source only: the source checks the timestamp, and the destination applies
     * what the source accepted.
     *
     * {@inheritdoc}
     */
    public function withRequestTimestamp(?DateTime $requestTimestamp, callable $callback): mixed
    {
        return parent::withRequestTimestamp(
            $requestTimestamp,
            fn (): mixed => $this->source->withRequestTimestamp($requestTimestamp, $callback),
        );
    }

    /**
     * Keep the source database's cache invalidation scope open until its outer
     * adapter transaction commits.
     */
    #[\Override]
    public function withTransaction(callable $callback): mixed
    {
        return $this->source->withTransaction($callback);
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
        $result = $this->source->create($database);

        if ($this->destination !== null) {
            $this->destination->create($database);
        }

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
    public function listCollections(int $limit = 25, int $offset = 0): array
    {
        $result = $this->silent(fn () => $this->source->find(self::METADATA, [
            Query::notEqual(Document::ID, self::SOURCE_ONLY_COLLECTIONS),
            Query::limit($limit),
            Query::offset($offset),
        ]));

        $this->trigger(Event::CollectionList, $result);

        $collections = [];
        foreach ($result as $doc) {
            $collections[] = $doc instanceof Collection ? $doc : Collection::fromArray($doc->getArrayCopy());
        }

        return $collections;
    }

    /**
     * {@inheritdoc}
     */
    public function createCollection(Collection $collection): Collection
    {
        $collectionId = $collection->id;

        $result = $this->source->createCollection($collection);

        if ($this->destination === null) {
            return $result;
        }

        try {
            $filtered = $result;
            foreach ($this->writeFilters as $filter) {
                $filtered = $filter->beforeCreateCollection(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collectionId,
                    collection: $filtered,
                );
                if ($filtered === null) {
                    return $result;
                }
            }
            $result = $filtered;

            $this->destination->createCollection($collection);

            $this->silent(function () use ($collectionId) {
                $this->createUpgrades();

                $this->source->createDocument('upgrades', new Document([
                    Document::ID => $collectionId,
                    'collectionId' => $collectionId,
                    'status' => 'upgraded',
                ]));
            });
        } catch (Throwable $err) {
            $this->logError('createCollection', $err);
        }

        return $result instanceof Collection ? $result : Collection::fromArray($result->getArrayCopy());
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
            $filtered = $result;
            foreach ($this->writeFilters as $filter) {
                $filtered = $filter->beforeUpdateCollection(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $id,
                    collection: $filtered,
                );
                if ($filtered === null) {
                    return $result;
                }
            }
            $result = $filtered;

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
            $filtered = $document;
            foreach ($this->writeFilters as $filter) {
                $filtered = $filter->beforeUpdateAttribute(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    attributeId: $id,
                    attribute: $filtered,
                );
                if ($filtered === null) {
                    return $document;
                }
            }
            $document = $filtered;

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
                $document = $filter->beforeCreateIndex(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    indexId: $index->key,
                    index: $document,
                );
                if ($document === null) {
                    break;
                }
            }

            if ($document !== null) {
                $filteredIndex = Index::fromDocument($document);
                $result = $this->destination->createIndex($collection, $filteredIndex);
            }
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
            return $this->decorate(Event::DocumentCreate, $collection, $document);
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $this->decorate(Event::DocumentCreate, $collection, $document);
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

            $this->awaitReplications($collection, [$document->getId()]);
            $destination = $this->destination;
            $destination->withPreserveDates(fn (): Document => $destination->createDocument($collection, $clone));

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

        return $this->decorate(Event::DocumentCreate, $collection, $document);
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
        $onNext = $this->decorating(Event::DocumentsCreate, $collection, $onNext);
        $modified = $this->skipDuplicates
            ? $this->source->skipDuplicates(
                fn () => $this->source->createDocuments($collection, $documents, $batchSize, $onNext, $onError)
            )
            : $this->source->createDocuments($collection, $documents, $batchSize, $onNext, $onError);

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

        // Forward every input to destination. "upgraded" status means the schema
        // is mirrored, not that every row is backfilled, so a row that is a
        // duplicate on source may not yet exist on destination. In skipDuplicates
        // mode the destination runs its own INSERT IGNORE and decides per-row.
        $clones = [];
        $destination = $this->destination;

        try {
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
        } catch (Throwable $err) {
            $this->logError('createDocuments', $err);

            return $modified;
        }

        $skipDuplicates = $this->skipDuplicates;

        $this->replicate('createDocuments', $collection, self::documentIds($documents), function () use ($destination, $collection, $clones, $batchSize, $skipDuplicates): void {
            if ($skipDuplicates) {
                $destination->skipDuplicates(
                    fn () => $destination->withPreserveDates(
                        fn () => $destination->createDocuments(
                            $collection,
                            $clones,
                            $batchSize,
                        )
                    )
                );
            } else {
                $destination->withPreserveDates(
                    fn () => $destination->createDocuments(
                        $collection,
                        $clones,
                        $batchSize,
                    )
                );
            }

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
            return $this->decorate(Event::DocumentUpdate, $collection, $document);
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));

        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $this->decorate(Event::DocumentUpdate, $collection, $document);
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

            $this->awaitReplications($collection, [$id]);
            $destination = $this->destination;
            $destination->withPreserveDates(fn (): Document => $destination->updateDocument($collection, $id, $clone));

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

        return $this->decorate(Event::DocumentUpdate, $collection, $document);
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
        $onNext = $this->decorating(Event::DocumentsUpdate, $collection, $onNext);
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

        try {
            foreach ($this->writeFilters as $filter) {
                $clone = $filter->beforeUpdateDocuments(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    updates: $clone,
                    queries: $queries,
                );
            }
        } catch (Throwable $err) {
            $this->logError('updateDocuments', $err);

            return $modified;
        }

        $this->replicate('updateDocuments', $collection, null, function () use ($destination, $collection, $clone, $queries, $batchSize): void {
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
        });

        return $modified;
    }

    /**
     * upsertDocument() and upsertDocuments() upsert through this method, so each of them writes,
     * and fires its events, once on the source.
     *
     * {@inheritdoc}
     */
    public function upsertDocumentsWithIncrease(
        string $collection,
        string $attribute,
        array $documents,
        ?callable $onNext = null,
        ?callable $onError = null,
        int $batchSize = self::INSERT_BATCH_SIZE,
    ): int {
        $onNext = $this->decorating(Event::DocumentsUpsert, $collection, $onNext);
        $modified = $this->source->upsertDocumentsWithIncrease(
            $collection,
            $attribute,
            $documents,
            $onNext,
            $onError,
            $batchSize,
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
        $action = $attribute === '' ? 'upsertDocuments' : 'upsertDocumentsWithIncrease';

        try {
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
        } catch (Throwable $err) {
            $this->logError($action, $err);

            return $modified;
        }

        $this->replicate($action, $collection, self::documentIds($documents), function () use ($destination, $collection, $attribute, $clones, $batchSize): void {
            $destination->withPreserveDates(
                fn () => $destination->upsertDocumentsWithIncrease(
                    $collection,
                    $attribute,
                    $clones,
                    batchSize: $batchSize,
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

        try {
            foreach ($this->writeFilters as $filter) {
                $filter->beforeDeleteDocument(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    documentId: $id,
                );
            }
        } catch (Throwable $err) {
            $this->logError('deleteDocument', $err);

            return $result;
        }

        $destination = $this->destination;
        $this->replicate('deleteDocument', $collection, [$id], function () use ($destination, $collection, $id): void {
            $destination->deleteDocument($collection, $id);

            foreach ($this->writeFilters as $filter) {
                $filter->afterDeleteDocument(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    documentId: $id,
                );
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

        try {
            foreach ($this->writeFilters as $filter) {
                $filter->beforeDeleteDocuments(
                    source: $this->source,
                    destination: $this->destination,
                    collectionId: $collection,
                    queries: $queries,
                );
            }
        } catch (Throwable $err) {
            $this->logError('deleteDocuments', $err);

            return $modified;
        }

        $destination = $this->destination;
        $this->replicate('deleteDocuments', $collection, null, function () use ($destination, $collection, $queries, $batchSize): void {
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
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float|string $value = 1, int|float|string|null $max = null): Document
    {
        $this->awaitReplications($collection, [$id]);

        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function decreaseDocumentAttribute(string $collection, string $id, string $attribute, int|float|string $value = 1, int|float|string|null $min = null): Document
    {
        $this->awaitReplications($collection, [$id]);

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

        $this->source->createCollection(new Collection(id: 'upgrades', attributes: [
            Attribute::string(key: 'collectionId', required: true),
            Attribute::string(key: 'status'),
        ], indexes: [
            Index::unique(key: '_unique_collection', attributes: ['collectionId'], lengths: [Database::LENGTH_KEY]),
            Index::key(key: '_status_index', attributes: ['status'], lengths: [Database::LENGTH_KEY], orders: [Order::Asc]),
        ]));
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

    /**
     * Applies the mirror's decorators to a document one of its writes returns. The source has none of them, so what
     * it wrote, and what replication clones from it, stays undecorated.
     */
    private function decorate(Event $event, string $collection, Document $document): Document
    {
        if ($this->decorators === []) {
            return $document;
        }

        return $this->decorateDocument($event, $this->silent(fn (): Collection => $this->getCollection($collection)), $document);
    }

    /**
     * Hands $onNext a decorated copy of each document a bulk write returns: the source may pass the very documents
     * the caller gave it, which replication clones afterwards.
     */
    private function decorating(Event $event, string $collection, ?callable $onNext): ?callable
    {
        if ($onNext === null || $this->decorators === []) {
            return $onNext;
        }

        return function (Document $document, mixed ...$arguments) use ($event, $collection, $onNext): void {
            $onNext($this->decorate($event, $collection, clone $document), ...$arguments);
        };
    }

    /**
     * Applies a write to the destination under the authorization, relationship, silence, tenant and toggle state the
     * caller has at the time of the call, without its request timestamp (the source checked it), and reports a
     * failure through onError(). Inside a coroutine the write runs in a coroutine
     * of its own, once every earlier replication that can reach the same documents has finished; outside one it runs
     * before this returns, since a task that yields outside a scheduler never resumes.
     *
     * @param  array<string>|null  $documentIds  The documents the write can reach, or null for every document of the collection
     * @param  Closure(): void  $write
     */
    private function replicate(string $action, string $collection, ?array $documentIds, Closure $write): void
    {
        $destination = $this->destination;
        if ($destination === null) {
            return;
        }

        $snapshot = $this->source->snapshot();
        $apply = function () use ($action, $destination, $snapshot, $write): void {
            try {
                $destination->withSnapshot(
                    $snapshot,
                    fn (): mixed => $destination->withRequestTimestamp(null, $write),
                );
            } catch (Throwable $error) {
                $this->logError($action, $error);
            }
        };

        if (! \extension_loaded('swoole') || Coroutine::getCid() <= 0) {
            $apply();

            return;
        }

        $earlier = $this->replicationsBefore($collection, $documentIds);
        $finished = new Channel(1);
        $this->queueReplication($collection, $documentIds, $finished);

        Promise::async(function () use ($apply, $earlier, $collection, $documentIds, $finished): void {
            try {
                foreach ($earlier as $replication) {
                    $replication->pop();
                }

                $apply();
            } finally {
                $this->releaseReplication($collection, $documentIds, $finished);
            }
        });
    }

    /**
     * Waits until every queued replication that can reach these documents has finished, so a write the caller
     * replicates itself reaches the destination after them.
     *
     * @param  array<string>  $documentIds
     */
    private function awaitReplications(string $collection, array $documentIds): void
    {
        foreach ($this->replicationsBefore($collection, $documentIds) as $replication) {
            $replication->pop();
        }
    }

    /**
     * @param  array<string>|null  $documentIds
     * @return array<int, Channel>
     */
    private function replicationsBefore(string $collection, ?array $documentIds): array
    {
        $pending = $this->documentReplications[$collection] ?? [];
        $earlier = $documentIds === null
            ? \array_values($pending)
            : \array_values(\array_intersect_key($pending, \array_flip($documentIds)));

        if (isset($this->collectionReplications[$collection])) {
            $earlier[] = $this->collectionReplications[$collection];
        }

        $unique = [];
        foreach ($earlier as $replication) {
            $unique[\spl_object_id($replication)] = $replication;
        }

        return $unique;
    }

    /**
     * @param  array<string>|null  $documentIds
     */
    private function queueReplication(string $collection, ?array $documentIds, Channel $finished): void
    {
        if ($documentIds === null) {
            unset($this->documentReplications[$collection]);
            $this->collectionReplications[$collection] = $finished;

            return;
        }

        foreach ($documentIds as $id) {
            $this->documentReplications[$collection][$id] = $finished;
        }
    }

    /**
     * @param  array<string>|null  $documentIds
     */
    private function releaseReplication(string $collection, ?array $documentIds, Channel $finished): void
    {
        $finished->close();

        if ($documentIds === null) {
            if (($this->collectionReplications[$collection] ?? null) === $finished) {
                unset($this->collectionReplications[$collection]);
            }

            return;
        }

        foreach ($documentIds as $id) {
            if (($this->documentReplications[$collection][$id] ?? null) === $finished) {
                unset($this->documentReplications[$collection][$id]);
            }
        }

        if (($this->documentReplications[$collection] ?? null) === []) {
            unset($this->documentReplications[$collection]);
        }
    }

    /**
     * @param  array<Document>  $documents
     * @return array<string>
     */
    private static function documentIds(array $documents): array
    {
        return \array_map(static fn (Document $document): string => $document->getId(), $documents);
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
        if ($hook instanceof Lifecycle) {
            $this->addLifecycleHook($hook);
        } else {
            parent::addHook($hook);
        }

        if ($hook instanceof Relationships) {
            $this->source->addHook(new Relationships($this->source));
            $this->destination?->addHook(new Relationships($this->destination));
        }

        if ($hook instanceof Write) {
            $this->destination?->getAdapter()->addWriteHook($hook);
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
