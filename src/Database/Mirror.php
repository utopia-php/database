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
use Utopia\Database\Cache\Query as ResultCache;
use Utopia\Database\Event\Domain;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Filter\Registry;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Hook\Write;
use Utopia\Database\Mirror\Failure;
use Utopia\Database\Mirror\Filter;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Queries\Base;
use Utopia\Query\OrderDirection;

/**
 * Wraps a source Database and replicates write operations to an optional destination Database.
 */
class Mirror extends Database
{
    protected Database $source;

    protected ?Database $destination = null;

    /**
     * @var array<Filter>
     */
    protected array $writeFilters = [];

    /**
     * @var array<callable(Failure): void>
     */
    protected array $errorCallbacks = [];

    protected const array SOURCE_ONLY_COLLECTIONS = [
        'upgrades',
    ];

    /**
     * Closed once the latest destination change queued through the mirror has been applied or has failed
     *
     * @var Channel|null
     */
    private ?object $latestReplication = null;

    /**
     * Coroutines applying a destination change, by coroutine id
     *
     * @var array<int, true>
     */
    private array $applying = [];

    /**
     * Set once the constructor has stored the source and destination. Database::__construct() calls
     * setAuthorization() before then, and that call must leave their authorization in place.
     */
    private bool $wrapped = false;

    /**
     * The mirror uses the source's authorization; the source and the destination keep their own.
     *
     * @param  array<Filter>  $filters
     */
    public function __construct(
        Database $source,
        ?Database $destination = null,
        array $filters = [],
    ) {
        parent::__construct(
            $source->getAdapter(),
            $source->getCache()
        );
        $this->source = $source;
        $this->destination = $destination;
        $this->writeFilters = $filters;
        $this->wrapped = true;
        parent::setAuthorization($source->getAuthorization());
    }

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
    #[\Override]
    protected function getDocumentsValidator(Document $collection, array $joinedCollections = []): Validator\Queries\Documents
    {
        return $this->source->getDocumentsValidator($collection, $joinedCollections);
    }

    /**
     * Delegate to the source database, so a narrow list is checked under the source's adapter and
     * query limits like every other list.
     *
     * @param  array<mixed>  $queries
     * @param  array<Document>  $joinedCollections
     */
    #[\Override]
    protected function getQueriesValidator(Document $collection, array $queries, array $joinedCollections = []): Base
    {
        return $this->source->getQueriesValidator($collection, $queries, $joinedCollections);
    }

    /**
     * Delegate metadata reads to the source database. Mirror's schema mutator
     * overrides forward writes to source and destination directly, so routing
     * reads through the source keeps its view of attributes and relationships
     * in lockstep with the authoritative database.
     */
    #[\Override]
    public function getCollection(string $collection): Collection
    {
        return $this->source->getCollection($collection);
    }

    #[\Override]
    public function findCollection(string $collection): ?Collection
    {
        return $this->source->findCollection($collection);
    }

    public function getDestination(): ?Database
    {
        return $this->destination;
    }

    /**
     * @param  callable(Failure $failure): void  $callback
     */
    public function onError(callable $callback): static
    {
        $this->errorCallbacks[] = $callback;

        return $this;
    }

    /**
     * Waits until every replication queued through the mirror so far has reached the destination or has been
     * reported to onError(), or until the timeout has passed. Outside a coroutine, and inside a replication, there is
     * nothing to wait for.
     *
     * @param  int|null  $timeout  Milliseconds to wait at most: null waits for as long as the replications take, and 0
     *                             does not wait
     *
     * @throws Exception When the timeout is negative
     */
    public function awaitReplications(?int $timeout = null): void
    {
        if ($timeout !== null && $timeout < 0) {
            throw new Exception("A replication timeout cannot be negative, {$timeout} given");
        }

        if ($timeout === 0 || $this->appliesInline()) {
            return;
        }

        $this->latestReplication?->pop($timeout === null ? -1 : $timeout / 1000);
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
        } catch (Throwable $error) {
            $this->logError($method, $error);
        }

        return $sourceResult;
    }

    /**
     * Calls the method on the source, then on the destination in order with the replications (see inOrder()); a
     * destination failure is reported to onError().
     *
     * @param  array<mixed>  $args
     */
    private function delegateInOrder(string $method, array $args = []): mixed
    {
        $sourceResult = $this->source->{$method}(...$args);

        $destination = $this->destination;
        if ($destination === null) {
            return $sourceResult;
        }

        try {
            $this->inOrder(fn (): mixed => $destination->{$method}(...$args));
        } catch (Throwable $error) {
            $this->logError($method, $error);
        }

        return $sourceResult;
    }

    #[\Override]
    public function setDatabase(string $name): static
    {
        parent::setDatabase($name);
        $this->source->setDatabase($name);
        $this->destination?->setDatabase($name);

        return $this;
    }

    #[\Override]
    public function setNamespace(string $namespace): static
    {
        parent::setNamespace($namespace);
        $this->source->setNamespace($namespace);
        $this->destination?->setNamespace($namespace);

        return $this;
    }

    #[\Override]
    public function setSharedTables(bool $sharedTables): static
    {
        parent::setSharedTables($sharedTables);
        $this->source->setSharedTables($sharedTables);
        $this->destination?->setSharedTables($sharedTables);

        return $this;
    }

    #[\Override]
    public function setTenant(int|string|null $tenant): static
    {
        parent::setTenant($tenant);
        $this->source->setTenant($tenant);
        $this->destination?->setTenant($tenant);

        return $this;
    }

    #[\Override]
    public function setMaxQueryValues(int $max): static
    {
        parent::setMaxQueryValues($max);
        $this->source->setMaxQueryValues($max);
        $this->destination?->setMaxQueryValues($max);

        return $this;
    }

    #[\Override]
    public function setCache(Cache $cache): static
    {
        parent::setCache($cache);
        $this->source->setCache($cache);
        $this->destination?->setCache($cache);

        return $this;
    }

    /**
     * The query cache is attached to the mirror last, so it takes its name and writer timeout from the mirror.
     */
    #[\Override]
    public function setQueryCache(?ResultCache $queryCache): static
    {
        $this->source->setQueryCache($queryCache);
        $this->destination?->setQueryCache($queryCache);

        return parent::setQueryCache($queryCache);
    }

    #[\Override]
    public function setCacheName(string $name): static
    {
        parent::setCacheName($name);
        $this->source->setCacheName($name);
        $this->destination?->setCacheName($name);

        return $this;
    }

    #[\Override]
    public function setCacheWriterTimeout(int $seconds): static
    {
        parent::setCacheWriterTimeout($seconds);
        $this->source->setCacheWriterTimeout($seconds);
        $this->destination?->setCacheWriterTimeout($seconds);

        return $this;
    }

    #[\Override]
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
     */
    #[\Override]
    public function setTimeout(int $milliseconds, Event $event = Event::All): static
    {
        $this->delegateInOrder(__FUNCTION__, \func_get_args());

        return $this;
    }

    #[\Override]
    public function clearTimeout(Event $event = Event::All): void
    {
        $this->delegateInOrder(__FUNCTION__, \func_get_args());
    }

    #[\Override]
    public function setGlobalCollections(array $collections): static
    {
        parent::setGlobalCollections($collections);
        $this->source->setGlobalCollections($collections);
        $this->destination?->setGlobalCollections($collections);

        return $this;
    }

    #[\Override]
    public function resetGlobalCollections(): void
    {
        parent::resetGlobalCollections();
        $this->source->resetGlobalCollections();
        $this->destination?->resetGlobalCollections();
    }

    #[\Override]
    public function setMetadata(string $key, mixed $value): static
    {
        parent::setMetadata($key, $value);
        $this->source->setMetadata($key, $value);
        $this->destination?->setMetadata($key, $value);

        return $this;
    }

    #[\Override]
    public function resetMetadata(): void
    {
        parent::resetMetadata();
        $this->source->resetMetadata();
        $this->destination?->resetMetadata();
    }

    /**
     * The source shares this database's adapter; the destination keeps its own mode.
     */
    #[\Override]
    public function setSchemaless(bool $schemaless): static
    {
        parent::setSchemaless($schemaless);
        $this->source->setSchemaless($schemaless);

        return $this;
    }

    #[\Override]
    public function setMigrating(bool $migrating): static
    {
        parent::setMigrating($migrating);
        $this->source->setMigrating($migrating);
        $this->destination?->setMigrating($migrating);

        return $this;
    }

    #[\Override]
    public function setFilters(Registry $filters): static
    {
        parent::setFilters($filters);
        $this->source->setFilters($filters);
        $this->destination?->setFilters($filters);

        return $this;
    }

    /**
     * A destination that cannot apply the setting is reported through onError(), like setTimeout().
     */
    #[\Override]
    public function setLocks(bool $locks): static
    {
        parent::setLocks($locks);
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    #[\Override]
    public function setFiltering(bool $filtering): static
    {
        parent::setFiltering($filtering);
        $this->source->setFiltering($filtering);
        $this->destination?->setFiltering($filtering);

        return $this;
    }

    #[\Override]
    public function withFiltering(bool $filtering, callable $callback, ?array $filters = null): mixed
    {
        $scoped = fn (): mixed => parent::withFiltering($filtering, $callback, $filters);
        $destination = $this->destination;

        return $this->source->withFiltering(
            $filtering,
            fn (): mixed => $destination === null ? $scoped() : $destination->withFiltering($filtering, $scoped, $filters),
            $filters,
        );
    }

    /**
     * The mirror queries through its source's adapter, so it reports to the source's profiler.
     */
    #[\Override]
    public function setProfiling(bool $profiling): static
    {
        $this->source->setProfiling($profiling);
        $this->destination?->setProfiling($profiling);
        $this->profiler = $this->source->getProfiler();

        return $this;
    }

    #[\Override]
    public function setDropUnknownAttributes(bool $drop): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        $this->dropUnknownAttributes = $drop;

        return $this;
    }

    #[\Override]
    public function setPreserveDates(bool $preserve): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return parent::setPreserveDates($preserve);
    }

    #[\Override]
    public function setPreserveSequence(bool $preserve): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return parent::setPreserveSequence($preserve);
    }

    #[\Override]
    public function setValidation(bool $validation): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return parent::setValidation($validation);
    }

    #[\Override]
    public function withValidation(bool $validation, callable $callback): mixed
    {
        $destination = $this->destination;
        $scoped = $destination === null ? $callback : fn (): mixed => $destination->withValidation($validation, $callback);

        return parent::withValidation($validation, fn (): mixed => $this->source->withValidation($validation, $scoped));
    }

    /**
     * Opens the scope on the mirror, its source and its destination, so the writes the mirror replicates before
     * returning use the tenant too; the replications it queues carry it (see replicate()).
     */
    #[\Override]
    public function withTenant(int|string|null $tenant, callable $callback): mixed
    {
        $destination = $this->destination;
        $scoped = $destination === null ? $callback : fn (): mixed => $destination->withTenant($tenant, $callback);

        return parent::withTenant($tenant, fn (): mixed => $this->source->withTenant($tenant, $scoped));
    }

    #[\Override]
    public function withPreserveDates(bool $preserve, callable $callback): mixed
    {
        $destination = $this->destination;
        $scoped = $destination === null ? $callback : fn (): mixed => $destination->withPreserveDates($preserve, $callback);

        return parent::withPreserveDates($preserve, fn (): mixed => $this->source->withPreserveDates($preserve, $scoped));
    }

    #[\Override]
    public function withPreserveSequence(bool $preserve, callable $callback): mixed
    {
        $destination = $this->destination;
        $scoped = $destination === null ? $callback : fn (): mixed => $destination->withPreserveSequence($preserve, $callback);

        return parent::withPreserveSequence($preserve, fn (): mixed => $this->source->withPreserveSequence($preserve, $scoped));
    }

    #[\Override]
    public function skipRelationships(callable $callback): mixed
    {
        return parent::skipRelationships(fn (): mixed => $this->source->skipRelationships($callback));
    }

    #[\Override]
    public function skipRelationshipsExistCheck(callable $callback): mixed
    {
        return parent::skipRelationshipsExistCheck(fn (): mixed => $this->source->skipRelationshipsExistCheck($callback));
    }

    /**
     * Lifecycle hooks are registered on the source, where the mirror's writes run; the mirror keeps its own
     * query-cache invalidator as well.
     */
    protected function addLifecycleHook(Lifecycle $hook): static
    {
        $this->source->addHook($hook);

        return $this;
    }

    /**
     * Lifecycle hooks are registered on the source (see addLifecycleHook()).
     */
    #[\Override]
    protected function listens(Event $event): array
    {
        return $this->source->listens($event);
    }

    /**
     * Lifecycle hooks are registered on the source (see addLifecycleHook()).
     */
    #[\Override]
    protected function dispatch(Domain $event, array $listeners): void
    {
        $this->source->dispatch($event, $listeners);
    }

    /**
     * Also invalidates a query cache the source holds that is not the mirror's own.
     */
    #[\Override]
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
    #[\Override]
    protected function dispatchPropagating(Domain $event, array $listeners): void
    {
        $this->source->dispatchPropagating($event, $listeners);
    }

    #[\Override]
    public function silent(callable $callback, ?array $hooks = null): mixed
    {
        return parent::silent(fn () => $this->source->silent($callback, $hooks), $hooks);
    }

    /**
     * Scoped to the mirror and its source only: the source checks the timestamp, and the destination applies
     * what the source accepted.
     */
    #[\Override]
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

    #[\Override]
    public function exists(?string $database = null): bool
    {
        /** @var bool $result */
        $result = $this->delegateInOrder(__FUNCTION__, \func_get_args());

        return $result;
    }

    #[\Override]
    public function collectionExists(string $collection, ?string $database = null): bool
    {
        /** @var bool $result */
        $result = $this->delegateInOrder(__FUNCTION__, \func_get_args());

        return $result;
    }

    #[\Override]
    public function update(string $database, string $new): bool
    {
        /** @var bool $result */
        $result = $this->delegateInOrder(__FUNCTION__, [$database, $new]);

        return $result;
    }

    #[\Override]
    public function create(?string $database = null): bool
    {
        $result = $this->source->create($database);

        $destination = $this->destination;
        if ($destination !== null) {
            $this->inOrder(fn (): bool => $destination->create($database));
        }

        return $result;
    }

    #[\Override]
    public function delete(?string $database = null): bool
    {
        /** @var bool $result */
        $result = $this->delegateInOrder(__FUNCTION__, \func_get_args());
        return $result;
    }

    #[\Override]
    public function listCollections(int $limit = 25, int $offset = 0): array
    {
        $result = $this->silent(fn () => $this->source->find(self::METADATA, [
            Query::notEqual(Document::ID, self::SOURCE_ONLY_COLLECTIONS),
            Query::limit($limit),
            Query::offset($offset),
        ]));

        $collections = [];
        foreach ($result as $doc) {
            $collections[] = Collection::fromDocument($doc);
        }

        $listeners = $this->listens(Event::CollectionList);
        if ($listeners !== []) {
            $this->dispatch(new Event\Collection\Listed($collections), $listeners);
        }

        return $collections;
    }

    #[\Override]
    public function createCollection(Collection $collection): Collection
    {
        $collectionId = $collection->getId();

        $result = $this->source->createCollection($collection);

        $destination = $this->destination;
        if ($destination === null) {
            return $result;
        }

        try {
            $replicated = $this->inOrder(function () use ($destination, $collection, $collectionId, $result): ?Document {
                $filtered = $result;
                foreach ($this->writeFilters as $filter) {
                    $filtered = $filter->beforeCreateCollection(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collectionId,
                        collection: $filtered,
                    );
                    if ($filtered === null) {
                        return null;
                    }
                }

                $destination->createCollection($collection);

                return $filtered;
            });
            if ($replicated === null) {
                return $result;
            }
            $result = $replicated;

            $this->silent(function () use ($collectionId) {
                $this->createUpgrades();

                $this->source->createDocument('upgrades', new Document([
                    Document::ID => $collectionId,
                    'collectionId' => $collectionId,
                    'status' => 'upgraded',
                ]));
            });
        } catch (Throwable $error) {
            $this->logError('createCollection', $error);
        }

        return Collection::fromDocument($result);
    }

    #[\Override]
    public function updateCollection(string $collection, CollectionUpdate $update): Collection
    {
        $result = $this->source->updateCollection($collection, $update);

        $destination = $this->destination;
        if ($destination === null) {
            return $result;
        }

        try {
            $this->inOrder(function () use ($destination, $collection, $update, $result): void {
                $filtered = $result;
                foreach ($this->writeFilters as $filter) {
                    $filtered = $filter->beforeUpdateCollection(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        collection: $filtered,
                    );
                    if ($filtered === null) {
                        return;
                    }
                }

                $destination->updateCollection($collection, $update);
            });
        } catch (Throwable $error) {
            $this->logError('updateCollection', $error);
        }

        return $result;
    }

    #[\Override]
    public function deleteCollection(string $collection): void
    {
        $this->source->deleteCollection($collection);

        $destination = $this->destination;
        if ($destination === null) {
            return;
        }

        try {
            $this->inOrder(function () use ($destination, $collection): void {
                $destination->deleteCollection($collection);

                foreach ($this->writeFilters as $filter) {
                    $filter->beforeDeleteCollection(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                    );
                }
            });
        } catch (Throwable $error) {
            $this->logError('deleteCollection', $error);
        }
    }

    #[\Override]
    public function createAttribute(string $collection, Attribute $attribute): Attribute
    {
        $result = $this->source->createAttribute($collection, $attribute);

        $destination = $this->destination;
        if ($destination === null) {
            return $result;
        }

        try {
            $this->inOrder(function () use ($destination, $collection, $result): void {
                $filtered = $this->filterCreatedAttribute($destination, $collection, $result);
                if ($filtered !== null) {
                    $destination->createAttribute($collection, $filtered);
                }
            });
        } catch (Throwable $error) {
            $this->logError('createAttribute', $error);
        }

        return $result;
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<Attribute>
     */
    #[\Override]
    public function createAttributes(string $collection, array $attributes): array
    {
        $result = $this->source->createAttributes($collection, $attributes);

        $destination = $this->destination;
        if ($destination === null) {
            return $result;
        }

        try {
            $this->inOrder(function () use ($destination, $collection, $result): void {
                $filtered = [];
                foreach ($result as $attribute) {
                    $attribute = $this->filterCreatedAttribute($destination, $collection, $attribute);
                    if ($attribute !== null) {
                        $filtered[] = $attribute;
                    }
                }

                if ($filtered !== []) {
                    $destination->createAttributes($collection, $filtered);
                }
            });
        } catch (Throwable $error) {
            $this->logError('createAttributes', $error);
        }

        return $result;
    }

    #[\Override]
    public function updateAttribute(string $collection, string $key, AttributeUpdate $update): Attribute
    {
        $result = $this->source->updateAttribute($collection, $key, $update);

        $destination = $this->destination;
        if ($destination === null) {
            return $result;
        }

        try {
            $this->inOrder(function () use ($destination, $collection, $key, $update, $result): void {
                $filtered = $result->toDocument();
                foreach ($this->writeFilters as $filter) {
                    $filtered = $filter->beforeUpdateAttribute(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        attributeId: $key,
                        attribute: $filtered,
                    );
                    if ($filtered === null) {
                        return;
                    }
                }

                $destination->updateAttribute(
                    $collection,
                    $key,
                    self::filteredUpdate($update, $result, Attribute::fromDocument($filtered)),
                );
            });
        } catch (Throwable $error) {
            $this->logError('updateAttribute', $error);
        }

        return $result;
    }

    #[\Override]
    public function deleteAttribute(string $collection, string $key): void
    {
        $this->source->deleteAttribute($collection, $key);

        $destination = $this->destination;
        if ($destination === null) {
            return;
        }

        try {
            $this->inOrder(function () use ($destination, $collection, $key): void {
                foreach ($this->writeFilters as $filter) {
                    $filter->beforeDeleteAttribute(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        attributeId: $key,
                    );
                }

                $destination->deleteAttribute($collection, $key);
            });
        } catch (Throwable $error) {
            $this->logError('deleteAttribute', $error);
        }
    }

    #[\Override]
    public function createIndex(string $collection, Index $index): Index
    {
        $result = $this->source->createIndex($collection, $index);

        $destination = $this->destination;
        if ($destination === null) {
            return $result;
        }

        try {
            $this->inOrder(function () use ($destination, $collection, $result): void {
                $document = $result->toDocument();

                foreach ($this->writeFilters as $filter) {
                    $document = $filter->beforeCreateIndex(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        indexId: $result->key,
                        index: $document,
                    );
                    if ($document === null) {
                        return;
                    }
                }

                $destination->createIndex($collection, Index::fromDocument($document));
            });
        } catch (Throwable $error) {
            $this->logError('createIndex', $error);
        }

        return $result;
    }

    /**
     * @param  list<Index>  $indexes
     * @return list<Index>
     */
    #[\Override]
    public function createIndexes(string $collection, array $indexes): array
    {
        $result = $this->source->createIndexes($collection, $indexes);

        $destination = $this->destination;
        if ($destination === null) {
            return $result;
        }

        try {
            $this->inOrder(function () use ($destination, $collection, $result): void {
                $filtered = [];
                foreach ($result as $index) {
                    $document = $index->toDocument();

                    foreach ($this->writeFilters as $filter) {
                        $document = $filter->beforeCreateIndex(
                            source: $this->source,
                            destination: $destination,
                            collectionId: $collection,
                            indexId: $index->key,
                            index: $document,
                        );
                        if ($document === null) {
                            continue 2;
                        }
                    }

                    $filtered[] = Index::fromDocument($document);
                }

                if ($filtered !== []) {
                    $destination->createIndexes($collection, $filtered);
                }
            });
        } catch (Throwable $error) {
            $this->logError('createIndexes', $error);
        }

        return $result;
    }

    #[\Override]
    public function deleteIndex(string $collection, string $key): void
    {
        $this->source->deleteIndex($collection, $key);

        $destination = $this->destination;
        if ($destination === null) {
            return;
        }

        try {
            $this->inOrder(function () use ($destination, $collection, $key): void {
                $destination->deleteIndex($collection, $key);

                foreach ($this->writeFilters as $filter) {
                    $filter->beforeDeleteIndex(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        indexId: $key,
                    );
                }
            });
        } catch (Throwable $error) {
            $this->logError('deleteIndex', $error);
        }
    }

    /**
     * The attribute the write filters let through to the destination, or null when one of them drops it.
     */
    private function filterCreatedAttribute(Database $destination, string $collection, Attribute $attribute): ?Attribute
    {
        $document = $attribute->toDocument();

        foreach ($this->writeFilters as $filter) {
            $document = $filter->beforeCreateAttribute(
                source: $this->source,
                destination: $destination,
                collectionId: $collection,
                attributeId: $attribute->key,
                attribute: $document,
            );
            if ($document === null) {
                return null;
            }
        }

        return Attribute::fromDocument($document);
    }

    /**
     * The update the destination applies: every field the source update set or a write filter changed, at the
     * filtered value, and nothing else, so an unchanged column is not rewritten.
     */
    private static function filteredUpdate(AttributeUpdate $update, Attribute $source, Attribute $filtered): AttributeUpdate
    {
        $formatChanged = $filtered->format?->name !== $source->format?->name
            || $filtered->format?->options !== $source->format?->options;

        return new AttributeUpdate(
            type: $update->type !== null || $filtered->type !== $source->type ? $filtered->type : null,
            size: $update->size !== null || $filtered->size !== $source->size ? $filtered->size : null,
            required: $update->required !== null || $filtered->required !== $source->required ? $filtered->required : null,
            default: $update->changesDefault() || $filtered->default !== $source->default ? $filtered->default : Unchanged::Value,
            signed: $update->signed !== null || $filtered->signed !== $source->signed ? $filtered->signed : null,
            array: $update->array !== null || $filtered->array !== $source->array ? $filtered->array : null,
            format: $update->changesFormat() || $formatChanged ? $filtered->format : Unchanged::Value,
            filters: $update->filters !== null || $filtered->filters !== $source->filters ? $filtered->filters : null,
            key: $update->key !== null || $filtered->key !== $source->key ? $filtered->key : null,
        );
    }

    #[\Override]
    public function createDocument(string $collection, Document $document): Document
    {
        $document = $this->source->createDocument($collection, $document);

        $destination = $this->destination;
        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $destination === null
        ) {
            return $this->decorate(Event::DocumentCreate, $collection, $document);
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $this->decorate(Event::DocumentCreate, $collection, $document);
        }

        try {
            $clone = clone $document;
            $this->inOrder(function () use ($destination, $collection, $clone): void {
                foreach ($this->writeFilters as $filter) {
                    $clone = $filter->beforeCreateDocument(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        document: $clone,
                    );
                }

                $destination->withPreserveDates(true, fn (): Document => $destination->createDocument($collection, $clone));

                foreach ($this->writeFilters as $filter) {
                    $filter->afterCreateDocument(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        document: $clone,
                    );
                }
            });
        } catch (Throwable $error) {
            $this->logError('createDocument', $error);
        }

        return $this->decorate(Event::DocumentCreate, $collection, $document);
    }

    #[\Override]
    public function createDocuments(
        string $collection,
        array $documents,
        int $batchSize = self::BATCH_SIZE,
        ?callable $onNext = null,
    ): int {
        $onNext = $this->decorating(Event::DocumentsCreate, $collection, $onNext);
        $modified = $this->isIgnoringDuplicates()
            ? $this->source->ignoreDuplicates(
                fn () => $this->source->createDocuments($collection, $documents, $batchSize, $onNext)
            )
            : $this->source->createDocuments($collection, $documents, $batchSize, $onNext);

        $destination = $this->destination;
        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $destination === null
        ) {
            return $modified;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $modified;
        }

        $clones = \array_map(static fn (Document $document): Document => clone $document, $documents);
        $ignoreDuplicates = $this->isIgnoringDuplicates();

        $this->replicate('createDocuments', function () use ($destination, $collection, $clones, $batchSize, $ignoreDuplicates): void {
            foreach ($clones as $index => $clone) {
                foreach ($this->writeFilters as $filter) {
                    $clone = $filter->beforeCreateDocument(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        document: $clone,
                    );
                }
                $clones[$index] = $clone;
            }

            $create = fn (): mixed => $destination->withPreserveDates(
                true,
                fn (): int => $destination->createDocuments($collection, $clones, $batchSize),
            );
            $ignoreDuplicates ? $destination->ignoreDuplicates($create) : $create();

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

    #[\Override]
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        $document = $this->source->updateDocument($collection, $id, $document);

        $destination = $this->destination;
        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $destination === null
        ) {
            return $this->decorate(Event::DocumentUpdate, $collection, $document);
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));

        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $this->decorate(Event::DocumentUpdate, $collection, $document);
        }

        try {
            $clone = clone $document;
            $this->inOrder(function () use ($destination, $collection, $id, $clone): void {
                foreach ($this->writeFilters as $filter) {
                    $clone = $filter->beforeUpdateDocument(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        document: $clone,
                    );
                }

                $destination->withPreserveDates(true, fn (): Document => $destination->updateDocument($collection, $id, $clone));

                foreach ($this->writeFilters as $filter) {
                    $filter->afterUpdateDocument(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        document: $clone,
                    );
                }
            });
        } catch (Throwable $error) {
            $this->logError('updateDocument', $error);
        }

        return $this->decorate(Event::DocumentUpdate, $collection, $document);
    }

    #[\Override]
    public function updateDocuments(
        string $collection,
        Document $updates,
        array $queries = [],
        int $batchSize = self::BATCH_SIZE,
        ?callable $onNext = null,
    ): int {
        $onNext = $this->decorating(Event::DocumentsUpdate, $collection, $onNext);
        $modified = $this->source->updateDocuments(
            $collection,
            $updates,
            $queries,
            $batchSize,
            $onNext,
        );

        $destination = $this->destination;
        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $destination === null
        ) {
            return $modified;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $modified;
        }

        $clone = clone $updates;

        $this->replicate('updateDocuments', function () use ($destination, $collection, $clone, $queries, $batchSize): void {
            foreach ($this->writeFilters as $filter) {
                $clone = $filter->beforeUpdateDocuments(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    updates: $clone,
                    queries: $queries,
                );
            }

            $destination->withPreserveDates(
                true,
                fn (): int => $destination->updateDocuments(
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

    #[\Override]
    public function upsertDocument(string $collection, Document $document): Document
    {
        $upserted = $this->source->upsertDocument($collection, $document);

        $destination = $this->destination;
        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $destination === null
        ) {
            return $this->decorate(Event::DocumentUpsert, $collection, $upserted);
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $this->decorate(Event::DocumentUpsert, $collection, $upserted);
        }

        $clone = clone $document;

        $this->replicate('upsertDocument', function () use ($destination, $collection, $clone): void {
            foreach ($this->writeFilters as $filter) {
                $clone = $filter->beforeCreateOrUpdateDocument(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    document: $clone,
                );
            }

            $destination->withPreserveDates(true, fn (): Document => $destination->upsertDocument($collection, $clone));

            foreach ($this->writeFilters as $filter) {
                $filter->afterCreateOrUpdateDocument(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    document: $clone,
                );
            }
        });

        return $this->decorate(Event::DocumentUpsert, $collection, $upserted);
    }

    #[\Override]
    public function upsertDocuments(
        string $collection,
        array $documents,
        int $batchSize = self::BATCH_SIZE,
        ?callable $onNext = null,
        ?string $increase = null,
    ): int {
        $onNext = $this->decorating(Event::DocumentsUpsert, $collection, $onNext);
        $modified = $this->source->upsertDocuments(
            $collection,
            $documents,
            $batchSize,
            $onNext,
            $increase,
        );

        $destination = $this->destination;
        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $destination === null
        ) {
            return $modified;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $modified;
        }

        $clones = \array_map(static fn (Document $document): Document => clone $document, $documents);

        $this->replicate('upsertDocuments', function () use ($destination, $collection, $increase, $clones, $batchSize): void {
            foreach ($clones as $index => $clone) {
                foreach ($this->writeFilters as $filter) {
                    $clone = $filter->beforeCreateOrUpdateDocument(
                        source: $this->source,
                        destination: $destination,
                        collectionId: $collection,
                        document: $clone,
                    );
                }
                $clones[$index] = $clone;
            }

            $destination->withPreserveDates(
                true,
                fn (): int => $destination->upsertDocuments(
                    $collection,
                    $clones,
                    $batchSize,
                    increase: $increase,
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

    #[\Override]
    public function deleteDocument(string $collection, string $id): bool
    {
        $result = $this->source->deleteDocument($collection, $id);

        $destination = $this->destination;
        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $destination === null
        ) {
            return $result;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $result;
        }

        $this->replicate('deleteDocument', function () use ($destination, $collection, $id): void {
            foreach ($this->writeFilters as $filter) {
                $filter->beforeDeleteDocument(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    documentId: $id,
                );
            }

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

    #[\Override]
    public function deleteDocuments(
        string $collection,
        array $queries = [],
        int $batchSize = self::BATCH_SIZE,
        ?callable $onNext = null,
    ): int {
        $modified = $this->source->deleteDocuments(
            $collection,
            $queries,
            $batchSize,
            $onNext,
        );

        $destination = $this->destination;
        if (
            \in_array($collection, self::SOURCE_ONLY_COLLECTIONS)
            || $destination === null
        ) {
            return $modified;
        }

        $upgrade = $this->silent(fn () => $this->getUpgradeStatus($collection));
        if ($upgrade === null || $upgrade->getAttribute('status', '') !== 'upgraded') {
            return $modified;
        }

        $this->replicate('deleteDocuments', function () use ($destination, $collection, $queries, $batchSize): void {
            foreach ($this->writeFilters as $filter) {
                $filter->beforeDeleteDocuments(
                    source: $this->source,
                    destination: $destination,
                    collectionId: $collection,
                    queries: $queries,
                );
            }

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

    #[\Override]
    public function renameAttribute(string $collection, string $old, string $new): void
    {
        $this->delegateInOrder(__FUNCTION__, \func_get_args());
    }

    #[\Override]
    public function createRelationship(string $collection, Relationship $relationship): Relationship
    {
        /** @var Relationship $result */
        $result = $this->delegateInOrder(__FUNCTION__, \func_get_args());
        return $result;
    }

    #[\Override]
    public function updateRelationship(string $collection, string $key, RelationshipUpdate $update): Relationship
    {
        /** @var Relationship $result */
        $result = $this->delegateInOrder(__FUNCTION__, \func_get_args());
        return $result;
    }

    #[\Override]
    public function deleteRelationship(string $collection, string $key): void
    {
        $this->delegateInOrder(__FUNCTION__, \func_get_args());
    }

    #[\Override]
    public function renameIndex(string $collection, string $old, string $new): void
    {
        $this->delegateInOrder(__FUNCTION__, \func_get_args());
    }

    #[\Override]
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float|string $value = 1, int|float|string|null $max = null): Document
    {
        /** @var Document $result */
        $result = $this->delegateInOrder(__FUNCTION__, \func_get_args());
        return $result;
    }

    #[\Override]
    public function decreaseDocumentAttribute(string $collection, string $id, string $attribute, int|float|string $value = 1, int|float|string|null $min = null): Document
    {
        /** @var Document $result */
        $result = $this->delegateInOrder(__FUNCTION__, \func_get_args());
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
        if ($this->source->findCollection('upgrades') !== null) {
            return;
        }

        $this->source->createCollection(Collection::create('upgrades', attributes: [
            Attribute::string('collectionId', required: true),
            Attribute::string('status'),
        ], indexes: [
            Index::unique('_unique_collection', ['collectionId'], [Database::LENGTH_KEY]),
            Index::key('_status_index', ['status'], [Database::LENGTH_KEY], [OrderDirection::Asc]),
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
     * failure through onError(). Inside a coroutine the write runs in a coroutine of its own, in order with every other
     * destination change made through the mirror (see inOrder()); outside one it runs before this returns, since a
     * task that yields outside a scheduler never resumes.
     *
     * @param  Closure(): void  $write
     */
    private function replicate(string $action, Closure $write): void
    {
        $destination = $this->destination;
        if ($destination === null) {
            return;
        }

        $snapshot = $this->source->snapshot();
        $apply = function () use ($action, $destination, $snapshot, $write): void {
            try {
                $destination->withSnapshot($snapshot, function () use ($destination, $write): void {
                    $destination->withRequestTimestamp(null, $write);
                });
            } catch (Throwable $error) {
                $this->logError($action, $error);
            }
        };

        if ($this->appliesInline()) {
            $apply();

            return;
        }

        Promise::async($this->queue($apply));
    }

    /**
     * Applies a destination change before returning, once every destination change queued through the mirror before
     * it has been applied or has failed; the changes queued after it wait for it. The destination therefore receives
     * the mirror's changes one at a time, in the order they were made, so they never share its connection and a write
     * never overtakes an earlier one, whichever documents, related documents or schema they reach.
     *
     * @template T
     *
     * @param  Closure(): T  $change
     * @return T
     */
    private function inOrder(Closure $change): mixed
    {
        if ($this->appliesInline()) {
            return $change();
        }

        return $this->queue($change)();
    }

    /**
     * Whether a destination change applies at once: outside a coroutine nothing is queued, and a change made while
     * this coroutine applies one, such as from onError() or a write filter, is part of that change.
     */
    private function appliesInline(): bool
    {
        $coroutine = self::coroutine();

        return $coroutine <= 0 || isset($this->applying[$coroutine]);
    }

    private static function coroutine(): int
    {
        /** @var int $coroutine */
        $coroutine = \extension_loaded('swoole') ? Coroutine::getCid() : -1;

        return $coroutine;
    }

    /**
     * Queues a destination change behind the latest one and returns what applies it once that one has been applied.
     *
     * @template T
     *
     * @param  Closure(): T  $change
     * @return Closure(): T
     */
    private function queue(Closure $change): Closure
    {
        $earlier = $this->latestReplication;
        $applied = new Channel(1);
        $this->latestReplication = $applied;

        return function () use ($earlier, $applied, $change): mixed {
            $coroutine = self::coroutine();

            try {
                $earlier?->pop();
                $this->applying[$coroutine] = true;

                return $change();
            } finally {
                unset($this->applying[$coroutine]);
                $applied->close();
                if ($this->latestReplication === $applied) {
                    $this->latestReplication = null;
                }
            }
        };
    }

    protected function logError(string $action, Throwable $error): void
    {
        $failure = new Failure($action, self::eventOf($action), $error);

        foreach ($this->errorCallbacks as $callback) {
            $callback($failure);
        }
    }

    private static function eventOf(string $method): ?Event
    {
        return match ($method) {
            'create' => Event::DatabaseCreate,
            'delete' => Event::DatabaseDelete,
            'createCollection' => Event::CollectionCreate,
            'updateCollection' => Event::CollectionUpdate,
            'deleteCollection' => Event::CollectionDelete,
            'createAttribute' => Event::AttributeCreate,
            'createAttributes' => Event::AttributesCreate,
            'updateAttribute' => Event::AttributeUpdate,
            'deleteAttribute' => Event::AttributeDelete,
            'createIndex' => Event::IndexCreate,
            'createIndexes' => Event::IndexesCreate,
            'renameIndex' => Event::IndexRename,
            'deleteIndex' => Event::IndexDelete,
            'createDocument' => Event::DocumentCreate,
            'createDocuments' => Event::DocumentsCreate,
            'updateDocument' => Event::DocumentUpdate,
            'updateDocuments' => Event::DocumentsUpdate,
            'upsertDocument' => Event::DocumentUpsert,
            'upsertDocuments' => Event::DocumentsUpsert,
            'deleteDocument' => Event::DocumentDelete,
            'deleteDocuments' => Event::DocumentsDelete,
            'increaseDocumentAttribute' => Event::DocumentIncrease,
            'decreaseDocumentAttribute' => Event::DocumentDecrease,
            default => null,
        };
    }

    #[\Override]
    public function setAuthorization(Authorization $authorization): static
    {
        parent::setAuthorization($authorization);

        if ($this->wrapped) {
            $this->source->setAuthorization($authorization);
            $this->destination?->setAuthorization($authorization);
        }

        return $this;
    }

    /**
     * A relationships hook is the mirror's own, and the source and the destination each attach a copy of it, so
     * both sides relate documents exactly as it is configured to. A write hook also intercepts the destination's
     * writes.
     */
    #[\Override]
    public function addHook(\Utopia\Query\Hook $hook): static
    {
        if ($hook instanceof Invalidator) {
            parent::addHook($hook);
            $this->source->addHook($hook);
        } elseif ($hook instanceof Lifecycle) {
            $this->addLifecycleHook($hook);
        } else {
            parent::addHook($hook);
        }

        if ($hook instanceof Relationships) {
            $this->source->addHook(clone $hook);
            $this->destination?->addHook(clone $hook);
        }

        if ($hook instanceof Write) {
            $this->destination?->getAdapter()->addWriteHook($hook);
        }

        return $this;
    }

    /**
     * Also removes the hook from where {@see self::addHook()} registered it on the source and the destination.
     */
    #[\Override]
    public function removeHook(\Utopia\Query\Hook|string $hook): static
    {
        parent::removeHook($hook);

        if ($hook instanceof Lifecycle || (\is_string($hook) && \is_a($hook, Lifecycle::class, true))) {
            $this->source->removeHook($hook);
        }

        if ($hook instanceof Relationships || (\is_string($hook) && \is_a($hook, Relationships::class, true))) {
            $this->source->removeHook(Relationships::class);
            $this->destination?->removeHook(Relationships::class);
        }

        if ($hook instanceof Write || (\is_string($hook) && \is_a($hook, Write::class, true))) {
            $this->destination?->getAdapter()->removeWriteHook($hook);
        }

        return $this;
    }

    /**
     * Set custom document class for a collection
     *
     * @param  string  $collection  Collection ID
     * @param  class-string<Document>  $className  Fully qualified class name that extends Document
     */
    #[\Override]
    public function setDocumentType(string $collection, string $className): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return parent::setDocumentType($collection, $className);
    }

    /**
     * Clear document type mapping for a collection
     *
     * @param  string  $collection  Collection ID
     */
    #[\Override]
    public function clearDocumentType(string $collection): void
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        parent::clearDocumentType($collection);
    }

    #[\Override]
    public function clearDocumentTypes(): void
    {
        $this->delegate(__FUNCTION__);

        parent::clearDocumentTypes();
    }
}
