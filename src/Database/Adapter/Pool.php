<?php

namespace Utopia\Database\Adapter;

use DateTime;
use Throwable;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Index;
use Utopia\Database\PermissionType;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Query\CursorDirection;

/**
 * Connection pool adapter that delegates database operations to pooled adapter instances.
 *
 * Pool implements all Feature interfaces because it is a complete proxy — every method
 * call is delegated to the underlying pooled adapter. If the pooled adapter does not
 * actually support a feature, the delegated call will throw at runtime.
 */
class Pool extends Adapter implements Feature\ConnectionId, Feature\InternalCasting, Feature\Relationships, Feature\SchemaAttributes, Feature\Spatial, Feature\Timeouts, Feature\Upserts, Feature\UTCCasting
{
    /**
     * @var UtopiaPool<covariant Adapter>
     */
    protected UtopiaPool $pool;

    /**
     * When a transaction is active, all delegate calls are routed through
     * this pinned adapter to ensure they run on the same connection.
     */
    protected ?Adapter $pinnedAdapter = null;

    /**
     * @param  UtopiaPool<covariant Adapter>  $pool  The pool to use for connections. Must contain instances of Adapter.
     */
    public function __construct(UtopiaPool $pool)
    {
        $this->pool = $pool;
    }

    /**
     * Forward method calls to the internal adapter instance via the pool.
     *
     * Required because __call() can't be used to implement abstract methods.
     *
     * @param  array<mixed>  $args
     *
     * @throws DatabaseException
     */
    public function delegate(string $method, array $args): mixed
    {
        if ($this->pinnedAdapter !== null) {
            return $this->pinnedAdapter->{$method}(...$args);
        }

        return $this->pool->use(function (Adapter $adapter) use ($method, $args) {
            // Run setters in case config changed since this connection was last used
            $adapter->setDatabase($this->getDatabase());
            $adapter->setNamespace($this->getNamespace());
            $adapter->setSharedTables($this->getSharedTables());
            $adapter->setTenant($this->getTenant());
            $adapter->setTenantPerDocument($this->getTenantPerDocument());
            $adapter->setAuthorization($this->authorization);

            if ($this->getTimeout() > 0) {
                $adapter->setTimeout($this->getTimeout());
            } else {
                $adapter->clearTimeout();
            }
            $adapter->resetDebug();
            foreach ($this->getDebug() as $key => $value) {
                $adapter->setDebug($key, $value);
            }
            $adapter->resetMetadata();
            foreach ($this->getMetadata() as $key => $value) {
                $adapter->setMetadata($key, $value);
            }
            $adapter->setProfiler($this->profiler);
            $adapter->resetTransforms();
            foreach ($this->queryTransforms as $tName => $tTransform) {
                $adapter->addTransform($tName, $tTransform);
            }
            // Sync write hooks for DML operations only (not DDL like createCollection)
            if (\in_array($method, ['createDocuments', 'updateDocuments', 'deleteDocuments', 'deleteDocument', 'upsertDocuments'])) {
                foreach ($this->writeHooks as $hook) {
                    if (empty(\array_filter($adapter->getWriteHooks(), fn ($h) => $h::class === $hook::class))) {
                        $adapter->addWriteHook($hook);
                    }
                }
            }
            return $adapter->{$method}(...$args);
        });
    }

    /**
     * Check if a specific capability is supported by the pooled adapter.
     *
     * @param Capability $feature The capability to check
     * @return bool
     */
    public function supports(Capability $feature): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * Get all capabilities supported by the pooled adapter.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        /** @var array<Capability> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * Register a named query transform hook on the pooled adapter.
     *
     * @param string $name The transform name
     * @param Transform $transform The transform instance
     * @return static
     */
    public function addTransform(string $name, Transform $transform): static
    {
        $this->queryTransforms[$name] = $transform;

        return $this;
    }

    /**
     * Remove a named query transform hook from the pooled adapter.
     *
     * @param string $name The transform name to remove
     * @return static
     */
    public function removeTransform(string $name): static
    {
        unset($this->queryTransforms[$name]);

        return $this;
    }

    /**
     * Set the maximum execution time for queries on the pooled adapter.
     *
     * @param int $milliseconds Timeout in milliseconds
     * @param Event $event The event scope for the timeout
     * @return void
     */
    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        $this->timeout = $milliseconds;
        $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * Start a database transaction via the pooled adapter.
     *
     * @return bool
     *
     * @throws DatabaseException
     */
    public function startTransaction(): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * Commit the current database transaction via the pooled adapter.
     *
     * @return bool
     *
     * @throws DatabaseException
     */
    public function commitTransaction(): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * Roll back the current database transaction via the pooled adapter.
     *
     * @return bool
     *
     * @throws DatabaseException
     */
    public function rollbackTransaction(): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * Pin a single connection from the pool for the entire transaction lifecycle.
     * This prevents startTransaction(), the callback, and commitTransaction()
     * from running on different connections.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     *
     * @throws Throwable
     */
    public function withTransaction(callable $callback): mixed
    {
        // If already inside a transaction, reuse the pinned adapter
        // so nested withTransaction calls use the same connection
        if ($this->pinnedAdapter !== null) {
            return $this->pinnedAdapter->withTransaction($callback);
        }

        return $this->pool->use(function (Adapter $adapter) use ($callback) {
            $adapter->setDatabase($this->getDatabase());
            $adapter->setNamespace($this->getNamespace());
            $adapter->setSharedTables($this->getSharedTables());
            $adapter->setTenant($this->getTenant());
            $adapter->setTenantPerDocument($this->getTenantPerDocument());
            $adapter->setAuthorization($this->authorization);

            if ($this->getTimeout() > 0) {
                $adapter->setTimeout($this->getTimeout());
            } else {
                $adapter->clearTimeout();
            }
            $adapter->resetDebug();
            foreach ($this->getDebug() as $key => $value) {
                $adapter->setDebug($key, $value);
            }
            $adapter->resetMetadata();
            foreach ($this->getMetadata() as $key => $value) {
                $adapter->setMetadata($key, $value);
            }
            $adapter->setProfiler($this->profiler);
            $adapter->resetTransforms();
            foreach ($this->queryTransforms as $tName => $tTransform) {
                $adapter->addTransform($tName, $tTransform);
            }

            $this->pinnedAdapter = $adapter;
            try {
                return $adapter->withTransaction($callback);
            } finally {
                $this->pinnedAdapter = null;
            }
        });
    }

    protected function quote(string $string): string
    {
        /** @var string $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function ping(): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function reconnect(): void
    {
        $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * {@inheritDoc}
     */
    public function create(string $name): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function list(): array
    {
        /** @var array<Document> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function delete(string $name): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteCollection(string $id): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function analyzeCollection(string $collection): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function createRelationship(Relationship $relationship): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteRelationship(Relationship $relationship): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function createDocument(Document $collection, Document $document): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function createDocuments(Document $collection, array $documents): array
    {
        /** @var array<Document> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        /** @var array<Document> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
    {
        /** @var array<Document> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        /** @var float|int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getSizeOfCollection(string $collection): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getLimitForString(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getLimitForInt(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getLimitForAttributes(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getLimitForIndexes(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getMaxIndexLength(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getMaxVarcharLength(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getMaxUIDLength(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getMinDateTime(): DateTime
    {
        /** @var DateTime $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getCountOfAttributes(Document $collection): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getCountOfIndexes(Document $collection): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getCountOfDefaultAttributes(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getCountOfDefaultIndexes(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getDocumentSizeLimit(): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getAttributeWidth(Document $collection): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getKeywords(): array
    {
        /** @var array<string> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    protected function getAttributeProjection(array $selections, string $prefix): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * {@inheritDoc}
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, float|int $value, string $updatedAt, float|int|null $min = null, float|int|null $max = null): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getConnectionId(): string
    {
        /** @var string $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getInternalIndexesKeys(): array
    {
        /** @var array<string> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaAttributes(string $collection): array
    {
        /** @var array<Document> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    public function getSupportForSchemaIndexes(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSchemaIndexes(string $collection): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * {@inheritDoc}
     */
    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        /** @var string $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    protected function execute(mixed $stmt): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getIdAttributeType(): string
    {
        /** @var string $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getSequences(string $collection, array $documents): array
    {
        /** @var array<Document> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * @return array<float>
     */
    public function decodePoint(string $wkb): array
    {
        /** @var array<float> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * @return array<array<float>>
     */
    public function decodeLinestring(string $wkb): array
    {
        /** @var array<array<float>> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * @return array<array<array<float>>>
     */
    public function decodePolygon(string $wkb): array
    {
        /** @var array<array<array<float>>> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function castingBefore(Document $collection, Document $document): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function castingAfter(Document $collection, Document $document): Document
    {
        /** @var Document $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function setUTCDatetime(string $value): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * {@inheritDoc}
     */
    public function setSupportForAttributes(bool $support): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * Set the authorization instance used for permission checks.
     *
     * @param Authorization $authorization The authorization instance
     * @return self
     */
    public function setAuthorization(Authorization $authorization): self
    {
        $this->authorization = $authorization;

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function getSupportNonUtfCharacters(): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function rawQuery(string $query, array $bindings = []): array
    {
        /** @var array<Document> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    public function rawMutation(string $query, array $bindings = []): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    public function getBuilder(string $collection): \Utopia\Query\Builder
    {
        /** @var \Utopia\Query\Builder $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    public function getSchema(): \Utopia\Query\Schema
    {
        /** @var \Utopia\Query\Schema $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

}
