<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

class Pool extends Adapter
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
     * The timeout each event is under, held here rather than on a connection.
     *
     * A timeout is adapter state, not a statement: every concrete adapter
     * records it and applies it to the SQL it builds afterwards, and none of
     * them contacts the server to set it. Delegating the call therefore opened
     * a connection for the sole purpose of writing a number onto whichever one
     * answered, which the pool took back moments later - so the timeout bound
     * one connection and none of its siblings, and merely building a handle
     * failed outright while the backing was unreachable, reporting a database
     * as down to a caller that had not yet issued a query.
     *
     * @var array<string, int>
     */
    private array $timeouts = [];

    /**
     * Answers to the getters that describe the adapter rather than a
     * connection, by method.
     *
     * Every connection in a pool is built by the same factory, so it is the
     * same adapter class on the same DSN, and these answers are the same
     * whichever connection gives them. Delegating them checked a connection
     * out and replayed the whole handle state onto it to read a constant, and
     * the hot path asks several per document. Getters that read connection or
     * handle state (the driver, the connection id, anything that varies with
     * shared tables or with how a connection was set up) still delegate, and
     * the one capability with a setter is updated by it.
     *
     * @var array<string, mixed>
     */
    private array $capabilities = [];

    /**
     * Attribute support this handle asked for, replayed on every checkout so
     * each connection runs with the value the memoized getter reports.
     */
    private ?bool $supportForAttributes = null;

    /**
     * @param UtopiaPool<covariant Adapter> $pool The pool to use for connections. Must contain instances of Adapter.
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
     * @param string $method
     * @param array<mixed> $args
     * @return mixed
     * @throws DatabaseException
     */
    public function delegate(string $method, array $args): mixed
    {
        if ($this->pinnedAdapter !== null) {
            if ($this->skipDuplicates) {
                return $this->pinnedAdapter->skipDuplicates(
                    fn () => $this->pinnedAdapter->{$method}(...$args)
                );
            }
            return $this->pinnedAdapter->{$method}(...$args);
        }

        return $this->pool->use(function (Adapter $adapter) use ($method, $args) {
            // Run setters in case config changed since this connection was last used
            $adapter->setDatabase($this->getDatabase());
            $adapter->setNamespace($this->getNamespace());
            $adapter->setSharedTables($this->getSharedTables());
            $adapter->setTenant($this->getTenant());
            $adapter->setAuthorization($this->authorization);
            if ($this->supportForAttributes !== null) {
                $adapter->setSupportForAttributes($this->supportForAttributes);
            }

            $this->syncTimeouts($adapter);
            $adapter->resetDebug();
            foreach ($this->getDebug() as $key => $value) {
                $adapter->setDebug($key, $value);
            }
            $adapter->resetMetadata();
            foreach ($this->getMetadata() as $key => $value) {
                $adapter->setMetadata($key, $value);
            }

            if ($this->skipDuplicates) {
                return $adapter->skipDuplicates(
                    fn () => $adapter->{$method}(...$args)
                );
            }
            return $adapter->{$method}(...$args);
        });
    }

    /**
     * Ask one connection for a capability the first time and keep the answer.
     *
     * @param string $method
     * @return mixed
     * @throws DatabaseException
     */
    protected function capability(string $method): mixed
    {
        if (!\array_key_exists($method, $this->capabilities)) {
            $this->capabilities[$method] = $this->delegate($method, []);
        }

        return $this->capabilities[$method];
    }

    public function getDriver(): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function before(string $event, string $name = '', ?callable $callback = null): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    protected function trigger(string $event, mixed $query): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * Zero is the value a caller's own default carries when it wants no
     * timeout, so it clears the event rather than being refused. A connection
     * is only ever asked for a timeout it can hold.
     */
    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        if ($milliseconds <= 0) {
            $this->clearTimeout($event);

            return;
        }

        $this->timeouts[$event] = $milliseconds;
        $this->timeout = $this->timeouts[Database::EVENT_ALL] ?? 0;

        $this->syncPin();
    }

    /**
     * Clearing one event leaves the others alone. The concrete adapters keep a
     * single timeout scalar that Postgres and Mongo apply to every statement,
     * so a clear forwarded verbatim would drop the timeout the caller still
     * has configured for everything else.
     */
    public function clearTimeout(string $event): void
    {
        unset($this->timeouts[$event]);
        $this->timeout = $this->timeouts[Database::EVENT_ALL] ?? 0;

        $this->syncPin();
    }

    /**
     * The pool's own map is what a checkout replays, so a clear has to empty
     * it. Inheriting the base implementation cleared almost nothing: it walks
     * the events it finds in `$transformations`, and this adapter delegates
     * `before()`, so its own array never holds more than `EVENT_ALL` however
     * many events a caller has set a timeout for.
     */
    public function clearTimeouts(): void
    {
        $this->timeouts = [];
        $this->timeout = 0;

        $this->syncPin();
    }

    /**
     * The connection this caller's open transaction is pinned to, if any.
     *
     * A seam: a subclass that keys the pin by coroutine rather than by object
     * overrides this, and the timeout setters reach the right connection
     * without knowing how the pin is held.
     */
    protected function pin(): ?Adapter
    {
        return $this->pinnedAdapter;
    }

    /**
     * A timeout changed inside a transaction has to reach the connection
     * running it. Every statement left in that transaction goes to the pinned
     * connection, and it will not be checked out again before the commit, so
     * waiting for the next checkout would leave the rest of the body running
     * under the timeout the caller just replaced.
     */
    private function syncPin(): void
    {
        $pinned = $this->pin();

        if ($pinned === null) {
            return;
        }

        $this->syncTimeouts($pinned);
    }

    /**
     * Put a connection into the timeout state this pool holds, as it is checked
     * out. The connection outlives the handle that configured it and is handed
     * on to handles that want a different timeout or none at all, so it is
     * reset first: a handle carrying no timeout must not inherit one, and a
     * handle carrying its own must not be left with an event the last holder
     * set.
     *
     * The global timeout is applied last, which decides what an engine with no
     * per-event timeout does with one. MariaDB and MySQL hang a hook on the
     * event and are unaffected; Postgres and Mongo take `$event` and discard
     * it, so every call lands on the one scalar they bound every statement by
     * and the last one wins. Applying the global last means a per-event
     * refinement those two cannot express is ignored there. The other order
     * would let a 5s read deadline silently bound every write on the handle,
     * which is the failure worth avoiding.
     */
    protected function syncTimeouts(Adapter $adapter): void
    {
        $adapter->clearTimeouts();

        foreach ($this->timeouts as $event => $milliseconds) {
            if ($event === Database::EVENT_ALL) {
                continue;
            }

            $adapter->setTimeout($milliseconds, $event);
        }

        if (isset($this->timeouts[Database::EVENT_ALL])) {
            $adapter->setTimeout($this->timeouts[Database::EVENT_ALL]);
        }
    }

    public function startTransaction(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function commitTransaction(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function rollbackTransaction(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * The hostname comes from the DSN the pool connects with. An empty answer
     * is what a SQL adapter reports when it cannot read it, so that one is
     * not kept.
     */
    public function getHostname(): string
    {
        if (!empty($this->capabilities[__FUNCTION__])) {
            return $this->capabilities[__FUNCTION__];
        }

        $hostname = $this->delegate(__FUNCTION__, \func_get_args());

        if ($hostname !== '') {
            $this->capabilities[__FUNCTION__] = $hostname;
        }

        return $hostname;
    }

    /**
     * Pin a single connection from the pool for the entire transaction lifecycle.
     * This prevents startTransaction(), the callback, and commitTransaction()
     * from running on different connections.
     *
     * @template T
     * @param callable(): T $callback
     * @return T
     * @throws \Throwable
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
            $adapter->setAuthorization($this->authorization);
            if ($this->supportForAttributes !== null) {
                $adapter->setSupportForAttributes($this->supportForAttributes);
            }

            $this->syncTimeouts($adapter);
            $adapter->resetDebug();
            foreach ($this->getDebug() as $key => $value) {
                $adapter->setDebug($key, $value);
            }
            $adapter->resetMetadata();
            foreach ($this->getMetadata() as $key => $value) {
                $adapter->setMetadata($key, $value);
            }

            $this->pinnedAdapter = $adapter;
            try {
                if ($this->skipDuplicates) {
                    return $adapter->skipDuplicates(
                        fn () => $adapter->withTransaction($callback)
                    );
                }
                return $adapter->withTransaction($callback);
            } finally {
                $this->pinnedAdapter = null;
            }
        });
    }

    protected function quote(string $string): string
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function ping(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function reconnect(): void
    {
        $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function create(string $name): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function exists(string $database, ?string $collection = null): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function list(): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function delete(string $name): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteCollection(string $id): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function analyzeCollection(string $collection): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createAttributes(string $collection, array $attributes): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function renameIndex(string $collection, string $old, string $new): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = [], int $ttl = 1): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createDocuments(Document $collection, array $documents): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSizeOfCollection(string $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getLimitForString(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getLimitForInt(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getLimitForBigInt(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForUnsignedBigInt(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getLimitForAttributes(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getLimitForIndexes(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getMaxIndexLength(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getMaxVarcharLength(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getMaxUIDLength(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getMinDateTime(): \DateTime
    {
        return clone $this->capability(__FUNCTION__);
    }

    public function getSupportForSchemas(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForAttributes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForSchemaAttributes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForIndex(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForIndexArray(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForCastIndexArray(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForUniqueIndex(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForFulltextIndex(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForFulltextWildcardIndex(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForPCRERegex(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForPOSIXRegex(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForTrigramIndex(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForCasting(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForQueryContains(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForTimeouts(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForRelationships(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForUpdateLock(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForBatchOperations(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForAttributeResizing(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForOperators(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForGetConnectionId(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForUpserts(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForUpsertOnUniqueIndex(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForVectors(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForCacheSkipOnFailure(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForCaching(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForReconnection(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForHostname(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForBatchCreateAttributes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForSpatialAttributes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForSpatialIndexNull(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getCountOfAttributes(Document $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getCountOfIndexes(Document $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getCountOfDefaultAttributes(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getCountOfDefaultIndexes(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getDocumentSizeLimit(): int
    {
        return $this->capability(__FUNCTION__);
    }

    public function getAttributeWidth(Document $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getKeywords(): array
    {
        return $this->capability(__FUNCTION__);
    }

    protected function getAttributeProjection(array $selections, string $prefix): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, float|int $value, string $updatedAt, float|int|null $min = null, float|int|null $max = null): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getConnectionId(): string
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getInternalIndexesKeys(): array
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSchemaAttributes(string $collection): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForSchemaIndexes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSchemaIndexes(string $collection): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    protected function execute(mixed $stmt): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getIdAttributeType(): string
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSequences(string $collection, array $documents): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForSpatialIndexOrder(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForDistanceBetweenMultiDimensionGeometryInMeters(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForSpatialAxisOrder(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForOptionalSpatialAttributeWithExistingRows(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForMultipleFulltextIndexes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForIdenticalIndexes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForOrderRandom(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function decodePoint(string $wkb): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function decodeLinestring(string $wkb): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function decodePolygon(string $wkb): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForObject(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForObjectIndexes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function castingBefore(Document $collection, Document $document): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function castingAfter(Document $collection, Document $document): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForInternalCasting(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForUTCCasting(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function setUTCDatetime(string $value): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * The setter returns the support the adapter ends up with, which is what
     * the getter has to report from now on.
     */
    public function setSupportForAttributes(bool $support): bool
    {
        $this->supportForAttributes = $support;

        return $this->capabilities['getSupportForAttributes'] = $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForIntegerBooleans(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function setAuthorization(Authorization $authorization): self
    {
        $this->authorization = $authorization;
        return $this;
    }

    public function getSupportForAlterLocks(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportNonUtfCharacters(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForTTLIndexes(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForTransactionRetries(): bool
    {
        return $this->capability(__FUNCTION__);
    }

    public function getSupportForNestedTransactions(): bool
    {
        return $this->capability(__FUNCTION__);
    }
}
