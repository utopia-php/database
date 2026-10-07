<?php

namespace Utopia\Database\Adapter;

use Throwable;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Index;
use Utopia\Database\PermissionType;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\State\Value;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Query\CursorDirection;

/**
 * Connection pool adapter that delegates database operations to pooled adapter instances.
 *
 * Pool is a proxy: optional Feature methods are forwarded to the borrowed adapter.
 * Feature support is reported by hasFeature(), not instanceof.
 */
class Pool extends Adapter
{
    /**
     * @var UtopiaPool<covariant Adapter>
     */
    protected UtopiaPool $pool;

    /**
     * @var Value<Adapter|null>|null The connection a coroutine's open transaction runs on, which that coroutine and
     *                               the coroutines it starts use for every call until the transaction ends
     */
    private ?Value $pinned = null;

    /**
     * The schemaless mode this handle puts every borrowed adapter in, or null to leave each in its own.
     */
    protected ?bool $schemaless = null;

    /**
     * Every connection of one pool runs the same adapter, and handles are often built per
     * request, so the answers are kept per pool rather than per handle.
     *
     * @var \WeakMap<UtopiaPool<covariant Adapter>, array<Capability>>|null
     */
    private static ?\WeakMap $capabilities = null;

    /**
     * @var \WeakMap<UtopiaPool<covariant Adapter>, array<class-string, bool>>|null
     */
    private static ?\WeakMap $features = null;

    /**
     * @var \WeakMap<UtopiaPool<covariant Adapter>, array<int, bool>>|null
     */
    private static ?\WeakMap $definedAttributes = null;

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
        return $this->borrowAndInvoke($method, $args);
    }

    /**
     * @param  class-string  $feature
     * @param  array<mixed>  $args
     */
    protected function delegateFeature(string $feature, string $method, array $args): mixed
    {
        return $this->borrowAndInvoke($method, $args, $feature);
    }

    /**
     * @param  array<mixed>  $args
     * @param  class-string|null  $feature
     */
    protected function borrowAndInvoke(string $method, array $args, ?string $feature = null): mixed
    {
        $pinned = $this->pin();
        if ($pinned !== null) {
            $this->syncBorrowedAdapter($pinned);

            return $pinned->withTenant(
                $this->getTenant(),
                fn (): mixed => $this->invokeDelegated($pinned, $method, $args, $feature),
            );
        }

        return $this->pool->use(function (Adapter $adapter) use ($method, $args, $feature) {
            try {
                $this->syncBorrowedAdapter($adapter);

                return $this->invokeDelegated($adapter, $method, $args, $feature);
            } finally {
                $this->releaseBorrowedAdapter($adapter);
            }
        });
    }

    /**
     * @param  array<mixed>  $args
     * @param  class-string|null  $feature
     */
    protected function invokeDelegated(Adapter $adapter, string $method, array $args, ?string $feature = null): mixed
    {
        if ($feature !== null && ! $adapter instanceof $feature) {
            throw new DatabaseException($this->unsupportedFeatureMessage($feature));
        }

        if ($this->skippingDuplicates()) {
            return $adapter->skipDuplicates(
                fn () => $adapter->{$method}(...$args)
            );
        }

        return $adapter->{$method}(...$args);
    }

    /**
     * @param  class-string  $feature
     */
    protected function unsupportedFeatureMessage(string $feature): string
    {
        return match ($feature) {
            Feature\Upserts::class => 'Adapter does not support upserts',
            Feature\RawQuery::class => 'Adapter does not support raw queries',
            Feature\QueryBuilder::class => 'Adapter does not support query builder',
            Feature\SchemaAttributes::class => 'Adapter does not support schema attributes',
            Feature\SchemaIndexes::class => 'Adapter does not support schema indexes',
            Feature\ColumnTypes::class => 'Adapter does not support column types',
            Feature\Spatial::class => 'Adapter does not support spatial',
            Feature\InternalCasting::class => 'Adapter does not support internal casting',
            Feature\UTCCasting::class => 'Adapter does not support UTC casting',
            Feature\ConnectionId::class => 'Adapter does not support connection id',
            Feature\Relationships::class => 'Adapter does not support relationships',
            Feature\Timeouts::class => 'Adapter does not support timeouts',
            Feature\Schemaless::class => 'Adapter does not support schemaless',
            default => 'Adapter does not support '.$feature,
        };
    }

    protected function syncBorrowedAdapter(Adapter $adapter): void
    {
        $adapter->setDatabase($this->getDatabase());
        $adapter->setNamespace($this->getNamespace());
        $adapter->setSharedTables($this->getSharedTables());
        $adapter->setTenant($this->getTenant());
        $adapter->setTenantPerDocument($this->getTenantPerDocument());
        $adapter->setAuthorization($this->authorization);
        $adapter->enableAlterLocks($this->alterLocks);

        if ($this->schemaless !== null && $adapter instanceof Feature\Schemaless) {
            $adapter->setSchemaless($this->schemaless);
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
        $adapter->setProfiler($this->profiler);
        $adapter->resetTransforms();
        foreach ($this->queryTransforms as $tName => $tTransform) {
            $adapter->addTransform($tName, $tTransform);
        }
        $this->syncWriteHooks($adapter);
    }

    /**
     * Take back what syncBorrowedAdapter() lent the connection for one checkout.
     * A subclass that checks connections out itself calls this before handing
     * the connection back to the pool.
     */
    protected function releaseBorrowedAdapter(Adapter $adapter): void
    {
        $adapter->setProfiler(null);
    }

    public function getDriver(): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    /**
     * Check if a specific capability is supported by the pooled adapter.
     *
     * Answered from the capabilities the pool's connections reported when first asked, except
     * DefinedAttributes: it reflects the schema mode a connection is in. Once this handle has set
     * that mode, every connection it borrows is put in it first, so the answer is kept per pool
     * and mode; before that, a connection keeps its own mode and is asked every time.
     *
     * @param Capability $feature The capability to check
     * @return bool
     */
    public function supports(Capability $feature): bool
    {
        if ($feature === Capability::DefinedAttributes) {
            return $this->supportsDefinedAttributes();
        }

        return \in_array($feature, $this->capabilities(), true);
    }

    private function supportsDefinedAttributes(): bool
    {
        $mode = $this->schemaless;
        if ($mode === null) {
            /** @var bool $result */
            $result = $this->delegate('supports', [Capability::DefinedAttributes]);

            return $result;
        }

        $known = self::$definedAttributes[$this->pool][(int) $mode] ?? null;
        if ($known !== null) {
            return $known;
        }

        /** @var bool $result */
        $result = $this->delegate('supports', [Capability::DefinedAttributes]);
        self::$definedAttributes ??= new \WeakMap();
        $answers = self::$definedAttributes[$this->pool] ?? [];
        $answers[(int) $mode] = $result;
        self::$definedAttributes[$this->pool] = $answers;

        return $result;
    }

    /**
     * Get all capabilities supported by the pooled adapter, as its connections reported them when first asked.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        $remembered = self::$capabilities[$this->pool] ?? null;
        if ($remembered !== null) {
            return $remembered;
        }

        /** @var array<Capability> $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        self::$capabilities ??= new \WeakMap();
        self::$capabilities[$this->pool] = $result;

        return $result;
    }

    /**
     * @param  class-string  $feature
     */
    public function hasFeature(string $feature): bool
    {
        $known = self::$features[$this->pool][$feature] ?? null;
        if ($known !== null) {
            return $known;
        }

        /** @var bool $result */
        $result = $this->delegate('hasFeature', [$feature]);

        self::$features ??= new \WeakMap();
        $features = self::$features[$this->pool] ?? [];
        $features[$feature] = $result;
        self::$features[$this->pool] = $features;

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
        // Zero is what a caller's own default carries when it wants no timeout,
        // so it clears the event rather than pinning every statement to 0.
        if ($milliseconds <= 0) {
            $this->clearTimeout($event);

            return;
        }

        $this->setTimeoutState($milliseconds, $event);
        $this->syncPinnedTimeouts();
    }

    public function clearTimeout(Event $event = Event::All): void
    {
        $this->clearTimeoutState($event);
        $this->syncPinnedTimeouts();
    }

    /**
     * A timeout is adapter state, not a statement: every concrete adapter records
     * it and applies it to the SQL it builds afterwards, and none of them contacts
     * the server to set it. Delegating the call therefore checked a connection out
     * for the sole purpose of writing a number onto whichever one answered, so the
     * timeout bound that connection and none of its siblings — and merely building
     * a handle failed outright while the backing was unreachable, reporting a
     * database as down to a caller that had not yet issued a query.
     *
     * The state is replayed onto each connection as it is borrowed
     * ({@see self::syncTimeouts()}), so the only connection that needs telling now
     * is one already pinned: a transaction does not check out again before its
     * commit, and the rest of its body must not run under the timeout the caller
     * just replaced.
     */
    private function syncPinnedTimeouts(): void
    {
        $pinned = $this->pin();

        if ($pinned !== null) {
            $this->syncTimeouts($pinned);
        }
    }

    /**
     * Which connection the calling coroutine's transaction, or the transaction of
     * the coroutine that started it, has pinned, if any. Read through a seam, so a
     * subclass that keeps its pins somewhere else is asked too.
     */
    protected function pin(): ?Adapter
    {
        return $this->pinned?->get();
    }

    /**
     * How many reads the calling coroutine can run at the same time, each on a connection of its own, without
     * waiting for one and while leaving an idle connection to other coroutines. One while a transaction has pinned a
     * connection, which runs one statement at a time.
     *
     * @return int<1, max>
     */
    public function getReadConcurrency(): int
    {
        if ($this->pin() !== null) {
            return 1;
        }

        return \max(1, $this->getReadPool()->count() - 1);
    }

    /**
     * The pool a read outside a transaction borrows its connection from.
     *
     * @return UtopiaPool<covariant Adapter>
     */
    protected function getReadPool(): UtopiaPool
    {
        return $this->pool;
    }

    /**
     * @return Value<Adapter|null>
     */
    private function pinned(): Value
    {
        if ($this->pinned === null) {
            /** @var Value<Adapter|null> $pinned */
            $pinned = new Value(null);
            $this->pinned = $pinned;
        }

        return $this->pinned;
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

    public function inTransaction(): bool
    {
        return $this->pin()?->inTransaction() ?? parent::inTransaction();
    }

    public function getHostname(): string
    {
        /** @var string $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * Pin a single connection from the pool for the entire transaction lifecycle.
     * This prevents startTransaction(), the callback, and commitTransaction()
     * from running on different connections. The pin belongs to the calling
     * coroutine and the coroutines it starts; other coroutines sharing the handle
     * borrow connections of their own and run outside the transaction.
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
        $pinned = $this->pin();
        if ($pinned !== null) {
            return $pinned->withTransaction($callback);
        }

        return $this->pool->use(function (Adapter $adapter) use ($callback) {
            try {
                $this->syncBorrowedAdapter($adapter);

                return $this->pinned()->with($adapter, function () use ($adapter, $callback): mixed {
                    if ($this->skippingDuplicates()) {
                        return $adapter->skipDuplicates(
                            fn () => $adapter->withTransaction($callback)
                        );
                    }

                    return $adapter->withTransaction($callback);
                });
            } finally {
                $this->releaseBorrowedAdapter($adapter);
            }
        });
    }

    protected function quote(string $string): string
    {
        /** @var string $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    protected function syncTimeouts(Adapter $adapter): void
    {
        if (! ($adapter instanceof Feature\Timeouts)) {
            // Setting a timeout no longer checks a connection out, so this is the
            // first moment the adapter's capabilities are known. Staying silent
            // here would drop a bound the caller asked for and run the statement
            // unbounded; the refusal belongs where the timeout would be applied,
            // not where a handle is merely being built.
            if ($this->timeouts !== []) {
                throw new DatabaseException($this->unsupportedFeatureMessage(Feature\Timeouts::class));
            }

            return;
        }

        if (empty($this->timeouts)) {
            $adapter->clearTimeout();

            return;
        }

        if (count($this->timeouts) === 1 && isset($this->timeouts[Event::All->value])) {
            $adapter->setTimeout($this->timeouts[Event::All->value]);

            return;
        }

        // The concrete adapters keep one timeout scalar, which Postgres writes
        // into SET statement_timeout and Mongo into maxTimeMS for every
        // statement, so the last value applied is the one every statement runs
        // under. Apply the per-event entries first and the global one last, or a
        // per-event timeout set after the global one bounds everything.
        $adapter->clearTimeout();
        foreach ($this->timeouts as $event => $milliseconds) {
            if ($event === Event::All->value) {
                continue;
            }

            $adapter->setTimeout($milliseconds, Event::from($event));
        }

        if (isset($this->timeouts[Event::All->value])) {
            $adapter->setTimeout($this->timeouts[Event::All->value]);
        }
    }

    private function syncWriteHooks(Adapter $adapter): void
    {
        $current = $adapter->getWriteHooks();
        if ($current === $this->writeHooks) {
            return;
        }

        foreach ($current as $childHook) {
            $adapter->removeWriteHook($childHook::class);
        }

        foreach ($this->writeHooks as $hook) {
            $adapter->addWriteHook($hook);
        }
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

    #[\Override]
    public function isRetryable(Throwable $failure): bool
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
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     */
    public function createCollection(string $collection, array $attributes = [], array $indexes = []): bool
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
     * @param  list<Attribute>  $attributes
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    public function updateAttribute(string $collection, string $key, Attribute $attribute): bool
    {
        /** @var bool $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    public function relaxAttributeRequired(string $collection, string $id): bool
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
    public function createRelationship(string $collection, Relationship $relationship): bool
    {
        /** @var bool $result */
        $result = $this->delegateFeature(Feature\Relationships::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update): bool
    {
        /** @var bool $result */
        $result = $this->delegateFeature(Feature\Relationships::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side): bool
    {
        /** @var bool $result */
        $result = $this->delegateFeature(Feature\Relationships::class, __FUNCTION__, \func_get_args());
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
     * @param  array<Change>  $changes
     * @return array<Document>
     */
    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        /** @var array<Document> $result */
        $result = $this->delegateFeature(Feature\Upserts::class, __FUNCTION__, \func_get_args());
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

    #[\Override]
    public function limits(): Limits
    {
        if ($this->limits === null) {
            /** @var Limits $limits */
            $limits = $this->delegate(__FUNCTION__, \func_get_args());
            $this->limits = $limits;
        }

        return $this->limits;
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
    public function getAttributeWidth(Document $collection): int
    {
        /** @var int $result */
        $result = $this->delegate(__FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, float|int|string $value, string $updatedAt, float|int|string|null $min = null, float|int|string|null $max = null): bool
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
        $result = $this->delegateFeature(Feature\ConnectionId::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * @return array<Document>
     */
    public function getSchemaAttributes(string $collection): array
    {
        /** @var array<Document> $result */
        $result = $this->delegateFeature(Feature\SchemaAttributes::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * @return array<Document>
     */
    public function getSchemaIndexes(string $collection): array
    {
        /** @var array<Document> $result */
        $result = $this->delegateFeature(Feature\SchemaIndexes::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    protected function execute(mixed $statement): bool
    {
        /** @var bool $result */
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
        $result = $this->delegateFeature(Feature\Spatial::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * @return array<array<float>>
     */
    public function decodeLinestring(string $wkb): array
    {
        /** @var array<array<float>> $result */
        $result = $this->delegateFeature(Feature\Spatial::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * @return array<array<array<float>>>
     */
    public function decodePolygon(string $wkb): array
    {
        /** @var array<array<array<float>>> $result */
        $result = $this->delegateFeature(Feature\Spatial::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function castingBefore(Document $collection, Document $document): Document
    {
        /** @var Document $result */
        $result = $this->delegateFeature(Feature\InternalCasting::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function castingAfter(Document $collection, Document $document): Document
    {
        /** @var Document $result */
        $result = $this->delegateFeature(Feature\InternalCasting::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     */
    public function castingAfterDocuments(Document $collection, array $documents): array
    {
        /** @var array<Document> $result */
        $result = $this->delegateFeature(Feature\InternalCasting::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function setUTCDatetime(string $value): mixed
    {
        return $this->delegateFeature(Feature\UTCCasting::class, __FUNCTION__, \func_get_args());
    }

    /**
     * Every adapter this handle borrows afterwards is put in the mode first.
     */
    public function setSchemaless(bool $schemaless): static
    {
        $this->delegateFeature(Feature\Schemaless::class, __FUNCTION__, \func_get_args());
        $this->schemaless = $schemaless;

        return $this;
    }

    public function isSchemaless(): bool
    {
        if ($this->schemaless !== null) {
            return $this->schemaless;
        }

        /** @var bool $result */
        $result = $this->delegateFeature(Feature\Schemaless::class, __FUNCTION__, \func_get_args());

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
     * @param  array<mixed>  $bindings
     * @return array<Document>
     */
    public function rawQuery(string $query, array $bindings = []): array
    {
        /** @var array<Document> $result */
        $result = $this->delegateFeature(Feature\RawQuery::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    /**
     * @param  array<mixed>  $bindings
     */
    public function rawMutation(string $query, array $bindings = []): int
    {
        /** @var int $result */
        $result = $this->delegateFeature(Feature\RawQuery::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    public function getBuilder(string $collection): \Utopia\Query\Builder
    {
        /** @var \Utopia\Query\Builder $result */
        $result = $this->delegateFeature(Feature\QueryBuilder::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    public function getSchema(): \Utopia\Query\Schema
    {
        /** @var \Utopia\Query\Schema $result */
        $result = $this->delegateFeature(Feature\QueryBuilder::class, __FUNCTION__, \func_get_args());
        return $result;
    }

    public function getColumnType(string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        /** @var string $result */
        $result = $this->delegateFeature(Feature\ColumnTypes::class, __FUNCTION__, \func_get_args());
        return $result;
    }

}
