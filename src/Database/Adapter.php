<?php

namespace Utopia\Database;

use Exception;
use Throwable;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Hook\Write;
use Utopia\Database\State\Value;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;
use Utopia\Query\Schema\IndexType;

/**
 * Abstract base class for all database adapters, providing shared state management and a contract for database operations.
 */
abstract class Adapter
{
    protected const string MAX_DATETIME = '9999-12-31 23:59:59';

    protected string $database = '';

    protected string $hostname = '';

    protected string $namespace = '';

    protected bool $sharedTables = false;

    /** @var Value<int|string|null>|null */
    private ?Value $scopedTenant = null;

    /** @var Value<bool>|null */
    private ?Value $ignoringDuplicates = null;

    protected bool $tenantPerDocument = false;

    protected int $inTransaction = 0;

    /**
     * withTransaction() calls in progress on this adapter. The outermost one owns the retries.
     */
    private int $transactionCalls = 0;

    protected bool $locks = false;

    /**
     * @var array<string, Transform>
     */
    protected array $transforms = [];

    /**
     * @var array<string, mixed>
     */
    protected array $metadata = [];

    /**
     * @var list<Write>
     */
    protected array $writeHooks = [];

    protected ?Profiler $profiler = null;

    protected Authorization $authorization;

    /** @var array<string, true>|null */
    protected ?array $capabilities = null;

    protected ?Limits $limits = null;

    public function supports(Capability $capability): bool
    {
        if ($this->capabilities === null) {
            $this->capabilities = [];
            foreach ($this->capabilities() as $declared) {
                $this->capabilities[$declared->name] = true;
            }
        }

        return isset($this->capabilities[$capability->name]);
    }

    /**
     * Whether this adapter offers the optional methods of a Feature interface. A proxy such as Pool answers for
     * the adapter it delegates to without implementing the interface itself, so callers ask this rather than
     * use instanceof.
     *
     * @param  class-string  $feature
     */
    public function hasFeature(string $feature): bool
    {
        return $this instanceof $feature;
    }

    /**
     * Get the list of capabilities this adapter supports.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        return [
            Capability::IndexKey,
            Capability::IndexArray,
            Capability::IndexUnique,
        ];
    }

    /**
     * @return $this
     */
    public function setAuthorization(Authorization $authorization): self
    {
        $this->authorization = $authorization;

        return $this;
    }

    /**
     * Get the authorization instance used for permission checks.
     *
     * @return Authorization The current authorization instance.
     */
    public function getAuthorization(): Authorization
    {
        return $this->authorization;
    }

    public function setProfiler(?Profiler $profiler): static
    {
        $this->profiler = $profiler;

        return $this;
    }

    public function getProfiler(): ?Profiler
    {
        return $this->profiler;
    }

    /**
     * @throws DatabaseException
     */
    public function setDatabase(string $name): static
    {
        $this->database = $this->filter($name);

        return $this;
    }

    /**
     * Get Database from current scope
     */
    public function getDatabase(): string
    {
        return $this->database;
    }

    /**
     * Set namespace to divide different scope of data sets
     *
     * @return $this
     *
     * @throws DatabaseException
     */
    public function setNamespace(string $namespace): static
    {
        $this->namespace = $this->filter($namespace);

        return $this;
    }

    /**
     * Get namespace of current set scope
     */
    public function getNamespace(): string
    {
        return $this->namespace;
    }

    /**
     * @return $this
     */
    public function setHostname(string $hostname): static
    {
        $this->hostname = $hostname;

        return $this;
    }

    /**
     * Whether tenants share tables, told apart by the tenant column.
     */
    public function setSharedTables(bool $sharedTables): static
    {
        if ($this->sharedTables !== $sharedTables) {
            $this->sharedTables = $sharedTables;
            $this->limits = null;
        }

        return $this;
    }

    public function hasSharedTables(): bool
    {
        return $this->sharedTables;
    }

    /**
     * The tenant statements run as under shared tables.
     */
    public function setTenant(int|string|null $tenant): static
    {
        $this->scopedTenant()->set($tenant);

        return $this;
    }

    /**
     * Get tenant to use for shared tables.
     *
     * `_tenant` is an INT UNSIGNED column, so the engine reads "001" and "1"
     * as the same tenant and returns both rows for either. Normalising every
     * digit-only string mirrors that. Keeping them apart in PHP would be worse
     * than the collapse: the scope comparison and the cache key would claim a
     * distinction the rows do not have.
     */
    public function getTenant(): int|string|null
    {
        $tenant = $this->currentTenant();
        if (\is_string($tenant) && \ctype_digit($tenant)) {
            return (int) $tenant;
        }

        return $tenant;
    }

    /**
     * Run the callback with the tenant set for the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function withTenant(int|string|null $tenant, callable $callback): mixed
    {
        return $this->scopedTenant()->with($tenant, $callback);
    }

    /**
     * The tenant the calling coroutine's statements run as, exactly as it was set.
     */
    protected function currentTenant(): int|string|null
    {
        return $this->scopedTenant()->get();
    }

    /**
     * @return Value<int|string|null>
     */
    private function scopedTenant(): Value
    {
        if ($this->scopedTenant === null) {
            /** @var Value<int|string|null> $scopedTenant */
            $scopedTenant = new Value(null);
            $this->scopedTenant = $scopedTenant;
        }

        return $this->scopedTenant;
    }

    /**
     * Whether a document carries its own tenant instead of the adapter's.
     */
    public function setTenantPerDocument(bool $tenantPerDocument): static
    {
        $this->tenantPerDocument = $tenantPerDocument;

        return $this;
    }

    public function isTenantPerDocument(): bool
    {
        return $this->tenantPerDocument;
    }

    /**
     * Set metadata for query comments
     *
     * @return $this
     */
    public function setMetadata(string $key, mixed $value): static
    {
        $this->metadata[$key] = $value;

        return $this;
    }

    /**
     * @return array<string, mixed>
     */
    public function getMetadata(): array
    {
        return $this->metadata;
    }

    public function resetMetadata(): void
    {
        $this->metadata = [];
    }

    /**
     * Whether ALTER TABLE statements take LOCK=SHARED, on the engines that support it.
     */
    public function setLocks(bool $locks): static
    {
        $this->locks = $locks;

        return $this;
    }

    /**
     * Register a write hook that intercepts document write operations.
     *
     * @param Write $hook The write hook to add.
     * @return $this
     */
    public function addWriteHook(Write $hook): static
    {
        $this->writeHooks[] = $hook;

        return $this;
    }

    /**
     * @internal
     */
    public function getTenantHook(): ?Hook\Tenancy
    {
        foreach ($this->writeHooks as $hook) {
            if ($hook instanceof Hook\Tenancy) {
                return $hook;
            }
        }

        return null;
    }

    /**
     * Remove a write hook, or every write hook of a class.
     *
     * @param  Write|class-string  $hook
     * @return $this
     */
    public function removeWriteHook(Write|string $hook): static
    {
        $this->writeHooks = \array_values(\array_filter(
            $this->writeHooks,
            static fn (Write $registered): bool => \is_string($hook) ? ! $registered instanceof $hook : $registered !== $hook,
        ));

        return $this;
    }

    /**
     * @internal
     *
     * @return list<Write>
     */
    public function getWriteHooks(): array
    {
        return $this->writeHooks;
    }

    /**
     * Register a named query transform hook that modifies queries before execution.
     *
     * @param string $name Unique name for the transform.
     * @param Transform $transform The query transform hook to add.
     * @return $this
     */
    public function addTransform(string $name, Transform $transform): static
    {
        $this->transforms[$name] = $transform;

        return $this;
    }

    /**
     * Remove a query transform hook by name.
     *
     * @param string $name The name of the transform to remove.
     * @return $this
     */
    public function removeTransform(string $name): static
    {
        unset($this->transforms[$name]);

        return $this;
    }

    public function resetTransforms(): void
    {
        $this->transforms = [];
    }

    /**
     * Start a new transaction.
     *
     * If a transaction is already active, this will only increment the transaction count and return true.
     *
     * @throws DatabaseException
     */
    abstract public function startTransaction(): bool;

    /**
     * Commit a transaction.
     *
     * If no transaction is active, this will be a no-op and will return false.
     * If there is more than one active transaction, this decrement the transaction count and return true.
     * If the transaction count is 1, it will be commited, the transaction count will be reset to 0, and return true.
     *
     * @throws DatabaseException
     */
    abstract public function commitTransaction(): bool;

    /**
     * Rollback a transaction.
     *
     * If no transaction is active, this will be a no-op and will return false.
     * If 1 or more transactions are active, this will roll back all transactions, reset the count to 0, and return true.
     *
     * @throws DatabaseException
     */
    abstract public function rollbackTransaction(): bool;

    /**
     * Check if a transaction is active.
     */
    public function inTransaction(): bool
    {
        return $this->inTransaction > 0;
    }

    /**
     * Run the callback with createDocuments() skipping the documents whose id or unique key already exists instead
     * of failing. Nestable, and scoped to the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function ignoreDuplicates(callable $callback): mixed
    {
        return $this->ignoringDuplicates()->with(true, $callback);
    }

    /**
     * Whether the calling coroutine runs under ignoreDuplicates().
     */
    protected function isIgnoringDuplicates(): bool
    {
        return $this->ignoringDuplicates()->get();
    }

    /**
     * @return Value<bool>
     */
    private function ignoringDuplicates(): Value
    {
        return $this->ignoringDuplicates ??= new Value(false);
    }

    /**
     * Run the callback in a transaction, retrying an attempt that failed transiently up to twice (see isRetryable());
     * any other failure is rethrown at once. Only the outermost call retries: a call nested in another
     * withTransaction() rolls back to its savepoint and rethrows, and the outermost call runs the whole unit again, so
     * the retries do not multiply. A call nested in a transaction begun with startTransaction() retries in its
     * savepoint. A nested call whose enclosing transaction is gone throws `Exception\Transaction`, or the
     * `Exception\Contention` that made the engine roll the transaction back, which the outermost call retries because
     * nothing of that attempt is stored. A callback that returns after its transaction was lost underneath it fails
     * the same way. The outermost call never leaves the connection holding what remains of a failed transaction.
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
        $sleep = 50_000; // 50 milliseconds
        $retries = 2;
        $depth = $this->inTransaction;
        $enclosed = $this->transactionCalls > 0;
        $outermost = $depth === 0 && ! $enclosed;
        $this->transactionCalls++;

        try {
            for ($attempts = 0; $attempts <= $retries; $attempts++) {
                $started = false;
                try {
                    $this->startTransaction();
                    $started = true;
                    $result = $callback();
                    if ($this->inTransaction <= $depth) {
                        throw new TransactionException('Failed to commit transaction: the transaction was lost before the callback returned');
                    }
                    $this->commitTransaction();

                    return $result;
                } catch (Throwable $action) {
                    $rollback = null;
                    $lost = $started && $this->inTransaction <= $depth;
                    if (! $lost) {
                        try {
                            $this->rollbackTransaction();
                        } catch (Throwable $rollbackError) {
                            $rollback = $rollbackError;
                            $this->inTransaction = 0;
                        }

                        $lost = $this->inTransaction < $depth;
                    }

                    if ($outermost) {
                        $this->abandonTransaction();
                    }

                    if ($lost) {
                        if (! $action instanceof ContentionException) {
                            throw new TransactionException('Failed to execute transaction: the transaction was lost before it could commit', previous: $action);
                        }

                        if ($depth > 0) {
                            throw $action;
                        }
                    }

                    if ($enclosed || ! $this->isRetryable($action)) {
                        throw $action;
                    }

                    if ($attempts < $retries) {
                        \usleep($sleep * ($attempts + 1));

                        continue;
                    }

                    throw $rollback ?? $action;
                }
            }

            throw new TransactionException('Failed to execute transaction');
        } finally {
            $this->transactionCalls--;
        }
    }

    /**
     * End what the connection still holds of a transaction the adapter no longer counts, such as one lost with the
     * connection or left open by a failed rollback, so that the connection's next statement runs outside it.
     */
    protected function abandonTransaction(): void
    {
    }

    /**
     * Whether withTransaction() runs an attempt that failed with this again: it can succeed when it runs again. The
     * transaction itself failing (a lock conflict, or a failed begin, commit or rollback) or a transient driver failure
     * anywhere in the chain can; a typed failure of this library, or any other failure, would fail the same way again.
     */
    public function isRetryable(Throwable $failure): bool
    {
        for ($cause = $failure; $cause !== null; $cause = $cause->getPrevious()) {
            if ($cause instanceof TransactionException) {
                return true;
            }

            if ($cause instanceof DatabaseException && $cause::class !== DatabaseException::class) {
                return false;
            }

            if ($this->isTransient($cause)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Whether the driver raised the error for a condition that can clear on its own, such as a lost connection.
     */
    protected function isTransient(Throwable $error): bool
    {
        return Connection::hasError($error);
    }

    abstract public function create(string $name): bool;

    /**
     * Rename a database, moving every collection, document, index and permission it holds.
     *
     * @throws NotFoundException when no database is named $name
     * @throws DuplicateException when a database is already named $new
     * @throws DatabaseException when the engine cannot move the database
     */
    abstract public function update(string $name, string $new): bool;

    abstract public function exists(string $database): bool;

    abstract public function collectionExists(string $database, string $collection): bool;

    /**
     * @return array<Document>
     */
    abstract public function list(): array;

    abstract public function delete(string $name): bool;

    /**
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     */
    abstract public function createCollection(string $collection, array $attributes = [], array $indexes = []): bool;

    abstract public function deleteCollection(string $collection): bool;

    /**
     * Analyze a collection updating its metadata on the database engine
     */
    abstract public function analyzeCollection(string $collection): bool;

    /**
     * @throws TimeoutException
     * @throws DuplicateException
     */
    abstract public function createAttribute(string $collection, Attribute $attribute): bool;

    /**
     * @param  list<Attribute>  $attributes
     *
     * @throws TimeoutException
     * @throws DuplicateException
     */
    abstract public function createAttributes(string $collection, array $attributes): bool;

    /**
     * Alter the column stored under $key to match $attribute, renaming it when $attribute->key differs.
     */
    abstract public function updateAttribute(string $collection, string $key, Attribute $attribute): bool;

    /**
     * Relax a column's null constraint when an attribute stops being required.
     *
     * Most engines carry required-ness in the structure validator rather than
     * the column once it exists, so this does nothing by default. Postgres
     * overrides it because it created the column NOT NULL and keeps that
     * through every other alter.
     */
    public function relaxAttributeRequired(string $collection, string $id): bool
    {
        return true;
    }

    abstract public function deleteAttribute(string $collection, string $key): bool;

    abstract public function renameAttribute(string $collection, string $old, string $new): bool;

    /**
     * The columns the engine holds for a collection. Empty where the adapter does not declare
     * Capability::SchemaIntrospection, which is what tells "not introspectable" from "no columns".
     *
     * @return list<Schema\Column>
     */
    abstract public function getSchemaAttributes(string $collection): array;

    /**
     * The indexes the engine holds for a collection. Empty where the adapter does not declare
     * Capability::SchemaIntrospection.
     *
     * @return list<Schema\Index>
     */
    abstract public function getSchemaIndexes(string $collection): array;

    /**
     * The type getSchemaIndexes() reports for an index created as $type: an engine that stores an index of one
     * type as another reports the type it stores.
     *
     * @internal
     */
    public function getSchemaIndexType(IndexType $type): IndexType
    {
        return $type;
    }

    /**
     * The native column type the adapter creates for an attribute, in the spelling of Schema\Column::$type; null where
     * the engine has no column types.
     */
    abstract public function getColumnType(Attribute $attribute): ?string;

    /**
     * @param  array<string, string>  $indexAttributeTypes
     * @param  array<string, mixed>  $collation
     */
    abstract public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool;

    abstract public function deleteIndex(string $collection, string $key): bool;

    abstract public function renameIndex(string $collection, string $old, string $new): bool;

    abstract public function createDocument(Document $collection, Document $document): Document;

    /**
     * Create Documents in batches
     *
     * @param  array<Document>  $documents
     * @return array<Document> The documents written; under ignoreDuplicates() the skipped ones are left out
     *
     * @throws DatabaseException
     */
    abstract public function createDocuments(Document $collection, array $documents): array;

    /**
     * @param  array<Query>  $queries
     */
    abstract public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document;

    abstract public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document;

    /**
     * Updates all documents which match the given query.
     *
     * @param  array<Document>  $documents
     * @param  array<string, true>  $skipPermissions  Ids of the documents whose permissions the update keeps, so a
     *                                               permissions write hook leaves their permission rows alone
     *
     * @throws DatabaseException
     */
    abstract public function updateDocuments(Document $collection, Document $updates, array $documents, array $skipPermissions = []): int;

    /**
     * Increase or decrease attribute value
     *
     * @throws Exception
     */
    abstract public function increaseDocumentAttribute(
        Document $collection,
        string $id,
        string $attribute,
        int|float|string $value,
        string $updatedAt,
        int|float|string|null $min = null,
        int|float|string|null $max = null
    ): bool;

    abstract public function deleteDocument(Document $collection, string $id): bool;

    /**
     * @param  array<string>  $sequences
     * @param  array<string>  $permissionIds
     */
    abstract public function deleteDocuments(Document $collection, array $sequences, array $permissionIds): int;

    /**
     * Find Documents
     *
     * Find data sets using chosen queries
     *
     * @param  array<Query>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<\Utopia\Query\OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<Document>
     */
    abstract public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array;

    /**
     * Count Documents
     *
     * @param  array<Query>  $queries
     */
    abstract public function count(Document $collection, array $queries = [], ?int $max = null): int;

    /**
     * Sum an attribute
     *
     * @param  array<Query>  $queries
     */
    abstract public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int;

    /**
     * @param  array<Document>  $documents
     * @return array<Document>
     */
    abstract public function getSequences(Document $collection, array $documents): array;

    abstract public function limits(): Limits;

    /**
     * Get Collection Size of the raw data
     *
     * @throws DatabaseException
     */
    abstract public function getSizeOfCollection(string $collection): int;

    /**
     * Get Collection Size on the disk
     *
     * @throws DatabaseException
     */
    abstract public function getSizeOfCollectionOnDisk(string $collection): int;

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     * Return 0 when no restrictions apply to row width
     */
    abstract public function getAttributeWidth(Document $collection): int;

    /**
     * Get current attribute count from collection document
     */
    abstract public function getCountOfAttributes(Document $collection): int;

    /**
     * The attributes a collection definition declares, also when it is a metadata row read straight from storage
     * that still holds them as JSON.
     *
     * @internal
     *
     * @return list<Attribute>
     *
     * @throws StructureException
     */
    protected static function collectionAttributes(Document $collection): array
    {
        return Collection::fromDocument($collection)->attributes();
    }

    /**
     * The attributes a collection definition declares followed by every internal attribute, `$tenant` included.
     *
     * @internal
     *
     * @return list<Attribute>
     *
     * @throws StructureException
     */
    protected static function collectionAttributesWithInternal(Document $collection): array
    {
        return Collection::fromDocument($collection)->attributesWith(Database::internalAttributesFor(true));
    }

    /**
     * The indexes a collection definition declares, also when it is a metadata row read straight from storage
     * that still holds them as JSON.
     *
     * @internal
     *
     * @return list<Index>
     *
     * @throws IndexException
     */
    protected static function collectionIndexes(Document $collection): array
    {
        return Collection::fromDocument($collection)->indexes();
    }

    /**
     * Get current index count from collection document
     */
    abstract public function getCountOfIndexes(Document $collection): int;

    protected function getInternalKeyForAttribute(string $attribute): string
    {
        return Storage::column($attribute);
    }

    /**
     * Process-lifetime cache for {@see self::filter()}. Keys are referentially
     * stable across the request lifetime and frequently re-queried per-row, so
     * caching the regex result amortizes the preg_replace cost across all
     * decode/encode/build passes. Bounded to avoid unbounded growth from
     * unusual input.
     *
     * @var array<string, string>
     */
    private static array $filteredKeyCache = [];

    private const int FILTERED_KEY_CACHE_LIMIT = 4096;

    /**
     * Filter Keys
     *
     * @throws DatabaseException
     */
    public function filter(string $value): string
    {
        if (isset(self::$filteredKeyCache[$value])) {
            return self::$filteredKeyCache[$value];
        }

        $filtered = \preg_replace("/[^A-Za-z0-9_\-]/", '', $value);

        if (\is_null($filtered)) {
            throw new DatabaseException('Failed to filter key');
        }

        if (\count(self::$filteredKeyCache) >= self::FILTERED_KEY_CACHE_LIMIT) {
            self::$filteredKeyCache = [];
        }

        return self::$filteredKeyCache[$value] = $filtered;
    }

    /**
     * The row as every write hook decorates a row written for the document.
     *
     * @param  array<string, mixed>  $row
     * @return array<string, mixed>
     */
    protected function decorateRow(array $row, Document $document): array
    {
        if ($this->writeHooks === []) {
            return $row;
        }

        $metadata = new Hook\RowMetadata($document->getTenant() ?? $this->currentTenant());
        foreach ($this->writeHooks as $hook) {
            $row = $hook->decorateRow($row, $metadata);
        }

        return $row;
    }

    /**
     * Run the callable once per registered write hook, in registration order.
     *
     * @param callable(Write): void $callback
     */
    protected function runWriteHooks(callable $callback): void
    {
        foreach ($this->writeHooks as $hook) {
            $callback($hook);
        }
    }

    /**
     * Get all selected attributes from queries
     *
     * @param  array<Query>  $queries
     * @return array<string>
     */
    protected function getAttributeSelections(array $queries): array
    {
        $selections = [];

        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select) {
                foreach ($query->getValues() as $value) {
                    /** @var string $value */
                    $selections[] = $value;
                }
            }
        }

        return $selections;
    }

    protected function escapeWildcards(string $value): string
    {
        $wildcards = [
            '%',
            '_',
            '[',
            ']',
            '^',
            '-',
            '.',
            '*',
            '+',
            '?',
            '(',
            ')',
            '{',
            '}',
            '|',
        ];

        foreach ($wildcards as $wildcard) {
            $value = \str_replace($wildcard, "\\$wildcard", $value);
        }

        return $value;
    }

    /**
     * The client the adapter talks to its engine through, such as a PDO or a MongoDB client.
     */
    abstract public function getDriver(): object;
}
