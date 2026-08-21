<?php

namespace Utopia\Database;

use DateTime;
use Exception;
use Throwable;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Hook\Write;
use Utopia\Database\Profiler\QueryProfiler;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;

/**
 * Abstract base class for all database adapters, providing shared state management and a contract for database operations.
 */
abstract class Adapter implements Feature\Attributes, Feature\Collections, Feature\Databases, Feature\Documents, Feature\Indexes, Feature\Transactions
{
    protected string $database = '';

    protected string $hostname = '';

    protected string $namespace = '';

    protected bool $sharedTables = false;

    protected int|string|null $tenant = null;

    protected bool $tenantPerDocument = false;

    protected int $timeout = 0;

    /**
     * @var array<string, int>
     */
    protected array $timeouts = [];

    protected int $inTransaction = 0;

    protected bool $alterLocks = false;

    protected bool $skipDuplicates = false;

    /**
     * @var array<string, mixed>
     */
    protected array $debug = [];

    /**
     * @var array<string, Transform>
     */
    protected array $queryTransforms = [];

    /**
     * @var array<string, mixed>
     */
    protected array $metadata = [];

    /**
     * @var list<Write>
     */
    protected array $writeHooks = [];

    protected ?QueryProfiler $profiler = null;

    protected Authorization $authorization;

    /** @var array<string, true>|null */
    protected ?array $capabilitySet = null;

    /**
     * Check if this adapter supports a given capability.
     *
     * @param  Capability  $feature  Capability enum case
     */
    public function supports(Capability $feature): bool
    {
        if ($this->capabilitySet === null) {
            $this->capabilitySet = [];
            foreach ($this->capabilities() as $cap) {
                $this->capabilitySet[$cap->name] = true;
            }
        }
        return isset($this->capabilitySet[$feature->name]);
    }

    /**
     * @template T of object
     *
     * @param  class-string<T>  $feature
     *
     * @phpstan-assert-if-true T $this
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
            Capability::Index,
            Capability::IndexArray,
            Capability::UniqueIndex,
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

    public function setProfiler(?QueryProfiler $profiler): static
    {
        $this->profiler = $profiler;

        return $this;
    }

    public function getProfiler(): ?QueryProfiler
    {
        return $this->profiler;
    }

    /**
     * Set Database.
     *
     * Set database to use for current scope
     *
     *
     * @throws DatabaseException
     */
    public function setDatabase(string $name): bool
    {
        $this->database = $this->filter($name);

        return true;
    }

    /**
     * Get Database.
     *
     * Get Database from current scope
     */
    public function getDatabase(): string
    {
        return $this->database;
    }

    /**
     * Set Namespace.
     *
     * Set namespace to divide different scope of data sets
     *
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
     * Get Namespace.
     *
     * Get namespace of current set scope
     */
    public function getNamespace(): string
    {
        return $this->namespace;
    }

    /**
     * Set Hostname.
     *
     * @return $this
     */
    public function setHostname(string $hostname): static
    {
        $this->hostname = $hostname;

        return $this;
    }

    /**
     * Get Hostname.
     */
    public function getHostname(): string
    {
        return $this->hostname;
    }

    /**
     * Set Shared Tables.
     *
     * Set whether to share tables between tenants
     */
    public function setSharedTables(bool $sharedTables): bool
    {
        $this->sharedTables = $sharedTables;

        return true;
    }

    /**
     * Get Share Tables.
     *
     * Get whether to share tables between tenants
     */
    public function getSharedTables(): bool
    {
        return $this->sharedTables;
    }

    /**
     * Set Tenant.
     *
     * Set tenant to use if tables are shared
     */
    public function setTenant(int|string|null $tenant): bool
    {
        $this->tenant = $tenant;

        return true;
    }

    /**
     * Get Tenant.
     *
     * Get tenant to use for shared tables.
     * Numeric values are normalized to int for consistent comparison
     * across adapters that may return string representations.
     */
    public function getTenant(): int|string|null
    {
        if (\is_string($this->tenant) && \ctype_digit($this->tenant)) {
            return (int) $this->tenant;
        }

        return $this->tenant;
    }

    /**
     * Set Tenant Per Document.
     *
     * Set whether to use a different tenant for each document
     */
    public function setTenantPerDocument(bool $tenantPerDocument): bool
    {
        $this->tenantPerDocument = $tenantPerDocument;

        return true;
    }

    /**
     * Get Tenant Per Document.
     *
     * Get whether to use a different tenant for each document
     */
    public function getTenantPerDocument(): bool
    {
        return $this->tenantPerDocument;
    }

    /**
     * Set a debug key-value pair for diagnostic purposes.
     *
     * @param string $key The debug key.
     * @param mixed $value The debug value.
     * @return $this
     */
    public function setDebug(string $key, mixed $value): static
    {
        $this->debug[$key] = $value;

        return $this;
    }

    /**
     * Get all collected debug data.
     *
     * @return array<string, mixed>
     */
    public function getDebug(): array
    {
        return $this->debug;
    }

    /**
     * Reset all debug data.
     *
     * @return $this
     */
    public function resetDebug(): static
    {
        $this->debug = [];

        return $this;
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
     * Get metadata
     *
     * @return array<string, mixed>
     */
    public function getMetadata(): array
    {
        return $this->metadata;
    }

    /**
     * Clear existing metadata
     *
     * @return $this
     */
    public function resetMetadata(): static
    {
        $this->metadata = [];

        return $this;
    }

    protected function setTimeoutState(int $milliseconds, Event $event): void
    {
        $this->timeouts[$event->value] = $milliseconds;

        if ($event === Event::All) {
            $this->timeout = $milliseconds;
        }
    }

    /**
     * Get the current query timeout value.
     *
     * @return int Timeout in milliseconds, or 0 if no timeout is set.
     */
    public function getTimeout(Event $event = Event::All): int
    {
        return $this->timeouts[$event->value]
            ?? $this->timeouts[Event::All->value]
            ?? $this->timeout;
    }

    protected function clearTimeoutState(Event $event): void
    {
        if ($event === Event::All) {
            $this->timeouts = [];
            $this->timeout = 0;

            return;
        }

        unset($this->timeouts[$event->value]);
    }

    /**
     * Enable or disable LOCK=SHARED during ALTER TABLE operations.
     *
     * @param bool $enable True to enable alter locks.
     * @return $this
     */
    public function enableAlterLocks(bool $enable): self
    {
        $this->alterLocks = $enable;

        return $this;
    }

    public function getAlterLocks(): bool
    {
        return $this->alterLocks;
    }

    /**
     * Set support for attributes
     */
    abstract public function setSupportForAttributes(bool $support): bool;

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

    public function hasPermissionHook(): bool
    {
        foreach ($this->writeHooks as $hook) {
            if ($hook instanceof Hook\Permissions) {
                return true;
            }
        }

        return false;
    }

    public function hasTenantHook(): bool
    {
        return $this->getTenantHook() !== null;
    }

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
     * Remove a write hook by its class name.
     *
     * @param string $class The fully qualified class name of the hook to remove.
     * @return $this
     */
    public function removeWriteHook(string $class): static
    {
        $this->writeHooks = \array_values(\array_filter(
            $this->writeHooks,
            fn (Write $h) => ! ($h instanceof $class)
        ));

        return $this;
    }

    /**
     * Get all registered write hooks.
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
        $this->queryTransforms[$name] = $transform;

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
        unset($this->queryTransforms[$name]);

        return $this;
    }

    /**
     * Remove all registered query transform hooks.
     *
     * @return $this
     */
    public function resetTransforms(): static
    {
        $this->queryTransforms = [];

        return $this;
    }

    /**
     * Ping Database
     */
    abstract public function ping(): bool;

    /**
     * Reconnect Database
     */
    abstract public function reconnect(): void;

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
     * Run a callback with skipDuplicates enabled.
     * Duplicate key errors during createDocuments() will be silently skipped
     * instead of thrown. Nestable — saves and restores previous state.
     *
     * @template T
     * @param callable(): T $callback
     * @return T
     */
    public function skipDuplicates(callable $callback): mixed
    {
        $previous = $this->skipDuplicates;
        $this->skipDuplicates = true;

        try {
            return $callback();
        } finally {
            $this->skipDuplicates = $previous;
        }
    }

    /**
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

        for ($attempts = 0; $attempts <= $retries; $attempts++) {
            try {
                $this->startTransaction();
                $result = $callback();
                $this->commitTransaction();

                return $result;
            } catch (Throwable $action) {
                $rollback = null;
                try {
                    $this->rollbackTransaction();
                } catch (Throwable $rollbackError) {
                    $rollback = $rollbackError;
                    $this->inTransaction = 0;
                }

                if (
                    $action instanceof DuplicateException ||
                    $action instanceof RestrictedException ||
                    $action instanceof AuthorizationException ||
                    $action instanceof RelationshipException ||
                    $action instanceof ConflictException ||
                    $action instanceof LimitException ||
                    $action instanceof TimeoutException
                ) {
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
    }

    /**
     * Create Database
     */
    abstract public function create(string $name): bool;

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param  string  $database  database name
     * @param  string|null  $collection  (optional) collection name
     */
    abstract public function exists(string $database, ?string $collection = null): bool;

    /**
     * List Databases
     *
     * @return array<Document>
     */
    abstract public function list(): array;

    /**
     * Delete Database
     */
    abstract public function delete(string $name): bool;

    /**
     * Create Collection
     *
     * @param  array<Attribute>  $attributes  (optional)
     * @param  array<Index>  $indexes  (optional)
     */
    abstract public function createCollection(string $name, array $attributes = [], array $indexes = []): bool;

    /**
     * Delete Collection
     */
    abstract public function deleteCollection(string $id): bool;

    /**
     * Analyze a collection updating its metadata on the database engine
     */
    abstract public function analyzeCollection(string $collection): bool;

    /**
     * Create Attribute
     *
     * @throws TimeoutException
     * @throws DuplicateException
     */
    abstract public function createAttribute(string $collection, Attribute $attribute): bool;

    /**
     * Create Attributes
     *
     * @param  array<Attribute>  $attributes
     *
     * @throws TimeoutException
     * @throws DuplicateException
     */
    abstract public function createAttributes(string $collection, array $attributes): bool;

    /**
     * Update Attribute
     */
    abstract public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool;

    /**
     * Delete Attribute
     */
    abstract public function deleteAttribute(string $collection, string $id): bool;

    /**
     * Rename Attribute
     */
    abstract public function renameAttribute(string $collection, string $old, string $new): bool;

    /**
     * @param  array<string, string>  $indexAttributeTypes
     * @param  array<string, mixed>  $collation
     */
    abstract public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool;

    /**
     * Delete Index
     */
    abstract public function deleteIndex(string $collection, string $id): bool;

    /**
     * Rename Index
     */
    abstract public function renameIndex(string $collection, string $old, string $new): bool;

    /**
     * Create Document
     */
    abstract public function createDocument(Document $collection, Document $document): Document;

    /**
     * Create Documents in batches
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    abstract public function createDocuments(Document $collection, array $documents): array;

    /**
     * Get Document
     *
     * @param  array<Query>  $queries
     */
    abstract public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document;

    /**
     * Update Document
     */
    abstract public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document;

    /**
     * Update documents
     *
     * Updates all documents which match the given query.
     *
     * @param  array<Document>  $documents
     *
     * @throws DatabaseException
     */
    abstract public function updateDocuments(Document $collection, Document $updates, array $documents): int;

    /**
     * Increase or decrease attribute value
     *
     * @throws Exception
     */
    abstract public function increaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float|string $value,
        string $updatedAt,
        int|float|string|null $min = null,
        int|float|string|null $max = null
    ): bool;

    /**
     * Delete Document
     */
    abstract public function deleteDocument(string $collection, string $id): bool;

    /**
     * Delete Documents
     *
     * @param  array<string>  $sequences
     * @param  array<string>  $permissionIds
     */
    abstract public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int;

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
    abstract public function getSequences(string $collection, array $documents): array;

    /**
     * Get max STRING limit
     */
    abstract public function getLimitForString(): int;

    /**
     * Get max INT limit
     */
    abstract public function getLimitForInt(): int;

    /**
     * Get max BIGINT limit
     *
     * @return int
     */
    abstract public function getLimitForBigInt(): int;

    /**
     * Get maximum attributes limit.
     */
    abstract public function getLimitForAttributes(): int;

    /**
     * Get maximum index limit.
     */
    abstract public function getLimitForIndexes(): int;

    /**
     * Get the maximum index key length in bytes.
     */
    abstract public function getMaxIndexLength(): int;

    /**
     * Get the maximum VARCHAR length for this adapter
     */
    abstract public function getMaxVarcharLength(): int;

    /**
     * Get the maximum UID length for this adapter
     */
    abstract public function getMaxUIDLength(): int;

    /**
     * Get the minimum supported DateTime value
     */
    abstract public function getMinDateTime(): DateTime;

    /**
     * Get the maximum supported DateTime value
     */
    public function getMaxDateTime(): DateTime
    {
        return new DateTime('9999-12-31 23:59:59');
    }

    /**
     * Get the primitive type of the primary key type for this adapter
     */
    abstract public function getIdAttributeType(): string;

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
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     */
    abstract public function getDocumentSizeLimit(): int;

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
     * Get current index count from collection document
     */
    abstract public function getCountOfIndexes(Document $collection): int;

    /**
     * Returns number of attributes used by default.
     */
    abstract public function getCountOfDefaultAttributes(): int;

    /**
     * Returns number of indexes used by default.
     */
    abstract public function getCountOfDefaultIndexes(): int;

    /**
     * Get list of keywords that cannot be used
     *
     * @return array<string>
     */
    abstract public function getKeywords(): array;

    /**
     * Get List of internal index keys names
     *
     * @return array<string>
     */
    abstract public function getInternalIndexesKeys(): array;

    protected function getInternalKeyForAttribute(string $attribute): string
    {
        return Storage::column($attribute);
    }

    /**
     * Get the query to check for tenant when in shared tables mode
     *
     * @param  string  $collection  The collection being queried
     * @param  string  $alias  The alias of the parent collection if in a subquery
     */
    abstract public function getTenantQuery(string $collection, string $alias = ''): string;

    /**
     * Handle non utf characters supported?
     */
    public function getSupportNonUtfCharacters(): bool
    {
        return false;
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

    private const FILTERED_KEY_CACHE_LIMIT = 4096;

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
     * Apply all write hooks' decorateRow to a row.
     *
     * @param  array<string, mixed>  $row
     * @param  array<string, mixed>  $metadata
     * @return array<string, mixed>
     */
    protected function decorateRow(array $row, array $metadata): array
    {
        foreach ($this->writeHooks as $hook) {
            $row = $hook->decorateRow($row, $metadata);
        }

        return $row;
    }

    /**
     * Run all write hooks concurrently when more than one is registered,
     * otherwise run sequentially. The provided callable receives a single
     * Write hook instance.
     *
     * @param callable(Write): void $fn
     */
    protected function runWriteHooks(callable $fn): void
    {
        foreach ($this->writeHooks as $hook) {
            $fn($hook);
        }
    }

    /**
     * @return array<string, mixed>
     */
    protected function documentMetadata(Document $document): array
    {
        return ['id' => $document->getId(), 'tenant' => $document->getTenant()];
    }

    /**
     * Get an attribute projection given a list of selected attributes
     *
     * @param  array<string>  $selections
     */
    abstract protected function getAttributeProjection(array $selections, string $prefix): mixed;

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
     * Quote a string
     */
    abstract protected function quote(string $string): string;

    abstract protected function execute(mixed $stmt): bool;

    /**
     * @return mixed
     */
    abstract public function getDriver(): mixed;
}
