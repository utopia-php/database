<?php

namespace Utopia\Database;

use Exception;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;

abstract class Adapter
{
    protected string $database = '';
    protected string $hostname = '';

    protected string $namespace = '';

    protected bool $sharedTables = false;

    protected ?int $tenant = null;

    protected bool $tenantPerDocument = false;

    protected int $timeout = 0;

    protected int $inTransaction = 0;

    /**
     * @var array<string, mixed>
     */
    protected array $debug = [];

    /**
     * @var array<string, array<callable>>
     */
    protected array $transformations = [
        '*' => [],
    ];

    /**
     * @var array<string, mixed>
     */
    protected array $metadata = [];

    /**
     * @param string $key
     * @param mixed $value
     *
     * @return $this
     */
    public function setDebug(string $key, mixed $value): static
    {
        $this->debug[$key] = $value;

        return $this;
    }

    /**
     * @return array<string, mixed>
     */
    public function getDebug(): array
    {
        return $this->debug;
    }

    /**
     * @return static
     */
    public function resetDebug(): static
    {
        $this->debug = [];

        return $this;
    }

    /**
     * Set Namespace.
     *
     * Set namespace to divide different scope of data sets
     *
     * @param string $namespace
     *
     * @return $this
     * @throws DatabaseException
     *
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
     *
     * @return string
     *
     */
    public function getNamespace(): string
    {
        return $this->namespace;
    }

    /**
     * Set Hostname.
     *
     * @param string $hostname
     * @return $this
     */
    public function setHostname(string $hostname): static
    {
        $this->hostname = $hostname;

        return $this;
    }

    /**
     * Get Hostname.
     *
     * @return string
     */
    public function getHostname(): string
    {
        return $this->hostname;
    }

    /**
     * Set Database.
     *
     * Set database to use for current scope
     *
     * @param string $name
     *
     * @return bool
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
     *
     * @return string
     *
     */
    public function getDatabase(): string
    {
        return $this->database;
    }

    /**
     * Set Shared Tables.
     *
     * Set whether to share tables between tenants
     *
     * @param bool $sharedTables
     *
     * @return bool
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
     *
     * @return bool
     */
    public function getSharedTables(): bool
    {
        return $this->sharedTables;
    }

    /**
     * Set Tenant.
     *
     * Set tenant to use if tables are shared
     *
     * @param ?int $tenant
     *
     * @return bool
     */
    public function setTenant(?int $tenant): bool
    {
        $this->tenant = $tenant;

        return true;
    }

    /**
     * Get Tenant.
     *
     * Get tenant to use for shared tables
     *
     * @return ?int
     */
    public function getTenant(): ?int
    {
        return $this->tenant;
    }

    /**
     * Set Tenant Per Document.
     *
     * Set whether to use a different tenant for each document
     *
     * @param bool $tenantPerDocument
     *
     * @return bool
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
     *
     * @return bool
     */
    public function getTenantPerDocument(): bool
    {
        return $this->tenantPerDocument;
    }

    /**
     * Set metadata for query comments
     *
     * @param string $key
     * @param mixed $value
     * @return $this
     */
    public function setMetadata(string $key, mixed $value): static
    {
        $this->metadata[$key] = $value;

        $output = '';
        foreach ($this->metadata as $key => $value) {
            $output .= "/* {$key}: {$value} */\n";
        }

        $this->before(Database::EVENT_ALL, 'metadata', function ($query) use ($output) {
            return $output . $query;
        });

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

    /**
     * Set a global timeout for database queries in milliseconds.
     *
     * This function allows you to set a maximum execution time for all database
     * queries executed using the library, or a specific event specified by the
     * event parameter. Once this timeout is set, any database query that takes
     * longer than the specified time will be automatically terminated by the library,
     * and an appropriate error or exception will be raised to handle the timeout condition.
     *
     * @param int $milliseconds The timeout value in milliseconds for database queries.
     * @param string $event     The event the timeout should fire for
     * @return void
     *
     * @throws Exception The provided timeout value must be greater than or equal to 0.
     */
    abstract public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void;

    public function getTimeout(): int
    {
        return $this->timeout;
    }

    /**
     * Clears a global timeout for database queries.
     *
     * @param string $event
     * @return void
     */
    public function clearTimeout(string $event): void
    {
        // Clear existing callback
        $this->before($event, 'timeout');
    }

    /**
     * Start a new transaction.
     *
     * If a transaction is already active, this will only increment the transaction count and return true.
     *
     * @return bool
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
     * @return bool
     * @throws DatabaseException
     */
    abstract public function commitTransaction(): bool;

    /**
     * Rollback a transaction.
     *
     * If no transaction is active, this will be a no-op and will return false.
     * If 1 or more transactions are active, this will roll back all transactions, reset the count to 0, and return true.
     *
     * @return bool
     * @throws DatabaseException
     */
    abstract public function rollbackTransaction(): bool;

    /**
     * Check if a transaction is active.
     *
     * @return bool
     */
    public function inTransaction(): bool
    {
        return $this->inTransaction > 0;
    }

    /**
     * @template T
     * @param callable(): T $callback
     * @return T
     * @throws \Throwable
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
            } catch (\Throwable $action) {
                try {
                    $this->rollbackTransaction();

                    if (
                        $action instanceof DuplicateException ||
                        $action instanceof RestrictedException ||
                        $action instanceof AuthorizationException ||
                        $action instanceof RelationshipException ||
                        $action instanceof ConflictException ||
                        $action instanceof LimitException
                    ) {
                        $this->inTransaction = 0;
                        throw $action;
                    }

                } catch (\Throwable $rollback) {
                    if ($attempts < $retries) {
                        \usleep($sleep * ($attempts + 1));
                        continue;
                    }

                    $this->inTransaction = 0;
                    throw $rollback;
                }

                if ($attempts < $retries) {
                    \usleep($sleep * ($attempts + 1));
                    continue;
                }

                $this->inTransaction = 0;
                throw $action;
            }
        }

        throw new TransactionException('Failed to execute transaction');
    }

    /**
     * Apply a transformation to a query before an event occurs
     *
     * @param string $event
     * @param string $name
     * @param ?callable $callback
     * @return static
     */
    public function before(string $event, string $name = '', ?callable $callback = null): static
    {
        if (!isset($this->transformations[$event])) {
            $this->transformations[$event] = [];
        }

        if (\is_null($callback)) {
            unset($this->transformations[$event][$name]);
        } else {
            $this->transformations[$event][$name] = $callback;
        }

        return $this;
    }

    protected function trigger(string $event, mixed $query): mixed
    {
        foreach ($this->transformations[Database::EVENT_ALL] as $callback) {
            $query = $callback($query);
        }
        foreach (($this->transformations[$event] ?? []) as $callback) {
            $query = $callback($query);
        }

        return $query;
    }

    /**
     * Quote a string
     *
     * @param string $string
     * @return string
     */
    abstract protected function quote(string $string): string;

    /**
     * Ping Database
     *
     * @return bool
     */
    abstract public function ping(): bool;

    /**
     * Reconnect Database
     */
    abstract public function reconnect(): void;

    /**
     * Create Database
     *
     * @param string $name
     *
     * @return bool
     */
    abstract public function create(string $name): bool;

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string $database database name
     * @param string|null $collection (optional) collection name
     *
     * @return bool
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
     *
     * @param string $name
     *
     * @return bool
     */
    abstract public function delete(string $name): bool;

    /**
     * Create Collection
     *
     * @param string $name
     * @param array<Document> $attributes (optional)
     * @param array<Document> $indexes (optional)
     * @return bool
     */
    abstract public function createCollection(string $name, array $attributes = [], array $indexes = []): bool;

    /**
     * Delete Collection
     *
     * @param string $id
     *
     * @return bool
     */
    abstract public function deleteCollection(string $id): bool;

    /**
     * Analyze a collection updating its metadata on the database engine
     *
     * @param string $collection
     * @return bool
     */
    abstract public function analyzeCollection(string $collection): bool;

    /**
     * Create Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @param bool $array
     * @return bool
     * @throws TimeoutException
     * @throws DuplicateException
     */
    abstract public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool;

    /**
     * Create Attributes
     *
     * @param string $collection
     * @param array<array<string, mixed>> $attributes
     * @return bool
     * @throws TimeoutException
     * @throws DuplicateException
     */
    abstract public function createAttributes(string $collection, array $attributes): bool;

    /**
     * Update Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @param bool $array
     * @param string|null $newKey
     *
     * @return bool
     */
    abstract public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null): bool;

    /**
     * Delete Attribute
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    abstract public function deleteAttribute(string $collection, string $id): bool;

    /**
     * Rename Attribute
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     */
    abstract public function renameAttribute(string $collection, string $old, string $new): bool;

    /**
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $id
     * @param string $twoWayKey
     * @return bool
     */
    abstract public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool;

    /**
     * Update Relationship
     *
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $key
     * @param string $twoWayKey
     * @param string $side
     * @param string|null $newKey
     * @param string|null $newTwoWayKey
     * @return bool
     */
    abstract public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null): bool;

    /**
     * Delete Relationship
     *
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $key
     * @param string $twoWayKey
     * @param string $side
     * @return bool
     */
    abstract public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool;

    /**
     * Rename Index
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     */
    abstract public function renameIndex(string $collection, string $old, string $new): bool;

    /**
     * Create Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     * @param array<int> $lengths
     * @param array<string> $orders
     * @param array<string,string> $indexAttributeTypes
     *
     * @return bool
     */
    abstract public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = []): bool;

    /**
     * Delete Index
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    abstract public function deleteIndex(string $collection, string $id): bool;

    /**
     * Get Document
     *
     * @param Document $collection
     * @param string $id
     * @param array<Query> $queries
     * @param bool $forUpdate
     * @return Document
     */
    abstract public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document;

    /**
     * Create Document
     *
     * @param Document $collection
     * @param Document $document
     *
     * @return Document
     */
    abstract public function createDocument(Document $collection, Document $document): Document;

    /**
     * Create Documents in batches
     *
     * @param Document $collection
     * @param array<Document> $documents
     *
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    abstract public function createDocuments(Document $collection, array $documents): array;

    /**
     * Update Document
     *
     * @param Document $collection
     * @param string $id
     * @param Document $document
     * @param bool $skipPermissions
     *
     * @return Document
     */
    abstract public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document;

    /**
     * Update documents
     *
     * Updates all documents which match the given query.
     *
     * @param Document $collection
     * @param Document $updates
     * @param array<Document> $documents
     *
     * @return int
     *
     * @throws DatabaseException
     */
    abstract public function updateDocuments(Document $collection, Document $updates, array $documents): int;

    /**
     * Create documents if they do not exist, otherwise update them.
     *
     * If attribute is not empty, only the specified attribute will be increased, by the new value in each document.
     *
     * @param Document $collection
     * @param string $attribute
     * @param array<Change> $changes
     * @return array<Document>
     */
    abstract public function createOrUpdateDocuments(
        Document $collection,
        string $attribute,
        array $changes
    ): array;

    /**
     * @param string $collection
     * @param array<Document> $documents
     * @return array<Document>
     */
    abstract public function getSequences(string $collection, array $documents): array;

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    abstract public function deleteDocument(string $collection, string $id): bool;

    /**
     * Delete Documents
     *
     * @param string $collection
     * @param array<string> $sequences
     * @param array<string> $permissionIds
     *
     * @return int
     */
    abstract public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int;

    /**
     * Find Documents
     *
     * Find data sets using chosen queries
     *
     * @param QueryContext $context
     * @param array<Query> $queries
     * @param int|null $limit
     * @param int|null $offset
     * @param array<string, mixed> $cursor
     * @param string $cursorDirection
     * @param string $forPermission
     * @param array<Query> $selects
     * @param array<Query> $filters
     * @param array<Query> $joins
     * @param array<Query> $orderQueries
     *
     * @return array<Document>
     */
    abstract public function find(
        QueryContext $context,
        array $queries = [],
        ?int $limit = 25,
        ?int $offset = null,
        array $cursor = [],
        string $cursorDirection = Database::CURSOR_AFTER,
        string $forPermission = Database::PERMISSION_READ,
        array $selects = [],
        array $filters = [],
        array $joins = [],
        array $orderQueries = []
    ): array;

    /**
     * Sum an attribute
     *
     * @param Document $collection
     * @param string $attribute
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int|float
     */
    abstract public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int;

    /**
     * Count Documents
     *
     * @param Document $collection
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int
     */
    abstract public function count(Document $collection, array $queries = [], ?int $max = null): int;

    /**
     * Get Collection Size of the raw data
     *
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    abstract public function getSizeOfCollection(string $collection): int;

    /**
     * Get Collection Size on the disk
     *
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    abstract public function getSizeOfCollectionOnDisk(string $collection): int;

    /**
     * Get max STRING limit
     *
     * @return int
     */
    abstract public function getLimitForString(): int;

    /**
     * Get max INT limit
     *
     * @return int
     */
    abstract public function getLimitForInt(): int;

    /**
     * Get maximum attributes limit.
     *
     * @return int
     */
    abstract public function getLimitForAttributes(): int;

    /**
     * Get maximum index limit.
     *
     * @return int
     */
    abstract public function getLimitForIndexes(): int;

    /**
     * @return int
     */
    abstract public function getMaxIndexLength(): int;

    /**
     * Get the minimum supported DateTime value
     *
     * @return \DateTime
     */
    abstract public function getMinDateTime(): \DateTime;

    /**
     * Get the primitive type of the primary key type for this adapter
     *
     * @return string
     */
    abstract public function getIdAttributeType(): string;

    /**
     * Get the maximum supported DateTime value
     *
     * @return \DateTime
     */
    public function getMaxDateTime(): \DateTime
    {
        return new \DateTime('9999-12-31 23:59:59');
    }

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    abstract public function getSupportForSchemas(): bool;

    /**
     * Are attributes supported?
     *
     * @return bool
     */
    abstract public function getSupportForAttributes(): bool;

    /**
     * Are schema attributes supported?
     *
     * @return bool
     */
    abstract public function getSupportForSchemaAttributes(): bool;

    /**
     * Is index supported?
     *
     * @return bool
     */
    abstract public function getSupportForIndex(): bool;

    /**
     * Is indexing array supported?
     *
     * @return bool
     */
    abstract public function getSupportForIndexArray(): bool;

    /**
     * Is cast index as array supported?
     *
     * @return bool
     */
    abstract public function getSupportForCastIndexArray(): bool;

    /**
     * Is unique index supported?
     *
     * @return bool
     */
    abstract public function getSupportForUniqueIndex(): bool;

    /**
     * Is fulltext index supported?
     *
     * @return bool
     */
    abstract public function getSupportForFulltextIndex(): bool;

    /**
     * Is fulltext wildcard supported?
     *
     * @return bool
     */
    abstract public function getSupportForFulltextWildcardIndex(): bool;


    /**
     * Does the adapter handle casting?
     *
     * @return bool
     */
    abstract public function getSupportForCasting(): bool;

    /**
     * Does the adapter handle array Contains?
     *
     * @return bool
     */
    abstract public function getSupportForQueryContains(): bool;

    /**
     * Are timeouts supported?
     *
     * @return bool
     */
    abstract public function getSupportForTimeouts(): bool;

    /**
     * Are relationships supported?
     *
     * @return bool
     */
    abstract public function getSupportForRelationships(): bool;

    abstract public function getSupportForUpdateLock(): bool;

    /**
     * Are batch operations supported?
     *
     * @return bool
     */
    abstract public function getSupportForBatchOperations(): bool;

    /**
     * Is attribute resizing supported?
     *
     * @return bool
     */
    abstract public function getSupportForAttributeResizing(): bool;

    /**
     * Is get connection id supported?
     *
     * @return bool
     */
    abstract public function getSupportForGetConnectionId(): bool;

    /**
     * Is upserting supported?
     *
     * @return bool
     */
    abstract public function getSupportForUpserts(): bool;

    /**
     * Is Cache Fallback supported?
     *
     * @return bool
     */
    abstract public function getSupportForCacheSkipOnFailure(): bool;

    /**
     * Is reconnection supported?
     *
     * @return bool
     */
    abstract public function getSupportForReconnection(): bool;

    /**
     * Is hostname supported?
     *
     * @return bool
     */
    abstract public function getSupportForHostname(): bool;

    /**
     * Is creating multiple attributes in a single query supported?
     *
     * @return bool
     */
    abstract public function getSupportForBatchCreateAttributes(): bool;

    /**
     * Is spatial attributes supported?
     *
     * @return bool
     */
    abstract public function getSupportForSpatialAttributes(): bool;

    /**
     * Does the adapter support null values in spatial indexes?
     *
     * @return bool
     */
    abstract public function getSupportForSpatialIndexNull(): bool;

    /**
     * Does the adapter support order attribute in spatial indexes?
     *
     * @return bool
     */
    abstract public function getSupportForSpatialIndexOrder(): bool;

    /**
     * Does the adapter includes boundary during spatial contains?
     *
     * @return bool
     */
    abstract public function getSupportForBoundaryInclusiveContains(): bool;

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    abstract public function getCountOfAttributes(Document $collection): int;

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    abstract public function getCountOfIndexes(Document $collection): int;

    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    abstract public function getCountOfDefaultAttributes(): int;

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    abstract public function getCountOfDefaultIndexes(): int;

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    abstract public function getDocumentSizeLimit(): int;

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     * Return 0 when no restrictions apply to row width
     *
     * @param Document $collection
     * @return int
     */
    abstract public function getAttributeWidth(Document $collection): int;

    /**
     * Get list of keywords that cannot be used
     *
     * @return array<string>
     */
    abstract public function getKeywords(): array;

    /**
     * @param array<Query> $selects
     * @return string
     */
    abstract protected function getAttributeProjection(array $selects): string;

    /**
     * Filter Keys
     *
     * @param string $value
     * @return string
     * @throws DatabaseException
     */
    public function filter(string $value): string
    {
        $value = \preg_replace("/[^A-Za-z0-9_\-]/", '', $value);

        if (\is_null($value)) {
            throw new DatabaseException('Failed to filter key');
        }

        return $value;
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
            '|'
        ];

        foreach ($wildcards as $wildcard) {
            $value = \str_replace($wildcard, "\\$wildcard", $value);
        }

        return $value;
    }

    /**
     * Increase or decrease attribute value
     *
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param string $updatedAt
     * @param int|float|null $min
     * @param int|float|null $max
     * @return bool
     * @throws Exception
     */
    abstract public function increaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float $value,
        string $updatedAt,
        int|float|null $min = null,
        int|float|null $max = null
    ): bool;

    /**
     * Returns the connection ID identifier
     *
     * @return string
     */
    abstract public function getConnectionId(): string;

    /**
     * Get List of internal index keys names
     *
     * @return array<string>
     */
    abstract public function getInternalIndexesKeys(): array;

    /**
     * Get Schema Attributes
     *
     * @param string $collection
     * @return array<Document>
     * @throws DatabaseException
     */
    abstract public function getSchemaAttributes(string $collection): array;

    /**
     * Get the query to check for tenant when in shared tables mode
     *
     * @param string $collection   The collection being queried
     * @param string $alias  The alias of the parent collection if in a subquery
     * @return string
     */
    abstract public function getTenantQuery(string $collection, string $alias = ''): string;

    /**
     * @param mixed $stmt
     * @return bool
     */
    abstract protected function execute(mixed $stmt): bool;
}
