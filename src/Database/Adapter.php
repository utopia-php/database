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
use Utopia\Database\Hook\Write;
use Utopia\Database\Validator\Authorization;

abstract class Adapter implements Feature\Attributes, Feature\Collections, Feature\Databases, Feature\Documents, Feature\Indexes, Feature\Transactions
{
    protected string $database = '';

    protected string $hostname = '';

    protected string $namespace = '';

    protected bool $sharedTables = false;

    protected ?int $tenant = null;

    protected bool $tenantPerDocument = false;

    protected int $timeout = 0;

    protected int $inTransaction = 0;

    protected bool $alterLocks = false;

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
     * @var list<Write>
     */
    protected array $writeHooks = [];

    protected Authorization $authorization;

    /**
     * Check if this adapter supports a given capability.
     *
     * @param  Capability  $feature  Capability enum case
     */
    public function supports(Capability $feature): bool
    {
        return \in_array($feature, $this->capabilities(), true);
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

    public function addWriteHook(Write $hook): static
    {
        $this->writeHooks[] = $hook;

        return $this;
    }

    public function removeWriteHook(string $class): static
    {
        $this->writeHooks = \array_values(\array_filter(
            $this->writeHooks,
            fn (Write $h) => ! ($h instanceof $class)
        ));

        return $this;
    }

    /**
     * @return list<Write>
     */
    public function getWriteHooks(): array
    {
        return $this->writeHooks;
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
     * @return array<string, mixed>
     */
    protected function documentMetadata(Document $document): array
    {
        return ['id' => $document->getId(), 'tenant' => $document->getTenant()];
    }

    /**
     * @return $this
     */
    public function setAuthorization(Authorization $authorization): self
    {
        $this->authorization = $authorization;

        return $this;
    }

    public function getAuthorization(): Authorization
    {
        return $this->authorization;
    }

    /**
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
    public function setTenant(?int $tenant): bool
    {
        $this->tenant = $tenant;

        return true;
    }

    /**
     * Get Tenant.
     *
     * Get tenant to use for shared tables
     */
    public function getTenant(): ?int
    {
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
     * Set metadata for query comments
     *
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
            return $output.$query;
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

    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        $this->timeout = $milliseconds;
    }

    public function getTimeout(): int
    {
        return $this->timeout;
    }

    /**
     * Clears a global timeout for database queries.
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
                try {
                    $this->rollbackTransaction();
                } catch (Throwable $rollback) {
                    if ($attempts < $retries) {
                        \usleep($sleep * ($attempts + 1));

                        continue;
                    }

                    $this->inTransaction = 0;
                    throw $rollback;
                }

                if (
                    $action instanceof DuplicateException ||
                    $action instanceof RestrictedException ||
                    $action instanceof AuthorizationException ||
                    $action instanceof RelationshipException ||
                    $action instanceof ConflictException ||
                    $action instanceof LimitException
                ) {
                    throw $action;
                }

                if ($attempts < $retries) {
                    \usleep($sleep * ($attempts + 1));

                    continue;
                }

                throw $action;
            }
        }

        throw new TransactionException('Failed to execute transaction');
    }

    /**
     * Apply a transformation to a query before an event occurs
     */
    public function before(string $event, string $name = '', ?callable $callback = null): static
    {
        if (! isset($this->transformations[$event])) {
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
     */
    abstract protected function quote(string $string): string;

    /**
     * Ping Database
     */
    abstract public function ping(): bool;

    /**
     * Reconnect Database
     */
    abstract public function reconnect(): void;

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

    public function createRelationship(Relationship $relationship): bool
    {
        return true;
    }

    public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        return true;
    }

    public function deleteRelationship(Relationship $relationship): bool
    {
        return true;
    }

    /**
     * Rename Index
     */
    abstract public function renameIndex(string $collection, string $old, string $new): bool;

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
     * Get Document
     *
     * @param  array<Query>  $queries
     */
    abstract public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document;

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
     * @param  array<Change>  $changes
     * @return array<Document>
     */
    public function upsertDocuments(
        Document $collection,
        string $attribute,
        array $changes
    ): array {
        return [];
    }

    /**
     * @param  array<Document>  $documents
     * @return array<Document>
     */
    abstract public function getSequences(string $collection, array $documents): array;

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
     * @param  array<string>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<Document>
     */
    abstract public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = CursorDirection::After->value, string $forPermission = PermissionType::Read->value): array;

    /**
     * Sum an attribute
     *
     * @param  array<Query>  $queries
     */
    abstract public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int;

    /**
     * Count Documents
     *
     * @param  array<Query>  $queries
     */
    abstract public function count(Document $collection, array $queries = [], ?int $max = null): int;

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
     * Get max STRING limit
     */
    abstract public function getLimitForString(): int;

    /**
     * Get max INT limit
     */
    abstract public function getLimitForInt(): int;

    /**
     * Get maximum attributes limit.
     */
    abstract public function getLimitForAttributes(): int;

    /**
     * Get maximum index limit.
     */
    abstract public function getLimitForIndexes(): int;

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
     * Get the primitive type of the primary key type for this adapter
     */
    abstract public function getIdAttributeType(): string;

    /**
     * Get the maximum supported DateTime value
     */
    public function getMaxDateTime(): DateTime
    {
        return new DateTime('9999-12-31 23:59:59');
    }

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
     * Get list of keywords that cannot be used
     *
     * @return array<string>
     */
    abstract public function getKeywords(): array;

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
            if ($query->getMethod() === Query::TYPE_SELECT) {
                foreach ($query->getValues() as $value) {
                    $selections[] = $value;
                }
            }
        }

        return $selections;
    }

    /**
     * Filter Keys
     *
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
            '|',
        ];

        foreach ($wildcards as $wildcard) {
            $value = \str_replace($wildcard, "\\$wildcard", $value);
        }

        return $value;
    }

    /**
     * Increase or decrease attribute value
     *
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

    public function getConnectionId(): string
    {
        return '';
    }

    /**
     * Get List of internal index keys names
     *
     * @return array<string>
     */
    abstract public function getInternalIndexesKeys(): array;

    /**
     * @return array<Document>
     */
    public function getSchemaAttributes(string $collection): array
    {
        return [];
    }

    /**
     * Get the expected column type for a given attribute type.
     *
     * Returns the database-native column type string (e.g. "VARCHAR(255)", "BIGINT")
     * that would be used when creating a column for the given attribute parameters.
     * Returns an empty string if the adapter does not support this operation.
     *
     * @throws DatabaseException For unknown types on adapters that support column-type resolution.
     */
    public function getColumnType(string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        return '';
    }

    /**
     * Get the query to check for tenant when in shared tables mode
     *
     * @param  string  $collection  The collection being queried
     * @param  string  $alias  The alias of the parent collection if in a subquery
     */
    abstract public function getTenantQuery(string $collection, string $alias = ''): string;

    abstract protected function execute(mixed $stmt): bool;

    public function castingBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function castingAfter(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function setUTCDatetime(string $value): mixed
    {
        return $value;
    }

    /**
     * Set support for attributes
     */
    abstract public function setSupportForAttributes(bool $support): bool;

    /**
     * @return $this
     */
    public function enableAlterLocks(bool $enable): self
    {
        $this->alterLocks = $enable;

        return $this;
    }

    /**
     * Handle non utf characters supported?
     */
    public function getSupportNonUtfCharacters(): bool
    {
        return false;
    }
}
