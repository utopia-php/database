<?php

namespace Utopia\Database\Adapter;

use DateTime as NativeDateTime;
use DateTimeZone;
use Exception;
use MongoDB\BSON\Int64;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use stdClass;
use Swoole\Coroutine;
use Throwable;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Exception\Unconfirmed as UnconfirmedException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Hook\Mongo\PermissionFilter as MongoPermissionFilter;
use Utopia\Database\Hook\Mongo\TenantFilter as MongoTenantFilter;
use Utopia\Database\Hook\Read;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Storage;
use Utopia\Database\Validator\BigInt;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Mongo\UnsentException;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * Database adapter for MongoDB, using the Utopia Mongo client for document-based storage.
 */
class Mongo extends Adapter implements Feature\Casting, Feature\Connection, Feature\Relationships, Feature\Schemaless, Feature\Timeouts, Feature\Upserts
{
    use Timeout;

    /**
     * @var array<string>
     */
    private array $operators = [
        '$eq',
        '$ne',
        '$lt',
        '$lte',
        '$gt',
        '$gte',
        '$in',
        '$nin',
        '$text',
        '$search',
        '$or',
        '$and',
        '$match',
        '$regex',
        '$not',
        '$nor',
        '$exists',
        '$elemMatch',
        '$all',
    ];

    protected Client $client;

    /**
     * @var list<Read>
     */
    protected array $readHooks = [];

    /**
     * Default batch size for cursor operations
     */
    private const DEFAULT_BATCH_SIZE = 1000;

    /**
     * The collation of the `_uid` index: a lookup or upsert by id must use it to match what the
     * index treats as the same id.
     */
    private const array UID_COLLATION = ['locale' => 'en', 'strength' => 1];

    /**
     * How many times a commit whose result is unknown is sent again after the first attempt.
     */
    private const int COMMIT_RETRIES = 3;

    /**
     * Microseconds to wait before each commit retry, multiplied by the retry number.
     */
    private const int COMMIT_RETRY_SLEEP = 50_000;

    /**
     * The write concern a commit retry must carry, per the MongoDB transactions specification.
     */
    private const array COMMIT_RETRY_WRITE_CONCERN = ['w' => 'majority', 'wtimeout' => 10_000];

    /**
     * Transaction/session state for MongoDB transactions
     *
     * @var array<mixed>|null
     */
    private ?array $session = null;

    protected int $inTransaction = 0;

    protected bool $schemaless = false;

    private const array PREFIX_SWAPPED_KEYS = ['permissions', 'createdAt', 'updatedAt', 'collection'];

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @throws MongoException
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
        $this->client->connect();
    }

    public function hostname(): string
    {
        return $this->client->getHost();
    }

    /**
     * The wire protocol has no connection id, so the client's object id names the connection: unique only within
     * the process and only while the client lives.
     */
    public function id(): string
    {
        return (string) \spl_object_id($this->client);
    }

    public function getDriver(): Client
    {
        return $this->client;
    }

    /**
     * Get the list of capabilities supported by the MongoDB adapter.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        return array_merge(parent::capabilities(), [
            Capability::Objects,
            Capability::IndexFulltext,
            Capability::IndexTtl,
            Capability::Caching,
            Capability::Operators,
            Capability::TransactionRetries,
        ]);
    }

    /**
     * Set the maximum execution time for queries.
     *
     * @param int $milliseconds Timeout in milliseconds
     * @param Event $event The event scope for the timeout
     * @return void
     */
    #[\Override]
    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        $this->timeout = $milliseconds;
    }

    /**
     * Clear the query execution timeout.
     *
     * @param Event $event The event scope to clear
     * @return void
     */
    #[\Override]
    public function clearTimeout(Event $event = Event::All): void
    {
        $this->timeout = 0;
    }

    public function setSchemaless(bool $schemaless): static
    {
        $this->schemaless = $schemaless;

        return $this;
    }

    public function isSchemaless(): bool
    {
        return $this->schemaless;
    }

    public function supports(Capability $capability): bool
    {
        if ($capability === Capability::DefinedAttributes) {
            return ! $this->schemaless;
        }

        return parent::supports($capability);
    }

    protected function syncWriteHooks(): void
    {
    }

    protected function syncReadHooks(): void
    {
        $this->readHooks = [new MongoPermissionFilter($this->authorization)];
    }

    /**
     * @param  array<string, mixed>  $filters
     * @return array<string, mixed>
     */
    protected function applyTenantFilter(array $filters, string $collection): array
    {
        $tenantFilter = new MongoTenantFilter(
            $this->sharedTables,
            $this->getTenantFilters(...),
        );

        return $tenantFilter->applyFilters($filters, $collection);
    }

    /**
     * @param  array<string, mixed>  $filters
     * @return array<string, mixed>
     */
    protected function applyReadFilters(array $filters, string $collection, PermissionType $forPermission): array
    {
        $filters = $this->applyTenantFilter($filters, $collection);

        $this->syncReadHooks();
        foreach ($this->readHooks as $hook) {
            $filters = $hook->applyFilters($filters, $collection, $forPermission);
        }

        return $filters;
    }

    /**
     * @throws Exception
     * @throws MongoException
     */
    public function ping(): bool
    {
        /** @var \stdClass|array<string, mixed>|int $result */
        $result = $this->getClient()->query([
            'ping' => 1,
            'skipReadConcern' => true,
        ]);

        if ($result instanceof \stdClass && isset($result->ok)) {
            return (bool) $result->ok;
        }

        return false;
    }

    public function reconnect(): void
    {
        $this->client->connect();
    }

    /**
     * @throws Exception
     */
    protected function getClient(): Client
    {
        return $this->client;
    }

    /**
     * Start a new database transaction or increment the nesting counter. A standalone server has no transactions.
     *
     * @return bool
     *
     * @throws DatabaseException If the transaction cannot be started.
     */
    public function startTransaction(): bool
    {
        if (! $this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0 && ! $this->session) {
                $this->session = $this->client->startSession();
                $this->client->startTransaction($this->session);
            }
            $this->inTransaction++;

            return true;
        } catch (Throwable $e) {
            $this->session = null;
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to start transaction: '.$e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Commit the current database transaction or decrement the nesting counter.
     *
     * @return bool
     *
     * @throws UnconfirmedException If the commit was sent but its result could not be confirmed.
     * @throws DatabaseException If the transaction cannot be committed, with an `Exception\Transaction` cause when
     *                           the server reports it aborted.
     */
    public function commitTransaction(): bool
    {
        if (! $this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0) {
                return false;
            }
            $this->inTransaction--;
            if ($this->inTransaction > 0) {
                return true;
            }
            if (! $this->session) {
                return false;
            }

            try {
                $this->commit($this->session);
            } finally {
                $this->endSession();
            }

            return true;
        } catch (Throwable $error) {
            $this->endSession();
            $this->inTransaction = 0;

            if ($error instanceof UnconfirmedException) {
                throw $error;
            }

            throw new DatabaseException('Failed to commit transaction: '.$error->getMessage(), $error->getCode(), $error);
        }
    }

    /**
     * Commit the session's transaction. When the result of the commit is unknown, only the commit is sent again.
     *
     * @param  array<mixed>  $session
     *
     * @throws TransactionException If the server reports the transaction aborted, so nothing of it is stored.
     * @throws UnconfirmedException If the commit was sent but its result could not be confirmed.
     * @throws Throwable
     */
    private function commit(array $session): void
    {
        try {
            $this->client->commitTransaction($session);
        } catch (Throwable $error) {
            if ($this->isUnknownCommitResult($error)) {
                $this->retryCommit($session, $error);

                return;
            }

            if (! $error instanceof MongoException) {
                throw new DatabaseException($error->getMessage(), $error->getCode(), $error);
            }

            throw $this->processException($error);
        }
    }

    /**
     * Send the commit again, up to COMMIT_RETRIES times after the first attempt, with a majority write concern so a
     * commit that already applied is reported as applied. A retry that was never sent is sent again.
     *
     * @param  array<mixed>  $session
     *
     * @throws TransactionException If the server reports the transaction aborted, so nothing of it is stored.
     * @throws UnconfirmedException If the commit still cannot be confirmed.
     */
    private function retryCommit(array $session, Throwable $unknown): void
    {
        for ($retry = 1; $retry <= self::COMMIT_RETRIES; $retry++) {
            $this->pause(self::COMMIT_RETRY_SLEEP * $retry);

            try {
                $this->client->commitTransaction($session, ['writeConcern' => self::COMMIT_RETRY_WRITE_CONCERN]);

                return;
            } catch (Throwable $error) {
                if ($error instanceof UnsentException || $this->isUnknownCommitResult($error)) {
                    continue;
                }

                if ($this->isAbortedCommit($error)) {
                    throw new TransactionException('The transaction was aborted while its commit was retried', previous: $error);
                }

                break;
            }
        }

        throw new UnconfirmedException('Failed to commit transaction: the commit could not be confirmed', previous: $unknown);
    }

    private function pause(int $microseconds): void
    {
        if (\extension_loaded('swoole') && Coroutine::getCid() > 0) {
            Coroutine::sleep($microseconds / 1_000_000);

            return;
        }

        \usleep($microseconds);
    }

    /**
     * Whether the commit reached the server but its result is unknown: the commit may have applied, so running the
     * transaction again could apply it twice. A commit that was never sent, or that the server labels transient,
     * applied nothing.
     */
    private function isUnknownCommitResult(Throwable $error): bool
    {
        if (! $error instanceof MongoException || $error instanceof UnsentException) {
            return false;
        }

        $labels = $error->getErrorLabels();
        if (\in_array(Client::TRANSIENT_TRANSACTION_ERROR, $labels, true)) {
            return false;
        }

        return \in_array(Client::UNKNOWN_TRANSACTION_COMMIT_RESULT, $labels, true)
            || $error->isNetworkError()
            || $this->client->isUnknownTransactionCommitResult($error);
    }

    private function isAbortedCommit(Throwable $error): bool
    {
        return $error instanceof MongoException
            && (
                \in_array(Client::TRANSIENT_TRANSACTION_ERROR, $error->getErrorLabels(), true)
                || $this->processException($error) instanceof TransactionException
            );
    }

    private function endSession(): void
    {
        if ($this->session !== null) {
            try {
                $this->client->endSessions([$this->session]);
            } catch (Throwable) {
                // Best effort: a dropped connection fails this, and that must not replace the outcome.
            }
        }
        $this->session = null;
    }

    /**
     * Roll back the current database transaction or decrement the nesting counter. A transaction the server already
     * aborted counts as rolled back.
     *
     * @return bool
     *
     * @throws DatabaseException If the rollback fails.
     */
    public function rollbackTransaction(): bool
    {
        if (! $this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0) {
                return false;
            }
            $this->inTransaction--;
            if ($this->inTransaction > 0) {
                return true;
            }
            if (! $this->session) {
                return false;
            }

            try {
                $this->client->abortTransaction($this->session);
            } catch (Throwable $e) {
                $e = $this->processException($e);
                if (! $e instanceof TransactionException) {
                    throw $e;
                }
            } finally {
                $this->endSession();
            }

            return true;
        } catch (Throwable $e) {
            $this->endSession();
            $this->inTransaction = 0;

            throw new DatabaseException('Failed to rollback transaction: '.$e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Run the callback in a transaction, retrying an attempt that failed transiently up to twice. Without savepoints
     * a call nested in an open transaction runs the callback in it, and a standalone server runs it without one.
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
        if (! $this->client->isReplicaSet() || $this->inTransaction > 0) {
            return $callback();
        }

        // An upsert with $setOnInsert hits WriteConflict (112) under the transaction's snapshot isolation.
        if ($this->skippingDuplicates()) {
            return $callback();
        }

        $sleep = 50_000;
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
                } catch (Throwable) {
                    // The attempt's failure is the one retried or thrown.
                } finally {
                    $this->endSession();
                    $this->inTransaction = 0;
                }

                if (! parent::isRetryable($action)) {
                    throw $action;
                }

                if ($attempts < $retries) {
                    \usleep($sleep * ($attempts + 1));

                    continue;
                }

                throw $action;
            }
        }

        throw new TransactionException('Transaction retry loop exited unexpectedly');
    }

    /**
     * A standalone server has no transactions, so withTransaction() runs the callback once and retries nothing. The
     * failure is classified first, so a failure that is never retried needs no round trip to a server that may be
     * gone.
     */
    #[\Override]
    public function isRetryable(Throwable $failure): bool
    {
        return parent::isRetryable($failure) && $this->client->isReplicaSet();
    }

    /**
     * A MongoDB error is transient when the server labels it so, when it is a network error, when the command was
     * never sent, or when the adapter maps it to an aborted transaction.
     */
    #[\Override]
    protected function isTransient(Throwable $error): bool
    {
        if (
            $error instanceof MongoException
            && (
                $error instanceof UnsentException
                || $error->isTransientError()
                || $this->processException($error) instanceof TransactionException
            )
        ) {
            return true;
        }

        return parent::isTransient($error);
    }

    /**
     * Create Database
     */
    public function create(string $name): bool
    {
        return true;
    }

    /**
     * Moves every collection into the new database with `renameCollection`, then drops the emptied one. A failure
     * part way moves the collections already moved back, newest first, and is thrown. A sharded cluster cannot move
     * a collection between databases, so it refuses the rename, as do shared tables, whose database other tenants
     * share. The client stays bound to the database it was built for: address the renamed one with a client built
     * for it.
     *
     * @throws DatabaseException
     */
    public function update(string $name, string $new): bool
    {
        if ($this->hasSharedTables()) {
            throw new DatabaseException('Cannot rename a database while shared tables are enabled');
        }

        $name = $this->filter($name);
        $new = $this->filter($new);
        $client = $this->getClient();

        /** @var stdClass $hello */
        $hello = $client->query(['hello' => 1], 'admin');
        if (($hello->msg ?? null) === 'isdbgrid') {
            throw new DatabaseException('Renaming a database is not supported on a sharded MongoDB cluster');
        }

        $databases = $this->getDatabaseNames();

        if (! \in_array($name, $databases, true)) {
            throw new NotFoundException('Database not found');
        }

        if (\in_array($new, $databases, true)) {
            throw new DuplicateException('Database already exists');
        }

        $moved = [];
        try {
            foreach ($this->getCollectionNames($name) as $collection) {
                $client->query(['renameCollection' => "{$name}.{$collection}", 'to' => "{$new}.{$collection}"], 'admin');
                $moved[] = $collection;
            }
        } catch (Throwable $error) {
            foreach (\array_reverse($moved) as $collection) {
                $client->query(['renameCollection' => "{$new}.{$collection}", 'to' => "{$name}.{$collection}"], 'admin');
            }

            throw $error instanceof MongoException ? $this->processException($error) : $error;
        }

        $client->dropDatabase([], $name);

        return true;
    }

    /**
     * The collections of a database a rename moves: every one but the server's own.
     *
     * @return list<string>
     *
     * @throws DatabaseException When the server pages the listing, which a rename cannot move in one pass
     */
    private function getCollectionNames(string $database): array
    {
        /** @var stdClass $listed */
        $listed = $this->getClient()->query(['listCollections' => 1, 'nameOnly' => true], $database);
        /** @var stdClass $cursor */
        $cursor = $listed->cursor;
        if (! empty($cursor->id)) {
            throw new DatabaseException('Database has more collections than one listing returns, so it cannot be renamed');
        }

        /** @var array<stdClass> $collections */
        $collections = $cursor->firstBatch ?? [];
        $names = [];
        foreach ($collections as $collection) {
            $collectionName = $collection->name ?? null;
            if (\is_string($collectionName) && ! \str_starts_with($collectionName, 'system.')) {
                $names[] = $collectionName;
            }
        }

        return $names;
    }

    /**
     * @return list<string>
     */
    private function getDatabaseNames(): array
    {
        /** @var stdClass $listed */
        $listed = $this->getClient()->listDatabaseNames();
        /** @var array<stdClass> $databases */
        $databases = $listed->databases ?? [];

        $names = [];
        foreach ($databases as $database) {
            $databaseName = $database->name ?? null;
            if (\is_string($databaseName)) {
                $names[] = $databaseName;
            }
        }

        return $names;
    }

    /**
     * MongoDB creates a database on its first write, so only a database holding data is listed and exists.
     *
     * @throws Exception
     */
    public function exists(string $database): bool
    {
        return \in_array($this->filter($database), $this->getDatabaseNames(), true);
    }

    /**
     * An empty database name asks the database the client was built for.
     */
    public function collectionExists(string $database, string $collection): bool
    {
        $database = $this->filter($database);

        try {
            /** @var \stdClass $result */
            $result = $this->getClient()->query([
                'listCollections' => 1,
                'filter' => ['name' => $this->getNamespace().'_'.$this->filter($collection)],
            ], $database === '' ? null : $database);

            /** @var \stdClass $cursor */
            $cursor = $result->cursor;
            /** @var array<mixed> $firstBatch */
            $firstBatch = $cursor->firstBatch;

            return ! empty($firstBatch);
        } catch (Exception) {
            return false;
        }
    }

    /**
     * List Databases
     *
     * @return array<Document>
     *
     * @throws Exception
     */
    public function list(): array
    {
        /** @var array<Document> $list */
        $list = [];

        /** @var \stdClass $databaseNames */
        $databaseNames = $this->getClient()->listDatabaseNames();
        /** @var array<Document> $databaseNamesArray */
        $databaseNamesArray = (array) $databaseNames;
        foreach ($databaseNamesArray as $value) {
            $list[] = $value;
        }

        return $list;
    }

    /**
     * Delete Database
     *
     *
     * @throws Exception
     */
    public function delete(string $name): bool
    {
        $this->getClient()->dropDatabase([], $this->filter($name));

        return true;
    }

    /**
     * Create Collection
     *
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     *
     * @throws Exception
     */
    public function createCollection(string $collection, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->getNamespace().'_'.$this->filter($collection);

        // In shared-tables mode or for metadata, the physical collection may
        // already exist for another tenant. Return early to avoid a
        // "Collection Exists" exception from the client.
        if (! $this->inTransaction && ($this->hasSharedTables() || $collection === Database::METADATA) && $this->collectionExists($this->getDatabase(), $collection)) {
            return true;
        }

        try {
            $options = $this->getTransactionOptions();
            $this->getClient()->createCollection($id, $options);
        } catch (MongoException $error) {
            if (\str_contains($error->getMessage(), 'Collection Exists')) {
                return true;
            }
            $error = $this->processException($error);
            if ($error instanceof DuplicateException && ($this->hasSharedTables() || $collection === Database::METADATA)) {
                return true;
            }
            throw $error;
        }

        $internalIndex = [
            [
                'key' => [Storage::UID => $this->getOrder(OrderDirection::Asc)],
                'name' => Storage::UID,
                'unique' => true,
                'collation' => [
                    'locale' => 'en',
                    'strength' => 1,
                ],
            ],
            [
                'key' => [Storage::CREATED_AT => $this->getOrder(OrderDirection::Asc)],
                'name' => Storage::CREATED_AT,
            ],
            [
                'key' => [Storage::UPDATED_AT => $this->getOrder(OrderDirection::Asc)],
                'name' => Storage::UPDATED_AT,
            ],
            [
                'key' => [Storage::PERMISSIONS => $this->getOrder(OrderDirection::Asc)],
                'name' => Storage::PERMISSIONS,
            ],
        ];

        if ($this->sharedTables) {
            foreach ($internalIndex as &$index) {
                $index['key'] = array_merge([Storage::TENANT => $this->getOrder(OrderDirection::Asc)], $index['key']);
            }
            unset($index);
        }

        try {
            $options = $this->getTransactionOptions();
            $indexesCreated = $this->client->createIndexes($id, $internalIndex, $options);
        } catch (Exception $error) {
            throw $this->processException($error);
        }

        if (! $indexesCreated) {
            return false;
        }

        if (! empty($indexes)) {
            /**
             * Each new index has format ['key' => [$attribute => $order], 'name' => $name, 'unique' => $unique]
             */
            $newIndexes = [];

            foreach ($indexes as $indexPosition => $index) {
                $key = [];
                $unique = false;
                $indexType = $index->type;

                if ($this->shouldAddTenantToIndex($index)) {
                    $key[Storage::TENANT] = $this->getOrder(OrderDirection::Asc);
                }

                foreach ($index->attributes as $attributePosition => $attribute) {
                    $attribute = $this->filter($this->getInternalKeyForAttribute($attribute));

                    switch ($indexType) {
                        case IndexType::Key:
                        case IndexType::Ttl:
                            $order = $this->getOrder($index->orders[$attributePosition] ?? OrderDirection::Asc);
                            break;
                        case IndexType::Fulltext:
                            $order = 'text';
                            break;
                        case IndexType::Unique:
                            $order = $this->getOrder($index->orders[$attributePosition] ?? OrderDirection::Asc);
                            $unique = true;
                            break;
                        default:
                            return false;
                    }

                    $key[$attribute] = $order;
                }

                $newIndexes[$indexPosition] = [
                    'key' => $key,
                    'name' => $this->filter($index->key),
                    'unique' => $unique,
                ];

                if ($indexType === IndexType::Fulltext) {
                    $newIndexes[$indexPosition]['default_language'] = 'none';
                }

                if ($indexType === IndexType::Ttl && $index->ttl > 0) {
                    $newIndexes[$indexPosition]['expireAfterSeconds'] = $index->ttl;
                }

                if (in_array($indexType, [IndexType::Unique, IndexType::Key])) {
                    $fields = [];
                    foreach ($index->attributes as $indexedAttribute) {
                        $attributeType = ColumnType::String;
                        foreach ($attributes as $collectionAttribute) {
                            if ($collectionAttribute->key === $indexedAttribute) {
                                $attributeType = $collectionAttribute->type;
                                break;
                            }
                        }

                        $fields[$this->filter($this->getInternalKeyForAttribute($indexedAttribute))] = $attributeType;
                    }
                    if (! empty($fields)) {
                        $newIndexes[$indexPosition]['partialFilterExpression'] = $this->getPartialFilterExpression($indexType, $fields);
                    }
                }
            }

            try {
                $options = $this->getTransactionOptions();
                $indexesCreated = $this->getClient()->createIndexes($id, \array_values($newIndexes), $options);
            } catch (Exception $error) {
                throw $this->processException($error);
            }

            if (! $indexesCreated) {
                return false;
            }
        }

        return true;
    }

    /**
     * List Collections
     *
     * @return array<Document>
     *
     * @throws Exception
     */
    protected function listCollections(): array
    {
        /** @var array<Document> $list */
        $list = [];

        // Note: listCollections is a metadata operation that should not run in transactions
        // to avoid transaction conflicts and readConcern issues
        /** @var \stdClass $collectionNames */
        $collectionNames = $this->getClient()->listCollectionNames();
        /** @var array<Document> $collectionNamesArray */
        $collectionNamesArray = (array) $collectionNames;
        foreach ($collectionNamesArray as $value) {
            $list[] = $value;
        }

        return $list;
    }

    /**
     * @throws Exception
     */
    public function deleteCollection(string $collection): bool
    {
        $id = $this->getNamespace().'_'.$this->filter($collection);

        return (bool) $this->getClient()->dropCollection($id);
    }

    /**
     * Analyze a collection updating it's metadata on the database engine
     */
    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

    /**
     * Create Attribute
     */
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        return true;
    }

    /**
     * Create Attributes
     *
     * @param  list<Attribute>  $attributes
     *
     * @throws DatabaseException
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        return true;
    }

    /**
     * Update Attribute.
     */
    public function updateAttribute(string $collection, string $key, Attribute $attribute): bool
    {
        if ($attribute->key !== $key) {
            return $this->renameAttribute($collection, $key, $attribute->key);
        }

        return true;
    }

    /**
     * @throws DatabaseException
     * @throws MongoException
     */
    public function deleteAttribute(string $collection, string $key): bool
    {
        $collection = $this->getNamespace().'_'.$this->filter($collection);

        $this->getClient()->update(
            $collection,
            [],
            ['$unset' => [$this->escapeMongoFieldName($this->getInternalKeyForAttribute($key)) => '']],
            multi: true
        );

        return true;
    }

    public function getSchemaAttributes(string $collection): array
    {
        return [];
    }

    public function getSchemaIndexes(string $collection): array
    {
        return [];
    }

    public function getColumnType(Attribute $attribute): ?string
    {
        return null;
    }

    /**
     * Rename Attribute.
     *
     * @throws DatabaseException
     * @throws MongoException
     */
    public function renameAttribute(string $collection, string $id, string $name): bool
    {
        $collection = $this->getNamespace().'_'.$this->filter($collection);

        $from = $this->escapeMongoFieldName($this->getInternalKeyForAttribute($id));
        $to = $this->escapeMongoFieldName($this->getInternalKeyForAttribute($name));
        $options = $this->getTransactionOptions();

        $this->getClient()->update(
            $collection,
            [],
            ['$rename' => [$from => $to]],
            multi: true,
            options: $options
        );

        return true;
    }

    /**
     * Create a relationship between collections. No-op for MongoDB since relationships are virtual.
     */
    public function createRelationship(string $collection, Relationship $relationship): bool
    {
        return true;
    }

    /**
     * @throws DatabaseException
     * @throws MongoException
     */
    public function updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update): bool
    {
        $collectionName = $this->getNamespace().'_'.$this->filter($collection);
        $relatedCollectionName = $this->getNamespace().'_'.$this->filter($relationship->relatedCollection);
        $key = $relationship->key ?? '';
        $twoWayKey = $relationship->twoWayKey ?? '';
        $newKey = $update->key;
        $newTwoWayKey = $update->twoWayKey;
        $twoWay = $update->twoWay ?? $relationship->twoWay;

        $renameKey = [
            '$rename' => [
                $this->escapeMongoFieldName($key) => $newKey === null ? null : $this->escapeMongoFieldName($newKey),
            ],
        ];

        $renameTwoWayKey = [
            '$rename' => [
                $this->escapeMongoFieldName($twoWayKey) => $newTwoWayKey === null ? null : $this->escapeMongoFieldName($newTwoWayKey),
            ],
        ];

        switch ($relationship->type) {
            case RelationshipType::OneToOne:
                if (($twoWay || $side === RelationshipSide::Parent) && $newKey !== null && $key !== $newKey) {
                    $this->getClient()->update($collectionName, updates: $renameKey, multi: true);
                }
                if (($twoWay || $side === RelationshipSide::Child) && $newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                    $this->getClient()->update($relatedCollectionName, updates: $renameTwoWayKey, multi: true);
                }
                break;
            case RelationshipType::OneToMany:
                if ($side === RelationshipSide::Parent) {
                    if ($newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                        $this->getClient()->update($relatedCollectionName, updates: $renameTwoWayKey, multi: true);
                    }
                } elseif ($newKey !== null && $key !== $newKey) {
                    $this->getClient()->update($collectionName, updates: $renameKey, multi: true);
                }
                break;
            case RelationshipType::ManyToOne:
                if ($side === RelationshipSide::Child) {
                    if ($newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                        $this->getClient()->update($relatedCollectionName, updates: $renameTwoWayKey, multi: true);
                    }
                } elseif ($newKey !== null && $key !== $newKey) {
                    $this->getClient()->update($collectionName, updates: $renameKey, multi: true);
                }
                break;
            case RelationshipType::ManyToMany:
                $junction = $this->getJunctionName($collection, $relationship->relatedCollection, $side);

                if ($newKey !== null && $key !== $newKey) {
                    $this->getClient()->update($junction, updates: $renameKey, multi: true);
                }
                if ($newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                    $this->getClient()->update($junction, updates: $renameTwoWayKey, multi: true);
                }
                break;
        }

        return true;
    }

    /**
     * @throws MongoException
     * @throws Exception
     */
    public function deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side): bool
    {
        $collectionName = $this->getNamespace().'_'.$this->filter($collection);
        $relatedCollectionName = $this->getNamespace().'_'.$this->filter($relationship->relatedCollection);
        $escapedKey = $this->escapeMongoFieldName($relationship->key ?? '');
        $escapedTwoWayKey = $this->escapeMongoFieldName($relationship->twoWayKey ?? '');

        switch ($relationship->type) {
            case RelationshipType::OneToOne:
                if ($side === RelationshipSide::Parent) {
                    $this->getClient()->update($collectionName, [], ['$unset' => [$escapedKey => '']], multi: true);
                    if ($relationship->twoWay) {
                        $this->getClient()->update($relatedCollectionName, [], ['$unset' => [$escapedTwoWayKey => '']], multi: true);
                    }
                } else {
                    $this->getClient()->update($relatedCollectionName, [], ['$unset' => [$escapedTwoWayKey => '']], multi: true);
                    if ($relationship->twoWay) {
                        $this->getClient()->update($collectionName, [], ['$unset' => [$escapedKey => '']], multi: true);
                    }
                }
                break;
            case RelationshipType::OneToMany:
                if ($side === RelationshipSide::Parent) {
                    $this->getClient()->update($relatedCollectionName, [], ['$unset' => [$escapedTwoWayKey => '']], multi: true);
                } else {
                    $this->getClient()->update($collectionName, [], ['$unset' => [$escapedKey => '']], multi: true);
                }
                break;
            case RelationshipType::ManyToOne:
                if ($side === RelationshipSide::Parent) {
                    $this->getClient()->update($collectionName, [], ['$unset' => [$escapedKey => '']], multi: true);
                } else {
                    $this->getClient()->update($relatedCollectionName, [], ['$unset' => [$escapedTwoWayKey => '']], multi: true);
                }
                break;
            case RelationshipType::ManyToMany:
                $this->getClient()->dropCollection($this->getJunctionName($collection, $relationship->relatedCollection, $side));
                break;
        }

        return true;
    }

    /**
     * The namespaced junction collection of a many-to-many relationship, named after the parent's sequence first.
     *
     * @throws DatabaseException
     */
    private function getJunctionName(string $collection, string $relatedCollection, RelationshipSide $side): string
    {
        $metadataCollection = new Document([Document::ID => Database::METADATA]);
        $collectionDocument = $this->getDocument($metadataCollection, $collection);
        $relatedCollectionDocument = $this->getDocument($metadataCollection, $relatedCollection);

        if ($collectionDocument->isEmpty() || $relatedCollectionDocument->isEmpty()) {
            throw new DatabaseException('Collection or related collection not found');
        }

        return $side === RelationshipSide::Parent
            ? $this->getNamespace().'_'.$this->filter('_'.$collectionDocument->getSequence().'_'.$relatedCollectionDocument->getSequence())
            : $this->getNamespace().'_'.$this->filter('_'.$relatedCollectionDocument->getSequence().'_'.$collectionDocument->getSequence());
    }

    /**
     * Create Index
     *
     * @param  array<string, string>  $indexAttributeTypes
     * @param  array<string, mixed>  $collation
     *
     * @throws Exception
     */
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
    {
        $name = $this->getNamespace().'_'.$this->filter($collection);
        $id = $this->filter($index->key);
        $type = $index->type;
        $indexedAttributes = $index->attributes;
        $attributes = $indexedAttributes;
        $ttl = $index->ttl;
        /** @var array<string, mixed> $indexes */
        $indexes = [];
        $options = [];
        $indexes['name'] = $id;

        /** @var array<string, int|string> $indexKey */
        $indexKey = [];

        if ($this->shouldAddTenantToIndex($type)) {
            $indexKey[Storage::TENANT] = $this->getOrder(OrderDirection::Asc);
        }

        foreach ($attributes as $position => $attribute) {
            if (isset($indexAttributeTypes[$attribute]) && \str_contains($attribute, '.') && $indexAttributeTypes[$attribute] === ColumnType::Object->value) {
                $dottedAttributes = \explode('.', $attribute);
                $expandedAttributes = array_map(fn (string $part): string => $this->filter($part), $dottedAttributes);
                $attributes[$position] = implode('.', $expandedAttributes);
            } else {
                $attributes[$position] = $this->filter($this->getInternalKeyForAttribute($attribute));
            }

            $orderType = $this->getOrder($index->orders[$position] ?? OrderDirection::Asc);
            $indexKey[$attributes[$position]] = $orderType;

            switch ($type) {
                case IndexType::Key:
                    break;
                case IndexType::Fulltext:
                    $indexKey[$attributes[$position]] = 'text';
                    break;
                case IndexType::Unique:
                    $indexes['unique'] = true;
                    break;
                case IndexType::Ttl:
                    break;
                default:
                    return false;
            }
        }

        $indexes['key'] = $indexKey;

        /**
         * Collation
         *  1.  Moved under $indexes.
         *  2.  Updated format.
         *  3.  Avoid adding collation to fulltext index
         */
        if (! empty($collation) &&
            $type !== IndexType::Fulltext) {
            $indexes['collation'] = [
                'locale' => 'en',
                'strength' => 1,
            ];
        }

        /**
         * Text index language configuration
         * Set to 'none' to disable stop words (words like 'other', 'the', 'a', etc.)
         * This ensures all words are indexed and searchable
         */
        if ($type === IndexType::Fulltext) {
            $indexes['default_language'] = 'none';
        }

        if ($type === IndexType::Ttl && $ttl > 0) {
            $indexes['expireAfterSeconds'] = $ttl;
        }

        if (in_array($type, [IndexType::Unique, IndexType::Key])) {
            $fields = [];
            foreach ($attributes as $position => $filteredAttribute) {
                $fields[$filteredAttribute] = self::indexedColumnType($indexAttributeTypes[$indexedAttributes[$position]] ?? '');
            }
            if (! empty($fields)) {
                $indexes['partialFilterExpression'] = $this->getPartialFilterExpression($type, $fields);
            }
        }
        try {
            $result = $this->client->createIndexes($name, [$indexes], $options);

            // Wait for unique index to be fully built before returning
            // MongoDB builds indexes asynchronously, so we need to wait for completion
            // to ensure unique constraints are enforced immediately
            if ($type === IndexType::Unique) {
                $maxRetries = 10;
                $retryCount = 0;
                $baseDelay = 50000; // 50ms
                $maxDelay = 500000; // 500ms

                while ($retryCount < $maxRetries) {
                    try {
                        /** @var \stdClass $indexList */
                        $indexList = $this->client->query([
                            'listIndexes' => $name,
                        ]);

                        /** @var \stdClass $indexListCursor */
                        $indexListCursor = $indexList->cursor;
                        if (isset($indexListCursor->firstBatch)) {
                            /** @var array<mixed> $firstBatch */
                            $firstBatch = $indexListCursor->firstBatch;
                            foreach ($firstBatch as $existingIndex) {
                                $indexArray = $this->client->toArray($existingIndex);

                                if (
                                    (isset($indexArray['name']) && $indexArray['name'] === $id) &&
                                    (! isset($indexArray['buildState']) || $indexArray['buildState'] === 'ready')
                                ) {
                                    return $result;
                                }
                            }
                        }
                    } catch (Exception $error) {
                        if ($retryCount >= $maxRetries - 1) {
                            throw new DatabaseException(
                                'Timeout waiting for index creation: '.$error->getMessage(),
                                $error->getCode(),
                                $error
                            );
                        }
                    }

                    $delay = \min($baseDelay * (2 ** $retryCount), $maxDelay);
                    \usleep((int) $delay);
                    $retryCount++;
                }

                throw new DatabaseException("Index {$id} creation timed out after {$maxRetries} retries");
            }

            return $result;
        } catch (Exception $error) {
            throw $this->processException($error);
        }
    }

    /**
     * @throws Exception
     */
    public function deleteIndex(string $collection, string $key): bool
    {
        $name = $this->getNamespace().'_'.$this->filter($collection);
        $id = $this->filter($key);
        $this->getClient()->dropIndexes($name, [$id]);

        return true;
    }

    /**
     * Rename Index.
     *
     *
     * @throws Exception
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $metadataCollection = new Document([Document::ID => Database::METADATA]);
        $collectionDocument = $this->getDocument($metadataCollection, $collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $index = null;
        foreach (self::collectionIndexes($collectionDocument) as $candidate) {
            if ($candidate->key === $old) {
                $index = $candidate;
                break;
            }
        }

        $indexAttributeTypes = [];
        if ($index !== null) {
            $attributes = self::collectionAttributes($collectionDocument);
            foreach ($index->attributes as $indexed) {
                foreach ($attributes as $attribute) {
                    if ($attribute->key === $indexed) {
                        $indexAttributeTypes[$indexed] = $attribute->type->value;
                        break;
                    }
                }
            }
        }

        try {
            if ($index === null) {
                throw new DatabaseException('Index not found: '.$old);
            }
            $deleted = $this->deleteIndex($collection, $old);
            $created = $this->createIndex($collection, $index->withKey($new), $indexAttributeTypes);
        } catch (Exception $e) {
            throw $this->processException($e);
        }

        return $deleted && $created;
    }

    /**
     * Get Document
     *
     * @param  Query[]  $queries
     *
     * @throws DatabaseException
     */
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $name = $this->getNamespace().'_'.$this->filter($collection->getId());

        $filters = [Storage::UID => $id];
        $filters = $this->applyTenantFilter($filters, $collection->getId());

        $options = $this->getTransactionOptions();

        $selections = $this->getAttributeSelections($queries);
        $hasProjection = ! empty($selections) && ! \in_array('*', $selections);

        if ($hasProjection) {
            $options['projection'] = $this->getAttributeProjection($selections);
        }

        try {
            $findResponse = $this->client->find($name, $filters, $options);
            /** @var \stdClass $findCursor */
            $findCursor = $findResponse->cursor;
            /** @var array<mixed> $result */
            $result = $findCursor->firstBatch;
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        if (empty($result)) {
            return new Document([]);
        }

        /** @var array<string, mixed>|null $resultArray */
        $resultArray = $this->client->toArray($result[0]);
        $result = $this->replaceCharacters('_', '$', $resultArray ?? []);
        $document = Document::fromStorage($result);
        $document = $this->castRead($this->getReadCasts($collection), $this->supports(Capability::DefinedAttributes), $document);

        // Ensure missing relationship attributes are set to null (MongoDB doesn't store null fields)
        if (! $hasProjection) {
            $this->ensureRelationshipDefaults($collection, $document);
        }

        return $document;
    }

    /**
     * Create Document
     *
     *
     * @throws Exception
     */
    public function createDocument(Document $collection, Document $document): Document
    {
        $this->syncWriteHooks();

        $name = $this->getNamespace().'_'.$this->filter($collection->getId());

        $sequence = $document->getSequence();

        $document->removeAttribute(Document::SEQUENCE);

        /** @var array<string, mixed> $documentArray */
        $documentArray = (array) $document;
        $record = $this->replaceCharacters('$', '_', $documentArray);
        $record = $this->decorateRow($record, $document);

        // Insert manual id if set
        if (! empty($sequence)) {
            $record[Storage::SEQUENCE] = $sequence;
        }
        $options = $this->getTransactionOptions();
        $result = $this->insertDocument($name, $this->removeNullKeys($record), $options);
        $result = $this->replaceCharacters('_', '$', $result);
        // in order to keep the original object refrence.
        foreach ($result as $key => $value) {
            $document->setAttribute($key, $value);
        }

        return $document;
    }

    /**
     * Create Documents in batches
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DuplicateException
     * @throws DatabaseException
     */
    public function createDocuments(Document $collection, array $documents): array
    {
        $this->syncWriteHooks();

        $name = $this->getNamespace().'_'.$this->filter($collection->getId());

        $options = $this->getTransactionOptions();
        $records = [];
        $hasSequence = null;
        $documents = \array_values(\array_map(fn ($doc) => clone $doc, $documents));

        foreach ($documents as $document) {
            $sequence = $document->getSequence();

            if ($hasSequence === null) {
                $hasSequence = ! empty($sequence);
            } elseif ($hasSequence == empty($sequence)) {
                throw new DatabaseException('All documents must have an sequence if one is set');
            }

            /** @var array<string, mixed> $documentArr */
            $documentArr = (array) $document;
            $record = $this->replaceCharacters('$', '_', $documentArr);
            $record = $this->decorateRow($record, $document);

            if (! empty($sequence)) {
                $record[Storage::SEQUENCE] = $sequence;
            }

            $records[] = $record;
        }

        // insertMany aborts the txn on any duplicate; upsert + $setOnInsert no-ops instead.
        if ($this->skippingDuplicates()) {
            if (empty($records)) {
                return [];
            }

            $provided = [];
            $sequences = [];
            $updates = [];
            foreach ($records as $index => $record) {
                if (isset($record[Storage::SEQUENCE])) {
                    $provided[] = $record[Storage::SEQUENCE];
                } else {
                    $record[Storage::SEQUENCE] = $this->client->createUuid();
                }
                $sequences[$index] = $record[Storage::SEQUENCE];

                $filter = [Storage::UID => $record[Storage::UID] ?? ''];
                if ($this->sharedTables) {
                    $filter[Storage::TENANT] = $record[Storage::TENANT] ?? $this->getTenant();
                }

                // Filter fields can't reappear in $setOnInsert (mongo path-conflict error).
                $setOnInsert = $record;
                unset($setOnInsert[Storage::UID], $setOnInsert[Storage::TENANT]);

                $updates[] = [
                    'q' => $filter,
                    'u' => $this->client->toObject(['$setOnInsert' => $setOnInsert]),
                    'upsert' => true,
                    'multi' => false,
                    'collation' => self::UID_COLLATION,
                ];
            }

            $stored = $provided === [] ? [] : $this->storedSequences($name, $provided, $options);

            try {
                $this->client->query(\array_merge(['update' => $name, 'updates' => $updates], $options));
            } catch (MongoException $e) {
                throw $this->processException($e);
            }

            $inserted = \array_diff_key($this->storedSequences($name, \array_values($sequences), $options), $stored);

            $created = [];
            foreach ($sequences as $index => $sequence) {
                $key = $this->stringifyIdentifier($sequence);
                if (isset($inserted[$key])) {
                    unset($inserted[$key]);
                    $created[] = $documents[$index];
                }
            }

            return $created;
        }

        try {
            $documents = $this->client->insertMany($name, $records, $options);
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        foreach ($documents as $index => $document) {
            /** @var array<string, mixed> $toArrayResult */
            $toArrayResult = $this->client->toArray($document) ?? [];
            $documents[$index] = $this->replaceCharacters('_', '$', $toArrayResult);
            $documents[$index] = new Document($documents[$index]);
        }

        return $documents;
    }

    /**
     * Update Document
     *
     * @throws DuplicateException
     * @throws DatabaseException
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        $name = $this->getNamespace().'_'.$this->filter($collection->getId());

        $record = $document->getArrayCopy();
        $record = $this->replaceCharacters('$', '_', $record);

        $filters = [Storage::UID => $id];
        $filters = $this->applyTenantFilter($filters, $collection->getId());

        try {
            unset($record[Storage::SEQUENCE]); // Don't update _id

            $options = $this->getTransactionOptions();

            $pipeline = $this->buildOperatorPipeline($record);
            if ($pipeline !== null) {
                $updated = $this->updateWithPipeline($name, $filters, $pipeline, $options);
            } else {
                $updateQuery = [
                    '$set' => $record,
                ];
                $updated = $this->client->update($name, $filters, $updateQuery, $options);
            }
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        return $document;
    }

    /**
     * Update documents
     *
     * Updates all documents which match the given query.
     *
     * @param  array<Document>  $documents
     * @param  array<string, true>  $skipPermissions
     *
     * @throws DatabaseException
     */
    public function updateDocuments(Document $collection, Document $updates, array $documents, array $skipPermissions = []): int
    {
        $name = $this->getNamespace().'_'.$this->filter($collection->getId());

        $options = $this->getTransactionOptions();
        $queries = [
            Query::equal(Document::SEQUENCE, \array_map(fn ($document) => $document->getSequence(), $documents)),
        ];

        /** @var array<string, mixed> $filters */
        $filters = $this->buildFilters($queries);
        $filters = $this->applyTenantFilter($filters, $collection->getId());

        $record = $updates->getArrayCopy();
        $record = $this->replaceCharacters('$', '_', $record);

        try {
            $pipeline = $this->buildOperatorPipeline($record);
            if ($pipeline !== null) {
                return $this->updateWithPipeline($name, $filters, $pipeline, $options, multi: true);
            }

            $updateQuery = [
                '$set' => $record,
            ];

            return $this->client->update(
                $name,
                $filters,
                $updateQuery,
                $options,
                multi: true,
            );
        } catch (MongoException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Build an aggregation pipeline update from a record containing operators.
     *
     * @param  array<string, mixed>  $record
     * @return array{0: array{'$set': array<string, mixed>}}|null
     *
     * @throws DatabaseException
     */
    private function buildOperatorPipeline(array $record): ?array
    {
        $hasOperators = false;
        foreach ($record as $value) {
            if ($value instanceof Operator) {
                $hasOperators = true;

                break;
            }
        }

        if (! $hasOperators) {
            return null;
        }

        $set = [];
        foreach ($record as $key => $value) {
            $set[$key] = $value instanceof Operator
                ? $this->getOperatorExpression($value, $key)
                : ['$literal' => $value];
        }

        return [['$set' => $set]];
    }

    /**
     * @param  array<string, mixed>  $filters
     * @param  array<int, array<string, mixed>>  $pipeline
     * @param  array<string, mixed>  $options
     *
     * @throws MongoException
     */
    private function updateWithPipeline(
        string $collection,
        array $filters,
        array $pipeline,
        array $options = [],
        bool $multi = false,
    ): int {
        $command = [
            'update' => $collection,
            'updates' => [[
                'q' => $this->client->toObject($filters),
                'u' => $pipeline,
                'multi' => $multi,
                'upsert' => false,
            ]],
        ];

        if (isset($options['session'])) {
            $command['session'] = $options['session'];
        }

        $result = $this->client->query($command);

        return \is_int($result) ? $result : 0;
    }

    /**
     * @param  array<int, array{filter: array<string, mixed>, update: array<mixed>}>  $operations
     * @param  array<string, mixed>  $options
     *
     * @throws MongoException
     */
    private function executeUpsert(string $collection, array $operations, array $options = []): int
    {
        $updates = [];
        foreach ($operations as $operation) {
            $updates[] = [
                'q' => $this->client->toObject($operation['filter']),
                'u' => $operation['update'],
                'upsert' => true,
                'multi' => false,
            ];
        }

        $result = $this->client->query(\array_merge([
            'update' => $collection,
            'updates' => $updates,
        ], $options));

        return \is_int($result) ? $result : 0;
    }

    /**
     * @throws DatabaseException
     */
    private function getOperatorExpression(Operator $operator, string $field): mixed
    {
        $reference = '$'.$field;
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            case OperatorType::Increment:
                $expression = ['$add' => [['$ifNull' => [$reference, 0]], $values[0] ?? 1]];
                if (isset($values[1])) {
                    $expression = ['$cond' => [['$lte' => [$expression, $values[1]]], $expression, ['$ifNull' => [$reference, 0]]]];
                }

                return $expression;

            case OperatorType::Decrement:
                $expression = ['$subtract' => [['$ifNull' => [$reference, 0]], $values[0] ?? 1]];
                if (isset($values[1])) {
                    $expression = ['$cond' => [['$gte' => [$expression, $values[1]]], $expression, ['$ifNull' => [$reference, 0]]]];
                }

                return $expression;

            case OperatorType::Multiply:
                $expression = ['$multiply' => [['$ifNull' => [$reference, 0]], $values[0] ?? 1]];
                if (isset($values[1])) {
                    $expression = ['$cond' => [['$lte' => [$expression, $values[1]]], $expression, ['$ifNull' => [$reference, 0]]]];
                }

                return $expression;

            case OperatorType::Divide:
                $expression = ['$divide' => [['$ifNull' => [$reference, 0]], $values[0]]];
                if (isset($values[1])) {
                    $expression = ['$cond' => [['$gte' => [$expression, $values[1]]], $expression, ['$ifNull' => [$reference, 0]]]];
                }

                return $expression;

            case OperatorType::Modulo:
                return ['$mod' => [['$ifNull' => [$reference, 0]], $values[0]]];

            case OperatorType::Power:
                $base = ['$ifNull' => [$reference, 0]];
                $exponent = $this->getNumericOperand($values, 0, 1, $method);
                $expression = ['$pow' => [$base, $exponent]];
                if (isset($values[1])) {
                    $expression = ['$cond' => [['$lte' => [$expression, $values[1]]], $expression, $base]];
                    $guards = [];
                    if ($exponent < 0) {
                        $guards[] = ['$eq' => [$base, 0]];
                    }
                    if (\floor($exponent) != $exponent) {
                        $guards[] = ['$lt' => [$base, 0]];
                    }
                    if (! empty($guards)) {
                        $undefined = \count($guards) === 1 ? $guards[0] : ['$or' => $guards];
                        $expression = ['$cond' => [$undefined, $base, $expression]];
                    }
                }

                return $expression;

            case OperatorType::StringConcat:
                return ['$concat' => [['$ifNull' => [$reference, '']], ['$literal' => $values[0] ?? '']]];

            case OperatorType::StringReplace:
                if (($values[0] ?? '') === '') {
                    return ['$ifNull' => [$reference, '']];
                }

                return ['$replaceAll' => [
                    'input' => ['$ifNull' => [$reference, '']],
                    'find' => ['$literal' => $values[0]],
                    'replacement' => ['$literal' => $values[1] ?? ''],
                ]];

            case OperatorType::Toggle:
                return ['$not' => [['$ifNull' => [$reference, false]]]];

            case OperatorType::ArrayAppend:
                return ['$concatArrays' => [['$ifNull' => [$reference, []]], ['$literal' => \array_values($values)]]];

            case OperatorType::ArrayPrepend:
                return ['$concatArrays' => [['$literal' => \array_values($values)], ['$ifNull' => [$reference, []]]]];

            case OperatorType::ArrayInsert:
                $index = $this->getIntegerOperand($values, 0, 0, $method);
                $value = $values[1] ?? null;
                $size = ['$size' => '$$array'];
                $before = ['$cond' => [['$lte' => [$index, 0]], [], ['$slice' => ['$$array', $index]]]];
                $after = ['$cond' => [['$gte' => [$index, $size]], [], ['$slice' => ['$$array', ['$subtract' => [$index, $size]]]]]];

                return ['$let' => [
                    'vars' => ['array' => ['$ifNull' => [$reference, []]]],
                    'in' => ['$concatArrays' => [$before, ['$literal' => [$value]], $after]],
                ]];

            case OperatorType::ArrayRemove:
                return ['$filter' => [
                    'input' => ['$ifNull' => [$reference, []]],
                    'cond' => ['$ne' => ['$$this', ['$literal' => $values[0] ?? null]]],
                ]];

            case OperatorType::ArrayUnique:
                return ['$reduce' => [
                    'input' => ['$ifNull' => [$reference, []]],
                    'initialValue' => [],
                    'in' => ['$cond' => [
                        ['$in' => ['$$this', '$$value']],
                        '$$value',
                        ['$concatArrays' => ['$$value', ['$$this']]],
                    ]],
                ]];

            case OperatorType::ArrayIntersect:
                return ['$filter' => [
                    'input' => ['$ifNull' => [$reference, []]],
                    'cond' => ['$in' => ['$$this', ['$literal' => \array_values($values)]]],
                ]];

            case OperatorType::ArrayDiff:
                return ['$filter' => [
                    'input' => ['$ifNull' => [$reference, []]],
                    'cond' => ['$not' => [['$in' => ['$$this', ['$literal' => \array_values($values)]]]]],
                ]];

            case OperatorType::ArrayFilter:
                return ['$filter' => [
                    'input' => ['$ifNull' => [$reference, []]],
                    'cond' => $this->getArrayFilterCondition($this->getStringOperand($values, 0, '', $method), $values[1] ?? null),
                ]];

            case OperatorType::DateAddDays:
                return ['$dateAdd' => [
                    'startDate' => ['$ifNull' => [$reference, '$$NOW']],
                    'unit' => 'day',
                    'amount' => $this->getIntegerOperand($values, 0, 0, $method),
                ]];

            case OperatorType::DateSubDays:
                return ['$dateSubtract' => [
                    'startDate' => ['$ifNull' => [$reference, '$$NOW']],
                    'unit' => 'day',
                    'amount' => $this->getIntegerOperand($values, 0, 0, $method),
                ]];

            case OperatorType::DateSetNow:
                return '$$NOW';
        }
    }

    /**
     * @param  array<mixed>  $values
     *
     * @throws DatabaseException
     */
    private function getNumericOperand(array $values, int $offset, int|float $default, OperatorType $method): int|float
    {
        $value = $values[$offset] ?? $default;
        if (! \is_int($value) && ! \is_float($value)) {
            throw new DatabaseException('Invalid numeric operand for operator '.$method->value);
        }

        return $value;
    }

    /**
     * @param  array<mixed>  $values
     *
     * @throws DatabaseException
     */
    private function getIntegerOperand(array $values, int $offset, int $default, OperatorType $method): int
    {
        $value = $values[$offset] ?? $default;
        if (! \is_int($value)) {
            throw new DatabaseException('Invalid integer operand for operator '.$method->value);
        }

        return $value;
    }

    /**
     * @param  array<mixed>  $values
     *
     * @throws DatabaseException
     */
    private function getStringOperand(array $values, int $offset, string $default, OperatorType $method): string
    {
        $value = $values[$offset] ?? $default;
        if (! \is_string($value)) {
            throw new DatabaseException('Invalid string operand for operator '.$method->value);
        }

        return $value;
    }

    /**
     * @return array<string, mixed>
     */
    private function getArrayFilterCondition(string $condition, mixed $compare): array
    {
        $value = ['$literal' => $compare];

        return match ($condition) {
            'equal' => ['$eq' => ['$$this', $value]],
            'notEqual' => ['$ne' => ['$$this', $value]],
            'greaterThan' => ['$gt' => ['$$this', $value]],
            'greaterThanEqual' => ['$gte' => ['$$this', $value]],
            'lessThan' => ['$lt' => ['$$this', $value]],
            'lessThanEqual' => ['$lte' => ['$$this', $value]],
            'isNull' => ['$eq' => ['$$this', null]],
            'isNotNull' => ['$ne' => ['$$this', null]],
            default => ['$literal' => true],
        };
    }

    /**
     * @throws DatabaseException
     */
    public function upsertDocument(Document $collection, Change $change): Document
    {
        return $this->upsertDocuments($collection, [$change])[0];
    }

    /**
     * @param  array<Change>  $changes
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    public function upsertDocuments(Document $collection, array $changes, ?string $increase = null): array
    {
        if ($changes === []) {
            return [];
        }

        $this->syncWriteHooks();

        try {
            $name = $this->getNamespace().'_'.$this->filter($collection->getId());
            $attribute = $this->filter($increase ?? '');

            $operations = [];
            $hasPipeline = false;
            foreach ($changes as $change) {
                $document = $change->new;
                $oldDocument = $change->old;
                /** @var array<string, mixed> $attributes */
                $attributes = $document->getAttributes();
                $attributes[Storage::UID] = $document->getId();
                $attributes[Storage::CREATED_AT] = $document[Document::CREATED_AT];
                $attributes[Storage::UPDATED_AT] = $document[Document::UPDATED_AT];
                $attributes[Storage::PERMISSIONS] = $document->getPermissions();

                if (! empty($document->getSequence())) {
                    $attributes[Storage::SEQUENCE] = $document->getSequence();
                }

                $filters = [Storage::UID => $document->getId()];

                if ($this->sharedTables) {
                    $tenant = $document->getTenant() ?? $this->getTenant();
                    $attributes[Storage::TENANT] = $tenant;
                    $filters[Storage::TENANT] = $this->getTenantFilters($collection->getId(), [$tenant]);
                }

                $record = $this->replaceCharacters('$', '_', $attributes);
                $record = $this->decorateRow($record, $document);

                unset($record[Storage::SEQUENCE]); // Don't update _id

                // Get fields to unset for schemaless mode
                $unsetFields = $this->getUpsertAttributeRemovals($oldDocument, $document, $record);

                if (! empty($attribute)) {
                    // Get the attribute value before removing it from $set
                    $attributeValue = $record[$attribute] ?? 0;

                    // Remove the attribute from $set since we're incrementing it
                    // it is requierd to mimic the behaver of SQL on duplicate key update
                    unset($record[$attribute]);

                    // Also remove from unset if it was there
                    unset($unsetFields[$attribute]);

                    // Increment the specific attribute and update all other fields
                    $update = [
                        '$inc' => [$attribute => $attributeValue],
                        '$set' => $record,
                    ];

                    if (! empty($unsetFields)) {
                        $update['$unset'] = $unsetFields;
                    }
                } else {
                    $pipeline = $this->buildOperatorPipeline($record);
                    if ($pipeline !== null) {
                        $set = $pipeline[0]['$set'];
                        if (empty($document->getSequence())) {
                            $set[Storage::SEQUENCE] = ['$ifNull' => ['$' . Storage::SEQUENCE, $this->client->createUuid()]];
                        }

                        $update = [['$set' => $set]];
                        if (! empty($unsetFields)) {
                            $update[] = ['$unset' => \array_keys($unsetFields)];
                        }
                        $hasPipeline = true;
                    } else {
                        $update = [
                            '$set' => $record,
                        ];

                        if (! empty($unsetFields)) {
                            $update['$unset'] = $unsetFields;
                        }

                        if (empty($document->getSequence())) {
                            $update['$setOnInsert'] = [
                                Storage::SEQUENCE => $this->client->createUuid(),
                            ];
                        }
                    }
                }

                $operations[] = [
                    'filter' => $filters,
                    'update' => $update,
                ];
            }

            $options = $this->getTransactionOptions();

            if ($hasPipeline) {
                $this->executeUpsert($name, $operations, $options);
            } else {
                $this->client->upsert(
                    $name,
                    $operations,
                    options: $options
                );
            }
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        return \array_map(static fn (Change $change): Document => $change->new, $changes);
    }

    /**
     * Delete Document
     *
     *
     * @throws Exception
     */
    public function deleteDocument(Document $collection, string $id): bool
    {
        $collectionId = $collection->getId();
        $name = $this->getNamespace().'_'.$this->filter($collectionId);

        $filters = [Storage::UID => $id];
        $filters = $this->applyTenantFilter($filters, $collectionId);

        $options = $this->getTransactionOptions();
        $result = $this->client->delete($name, $filters, 1, [], $options);

        return (bool) $result;
    }

    /**
     * Delete Documents
     *
     * @param  array<string>  $sequences
     * @param  array<string>  $permissionIds
     *
     * @throws DatabaseException
     */
    public function deleteDocuments(Document $collection, array $sequences, array $permissionIds): int
    {
        $collectionId = $collection->getId();
        $name = $this->getNamespace().'_'.$this->filter($collectionId);

        foreach ($sequences as $index => $sequence) {
            $sequences[$index] = $sequence;
        }

        /** @var array<string, mixed> $filters */
        $filters = $this->buildFilters([new Query(Method::Equal, Storage::SEQUENCE, $sequences)]);
        $filters = $this->applyTenantFilter($filters, $collectionId);

        $filters = $this->replaceInternalIdsKeys($filters, '$', '_', $this->operators);

        $options = $this->getTransactionOptions();

        try {
            return $this->client->delete(
                collection: $name,
                filters: $filters,
                limit: 0,
                options: $options
            );
        } catch (MongoException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Increase or decrease an attribute value
     *
     * @throws DatabaseException
     * @throws MongoException
     * @throws Exception
     */
    public function increaseDocumentAttribute(Document $collection, string $id, string $attribute, int|float|string $value, string $updatedAt, int|float|string|null $min = null, int|float|string|null $max = null): bool
    {
        $collectionId = $collection->getId();
        $value = $this->normalizeAtomicNumber($value, 'value');
        $min = $min === null ? null : $this->normalizeAtomicNumber($min, 'minimum');
        $max = $max === null ? null : $this->normalizeAtomicNumber($max, 'maximum');

        $attribute = $this->filter($attribute);
        $current = ['$ifNull' => ['$'.$attribute, 0]];
        $filters = [Storage::UID => $id];
        $filters = $this->applyTenantFilter($filters, $collectionId);

        $bounds = [];
        if ($max !== null) {
            $bounds[] = ['$lte' => [$current, $max]];
        }
        if ($min !== null) {
            $bounds[] = ['$gte' => [$current, $min]];
        }
        if ($bounds !== []) {
            $filters['$expr'] = \count($bounds) === 1 ? $bounds[0] : ['$and' => $bounds];
        }

        $pipeline = [['$set' => [
            $attribute => ['$add' => [$current, $value]],
            Storage::UPDATED_AT => ['$literal' => $this->toMongoDatetime($updatedAt)],
        ]]];

        try {
            $this->updateWithPipeline(
                $this->getNamespace().'_'.$this->filter($collectionId),
                $filters,
                $pipeline,
                $this->getTransactionOptions(),
            );
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        return true;
    }

    private function normalizeAtomicNumber(int|float|string $value, string $name): int|float
    {
        if (! \is_string($value)) {
            return $value;
        }
        if (! BigInt::fitsPhpInt($value)) {
            throw new TypeException("MongoDB cannot safely apply {$name} outside the signed 64-bit integer range.");
        }

        return (int) $value;
    }

    /**
     * Find Documents
     *
     * Find data sets using chosen queries
     *
     * @param  array<Query>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<Document>
     *
     * @throws Exception
     * @throws TimeoutException
     */
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
    {
        $name = $this->getNamespace().'_'.$this->filter($collection->getId());
        $queries = array_map(fn ($query) => clone $query, $queries);

        // Escape query attribute names that contain dots and match collection attributes
        // (to distinguish from nested object paths like profile.level1.value)
        $this->escapeQueryAttributes($collection, $queries);

        /** @var array<string, mixed> $filters */
        $filters = $this->buildFilters($queries);
        $filters = $this->applyReadFilters($filters, $collection->getId(), $forPermission);

        $options = [];

        if (! \is_null($limit)) {
            $options['limit'] = $limit;
        }
        if (! \is_null($offset)) {
            $options['skip'] = $offset;
        }

        if ($this->timeout) {
            $options['maxTimeMS'] = $this->timeout;
        }

        $selections = $this->getAttributeSelections($queries);
        $hasProjection = ! empty($selections) && ! \in_array('*', $selections);
        if ($hasProjection) {
            $options['projection'] = $this->getAttributeProjection($selections);
        }

        // Add transaction context to options
        $options = $this->getTransactionOptions($options);

        $orFilters = [];
        /** @var array<string, int> $sortOptions */
        $sortOptions = [];

        foreach ($orderAttributes as $i => $originalAttribute) {
            $attribute = $this->getInternalKeyForAttribute($originalAttribute);
            $attribute = $this->filter($attribute);

            $orderType = $orderTypes[$i] ?? OrderDirection::Asc;
            $direction = $orderType;

            /** Get sort direction  ASC || DESC **/
            if ($cursorDirection === CursorDirection::Before) {
                $direction = ($direction === OrderDirection::Asc)
                    ? OrderDirection::Desc
                    : OrderDirection::Asc;
            }

            $sortOptions[$attribute] = $this->getOrder($direction);
            $options['sort'] = $sortOptions;

            /** Get operator sign  '$lt' ? '$gt' **/
            $operator = $cursorDirection === CursorDirection::After
                ? ($orderType === OrderDirection::Desc ? Method::LessThan : Method::GreaterThan)
                : ($orderType === OrderDirection::Desc ? Method::GreaterThan : Method::LessThan);

            $operator = $this->getQueryOperator($operator);

            if (! empty($cursor)) {

                $andConditions = [];
                for ($j = 0; $j < $i; $j++) {
                    $originalPrev = $orderAttributes[$j];
                    $prevAttr = $this->filter($this->getInternalKeyForAttribute($originalPrev));
                    $tmp = $cursor[$originalPrev];
                    $andConditions[] = [
                        $prevAttr => $tmp,
                    ];
                }

                $tmp = $cursor[$originalAttribute];

                if ($originalAttribute === Document::SEQUENCE) {
                    /** If there is only $sequence attribute in $orderAttributes skip Or And  operators **/
                    if (count($orderAttributes) === 1) {
                        $filters[$attribute] = [
                            $operator => $tmp,
                        ];
                        break;
                    }
                }

                $andConditions[] = [
                    $attribute => [
                        $operator => $tmp,
                    ],
                ];

                $orFilters[] = [
                    '$and' => $andConditions,
                ];
            }
        }

        if (! empty($orFilters)) {
            $filters['$or'] = $orFilters;
        }

        // Translate operators and handle time filters
        /** @var array<string, mixed> $filters */
        $filters = $this->replaceInternalIdsKeys($filters, '$', '_', $this->operators);

        $found = [];
        /** @var int|null $cursorId */
        $cursorId = null;

        try {
            // Use proper cursor iteration with reasonable batch size
            $options['batchSize'] = self::DEFAULT_BATCH_SIZE;

            $response = $this->client->find($name, $filters, $options);
            /** @var \stdClass $responseCursorFind */
            $responseCursorFind = $response->cursor;
            /** @var array<mixed> $results */
            $results = $responseCursorFind->firstBatch ?? [];
            // Process first batch
            foreach ($results as $result) {
                /** @var array<string, mixed> $resultCast */
                $resultCast = (array) $result;
                $record = $this->replaceCharacters('_', '$', $resultCast);
                /** @var array<string, mixed> $convertedRecord */
                $convertedRecord = $this->convertStdClassToArray($record);
                $found[] = Document::fromStorage($convertedRecord);
            }

            // Get cursor ID for subsequent batches
            if (isset($responseCursorFind->id)) {
                /** @var mixed $responseCursorFindId */
                $responseCursorFindId = $responseCursorFind->id;
                $cursorId = \is_int($responseCursorFindId) ? $responseCursorFindId : (\is_scalar($responseCursorFindId) ? (int) $responseCursorFindId : null);
                if ($cursorId === 0) {
                    $cursorId = null;
                }
            } else {
                $cursorId = null;
            }

            // Continue fetching with getMore
            while ($cursorId !== null) {
                $moreResponse = $this->client->getMore($cursorId, $name, self::DEFAULT_BATCH_SIZE);
                /** @var \stdClass $moreCursorFind */
                $moreCursorFind = $moreResponse->cursor;
                /** @var array<mixed> $moreResults */
                $moreResults = $moreCursorFind->nextBatch ?? [];

                if (empty($moreResults)) {
                    break;
                }

                foreach ($moreResults as $result) {
                    /** @var array<string, mixed> $resultCast */
                    $resultCast = (array) $result;
                    $record = $this->replaceCharacters('_', '$', $resultCast);
                    /** @var array<string, mixed> $convertedRecord */
                    $convertedRecord = $this->convertStdClassToArray($record);
                    $found[] = Document::fromStorage($convertedRecord);
                }

                if (isset($moreCursorFind->id)) {
                    /** @var mixed $moreCursorFindId */
                    $moreCursorFindId = $moreCursorFind->id;
                    $cursorId = \is_int($moreCursorFindId) ? $moreCursorFindId : (\is_scalar($moreCursorFindId) ? (int) $moreCursorFindId : null);
                    if ($cursorId === 0) {
                        $cursorId = null;
                    }
                } else {
                    $cursorId = null;
                }
            }
        } catch (MongoException $e) {
            throw $this->processException($e);
        } finally {
            // Ensure cursor is killed if still active to prevent resource leak
            if (isset($cursorId) && $cursorId !== 0) {
                try {
                    $this->client->query([
                        'killCursors' => $name,
                        'cursors' => [$cursorId],
                    ]);
                } catch (Exception $e) {
                    // Ignore errors during cursor cleanup
                }
            }
        }

        if ($cursorDirection === CursorDirection::Before) {
            $found = array_reverse($found);
        }

        // Ensure missing relationship attributes are set to null (MongoDB doesn't store null fields)
        if (! $hasProjection) {
            foreach ($found as $document) {
                $this->ensureRelationshipDefaults($collection, $document);
            }
        }

        return $found;
    }

    /**
     * Count Documents
     *
     * @param  array<Query>  $queries
     *
     * @throws Exception
     */
    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        $name = $this->getNamespace().'_'.$this->filter($collection->getId());

        $queries = array_map(fn ($query) => clone $query, $queries);

        // Escape query attribute names that contain dots and match collection attributes
        $this->escapeQueryAttributes($collection, $queries);

        $filters = [];
        $options = [];

        if (! \is_null($max) && $max > 0) {
            $options['limit'] = $max;
        }

        if ($this->timeout) {
            $options['maxTimeMS'] = $this->timeout;
        }

        // Build filters from queries
        /** @var array<string, mixed> $filters */
        $filters = $this->buildFilters($queries);
        $filters = $this->applyReadFilters($filters, $collection->getId(), PermissionType::Read);

        /**
         * Use MongoDB aggregation pipeline for accurate counting
         * Accuracy and Sharded Clusters
         * "On a sharded cluster, the count command when run without a query predicate can result in an inaccurate
         * count if orphaned documents exist or if a chunk migration is in progress.
         * To avoid these situations, on a sharded cluster, use the db.collection.aggregate() method"
         * https://www.mongodb.com/docs/manual/reference/command/count/#response
         **/
        $options = $this->getTransactionOptions();

        if ($this->timeout) {
            $options['maxTimeMS'] = $this->timeout;
        }

        $pipeline = [];

        // Add match stage if filters are provided
        if (! empty($filters)) {
            $pipeline[] = ['$match' => $this->client->toObject($filters)];
        }

        // Add limit stage if specified
        if (! \is_null($max) && $max > 0) {
            $pipeline[] = ['$limit' => $max];
        }

        // Use $group and $sum when limit is specified, $count when no limit
        // Note: $count stage doesn't works well with $limit in the same pipeline
        // When limit is specified, we need to use $group + $sum to count the limited documents
        if (! \is_null($max) && $max > 0) {
            // When limit is specified, use $group and $sum to count limited documents
            $pipeline[] = [
                '$group' => [
                    Storage::SEQUENCE => null,
                    'total' => ['$sum' => 1]],
            ];
        } else {
            // When no limit is passed, use $count for better performance
            $pipeline[] = [
                '$count' => 'total',
            ];
        }

        try {

            $result = $this->client->aggregate($name, $pipeline, $options);

            // Aggregation returns stdClass with cursor property containing firstBatch
            if (isset($result->cursor)) {
                /** @var \stdClass $aggCursor */
                $aggCursor = $result->cursor;
                if (! empty($aggCursor->firstBatch)) {
                    /** @var array<mixed> $aggFirstBatch */
                    $aggFirstBatch = $aggCursor->firstBatch;
                    /** @var \stdClass $firstResult */
                    $firstResult = $aggFirstBatch[0];

                    // Handle both $count and $group response formats
                    if (isset($firstResult->total)) {
                        /** @var mixed $totalVal */
                        $totalVal = $firstResult->total;
                        return \is_int($totalVal) ? $totalVal : (\is_numeric($totalVal) ? (int) $totalVal : 0);
                    }
                }
            }

            return 0;
        } catch (MongoException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Sum an attribute
     *
     * @param  array<Query>  $queries
     *
     * @throws Exception
     */
    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        $name = $this->getNamespace().'_'.$this->filter($collection->getId());

        $queries = array_map(fn ($query) => clone $query, $queries);
        $this->escapeQueryAttributes($collection, $queries);
        $field = $this->getEscapedAttributes($collection)[$attribute] ?? $attribute;

        /** @var array<string, mixed> $filters */
        $filters = $this->buildFilters($queries);
        $filters = $this->applyReadFilters($filters, $collection->getId(), PermissionType::Read);

        // using aggregation to get sum an attribute as described in
        // https://docs.mongodb.com/manual/reference/method/db.collection.aggregate/
        // Pipeline consists of stages to aggregation, so first we set $match
        // that will load only documents that matches the filters provided and passes to the next stage
        // then we set $limit (if $max is provided) so that only $max documents will be passed to the next stage
        // finally we use $group stage to sum the provided attribute that matches the given filters and max
        // We pass the $pipeline to the aggregate method, which returns a cursor, then we get
        // the array of results from the cursor, and we return the total sum of the attribute
        $pipeline = [];
        if (! empty($filters)) {
            $pipeline[] = ['$match' => $filters];
        }
        if (! empty($max)) {
            $pipeline[] = ['$limit' => $max];
        }
        $pipeline[] = [
            '$group' => [
                Storage::SEQUENCE => null,
                'total' => ['$sum' => '$'.$field],
            ],
        ];

        $options = $this->getTransactionOptions();

        if ($this->timeout) {
            $options['maxTimeMS'] = $this->timeout;
        }

        try {
            $sumResult = $this->client->aggregate($name, $pipeline, $options);
            /** @var \stdClass $sumCursor */
            $sumCursor = $sumResult->cursor;
            /** @var array<mixed> $sumFirstBatch */
            $sumFirstBatch = $sumCursor->firstBatch;
            if (empty($sumFirstBatch)) {
                return 0;
            }
            /** @var \stdClass $sumFirstResult */
            $sumFirstResult = $sumFirstBatch[0];
            if (! isset($sumFirstResult->total)) {
                return 0;
            }
            /** @var mixed $sumTotal */
            $sumTotal = $sumFirstResult->total;
            if (\is_int($sumTotal) || \is_float($sumTotal)) {
                return $sumTotal;
            }

            return \is_numeric($sumTotal) ? (int) $sumTotal : 0;
        } catch (MongoException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Get sequences for documents that were created
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DatabaseException
     * @throws MongoException
     */
    public function getSequences(Document $collection, array $documents): array
    {
        $collectionId = $collection->getId();
        $documentIds = [];
        $documentTenants = [];
        foreach ($documents as $document) {
            if (empty($document->getSequence())) {
                $documentIds[] = $document->getId();

                if ($this->sharedTables) {
                    $documentTenants[] = $document->getTenant() ?? $this->getTenant();
                }
            }
        }

        if (empty($documentIds)) {
            return $documents;
        }

        $sequences = [];
        $name = $this->getNamespace().'_'.$this->filter($collectionId);

        $filters = [Storage::UID => ['$in' => \array_values(\array_unique($documentIds))]];

        if ($this->sharedTables) {
            $filters[Storage::TENANT] = $this->getTenantFilters($collectionId, \array_values(\array_unique($documentTenants)));
        }
        try {
            // Use cursor paging for large result sets
            $options = [
                'projection' => [Storage::UID => 1, Storage::SEQUENCE => 1, Storage::TENANT => 1],
                'batchSize' => self::DEFAULT_BATCH_SIZE,
            ];

            $options = $this->getTransactionOptions($options);
            $response = $this->client->find($name, $filters, $options);
            /** @var \stdClass $responseCursor */
            $responseCursor = $response->cursor;
            /** @var array<\stdClass> $results */
            $results = $responseCursor->firstBatch ?? [];

            $this->collectSequences($results, $sequences);

            // Get cursor ID for subsequent batches
            /** @var int|null $cursorId */
            $cursorId = null;
            if (isset($responseCursor->id)) {
                /** @var mixed $rcId */
                $rcId = $responseCursor->id;
                $cursorId = \is_int($rcId) ? $rcId : (\is_scalar($rcId) ? (int) $rcId : null);
                if ($cursorId === 0) {
                    $cursorId = null;
                }
            }

            // Continue fetching with getMore
            while ($cursorId !== null) {
                $moreResponse = $this->client->getMore($cursorId, $name, self::DEFAULT_BATCH_SIZE);
                /** @var \stdClass $moreCursor */
                $moreCursor = $moreResponse->cursor;
                /** @var array<\stdClass> $moreResults */
                $moreResults = $moreCursor->nextBatch ?? [];

                if (empty($moreResults)) {
                    break;
                }

                $this->collectSequences($moreResults, $sequences);

                // Update cursor ID for next iteration
                if (isset($moreCursor->id)) {
                    /** @var mixed $moreCursorIdVal */
                    $moreCursorIdVal = $moreCursor->id;
                    $cursorId = \is_int($moreCursorIdVal) ? $moreCursorIdVal : (\is_scalar($moreCursorIdVal) ? (int) $moreCursorIdVal : null);
                    if ($cursorId === 0) {
                        $cursorId = null;
                    }
                } else {
                    $cursorId = null;
                }
            }
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        foreach ($documents as $document) {
            $tenant = $this->sharedTables ? ($document->getTenant() ?? $this->getTenant()) : null;
            $key = $this->sequenceKey($tenant, $document->getId());
            if (isset($sequences[$key])) {
                $document[Document::SEQUENCE] = $sequences[$key];
            }
        }

        return $documents;
    }

    /**
     * Which of the given `_id`s are stored. An upsert that matched a stored document leaves the `_id` it would have
     * inserted absent, so reading them back after the upserts tells which documents they inserted.
     *
     * @param  list<mixed>  $sequences
     * @param  array<string, mixed>  $options
     * @return array<string, true>
     *
     * @throws DatabaseException
     */
    private function storedSequences(string $name, array $sequences, array $options): array
    {
        try {
            $response = $this->client->find($name, [Storage::SEQUENCE => ['$in' => $sequences]], \array_merge($options, [
                'projection' => [Storage::SEQUENCE => 1],
                'batchSize' => \count($sequences) + 1,
                'singleBatch' => true,
            ]));
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        /** @var \stdClass $cursor */
        $cursor = $response->cursor;
        /** @var array<\stdClass> $rows */
        $rows = $cursor->firstBatch ?? [];

        $stored = [];
        foreach ($rows as $row) {
            $stored[$this->stringifyIdentifier($row->{Storage::SEQUENCE} ?? null)] = true;
        }

        return $stored;
    }

    /**
     * @param  array<\stdClass>  $rows
     * @param  array<string, string>  $sequences
     */
    private function collectSequences(array $rows, array &$sequences): void
    {
        foreach ($rows as $row) {
            $tenant = $this->sharedTables ? ($row->{Storage::TENANT} ?? null) : null;
            $key = $this->sequenceKey($tenant, $this->stringifyIdentifier($row->{Storage::UID} ?? null));
            $sequences[$key] = $this->stringifyIdentifier($row->{Storage::SEQUENCE} ?? null);
        }
    }

    /**
     * `_uid` is unique only per tenant under shared tables, so a batch spanning tenants
     * must match each row back to the document of the same tenant.
     */
    private function sequenceKey(mixed $tenant, string $id): string
    {
        $tenant = $tenant === null ? '' : $this->stringifyIdentifier($tenant);

        return $tenant."\0".$id;
    }

    /**
     * Collections hold any number of attributes and documents of any width, so both caps are 0.
     */
    public function limits(): Limits
    {
        return $this->limits ??= new Limits(
            string: 2147483647,
            varchar: 2147483647,
            integer: 4294967295,
            bigInteger: Database::MAX_BIG_INT,
            attributes: 0,
            indexes: 64,
            defaultAttributes: \count(Database::internalAttributesFor(true)),
            defaultIndexes: \count(Database::INTERNAL_INDEXES),
            indexLength: 1024,
            uidLength: 255,
            documentSize: 0,
            minDateTime: new NativeDateTime('-9999-01-01 00:00:00'),
            maxDateTime: new NativeDateTime(self::MAX_DATETIME),
            idType: ColumnType::Uuid7,
            keywords: [],
            internalIndexKeys: [],
        );
    }

    /**
     * Get current attribute count from collection document
     */
    public function getCountOfAttributes(Document $collection): int
    {
        return \count(self::collectionAttributes($collection)) + $this->limits()->defaultAttributes;
    }

    /**
     * Get current index count from collection document
     */
    public function getCountOfIndexes(Document $collection): int
    {
        return \count(self::collectionIndexes($collection)) + $this->limits()->defaultIndexes;
    }

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     * Return 0 when no restrictions apply to row width
     */
    public function getAttributeWidth(Document $collection): int
    {
        return 0;
    }

    /**
     * Get Collection Size of raw data
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollection(string $collection): int
    {
        $namespace = $this->getNamespace();
        $collection = $this->filter($collection);
        $collection = $namespace.'_'.$collection;

        $command = [
            'collStats' => $collection,
            'scale' => 1,
        ];

        try {
            /** @var \stdClass $result */
            $result = $this->getClient()->query($command);
            if (isset($result->totalSize)) {
                /** @var mixed $totalSizeVal */
                $totalSizeVal = $result->totalSize;
                return \is_int($totalSizeVal) ? $totalSizeVal : (\is_numeric($totalSizeVal) ? (int) $totalSizeVal : 0);
            } else {
                throw new DatabaseException('No size found');
            }
        } catch (Exception $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }
    }

    /**
     * Get Collection Size on disk
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->getSizeOfCollection($collection);
    }

    /**
     * @param  array<int|string|null>  $tenants
     * @return int|string|null|array<string, array<int|string|null>>
     */
    protected function getTenantFilters(
        string $collection,
        array $tenants = [],
    ): int|string|null|array {
        if (! $this->sharedTables) {
            return null;
        }

        /** @var array<int|string|null> $values */
        $values = [];

        if (\count($tenants) === 0) {
            $tenant = $this->getTenant();
            if ($tenant !== null) {
                $values[] = $tenant;
            }
        } else {
            for ($index = 0; $index < \count($tenants); $index++) {
                $values[] = $tenants[$index];
            }
        }

        if ($collection === Database::METADATA && !empty($values)) {
            // Include both tenant-specific and tenant-null documents for metadata collections
            // by returning the $in filter which covers tenant documents
            // (null tenant docs are accessible to all tenants for metadata)
            return ['$in' => [...$values, null]];
        }

        if (empty($values)) {
            return null;
        }

        if (\count($values) === 1) {
            return $values[0];
        }

        return ['$in' => $values];
    }

    /**
     * @throws Exception
     */
    public function castBefore(Document $collection, Document $document): Document
    {
        if ($document->isEmpty()) {
            return $document;
        }

        foreach (self::collectionAttributesWithInternal($collection) as $attribute) {
            $key = $attribute->key;
            $type = $attribute->type;
            $array = $attribute->array;

            $value = $document->getAttribute($key);
            if (is_null($value)) {
                continue;
            }

            if (Operator::isOperator($value)) {
                if ($attribute->isInteger()) {
                    /** @var Operator $value */
                    $values = $value->getValues();
                    foreach ($values as $index => $operand) {
                        if (! \is_string($operand) || ! BigInt::isIntegerString($operand)) {
                            continue;
                        }
                        if (! BigInt::fitsPhpInt($operand)) {
                            throw new TypeException('MongoDB cannot safely apply an integer operator outside the signed 64-bit range.');
                        }
                        $values[$index] = (int) $operand;
                    }
                    $value->setValues($values);
                }
                continue;
            }

            if ($array) {
                if (is_string($value)) {
                    $decoded = json_decode($value, true);
                    if (json_last_error() !== JSON_ERROR_NONE) {
                        throw new DatabaseException('Failed to decode JSON for attribute '.$key.': '.json_last_error_msg());
                    }
                    $value = $decoded;
                }
                if (!\is_array($value)) {
                    $value = [$value];
                }
            } else {
                $value = [$value];
            }

            /** @var array<mixed> $value */
            foreach ($value as $index => $node) {
                switch ($type) {
                    case ColumnType::Datetime:
                        if (! ($node instanceof UTCDateTime)) {
                            /** @var mixed $node */
                            $nodeStr = \is_string($node) ? $node : (\is_scalar($node) ? (string) $node : '');
                            if (\is_numeric($nodeStr)) {
                                $node = new UTCDateTime((int) $nodeStr);
                            } else {
                                $node = new UTCDateTime(new NativeDateTime($nodeStr));
                            }
                        }
                        break;
                    case ColumnType::Object:
                        /** @var mixed $node */
                        $nodeStr = \is_string($node) ? $node : (\is_scalar($node) ? (string) $node : '');
                        $node = json_decode($nodeStr);
                        break;
                    default:
                        break;
                }
                $value[$index] = $node;
            }
            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }

        if (! $this->supports(Capability::DefinedAttributes)) {
            foreach ($document->getArrayCopy() as $key => $value) {
                $key = (string) $key;
                if (in_array($this->getInternalKeyForAttribute($key), Database::INTERNAL_ATTRIBUTE_KEYS)) {
                    continue;
                }
                if (is_string($value) && $this->isExtendedIsoDatetime($value)) {
                    try {
                        $newValue = new UTCDateTime(new NativeDateTime($value));
                        $document->setAttribute($key, $newValue);
                    } catch (Throwable $th) {
                        // skip -> a valid string
                    }
                }
            }
        }

        return $document;
    }

    /**
     * {@inheritDoc}
     */
    public function castAfter(Document $collection, array $documents): array
    {
        $casts = $this->getReadCasts($collection);
        $defined = $this->supports(Capability::DefinedAttributes);

        foreach ($documents as $index => $document) {
            $documents[$index] = $this->castRead($casts, $defined, $document);
        }

        return $documents;
    }

    /**
     * The key, type and array flag of every collection attribute, then of every internal attribute.
     *
     * @return list<array{0: string, 1: ColumnType, 2: bool}>
     */
    private function getReadCasts(Document $collection): array
    {
        $casts = [];
        foreach (self::collectionAttributesWithInternal($collection) as $attribute) {
            $casts[] = [$attribute->key, $attribute->type, $attribute->array];
        }

        return $casts;
    }

    /**
     * @param  list<array{0: string, 1: ColumnType, 2: bool}>  $casts
     */
    private function castRead(array $casts, bool $defined, Document $document): Document
    {
        if ($document->isEmpty()) {
            return $document;
        }

        foreach ($casts as [$key, $type, $array]) {
            $stored = $document->getAttribute($key);
            if (is_null($stored)) {
                continue;
            }

            if (Operator::isOperator($stored)) {
                continue;
            }

            $value = $stored;
            if ($array) {
                if (is_string($value)) {
                    $decoded = json_decode($value, true);
                    if (json_last_error() !== JSON_ERROR_NONE) {
                        throw new DatabaseException('Failed to decode JSON for attribute '.$key.': '.json_last_error_msg());
                    }
                    $value = $decoded;
                }
                if (!\is_array($value)) {
                    $value = [$value];
                }
            } else {
                $value = [$value];
            }

            /** @var array<mixed> $value */
            foreach ($value as $index => $node) {
                $cast = match ($type) {
                    ColumnType::BigInteger, ColumnType::Integer => \is_int($node)
                        ? $node
                        : ($node instanceof Int64
                            ? (int) (string) $node
                            : (\is_numeric($node) ? (int) $node : 0)),
                    ColumnType::String, ColumnType::Id => \is_string($node) ? $node : (\is_scalar($node) ? (string) $node : $node),
                    ColumnType::Float, ColumnType::Double => \is_float($node) ? $node : (\is_numeric($node) ? (float) $node : 0.0),
                    ColumnType::Boolean => \is_scalar($node) ? (bool) $node : $node,
                    ColumnType::Datetime => $this->convertUtcDateToString($node),
                    ColumnType::Object => is_object($node) && get_class($node) === stdClass::class
                        ? $this->convertStdClassToArray($node)
                        : $node,
                    default => $node,
                };
                if ($cast !== $node) {
                    $value[$index] = $cast;
                }
            }

            $value = $array ? $value : $value[0];
            if ($value !== $stored || $key === Document::PERMISSIONS) {
                $document->setAttribute($key, $value);
            }
        }

        if (! $defined) {
            foreach ($document->getArrayCopy() as $key => $value) {
                // mongodb results out a stdclass for objects
                if (is_object($value) && get_class($value) === stdClass::class) {
                    $document->setAttribute($key, $this->convertStdClassToArray($value));
                } elseif ($value instanceof UTCDateTime) {
                    $document->setAttribute($key, $this->convertUtcDateToString($value));
                }
            }
        }

        return $document;
    }

    public function castDatetime(string $value): mixed
    {
        return new UTCDateTime(new NativeDateTime($value));
    }

    /**
     * Escape a field name for MongoDB storage.
     * MongoDB field names cannot start with $ or contain dots.
     */
    protected function escapeMongoFieldName(string $name): string
    {
        if (\str_starts_with($name, '$')) {
            $name = '_'.\substr($name, 1);
        }
        if (\str_contains($name, '.')) {
            $name = \str_replace('.', '__dot__', $name);
        }

        return $name;
    }

    /**
     * Escape query attribute names that contain dots and match known collection attributes.
     * This distinguishes field names with dots (like 'collectionSecurity.Parent') from
     * nested object paths (like 'profile.level1.value').
     *
     * @param  array<Query>  $queries
     */
    protected function escapeQueryAttributes(Document $collection, array $queries): void
    {
        $dotAttributes = $this->getEscapedAttributes($collection);

        if (empty($dotAttributes)) {
            return;
        }

        $this->escapeQueryFields($queries, $dotAttributes);
    }

    /**
     * @param  array<mixed>  $queries
     * @param  array<string, string>  $dotAttributes
     */
    private function escapeQueryFields(array $queries, array $dotAttributes): void
    {
        foreach ($queries as $query) {
            if (! $query instanceof Query) {
                continue;
            }

            $method = $query->getMethod();
            if ($method === Method::And || $method === Method::Or) {
                $this->escapeQueryFields($query->getValues(), $dotAttributes);

                continue;
            }

            if ($method === Method::Exists || $method === Method::NotExists) {
                $query->setValues(\array_map(
                    static fn (mixed $field): mixed => \is_string($field) ? $dotAttributes[$field] ?? $field : $field,
                    $query->getValues(),
                ));

                continue;
            }

            $attribute = $query->getAttribute();
            if (isset($dotAttributes[$attribute])) {
                $query->setAttribute($dotAttributes[$attribute]);
            }
        }
    }

    /**
     * The stored field name of each collection attribute whose key holds a dot or starts with `$`.
     *
     * @return array<string, string>
     */
    private function getEscapedAttributes(Document $collection): array
    {
        $dotAttributes = [];
        foreach (self::collectionAttributes($collection) as $attribute) {
            $key = $attribute->key;
            if (\str_contains($key, '.') || \str_starts_with($key, '$')) {
                $dotAttributes[$key] = $this->escapeMongoFieldName($key);
            }
        }

        return $dotAttributes;
    }

    /**
     * Ensure relationship attributes have default null values in MongoDB documents.
     * MongoDB doesn't store null fields, so we need to add them for schema compatibility.
     */
    protected function ensureRelationshipDefaults(Document $collection, Document $document): void
    {
        foreach (self::collectionAttributes($collection) as $attribute) {
            $relationship = $attribute->relationship;
            if ($relationship === null || $document->offsetExists($attribute->key)) {
                continue;
            }

            $parent = $attribute->side === RelationshipSide::Parent;
            $storesData = match ($relationship->type) {
                RelationshipType::OneToOne => $parent || $relationship->twoWay,
                RelationshipType::OneToMany => ! $parent,
                RelationshipType::ManyToOne => $parent,
                RelationshipType::ManyToMany => false,
            };

            if ($storesData) {
                $document->setAttribute($attribute->key, null);
            }
        }
    }

    /**
     * Keys cannot begin with $ in MongoDB
     * Convert $ prefix to _ on $id, $permissions, and $collection
     *
     * @param  array<mixed>  $array  A document's fields, or a nested value of one (a list keeps its keys)
     * @return array<string, mixed>
     */
    protected function replaceCharacters(string $from, string $to, array $array): array
    {
        // First pass: recursively process array values and collect keys to rename
        $keysToRename = [];
        foreach ($array as $k => $v) {
            if (is_array($v)) {
                $array[$k] = $this->replaceCharacters($from, $to, $v);
            }

            if (\is_int($k)) {
                continue;
            }

            $newKey = $k;

            // Handle key replacement for filtered attributes
            $clean_key = str_replace($from, '', $k);
            if (in_array($clean_key, self::PREFIX_SWAPPED_KEYS)) {
                $newKey = str_replace($from, $to, $k);
            } elseif (\str_starts_with($k, $from) && ! in_array($k, [Document::ID, Document::SEQUENCE, Document::TENANT, Storage::UID, Storage::SEQUENCE, Storage::TENANT])) {
                // Handle any other key starting with the 'from' char (e.g. user-defined $-prefixed keys)
                $newKey = $to.\substr($k, \strlen($from));
            }

            // Handle dot escaping in MongoDB field names
            if ($from === '$' && \str_contains($newKey, '.')) {
                $newKey = \str_replace('.', '__dot__', $newKey);
            } elseif ($from === '_' && \str_contains($k, '__dot__')) {
                $newKey = \str_replace('__dot__', '.', $newKey);
            }

            if ($newKey !== $k) {
                $keysToRename[$k] = $newKey;
            }
        }

        foreach ($keysToRename as $oldKey => $newKey) {
            $array[$newKey] = $array[$oldKey];
            unset($array[$oldKey]);
        }

        // Handle special attribute mappings
        if ($from === '_') {
            if (isset($array[Storage::SEQUENCE])) {
                $array[Document::SEQUENCE] = $this->stringifyIdentifier($array[Storage::SEQUENCE]);
                unset($array[Storage::SEQUENCE]);
            }
            if (isset($array[Storage::UID])) {
                $array[Document::ID] = $this->stringifyIdentifier($array[Storage::UID]);
                unset($array[Storage::UID]);
            }
            if (\array_key_exists(Storage::TENANT, $array)) {
                $tenant = $array[Storage::TENANT];
                $array[Document::TENANT] = \is_int($tenant) || $tenant === null ? $tenant : $this->stringifyIdentifier($tenant);
                unset($array[Storage::TENANT]);
            }
        } elseif ($from === '$') {
            if (isset($array[Document::ID])) {
                $array[Storage::UID] = $array[Document::ID];
                unset($array[Document::ID]);
            }
            if (isset($array[Document::SEQUENCE])) {
                $array[Storage::SEQUENCE] = $array[Document::SEQUENCE];
                unset($array[Document::SEQUENCE]);
            }
            if (\array_key_exists(Document::TENANT, $array)) {
                $array[Storage::TENANT] = $array[Document::TENANT];
                unset($array[Document::TENANT]);
            }
        }

        /** @var array<string, mixed> $array */
        return $array;
    }

    private function stringifyIdentifier(mixed $value): string
    {
        if (\is_string($value)) {
            return $value;
        }

        if (\is_scalar($value)) {
            return (string) $value;
        }

        if (\is_object($value) && \method_exists($value, '__toString')) {
            return (string) $value;
        }

        return '';
    }

    /**
     * @param  array<Query>  $queries
     * @return array<mixed>
     *
     * @throws Exception
     */
    protected function buildFilters(array $queries, string $separator = '$and'): array
    {
        $filters = [];
        $queries = Query::groupByType($queries)->filters;

        foreach ($queries as $query) {
            /* @var $query Query */
            if ($query->isNested()) {
                if ($query->getMethod() === Method::ElemMatch) {
                    /** @var array<Query> $elemMatchValues */
                    $elemMatchValues = $query->getValues();
                    $filters[$separator][] = [
                        $query->getAttribute() => [
                            '$elemMatch' => $this->buildFilters($elemMatchValues, $separator),
                        ],
                    ];

                    continue;
                }

                $operator = $this->getQueryOperator($query->getMethod());

                /** @var array<Query> $nestedValues */
                $nestedValues = $query->getValues();
                $filters[$separator][] = $this->buildFilters($nestedValues, $operator);
            } else {
                $filters[$separator][] = $this->buildFilter($query);
            }
        }

        return $filters;
    }

    /**
     * @return array<mixed>
     *
     * @throws Exception
     */
    protected function buildFilter(Query $query): array
    {
        // Normalize extended ISO 8601 datetime strings in query values to UTCDateTime
        // so they can be correctly compared against datetime fields stored in MongoDB.
        if (! $this->supports(Capability::DefinedAttributes) || \in_array($query->getAttribute(), [Document::CREATED_AT, Document::UPDATED_AT], true)) {
            $values = $query->getValues();
            foreach ($values as $k => $value) {
                if (is_string($value) && $this->isExtendedIsoDatetime($value)) {
                    try {
                        $values[$k] = $this->toMongoDatetime($value);
                    } catch (Throwable $th) {
                        // Leave value as-is if it cannot be parsed as a datetime
                    }
                }
            }
            $query->setValues($values);
        }

        if ($query->getAttribute() === Document::ID) {
            $query->setAttribute(Storage::UID);
        } elseif ($query->getAttribute() === Document::SEQUENCE) {
            $query->setAttribute(Storage::SEQUENCE);
            $values = $query->getValues();
            foreach ($values as $k => $v) {
                $values[$k] = $v;
            }
            $query->setValues($values);
        } elseif ($query->getAttribute() === Document::CREATED_AT) {
            $query->setAttribute(Storage::CREATED_AT);
        } elseif ($query->getAttribute() === Document::UPDATED_AT) {
            $query->setAttribute(Storage::UPDATED_AT);
        } elseif (\str_starts_with($query->getAttribute(), '$')) {
            // Escape $ prefix and dots in user-defined $-prefixed attribute names for MongoDB
            $query->setAttribute($this->escapeMongoFieldName($query->getAttribute()));
        }

        $attribute = $query->getAttribute();
        $operator = $this->getQueryOperator($query->getMethod());

        $value = match ($query->getMethod()) {
            Method::IsNull,
            Method::IsNotNull => null,
            Method::Exists => true,
            Method::NotExists => false,
            default => $this->getQueryValue(
                $query->getMethod(),
                count($query->getValues()) > 1
                    ? $query->getValues()
                    : $query->getValues()[0]
            ),
        };

        /** @var array<string, mixed> $filter */
        $filter = [];
        if ($query->isObjectAttribute() && ! \str_contains($attribute, '.') && in_array($query->getMethod(), [Method::Equal, Method::Contains, Method::ContainsAny, Method::ContainsAll, Method::NotContains, Method::NotEqual])) {
            $this->handleObjectFilters($query, $filter);

            return $filter;
        }

        if ($operator == '$eq' && \is_array($value)) {
            /** @var array<string, mixed> $attrFilter1 */
            $attrFilter1 = [];
            $attrFilter1['$in'] = $value;
            $filter[$attribute] = $attrFilter1;
        } elseif ($operator == '$ne' && \is_array($value)) {
            /** @var array<string, mixed> $attrFilter2 */
            $attrFilter2 = [];
            $attrFilter2['$nin'] = $value;
            $filter[$attribute] = $attrFilter2;
        } elseif ($operator == '$all') {
            /** @var array<string, mixed> $attrFilter3 */
            $attrFilter3 = [];
            $attrFilter3['$all'] = $query->getValues();
            $filter[$attribute] = $attrFilter3;
        } elseif ($operator == '$in') {
            if (in_array($query->getMethod(), [Method::Contains, Method::ContainsAny]) && ! $query->onArray()) {
                // contains support array values
                if (is_array($value)) {
                    $filter['$or'] = array_map(fn ($item) => [
                        $attribute => [
                            '$regex' => $this->createSafeRegex(
                                \is_string($item) ? $item : (\is_scalar($item) ? (string) $item : ''),
                                '.*%s.*',
                                'i'
                            ),
                        ],
                    ], $value);
                } else {
                    $valueStr = \is_string($value) ? $value : (\is_scalar($value) ? (string) $value : '');
                    /** @var array<string, mixed> $attrFilter4 */
                    $attrFilter4 = [];
                    $attrFilter4['$regex'] = $this->createSafeRegex($valueStr, '.*%s.*');
                    $filter[$attribute] = $attrFilter4;
                }
            } else {
                /** @var array<string, mixed> $attrFilter5 */
                $attrFilter5 = [];
                $attrFilter5['$in'] = $query->getValues();
                $filter[$attribute] = $attrFilter5;
            }
        } elseif ($operator === 'notContains') {
            if (! $query->onArray()) {
                $valueStr = \is_string($value) ? $value : (\is_scalar($value) ? (string) $value : '');
                $filter[$attribute] = ['$not' => $this->createSafeRegex($valueStr, '.*%s.*')];
            } else {
                /** @var array<string, mixed> $attrFilter6 */
                $attrFilter6 = [];
                $attrFilter6['$nin'] = $query->getValues();
                $attrFilter6['$ne'] = null;
                $filter[$attribute] = $attrFilter6;
            }
        } elseif ($operator == '$search') {
            if ($query->getMethod() === Method::NotSearch) {
                // MongoDB doesn't support negating $text expressions directly
                // Use regex as fallback for NOT search while keeping fulltext for positive search
                if (empty($value)) {
                    // If value is not passed, don't add any filter - this will match all documents
                } else {
                    $valueStr = \is_string($value) ? $value : (\is_scalar($value) ? (string) $value : '');
                    $filter[$attribute] = ['$not' => $this->createSafeRegex($valueStr, '.*%s.*')];
                }
            } else {
                /** @var array<string, mixed> $textFilter */
                $textFilter = \is_array($filter['$text'] ?? null) ? $filter['$text'] : [];
                $textFilter[$operator] = $value;
                $filter['$text'] = $textFilter;
            }
        } elseif ($query->getMethod() === Method::Between) {
            /** @var array<mixed> $valueArray */
            $valueArray = \is_array($value) ? $value : [];
            /** @var array<string, mixed> $attrFilter7 */
            $attrFilter7 = [];
            $attrFilter7['$lte'] = $valueArray[1] ?? null;
            $attrFilter7['$gte'] = $valueArray[0] ?? null;
            $filter[$attribute] = $attrFilter7;
        } elseif ($query->getMethod() === Method::NotBetween) {
            /** @var array<mixed> $valueArray2 */
            $valueArray2 = \is_array($value) ? $value : [];
            $filter['$or'] = [
                [$attribute => ['$lt' => $valueArray2[0] ?? null]],
                [$attribute => ['$gt' => $valueArray2[1] ?? null]],
            ];
        } elseif ($operator === '$regex' && $query->getMethod() === Method::NotStartsWith) {
            $valueStr = \is_string($value) ? $value : (\is_scalar($value) ? (string) $value : '');
            $filter[$attribute] = ['$not' => $this->createSafeRegex($valueStr, '^%s')];
        } elseif ($operator === '$regex' && $query->getMethod() === Method::NotEndsWith) {
            $valueStr = \is_string($value) ? $value : (\is_scalar($value) ? (string) $value : '');
            $filter[$attribute] = ['$not' => $this->createSafeRegex($valueStr, '%s$')];
        } elseif ($operator === '$exists') {
            /** @var array<mixed> $existsOr */
            $existsOr = \is_array($filter['$or'] ?? null) ? $filter['$or'] : [];
            foreach ($query->getValues() as $existsAttribute) {
                $existsAttrStr = \is_string($existsAttribute) ? $existsAttribute : (\is_scalar($existsAttribute) ? (string) $existsAttribute : '');
                $existsOr[] = [$existsAttrStr => [$operator => $value]];
            }
            $filter['$or'] = $existsOr;
        } else {
            /** @var array<string, mixed> $attrFilterDefault */
            $attrFilterDefault = \is_array($filter[$attribute] ?? null) ? $filter[$attribute] : [];
            $attrFilterDefault[$operator] = $value;
            $filter[$attribute] = $attrFilterDefault;
        }

        return $filter;
    }

    /**
     * Get Query Operator
     *
     *
     * @throws Exception
     */
    protected function getQueryOperator(Method $operator): string
    {
        return match ($operator) {
            Method::Equal,
            Method::IsNull => '$eq',
            Method::NotEqual,
            Method::IsNotNull => '$ne',
            Method::LessThan => '$lt',
            Method::LessThanEqual => '$lte',
            Method::GreaterThan => '$gt',
            Method::GreaterThanEqual => '$gte',
            Method::Contains => '$in',
            Method::ContainsAny => '$in',
            Method::ContainsAll => '$all',
            Method::NotContains => 'notContains',
            Method::Search => '$search',
            Method::NotSearch => '$search',
            Method::Between => 'between',
            Method::NotBetween => 'notBetween',
            Method::StartsWith,
            Method::NotStartsWith,
            Method::EndsWith,
            Method::NotEndsWith,
            Method::Regex => '$regex',
            Method::Or => '$or',
            Method::And => '$and',
            Method::Exists,
            Method::NotExists => '$exists',
            Method::ElemMatch => '$elemMatch',
            default => throw new DatabaseException('Unknown operator: '.$operator->value),
        };
    }

    protected function getQueryValue(Method $method, mixed $value): mixed
    {
        return match ($method) {
            Method::StartsWith => '^'.preg_quote(\is_string($value) ? $value : (\is_scalar($value) ? (string) $value : ''), '/'),
            Method::EndsWith => preg_quote(\is_string($value) ? $value : (\is_scalar($value) ? (string) $value : ''), '/').'$',
            default => $value,
        };
    }

    /**
     * Get Mongo Order
     *
     *
     * @throws Exception
     */
    protected function getOrder(OrderDirection $order): int
    {
        return match ($order) {
            OrderDirection::Asc => 1,
            OrderDirection::Desc => -1,
            OrderDirection::Random => throw new QueryException('Random order is not supported by this adapter'),
        };
    }

    private static function indexedColumnType(string $type): ColumnType
    {
        try {
            return Attribute::typeFromStored($type);
        } catch (StructureException) {
            return ColumnType::String;
        }
    }

    /**
     * Check if tenant should be added to index
     *
     * @param  Document|string  $indexOrType  Index document or index type string
     */
    protected function shouldAddTenantToIndex(Index|Document|string|IndexType $indexOrType): bool
    {
        if (! $this->sharedTables) {
            return false;
        }

        if ($indexOrType instanceof Index) {
            $indexType = $indexOrType->type;
        } elseif ($indexOrType instanceof Document) {
            $rawIndexType = $indexOrType->getAttribute('type');
            $indexTypeValue = \is_string($rawIndexType) ? $rawIndexType : (\is_scalar($rawIndexType) ? (string) $rawIndexType : '');
            $indexType = IndexType::tryFrom($indexTypeValue) ?? IndexType::Key;
        } elseif ($indexOrType instanceof IndexType) {
            $indexType = $indexOrType;
        } else {
            $indexType = IndexType::tryFrom($indexOrType) ?? IndexType::Key;
        }

        return $indexType !== IndexType::Ttl;
    }

    /**
     * @param  array<string>  $selections
     * @return array<string, int>
     */
    private function getAttributeProjection(array $selections): array
    {
        $projection = [];

        $internalKeys = \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            Database::internalAttributesFor(true),
        );

        foreach ($selections as $selection) {
            // Skip internal attributes since all are selected by default
            if (\in_array($selection, $internalKeys)) {
                continue;
            }

            $projection[$selection] = 1;
        }

        $projection[Storage::UID] = 1;
        $projection[Storage::SEQUENCE] = 1;
        $projection[Storage::CREATED_AT] = 1;
        $projection[Storage::UPDATED_AT] = 1;
        $projection[Storage::PERMISSIONS] = 1;

        return $projection;
    }

    /**
     * Flattens the array.
     *
     * @return array<mixed>
     */
    protected function flattenArray(mixed $list): array
    {
        if (! is_array($list)) {
            // make sure the input is an array
            return [$list];
        }

        $newArray = [];

        foreach ($list as $value) {
            $newArray = array_merge($newArray, $this->flattenArray($value));
        }

        return $newArray;
    }

    /**
     * @param  array<string, mixed>|Document  $target
     * @return array<string, mixed>
     */
    protected function removeNullKeys(array|Document $target): array
    {
        $target = \is_array($target) ? $target : $target->getArrayCopy();
        $cleaned = [];

        foreach ($target as $key => $value) {
            if (\is_null($value)) {
                continue;
            }

            $cleaned[$key] = $value;
        }

        return $cleaned;
    }

    protected function processException(Throwable $e): Throwable
    {
        // Timeout
        if ($e->getCode() === 50 || $e->getCode() === 262) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Duplicate key error
        if ($e->getCode() === 11000 || $e->getCode() === 11001) {
            $index = $this->getViolatedIndex($e->getMessage());
            if ($index !== null && $index !== Storage::UID && $index !== '_id_') {
                return new UniqueException(UniqueException::MESSAGE, $e->getCode(), $e);
            }

            return new DuplicateException('Document already exists', $e->getCode(), $e);
        }

        // Collection already exists
        if ($e->getCode() === 48) {
            return new DuplicateException('Collection already exists', $e->getCode(), $e);
        }

        // Index already exists
        if ($e->getCode() === 85) {
            return new DuplicateException('Index already exists', $e->getCode(), $e);
        }

        // No transaction
        if ($e->getCode() === 251) {
            return new TransactionException('No active transaction', $e->getCode(), $e);
        }

        // Aborted transaction
        if ($e->getCode() === 112) {
            return new TransactionException('Transaction aborted', $e->getCode(), $e);
        }

        // Invalid operation (MongoDB error code 14)
        if ($e->getCode() === 14) {
            return new TypeException('Invalid operation', $e->getCode(), $e);
        }

        if ($e->getCode() === 28764) {
            return new LimitException('Value out of range', $e->getCode(), $e);
        }

        return $e;
    }

    protected function getViolatedIndex(string $message): ?string
    {
        if (\preg_match('/index:\s*(\S+)\s+dup key/', $message, $matches) !== 1) {
            return null;
        }

        return $matches[1];
    }

    protected function isExtendedIsoDatetime(string $value): bool
    {
        /**
         * Min:
         *   YYYY-MM-DDTHH:mm:ssZ             (20)
         *   YYYY-MM-DDTHH:mm:ss+HH:MM        (25)
         *
         * Max:
         *   YYYY-MM-DDTHH:mm:ss.fffffZ       (26)
         *   YYYY-MM-DDTHH:mm:ss.fffff+HH:MM  (31)
         */
        $length = strlen($value);

        // absolute minimum
        if ($length < 20) {
            return false;
        }

        // fixed datetime fingerprints
        if (
            ! isset($value[19]) ||
            $value[4] !== '-' ||
            $value[7] !== '-' ||
            $value[10] !== 'T' ||
            $value[13] !== ':' ||
            $value[16] !== ':'
        ) {
            return false;
        }

        // timezone detection
        $hasZ = ($value[$length - 1] === 'Z');

        $hasOffset = (
            $length >= 25 &&
            ($value[$length - 6] === '+' || $value[$length - 6] === '-') &&
            $value[$length - 3] === ':'
        );

        if (! $hasZ && ! $hasOffset) {
            return false;
        }

        if ($hasOffset && $length > 31) {
            return false;
        }

        if ($hasZ && $length > 26) {
            return false;
        }

        $digitPositions = [
            0, 1, 2, 3,
            5, 6,
            8, 9,
            11, 12,
            14, 15,
            17, 18,
        ];

        $timeEnd = $hasZ ? $length - 1 : $length - 6;

        // fractional seconds
        if ($timeEnd > 19) {
            if ($value[19] !== '.' || $timeEnd < 21) {
                return false;
            }
            for ($i = 20; $i < $timeEnd; $i++) {
                $digitPositions[] = $i;
            }
        }

        // timezone offset numeric digits
        if ($hasOffset) {
            foreach ([$length - 5, $length - 4, $length - 2, $length - 1] as $i) {
                $digitPositions[] = $i;
            }
        }

        foreach ($digitPositions as $i) {
            if (! ctype_digit($value[$i])) {
                return false;
            }
        }

        return true;
    }

    protected function convertUtcDateToString(mixed $node): mixed
    {
        if ($node instanceof UTCDateTime) {
            // Handle UTCDateTime objects
            $node = DateTime::format($node->toDateTime());
        } elseif (is_array($node) && isset($node['$date'])) {
            // Handle Extended JSON format from (array) cast
            // Format: {"$date":{"$numberLong":"1760405478290"}}
            if (is_array($node['$date']) && isset($node['$date']['$numberLong'])) {
                /** @var mixed $numberLongVal */
                $numberLongVal = $node['$date']['$numberLong'];
                $milliseconds = \is_int($numberLongVal) ? $numberLongVal : (\is_numeric($numberLongVal) ? (int) $numberLongVal : 0);
                $seconds = intdiv($milliseconds, 1000);
                $microseconds = ($milliseconds % 1000) * 1000;
                $dateTime = NativeDateTime::createFromFormat('U.u', $seconds.'.'.str_pad((string) $microseconds, 6, '0'));
                if ($dateTime) {
                    $dateTime->setTimezone(new DateTimeZone('UTC'));
                    $node = DateTime::format($dateTime);
                }
            }
        } elseif (is_string($node)) {
            // Already a string, validate and pass through
            try {
                new NativeDateTime($node);
            } catch (Exception $e) {
                // Invalid date string, skip
            }
        }

        return $node;
    }

    /**
     * Helper to add transaction/session context to command options if in transaction
     * Includes defensive check to ensure session is valid
     *
     * @param  array<string, mixed>  $options
     * @return array<string, mixed>
     */
    private function getTransactionOptions(array $options = []): array
    {
        if ($this->inTransaction > 0 && $this->session !== null) {
            // Pass the session array directly - the client will handle the transaction state internally
            $options['session'] = $this->session;
        }

        return $options;
    }

    /**
     * Create a safe MongoDB regex pattern by escaping special characters
     *
     * @param  string  $value  The user input to escape
     * @param  string  $pattern  The pattern template (e.g., ".*%s.*" for contains)
     */
    private function createSafeRegex(string $value, string $pattern = '%s', string $flags = 'i'): Regex
    {
        $escaped = preg_quote($value, '/');

        $finalPattern = sprintf($pattern, $escaped);

        return new Regex($finalPattern, $flags);
    }

    /**
     * @param  array<string, mixed>  $document
     * @param  array<string, mixed>  $options
     * @return array<string, mixed>
     *
     * @throws DuplicateException
     * @throws Exception
     */
    private function insertDocument(string $name, array $document, array $options = []): array
    {
        try {
            $this->client->insert($name, $document, $options);
            $filters = [Storage::UID => $document[Storage::UID]];
            if ($this->sharedTables) {
                $filters[Storage::TENANT] = $document[Storage::TENANT] ?? null;
            }

            try {
                $findResult = $this->client->find(
                    $name,
                    $filters,
                    array_merge(['limit' => 1], $options)
                );
                /** @var \stdClass $findResultCursor */
                $findResultCursor = $findResult->cursor;
                /** @var array<mixed> $firstBatch */
                $firstBatch = $findResultCursor->firstBatch;
                $result = $firstBatch[0];
            } catch (MongoException $e) {
                throw $this->processException($e);
            }

            /** @var array<string, mixed> $toArrayResult */
            $toArrayResult = $this->client->toArray($result) ?? [];
            return $toArrayResult;
        } catch (MongoException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * MongoDB uses a partial index for a query only when the query implies its filter, and a filter on a value implies
     * `$exists` but never `$type`. A unique index requires every field to exist with its stored type, so null values
     * never collide. A key index requires only its leading field to exist, so a filter on that field, alone or with
     * the following ones, can use it.
     *
     * @param  non-empty-array<string, ColumnType>  $fields  stored field name => attribute type, in index order
     * @return array<string, array<string, mixed>>
     */
    private function getPartialFilterExpression(IndexType $type, array $fields): array
    {
        if ($type !== IndexType::Unique) {
            return [\array_key_first($fields) => ['$exists' => true]];
        }

        $filter = [];
        foreach ($fields as $field => $attributeType) {
            $filter[$field] = ['$exists' => true, '$type' => $this->getMongoTypeCode($attributeType)];
        }

        return $filter;
    }

    /**
     * The BSON types a stored value of the column type can have. PHP integers are written as int
     * or long by magnitude, and a float attribute also accepts integers.
     *
     * @return string|list<string>
     */
    private function getMongoTypeCode(ColumnType $type): string|array
    {
        return match ($type) {
            ColumnType::String,
            ColumnType::Varchar,
            ColumnType::Text,
            ColumnType::MediumText,
            ColumnType::LongText,
            ColumnType::Id,
            ColumnType::Uuid7 => 'string',
            ColumnType::BigInteger,
            ColumnType::Integer => ['int', 'long'],
            ColumnType::Float,
            ColumnType::Double => ['double', 'int', 'long'],
            ColumnType::Boolean => 'bool',
            ColumnType::Datetime => 'date',
            default => 'string'
        };
    }

    /**
     * Converts timestamp to Mongo\BSON datetime format.
     *
     * @throws Exception
     */
    private function toMongoDatetime(string $dt): UTCDateTime
    {
        return new UTCDateTime(new NativeDateTime($dt));
    }

    /**
     * Recursive function to replace chars in array keys, while
     * skipping any that are explicitly excluded.
     *
     * @param  array<string, mixed>  $array
     * @param  array<string>  $exclude
     * @return array<string, mixed>
     */
    private function replaceInternalIdsKeys(array $array, string $from, string $to, array $exclude = []): array
    {
        $result = [];

        foreach ($array as $key => $value) {
            if (! in_array($key, $exclude)) {
                $key = str_replace($from, $to, $key);
            }

            if (is_array($value)) {
                /** @var array<string, mixed> $value */
                $result[$key] = $this->replaceInternalIdsKeys($value, $from, $to, $exclude);
            } else {
                $result[$key] = $value;
            }
        }

        return $result;
    }

    /**
     * @param  array<string, mixed>  $filter
     */
    private function handleObjectFilters(Query $query, array &$filter): void
    {
        $conditions = [];
        $isNot = in_array($query->getMethod(), [Method::NotContains, Method::NotEqual]);
        $values = $query->getValues();
        foreach ($values as $attribute => $value) {
            $flattendQuery = $this->flattenWithDotNotation(is_string($attribute) ? $attribute : '', $value);
            $flattenedObjectKey = array_key_first($flattendQuery);
            if ($flattenedObjectKey === null) {
                continue;
            }
            $queryValue = $flattendQuery[$flattenedObjectKey];
            $queryAttribute = $query->getAttribute();
            $flattenedQueryField = array_key_first($flattendQuery);
            $flattenedObjectKey = $flattenedQueryField === '' ? $queryAttribute : $queryAttribute.'.'.array_key_first($flattendQuery);
            switch ($query->getMethod()) {

                case Method::Contains:
                case Method::ContainsAny:
                case Method::ContainsAll:
                case Method::NotContains:
                    $arrayValue = \is_array($queryValue) ? $queryValue : [$queryValue];
                    $operator = $isNot ? '$nin' : '$in';
                    $conditions[] = [$flattenedObjectKey => [$operator => $arrayValue]];
                    break;

                case Method::Equal:
                case Method::NotEqual:
                    if (\is_array($queryValue)) {
                        $operator = $isNot ? '$nin' : '$in';
                        $conditions[] = [$flattenedObjectKey => [$operator => $queryValue]];
                    } else {
                        $operator = $isNot ? '$ne' : '$eq';
                        $conditions[] = [$flattenedObjectKey => [$operator => $queryValue]];
                    }

                    break;

            }
        }

        $logicalOperator = $isNot ? '$and' : '$or';
        if (count($conditions) && isset($filter[$logicalOperator])) {
            $existingLogical = $filter[$logicalOperator];
            /** @var array<mixed> $existingLogicalArr */
            $existingLogicalArr = \is_array($existingLogical) ? $existingLogical : [];
            $filter[$logicalOperator] = array_merge($existingLogicalArr, $conditions);
        } else {
            $filter[$logicalOperator] = $conditions;
        }
    }

    /**
     * Flatten a nested associative array into Mongo-style dot notation.
     *
     * @return array<string, mixed>
     */
    private function flattenWithDotNotation(string $key, mixed $value, string $prefix = ''): array
    {
        /** @var array<string, mixed> $result */
        $result = [];

        /** @var array<array{0: string, 1: mixed}> $stack */
        $stack = [];

        $initialKey = $prefix === '' ? $key : $prefix.'.'.$key;
        $stack[] = [$initialKey, $value];
        while (! empty($stack)) {
            $item = array_pop($stack);
            /** @var array{0: string, 1: mixed} $item */
            [$currentPath, $currentValue] = $item;
            if (is_array($currentValue) && ! array_is_list($currentValue)) {
                foreach ($currentValue as $nextKey => $nextValue) {
                    $nextKeyStr = (string) $nextKey;
                    $nextPath = $currentPath === '' ? $nextKeyStr : $currentPath.'.'.$nextKeyStr;
                    $stack[] = [$nextPath, $nextValue];
                }
            } else {
                // leaf node
                $result[$currentPath] = $currentValue;
            }
        }

        return $result;
    }

    private function convertStdClassToArray(mixed $value): mixed
    {
        if (is_object($value) && get_class($value) === stdClass::class) {
            $properties = get_object_vars($value);

            return $properties === [] ? $value : $this->convertStdClassValues($properties);
        }

        if (is_array($value)) {
            return $this->convertStdClassValues($value);
        }

        return $value;
    }

    /**
     * @param  array<mixed>  $values
     * @return array<mixed>
     */
    private function convertStdClassValues(array $values): array
    {
        foreach ($values as $key => $value) {
            if (\is_array($value) || \is_object($value)) {
                $values[$key] = $this->convertStdClassToArray($value);
            }
        }

        return $values;
    }

    /**
     * Get fields to unset for schemaless upsert operations
     *
     * @param  array<string, mixed>  $record
     * @return array<string, string>
     */
    private function getUpsertAttributeRemovals(Document $oldDocument, Document $newDocument, array $record): array
    {
        $unsetFields = [];

        if ($this->supports(Capability::DefinedAttributes) || $oldDocument->isEmpty()) {
            return $unsetFields;
        }

        $oldUserAttributes = $oldDocument->getAttributes();
        $newUserAttributes = $newDocument->getAttributes();

        $protectedFields = [Storage::UID, Storage::SEQUENCE, Storage::CREATED_AT, Storage::UPDATED_AT, Storage::PERMISSIONS, Storage::TENANT];

        foreach ($oldUserAttributes as $originalKey => $originalValue) {
            if (in_array($originalKey, $protectedFields) || array_key_exists($originalKey, $newUserAttributes)) {
                continue;
            }

            $transformed = $this->replaceCharacters('$', '_', [$originalKey => $originalValue]);
            $dbKey = array_key_first($transformed);

            if ($dbKey && ! array_key_exists($dbKey, $record) && ! in_array($dbKey, $protectedFields)) {
                $unsetFields[$dbKey] = '';
            }
        }

        return $unsetFields;
    }
}
