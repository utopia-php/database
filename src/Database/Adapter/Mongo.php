<?php

namespace Utopia\Database\Adapter;

use DateTime as NativeDateTime;
use DateTimeZone;
use Exception;
use MongoDB\BSON\Int64;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use stdClass;
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
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Hook\MongoPermissionFilter;
use Utopia\Database\Hook\MongoTenantFilter;
use Utopia\Database\Hook\Read;
use Utopia\Database\Hook\Tenant;
use Utopia\Database\Index;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * Database adapter for MongoDB, using the Utopia Mongo client for document-based storage.
 */
class Mongo extends Adapter implements Feature\InternalCasting, Feature\Relationships, Feature\Timeouts, Feature\Upserts, Feature\UTCCasting
{
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
        '$exists',
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
     * Transaction/session state for MongoDB transactions
     *
     * @var array<mixed>|null
     */
    private ?array $session = null; // Store session array from startSession

    protected int $inTransaction = 0;

    protected bool $supportForAttributes = true;

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

    /**
     * Get the list of capabilities supported by the MongoDB adapter.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        return array_merge(parent::capabilities(), [
            Capability::Objects,
            Capability::Fulltext,
            Capability::TTLIndexes,
            Capability::Regex,
            Capability::BatchCreateAttributes,
            Capability::Hostname,
            Capability::PCRE,
        ]);
    }

    /**
     * Set the maximum execution time for queries.
     *
     * @param int $milliseconds Timeout in milliseconds
     * @param Event $event The event scope for the timeout
     * @return void
     */
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
    public function clearTimeout(Event $event = Event::All): void
    {
        $this->timeout = 0;
    }

    /**
     * Set whether the adapter supports schema-based attribute definitions.
     *
     * @param bool $support Whether to enable attribute support
     * @return bool
     */
    public function setSupportForAttributes(bool $support): bool
    {
        $this->supportForAttributes = $support;

        return $this->supportForAttributes;
    }

    protected function syncWriteHooks(): void
    {
    }

    protected function syncReadHooks(): void
    {
        $this->readHooks = [];

        $this->readHooks[] = new MongoTenantFilter(
            $this->tenant,
            $this->sharedTables,
            fn (string $collection, array $tenants = []) => $this->getTenantFilters($collection, $tenants),
        );

        $this->readHooks[] = new MongoPermissionFilter($this->authorization);
    }

    /**
     * @param  array<string, mixed>  $filters
     * @return array<string, mixed>
     */
    protected function applyReadFilters(array $filters, string $collection, string $forPermission = 'read'): array
    {
        $this->syncReadHooks();
        foreach ($this->readHooks as $hook) {
            $filters = $hook->applyFilters($filters, $collection, $forPermission);
        }

        return $filters;
    }

    /**
     * Ping Database
     *
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

    /**
     * Reconnect to the MongoDB server.
     *
     * @return void
     */
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
     * Start a new database transaction or increment the nesting counter.
     *
     * @return bool
     *
     * @throws DatabaseException If the transaction cannot be started.
     */
    public function startTransaction(): bool
    {
        // If the database is not a replica set, we can't use transactions
        if (! $this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0) {
                if (! $this->session) {
                    $this->session = $this->client->startSession(); // Get session array
                    $this->client->startTransaction($this->session); // Start the transaction
                }
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
     * @throws DatabaseException If the transaction cannot be committed.
     */
    public function commitTransaction(): bool
    {
        // If the database is not a replica set, we can't use transactions
        if (! $this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0) {
                return false;
            }
            $this->inTransaction--;
            if ($this->inTransaction === 0) {
                if (! $this->session) {
                    return false;
                }
                try {
                    $result = $this->client->commitTransaction($this->session);
                } catch (MongoException $e) {
                    // If there's no active transaction, it may have been auto-aborted due to an error.
                    // This is not necessarily a failure, just return success since the transaction was already terminated.
                    $e = $this->processException($e);
                    if ($e instanceof TransactionException) {
                        $this->client->endSessions([$this->session]);
                        $this->session = null;
                        $this->inTransaction = 0;  // Reset counter when transaction is already terminated

                        return true;
                    }
                    throw $e;
                } catch (Throwable $e) {
                    throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                } finally {
                    if ($this->session) {
                        $this->client->endSessions([$this->session]);
                    }
                    $this->session = null;
                }

                return true;
            }

            return true;
        } catch (Throwable $e) {
            // Ensure cleanup on any failure
            try {
                if ($this->session !== null) {
                    $this->client->endSessions([$this->session]);
                }
            } catch (Throwable $endSessionError) {
                // Ignore errors when ending session during error cleanup
            }
            $this->session = null;
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to commit transaction: '.$e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Roll back the current database transaction or decrement the nesting counter.
     *
     * @return bool
     *
     * @throws DatabaseException If the rollback fails.
     */
    public function rollbackTransaction(): bool
    {
        // If the database is not a replica set, we can't use transactions
        if (! $this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0) {
                return false;
            }
            $this->inTransaction--;
            if ($this->inTransaction === 0) {
                if (! $this->session) {
                    return false;
                }

                try {
                    $this->client->abortTransaction($this->session);
                } catch (Throwable $e) {
                    $e = $this->processException($e);

                    if ($e instanceof TransactionException) {
                        // If there's no active transaction, it may have been auto-aborted due to an error.
                        // Just return success since the transaction was already terminated.
                        return true;
                    }

                    throw $e;
                } finally {
                    $this->client->endSessions([$this->session]);
                    $this->session = null;
                }

                return true;
            }

            return true;
        } catch (Throwable $e) {
            try {
                if ($this->session !== null) {
                    $this->client->endSessions([$this->session]);
                }
            } catch (Throwable) {
                // Ignore errors when ending session during error cleanup
            }
            $this->session = null;
            $this->inTransaction = 0;

            throw new DatabaseException('Failed to rollback transaction: '.$e->getMessage(), $e->getCode(), $e);
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
        // If the database is not a replica set, we can't use transactions
        if (! $this->client->isReplicaSet()) {
            return $callback();
        }

        // MongoDB doesn't support nested transactions/savepoints.
        // If already in a transaction, just run the callback directly.
        if ($this->inTransaction > 0) {
            return $callback();
        }

        try {
            $this->startTransaction();
            $result = $callback();
            $this->commitTransaction();

            return $result;
        } catch (Throwable $action) {
            try {
                $this->rollbackTransaction();
            } catch (Throwable) {
                // Throw the original exception, not the rollback one
                // Since if it's a duplicate key error, the rollback will fail,
                // and we want to throw the original exception.
            } finally {
                // Ensure state is cleaned up even if rollback fails
                if ($this->session) {
                    try {
                        /** @var array<string, mixed> $session */
                        $session = $this->session;
                        $this->client->endSessions([$session]);
                    } catch (Throwable $endSessionError) {
                        // Ignore errors when ending session during error cleanup
                    }
                }
                $this->inTransaction = 0;
                $this->session = null;
            }

            throw $action;
        }
    }

    /**
     * Create Database
     */
    public function create(string $name): bool
    {
        return true;
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param  string  $database  database name
     * @param  string|null  $collection  (optional) collection name
     *
     * @throws Exception
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        if (! \is_null($collection)) {
            $collection = $this->getNamespace().'_'.$collection;
            try {
                // Use listCollections command with filter for O(1) lookup
                /** @var \stdClass $result */
                $result = $this->getClient()->query([
                    'listCollections' => 1,
                    'filter' => ['name' => $collection],
                ]);

                /** @var \stdClass $cursor */
                $cursor = $result->cursor;
                /** @var array<mixed> $firstBatch */
                $firstBatch = $cursor->firstBatch;
                return ! empty($firstBatch);
            } catch (Exception $e) {
                return false;
            }
        }

        return $this->getClient()->selectDatabase() != null;
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
        $this->getClient()->dropDatabase([], $name);

        return true;
    }

    /**
     * Create Collection
     *
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     *
     * @throws Exception
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->getNamespace().'_'.$this->filter($name);

        // In shared-tables mode or for metadata, the physical collection may
        // already exist for another tenant. Return early to avoid a
        // "Collection Exists" exception from the client.
        if (! $this->inTransaction && ($this->getSharedTables() || $name === Database::METADATA) && $this->exists($this->getNamespace(), $name)) {
            return true;
        }

        // Returns an array/object with the result document
        try {
            $options = $this->getTransactionOptions();
            $this->getClient()->createCollection($id, $options);
        } catch (MongoException $e) {
            // Client throws "Collection Exists" (code 0) if it already exists
            if (\str_contains($e->getMessage(), 'Collection Exists')) {
                return true;
            }
            $e = $this->processException($e);
            if ($e instanceof DuplicateException) {
                return true;
            }
            // Client throws code-0 "Collection Exists" when its pre-check
            // finds the collection. In shared-tables/metadata context this
            // is a no-op; otherwise re-throw as DuplicateException so
            // Database::createCollection() can run orphan reconciliation.
            if ($e->getCode() === 0 && stripos($e->getMessage(), 'Collection Exists') !== false) {
                if ($this->getSharedTables() || $name === Database::METADATA) {
                    return true;
                }
                throw new DuplicateException('Collection already exists', $e->getCode(), $e);
            }
            throw $e;
        }

        $internalIndex = [
            [
                'key' => ['_uid' => $this->getOrder(OrderDirection::Asc)],
                'name' => '_uid',
                'unique' => true,
                'collation' => [
                    'locale' => 'en',
                    'strength' => 1,
                ],
            ],
            [
                'key' => ['_createdAt' => $this->getOrder(OrderDirection::Asc)],
                'name' => '_createdAt',
            ],
            [
                'key' => ['_updatedAt' => $this->getOrder(OrderDirection::Asc)],
                'name' => '_updatedAt',
            ],
            [
                'key' => ['_permissions' => $this->getOrder(OrderDirection::Asc)],
                'name' => '_permissions',
            ],
        ];

        if ($this->sharedTables) {
            foreach ($internalIndex as &$index) {
                $index['key'] = array_merge(['_tenant' => $this->getOrder(OrderDirection::Asc)], $index['key']);
            }
            unset($index);
        }

        try {
            $options = $this->getTransactionOptions();
            $indexesCreated = $this->client->createIndexes($id, $internalIndex, $options);
        } catch (Exception $e) {
            throw $this->processException($e);
        }

        if (! $indexesCreated) {
            return false;
        }

        // Since attributes are not used by this adapter
        // Only act when $indexes is provided

        if (! empty($indexes)) {
            /**
             * Each new index has format ['key' => [$attribute => $order], 'name' => $name, 'unique' => $unique]
             */
            $newIndexes = [];

            $collectionAttributes = $attributes;

            // using $i and $j as counters to distinguish from $key
            foreach ($indexes as $i => $index) {

                $key = [];
                $unique = false;
                $attributes = $index->attributes;
                $orders = $index->orders;

                // If sharedTables, always add _tenant as the first key
                if ($this->shouldAddTenantToIndex($index)) {
                    $key['_tenant'] = $this->getOrder(OrderDirection::Asc);
                }

                foreach ($attributes as $j => $attribute) {
                    $attribute = $this->filter($this->getInternalKeyForAttribute((string) $attribute));

                    switch ($index->type) {
                        case IndexType::Key:
                            $order = $this->getOrder(OrderDirection::tryFrom(\strtoupper((string) ($orders[$j] ?? ''))) ?? OrderDirection::Asc);
                            break;
                        case IndexType::Fulltext:
                            // MongoDB fulltext index is just 'text'
                            $order = 'text';
                            break;
                        case IndexType::Unique:
                            $order = $this->getOrder(OrderDirection::tryFrom(\strtoupper((string) ($orders[$j] ?? ''))) ?? OrderDirection::Asc);
                            $unique = true;
                            break;
                        case IndexType::Ttl:
                            $order = $this->getOrder(OrderDirection::tryFrom(\strtoupper((string) ($orders[$j] ?? ''))) ?? OrderDirection::Asc);
                            break;
                        default:
                            // index not supported
                            return false;
                    }

                    $key[$attribute] = $order;
                }

                $newIndexes[$i] = [
                    'key' => $key,
                    'name' => $this->filter($index->key),
                    'unique' => $unique,
                ];

                if ($index->type === IndexType::Fulltext) {
                    $newIndexes[$i]['default_language'] = 'none';
                }

                // Handle TTL indexes
                if ($index->type === IndexType::Ttl) {
                    $ttl = $index->ttl;
                    if ($ttl > 0) {
                        $newIndexes[$i]['expireAfterSeconds'] = $ttl;
                    }
                }

                // Add partial filter for indexes to avoid indexing null values
                if (in_array($index->type, [
                    IndexType::Unique,
                    IndexType::Key,
                ])) {
                    $partialFilter = [];
                    foreach ($attributes as $attr) {
                        $attr = (string) $attr;
                        // Find the matching attribute in collectionAttributes to get its type
                        $attrType = 'string'; // Default fallback
                        foreach ($collectionAttributes as $collectionAttr) {
                            if ($collectionAttr->key === $attr) {
                                $attrType = $this->getMongoTypeCode($collectionAttr->type);
                                break;
                            }
                        }

                        $attr = $this->filter($this->getInternalKeyForAttribute($attr));

                        // Use both $exists: true and $type to exclude nulls and ensure correct type
                        $partialFilter[$attr] = [
                            '$exists' => true,
                            '$type' => $attrType,
                        ];
                    }
                    if (! empty($partialFilter)) {
                        $newIndexes[$i]['partialFilterExpression'] = $partialFilter;
                    }
                }
            }

            try {
                $options = $this->getTransactionOptions();
                $indexesCreated = $this->getClient()->createIndexes($id, \array_values($newIndexes), $options);
            } catch (Exception $e) {
                throw $this->processException($e);
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
    public function listCollections(): array
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
     * Delete Collection
     *
     * @throws Exception
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->getNamespace().'_'.$this->filter($id);

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
     * @param  array<Attribute>  $attributes
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
    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
    {
        if (! empty($newKey) && $newKey !== $attribute->key) {
            return $this->renameAttribute($collection, $attribute->key, $newKey);
        }

        return true;
    }

    /**
     * Delete Attribute
     *
     *
     * @throws DatabaseException
     * @throws MongoException
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $collection = $this->getNamespace().'_'.$this->filter($collection);

        $this->getClient()->update(
            $collection,
            [],
            ['$unset' => [$id => '']],
            multi: true
        );

        return true;
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

        $from = $this->filter($this->getInternalKeyForAttribute($id));
        $to = $this->filter($this->getInternalKeyForAttribute($name));
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
     *
     * @param Relationship $relationship The relationship definition
     * @return bool
     */
    public function createRelationship(Relationship $relationship): bool
    {
        return true;
    }

    /**
     * @throws DatabaseException
     * @throws MongoException
     */
    public function updateRelationship(
        Relationship $relationship,
        ?string $newKey = null,
        ?string $newTwoWayKey = null
    ): bool {
        $collectionName = $this->getNamespace().'_'.$this->filter($relationship->collection);
        $relatedCollectionName = $this->getNamespace().'_'.$this->filter($relationship->relatedCollection);

        $escapedKey = $this->escapeMongoFieldName($relationship->key);
        $escapedNewKey = ! \is_null($newKey) ? $this->escapeMongoFieldName($newKey) : null;
        $escapedTwoWayKey = $this->escapeMongoFieldName($relationship->twoWayKey);
        $escapedNewTwoWayKey = ! \is_null($newTwoWayKey) ? $this->escapeMongoFieldName($newTwoWayKey) : null;

        $renameKey = [
            '$rename' => [
                $escapedKey => $escapedNewKey,
            ],
        ];

        $renameTwoWayKey = [
            '$rename' => [
                $escapedTwoWayKey => $escapedNewTwoWayKey,
            ],
        ];

        switch ($relationship->type) {
            case RelationType::OneToOne:
                if (! \is_null($newKey) && $relationship->key !== $newKey) {
                    $this->getClient()->update($collectionName, updates: $renameKey, multi: true);
                }
                if ($relationship->twoWay && ! \is_null($newTwoWayKey) && $relationship->twoWayKey !== $newTwoWayKey) {
                    $this->getClient()->update($relatedCollectionName, updates: $renameTwoWayKey, multi: true);
                }
                break;
            case RelationType::OneToMany:
                if ($relationship->twoWay && ! \is_null($newTwoWayKey) && $relationship->twoWayKey !== $newTwoWayKey) {
                    $this->getClient()->update($relatedCollectionName, updates: $renameTwoWayKey, multi: true);
                }
                break;
            case RelationType::ManyToOne:
                if (! \is_null($newKey) && $relationship->key !== $newKey) {
                    $this->getClient()->update($collectionName, updates: $renameKey, multi: true);
                }
                break;
            case RelationType::ManyToMany:
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collectionDoc = $this->getDocument($metadataCollection, $relationship->collection);
                $relatedCollectionDoc = $this->getDocument($metadataCollection, $relationship->relatedCollection);

                if ($collectionDoc->isEmpty() || $relatedCollectionDoc->isEmpty()) {
                    throw new DatabaseException('Collection or related collection not found');
                }

                $junction = $relationship->side === RelationSide::Parent
                    ? $this->getNamespace().'_'.$this->filter('_'.$collectionDoc->getSequence().'_'.$relatedCollectionDoc->getSequence())
                    : $this->getNamespace().'_'.$this->filter('_'.$relatedCollectionDoc->getSequence().'_'.$collectionDoc->getSequence());

                if (! \is_null($newKey) && $relationship->key !== $newKey) {
                    $this->getClient()->update($junction, updates: $renameKey, multi: true);
                }
                if ($relationship->twoWay && ! \is_null($newTwoWayKey) && $relationship->twoWayKey !== $newTwoWayKey) {
                    $this->getClient()->update($junction, updates: $renameTwoWayKey, multi: true);
                }
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    /**
     * @throws MongoException
     * @throws Exception
     */
    public function deleteRelationship(
        Relationship $relationship
    ): bool {
        $collectionName = $this->getNamespace().'_'.$this->filter($relationship->collection);
        $relatedCollectionName = $this->getNamespace().'_'.$this->filter($relationship->relatedCollection);
        $escapedKey = $this->escapeMongoFieldName($relationship->key);
        $escapedTwoWayKey = $this->escapeMongoFieldName($relationship->twoWayKey);

        switch ($relationship->type) {
            case RelationType::OneToOne:
                if ($relationship->side === RelationSide::Parent) {
                    $this->getClient()->update($collectionName, [], ['$unset' => [$escapedKey => '']], multi: true);
                    if ($relationship->twoWay) {
                        $this->getClient()->update($relatedCollectionName, [], ['$unset' => [$escapedTwoWayKey => '']], multi: true);
                    }
                } elseif ($relationship->side === RelationSide::Child) {
                    $this->getClient()->update($relatedCollectionName, [], ['$unset' => [$escapedTwoWayKey => '']], multi: true);
                    if ($relationship->twoWay) {
                        $this->getClient()->update($collectionName, [], ['$unset' => [$escapedKey => '']], multi: true);
                    }
                }
                break;
            case RelationType::OneToMany:
                if ($relationship->side === RelationSide::Parent) {
                    $this->getClient()->update($relatedCollectionName, [], ['$unset' => [$escapedTwoWayKey => '']], multi: true);
                } else {
                    $this->getClient()->update($collectionName, [], ['$unset' => [$escapedKey => '']], multi: true);
                }
                break;
            case RelationType::ManyToOne:
                if ($relationship->side === RelationSide::Parent) {
                    $this->getClient()->update($collectionName, [], ['$unset' => [$escapedKey => '']], multi: true);
                } else {
                    $this->getClient()->update($relatedCollectionName, [], ['$unset' => [$escapedTwoWayKey => '']], multi: true);
                }
                break;
            case RelationType::ManyToMany:
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collectionDoc = $this->getDocument($metadataCollection, $relationship->collection);
                $relatedCollectionDoc = $this->getDocument($metadataCollection, $relationship->relatedCollection);

                if ($collectionDoc->isEmpty() || $relatedCollectionDoc->isEmpty()) {
                    throw new DatabaseException('Collection or related collection not found');
                }

                $junction = $relationship->side === RelationSide::Parent
                    ? $this->getNamespace().'_'.$this->filter('_'.$collectionDoc->getSequence().'_'.$relatedCollectionDoc->getSequence())
                    : $this->getNamespace().'_'.$this->filter('_'.$relatedCollectionDoc->getSequence().'_'.$collectionDoc->getSequence());

                $this->getClient()->dropCollection($junction);
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
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
        $attributes = $index->attributes;
        $orders = $index->orders;
        $ttl = $index->ttl;
        /** @var array<string, mixed> $indexes */
        $indexes = [];
        $options = [];
        $indexes['name'] = $id;

        /** @var array<string, int|string> $indexKey */
        $indexKey = [];

        // If sharedTables, always add _tenant as the first key
        if ($this->shouldAddTenantToIndex($type)) {
            $indexKey['_tenant'] = $this->getOrder(OrderDirection::Asc);
        }

        foreach ($attributes as $i => $attribute) {
            $attribute = (string) $attribute;

            if (isset($indexAttributeTypes[$attribute]) && \str_contains($attribute, '.') && $indexAttributeTypes[$attribute] === ColumnType::Object->value) {
                $dottedAttributes = \explode('.', $attribute);
                $expandedAttributes = array_map(fn ($attr) => $this->filter($attr), $dottedAttributes);
                $attributes[$i] = implode('.', $expandedAttributes);
            } else {
                $attributes[$i] = $this->filter($this->getInternalKeyForAttribute($attribute));
            }

            $orderType = $this->getOrder(OrderDirection::tryFrom(\strtoupper((string) ($orders[$i] ?? ''))) ?? OrderDirection::Asc);
            $indexKey[$attributes[$i]] = $orderType;

            switch ($type) {
                case IndexType::Key:
                    break;
                case IndexType::Fulltext:
                    $indexKey[$attributes[$i]] = 'text';
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

        // Handle TTL indexes
        if ($type === IndexType::Ttl && $ttl > 0) {
            $indexes['expireAfterSeconds'] = $ttl;
        }

        // Add partial filter for indexes to avoid indexing null values
        if (in_array($type, [IndexType::Unique, IndexType::Key])) {
            $partialFilter = [];
            foreach ($attributes as $i => $attr) {
                $attrType = ColumnType::tryFrom($indexAttributeTypes[$i] ?? '') ?? ColumnType::String;
                $attrType = $this->getMongoTypeCode($attrType);
                $partialFilter[$attr] = ['$exists' => true, '$type' => $attrType];
            }
            if (! empty($partialFilter)) {
                $indexes['partialFilterExpression'] = $partialFilter;
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
                    } catch (Exception $e) {
                        if ($retryCount >= $maxRetries - 1) {
                            throw new DatabaseException(
                                'Timeout waiting for index creation: '.$e->getMessage(),
                                $e->getCode(),
                                $e
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
        } catch (Exception $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Delete Index
     *
     *
     * @throws Exception
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $name = $this->getNamespace().'_'.$this->filter($collection);
        $id = $this->filter($id);
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
        $metadataCollection = new Document(['$id' => Database::METADATA]);
        $collectionDocument = $this->getDocument($metadataCollection, $collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $rawIndexes = $collectionDocument->getAttribute('indexes', '[]');
        /** @var array<int, array<string, mixed>> $indexes */
        $indexes = json_decode((string) (is_string($rawIndexes) ? $rawIndexes : '[]'), true) ?? [];
        /** @var array<string, mixed>|null $index */
        $index = null;

        foreach ($indexes as $node) {
            /** @var array<string, mixed> $node */
            $nodeId = $node['$id'] ?? $node['key'] ?? '';
            $nodeIdStr = \is_string($nodeId) ? $nodeId : (\is_scalar($nodeId) ? (string) $nodeId : '');
            if ($nodeIdStr === $old) {
                $index = $node;
                break;
            }
        }

        // Extract attribute types from the collection document
        $indexAttributeTypes = [];
        $rawAttributes = $collectionDocument->getAttribute('attributes');
        if ($rawAttributes !== null) {
            /** @var array<int, array<string, mixed>> $attributes */
            $attributes = json_decode((string) (is_string($rawAttributes) ? $rawAttributes : '[]'), true) ?? [];
            if ($attributes && $index) {
                // Map index attributes to their types
                /** @var array<string> $indexAttrs */
                $indexAttrs = $index['attributes'] ?? [];
                foreach ($indexAttrs as $attrName) {
                    foreach ($attributes as $attr) {
                        /** @var array<string, mixed> $attr */
                        $attrKey = $attr['key'] ?? '';
                        $attrKeyStr = \is_string($attrKey) ? $attrKey : (\is_scalar($attrKey) ? (string) $attrKey : '');
                        if ($attrKeyStr === $attrName) {
                            $attrType = $attr['type'] ?? '';
                            $indexAttributeTypes[$attrName] = \is_string($attrType) ? $attrType : (\is_scalar($attrType) ? (string) $attrType : '');
                            break;
                        }
                    }
                }
            }
        }

        try {
            if (! $index) {
                throw new DatabaseException('Index not found: '.$old);
            }
            $deletedindex = $this->deleteIndex($collection, $old);
            /** @var array<string> $indexAttributes */
            $indexAttributes = $index['attributes'] ?? [];
            /** @var array<int> $indexLengths */
            $indexLengths = $index['lengths'] ?? [];
            /** @var array<string> $indexOrders */
            $indexOrders = $index['orders'] ?? [];
            $rawIndexType = $index['type'] ?? 'key';
            $indexTypeStr = \is_string($rawIndexType) ? $rawIndexType : (\is_scalar($rawIndexType) ? (string) $rawIndexType : 'key');
            $rawIndexTtl = $index['ttl'] ?? 0;
            $indexTtlInt = \is_int($rawIndexTtl) ? $rawIndexTtl : (\is_numeric($rawIndexTtl) ? (int) $rawIndexTtl : 0);
            $createdindex = $this->createIndex($collection, new Index(
                key: $new,
                type: IndexType::from($indexTypeStr),
                attributes: $indexAttributes,
                lengths: $indexLengths,
                orders: $indexOrders,
                ttl: $indexTtlInt,
            ), $indexAttributeTypes);
        } catch (Exception $e) {
            throw $this->processException($e);
        }

        if ($deletedindex && $createdindex) {
            return true;
        }

        return false;
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

        $filters = ['_uid' => $id];

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection->getId());

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
        $result = $this->replaceChars('_', '$', $resultArray ?? []);
        $document = new Document($result);
        $document = $this->castingAfter($collection, $document);

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

        $document->removeAttribute('$sequence');

        /** @var array<string, mixed> $documentArray */
        $documentArray = (array) $document;
        $record = $this->replaceChars('$', '_', $documentArray);
        $record = $this->decorateRow($record, $this->documentMetadata($document));

        // Insert manual id if set
        if (! empty($sequence)) {
            $record['_id'] = $sequence;
        }
        $options = $this->getTransactionOptions();
        $result = $this->insertDocument($name, $this->removeNullKeys($record), $options);
        $result = $this->replaceChars('_', '$', $result);
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
        $documents = \array_map(fn ($doc) => clone $doc, $documents);

        foreach ($documents as $document) {
            $sequence = $document->getSequence();

            if ($hasSequence === null) {
                $hasSequence = ! empty($sequence);
            } elseif ($hasSequence == empty($sequence)) {
                throw new DatabaseException('All documents must have an sequence if one is set');
            }

            /** @var array<string, mixed> $documentArr */
            $documentArr = (array) $document;
            $record = $this->replaceChars('$', '_', $documentArr);
            $record = $this->decorateRow($record, $this->documentMetadata($document));

            if (! empty($sequence)) {
                $record['_id'] = $sequence;
            }

            $records[] = $record;
        }

        try {
            $documents = $this->client->insertMany($name, $records, $options);
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        foreach ($documents as $index => $document) {
            /** @var array<string, mixed> $toArrayResult */
            $toArrayResult = $this->client->toArray($document) ?? [];
            $documents[$index] = $this->replaceChars('_', '$', $toArrayResult);
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
        $record = $this->replaceChars('$', '_', $record);

        $filters = ['_uid' => $id];

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection->getId());

        try {
            unset($record['_id']); // Don't update _id

            $options = $this->getTransactionOptions();
            $updateQuery = [
                '$set' => $record,
            ];
            $this->client->update($name, $filters, $updateQuery, $options);
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
     *
     * @throws DatabaseException
     */
    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        $name = $this->getNamespace().'_'.$this->filter($collection->getId());

        $options = $this->getTransactionOptions();
        $queries = [
            Query::equal('$sequence', \array_map(fn ($document) => $document->getSequence(), $documents)),
        ];

        /** @var array<string, mixed> $filters */
        $filters = $this->buildFilters($queries);

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection->getId());

        $record = $updates->getArrayCopy();
        $record = $this->replaceChars('$', '_', $record);
        unset($record['_version']);

        $updateQuery = [
            '$set' => $record,
            '$inc' => ['_version' => 1],
        ];

        try {
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
     * @param  array<Change>  $changes
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        if (empty($changes)) {
            return $changes;
        }

        $this->syncWriteHooks();
        $this->syncReadHooks();

        try {
            $name = $this->getNamespace().'_'.$this->filter($collection->getId());
            $attribute = $this->filter($attribute);

            $operations = [];
            foreach ($changes as $change) {
                $document = $change->getNew();
                $oldDocument = $change->getOld();
                /** @var array<string, mixed> $attributes */
                $attributes = $document->getAttributes();
                $attributes['_uid'] = $document->getId();
                $attributes['_createdAt'] = $document['$createdAt'];
                $attributes['_updatedAt'] = $document['$updatedAt'];
                $attributes['_permissions'] = $document->getPermissions();

                if (! empty($document->getSequence())) {
                    $attributes['_id'] = $document->getSequence();
                }

                $record = $this->replaceChars('$', '_', $attributes);
                $record = $this->decorateRow($record, $this->documentMetadata($document));

                // Build filter for upsert
                $filters = ['_uid' => $document->getId()];
                $filters = $this->applyReadFilters($filters, $collection->getId());

                unset($record['_id']); // Don't update _id

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
                    // Update all fields
                    $update = [
                        '$set' => $record,
                    ];

                    if (! empty($unsetFields)) {
                        $update['$unset'] = $unsetFields;
                    }

                    // Add UUID7 _id for new documents in upsert operations
                    if (empty($document->getSequence())) {
                        $update['$setOnInsert'] = [
                            '_id' => $this->client->createUuid(),
                        ];
                    }
                }

                $operations[] = [
                    'filter' => $filters,
                    'update' => $update,
                ];
            }

            $options = $this->getTransactionOptions();

            $this->client->upsert(
                $name,
                $operations,
                options: $options
            );
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        return \array_map(fn ($change) => $change->getNew(), $changes);
    }

    /**
     * Delete Document
     *
     *
     * @throws Exception
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $name = $this->getNamespace().'_'.$this->filter($collection);

        $filters = ['_uid' => $id];

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection);

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
    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        $name = $this->getNamespace().'_'.$this->filter($collection);

        foreach ($sequences as $index => $sequence) {
            $sequences[$index] = $sequence;
        }

        /** @var array<string, mixed> $filters */
        $filters = $this->buildFilters([new Query(Method::Equal, '_id', $sequences)]);

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection);

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
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, string $updatedAt, int|float|null $min = null, int|float|null $max = null): bool
    {
        $attribute = $this->filter($attribute);
        $filters = ['_uid' => $id];

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection);

        if ($max !== null || $min !== null) {
            /** @var array<string, int|float> $attributeFilter */
            $attributeFilter = [];
            if ($max !== null) {
                $attributeFilter['$lte'] = $max;
            }
            if ($min !== null) {
                $attributeFilter['$gte'] = $min;
            }
            $filters[$attribute] = $attributeFilter;
        }

        $options = $this->getTransactionOptions();
        try {
            $this->client->update(
                $this->getNamespace().'_'.$this->filter($collection),
                $filters,
                [
                    '$inc' => [$attribute => $value],
                    '$set' => ['_updatedAt' => $this->toMongoDatetime($updatedAt)],
                ],
                options: $options
            );
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        return true;
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

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection->getId(), $forPermission->value);

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

                if ($originalAttribute === '$sequence') {
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
                $record = $this->replaceChars('_', '$', $resultCast);
                /** @var array<string, mixed> $convertedRecord */
                $convertedRecord = $this->convertStdClassToArray($record);
                $found[] = new Document($convertedRecord);
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
                    $record = $this->replaceChars('_', '$', $resultCast);
                    /** @var array<string, mixed> $convertedRecord */
                    $convertedRecord = $this->convertStdClassToArray($record);
                    $found[] = new Document($convertedRecord);
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

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection->getId());

        /**
         * Use MongoDB aggregation pipeline for accurate counting
         * Accuracy and Sharded Clusters
         * "On a sharded cluster, the count command when run without a query predicate can result in an inaccurate
         * count if orphaned documents exist or if a chunk migration is in progress.
         * To avoid these situations, on a sharded cluster, use the db.collection.aggregate() method"
         * https://www.mongodb.com/docs/manual/reference/command/count/#response
         **/
        $options = $this->getTransactionOptions();
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
                    '_id' => null,
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
            return 0;
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

        // queries
        $queries = array_map(fn ($query) => clone $query, $queries);
        /** @var array<string, mixed> $filters */
        $filters = $this->buildFilters($queries);

        $this->syncReadHooks();
        $filters = $this->applyReadFilters($filters, $collection->getId());

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
                '_id' => null,
                'total' => ['$sum' => '$'.$attribute],
            ],
        ];

        $options = $this->getTransactionOptions();

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
        if (!isset($sumFirstResult->total)) {
            return 0;
        }
        /** @var mixed $sumTotal */
        $sumTotal = $sumFirstResult->total;
        if (\is_int($sumTotal) || \is_float($sumTotal)) {
            return $sumTotal;
        }
        return \is_numeric($sumTotal) ? (int) $sumTotal : 0;
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
    public function getSequences(string $collection, array $documents): array
    {
        $documentIds = [];
        /** @var array<int> $documentTenants */
        $documentTenants = [];
        foreach ($documents as $document) {
            if (empty($document->getSequence())) {
                $documentIds[] = $document->getId();

                if ($this->sharedTables) {
                    $tenant = $document->getTenant();
                    if ($tenant !== null) {
                        $documentTenants[] = $tenant;
                    }
                }
            }
        }

        if (empty($documentIds)) {
            return $documents;
        }

        $sequences = [];
        $name = $this->getNamespace().'_'.$this->filter($collection);

        $filters = ['_uid' => ['$in' => $documentIds]];

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection, $documentTenants);
        }
        try {
            // Use cursor paging for large result sets
            $options = [
                'projection' => ['_uid' => 1, '_id' => 1],
                'batchSize' => self::DEFAULT_BATCH_SIZE,
            ];

            $options = $this->getTransactionOptions($options);
            $response = $this->client->find($name, $filters, $options);
            /** @var \stdClass $responseCursor */
            $responseCursor = $response->cursor;
            /** @var array<\stdClass> $results */
            $results = $responseCursor->firstBatch ?? [];

            // Process first batch
            foreach ($results as $result) {
                /** @var \stdClass $result */
                /** @var mixed $uid */
                $uid = $result->_uid;
                /** @var mixed $oid */
                $oid = $result->_id;
                $uidStr = \is_string($uid) ? $uid : (\is_scalar($uid) ? (string) $uid : '');
                $oidStr = \is_string($oid) ? $oid : (\is_scalar($oid) ? (string) $oid : '');
                $sequences[$uidStr] = $oidStr;
            }

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

                foreach ($moreResults as $result) {
                    /** @var \stdClass $result */
                    /** @var mixed $uid */
                    $uid = $result->_uid;
                    /** @var mixed $oid */
                    $oid = $result->_id;
                    $uidStr = \is_string($uid) ? $uid : (\is_scalar($uid) ? (string) $uid : '');
                    $oidStr = \is_string($oid) ? $oid : (\is_scalar($oid) ? (string) $oid : '');
                    $sequences[$uidStr] = $oidStr;
                }

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
            if (isset($sequences[$document->getId()])) {
                $document['$sequence'] = $sequences[$document->getId()];
            }
        }

        return $documents;
    }

    /**
     * Get max STRING limit
     */
    public function getLimitForString(): int
    {
        return 2147483647;
    }

    /**
     * Get max INT limit
     */
    public function getLimitForInt(): int
    {
        // Mongo does not handle integers directly, so using MariaDB limit for now
        return 4294967295;
    }

    /**
     * Get maximum column limit.
     * Returns 0 to indicate no limit
     */
    public function getLimitForAttributes(): int
    {
        return 0;
    }

    /**
     * Get maximum index limit.
     * https://docs.mongodb.com/manual/reference/limits/#mongodb-limit-Number-of-Indexes-per-Collection
     */
    public function getLimitForIndexes(): int
    {
        return 64;
    }

    /**
     * Get the maximum combined index key length in bytes.
     *
     * @return int
     */
    public function getMaxIndexLength(): int
    {
        return 1024;
    }

    /**
     * Get the maximum VARCHAR length. MongoDB has no distinction, so returns the same as string limit.
     *
     * @return int
     */
    public function getMaxVarcharLength(): int
    {
        return 2147483647;
    }

    /**
     * Get the maximum length for unique document IDs.
     *
     * @return int
     */
    public function getMaxUIDLength(): int
    {
        return 255;
    }

    /**
     * Get the minimum supported datetime value for MongoDB.
     *
     * @return NativeDateTime
     */
    public function getMinDateTime(): NativeDateTime
    {
        return new NativeDateTime('-9999-01-01 00:00:00');
    }

    /**
     * Get current attribute count from collection document
     */
    public function getCountOfAttributes(Document $collection): int
    {
        $rawAttrCount = $collection->getAttribute('attributes');
        $attrArray = \is_array($rawAttrCount) ? $rawAttrCount : [];
        $attributes = \count($attrArray);

        return $attributes + static::getCountOfDefaultAttributes();
    }

    /**
     * Get current index count from collection document
     */
    public function getCountOfIndexes(Document $collection): int
    {
        $rawIdxCount = $collection->getAttribute('indexes');
        $idxArray = \is_array($rawIdxCount) ? $rawIdxCount : [];
        $indexes = \count($idxArray);

        return $indexes + static::getCountOfDefaultIndexes();
    }

    /**
     * Returns number of attributes used by default.
     *p
     */
    public function getCountOfDefaultAttributes(): int
    {
        return \count(Database::internalAttributes());
    }

    /**
     * Returns number of indexes used by default.
     */
    public function getCountOfDefaultIndexes(): int
    {
        return \count(Database::INTERNAL_INDEXES);
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     */
    public function getDocumentSizeLimit(): int
    {
        return 0;
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
     * Get reserved keywords that cannot be used as identifiers. MongoDB has none.
     *
     * @return array<string>
     */
    public function getKeywords(): array
    {
        return [];
    }

    /**
     * Get the keys of internally managed indexes. MongoDB has none exposed.
     *
     * @return array<string>
     */
    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    /**
     * Get the internal ID attribute type used by MongoDB (UUID v7).
     *
     * @return string
     */
    public function getIdAttributeType(): string
    {
        return ColumnType::Uuid7->value;
    }

    /**
     * Get the query to check for tenant when in shared tables mode
     *
     * @param  string  $collection  The collection being queried
     * @param  string  $alias  The alias of the parent collection if in a subquery
     */
    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        return '';
    }

    /**
     * Check whether the adapter supports storing non-UTF characters. MongoDB does not.
     *
     * @return bool
     */
    public function getSupportNonUtfCharacters(): bool
    {
        return false;
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
     * @param  array<int|string>  $tenants
     * @return int|string|null|array<string, array<int|string>>
     */
    public function getTenantFilters(
        string $collection,
        array $tenants = [],
    ): int|string|null|array {
        if (! $this->sharedTables) {
            return null;
        }

        /** @var array<int|string> $values */
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
            return ['$in' => $values];
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
     * Returns the document after casting to
     *
     * @throws Exception
     */
    public function castingBefore(Document $collection, Document $document): Document
    {
        if ($document->isEmpty()) {
            return $document;
        }

        $rawCbAttributes = $collection->getAttribute('attributes', []);
        /** @var array<int, array<string, mixed>> $cbAttributes */
        $cbAttributes = \is_array($rawCbAttributes) ? $rawCbAttributes : [];

        $internalCbAttributeArrays = \array_map(
            fn (Attribute $a) => ['$id' => $a->key, 'type' => $a->type, 'array' => $a->array],
            Database::internalAttributes()
        );

        /** @var array<int, array<string, mixed>> $attributes */
        $attributes = \array_merge($cbAttributes, $internalCbAttributeArrays);

        foreach ($attributes as $attribute) {
            /** @var array<string, mixed> $attribute */
            $rawCbId = $attribute['$id'] ?? null;
            $key = \is_string($rawCbId) ? $rawCbId : '';
            $rawCbType = $attribute['type'] ?? null;
            $type = $rawCbType instanceof ColumnType
                ? $rawCbType
                : (\is_string($rawCbType) ? ColumnType::tryFrom($rawCbType) : null);
            $array = (bool) ($attribute['array'] ?? false);

            $value = $document->getAttribute($key);
            if (is_null($value)) {
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
            foreach ($value as &$node) {
                switch ($type) {
                    case ColumnType::Datetime:
                        if (! ($node instanceof UTCDateTime)) {
                            /** @var mixed $node */
                            $nodeStr = \is_string($node) ? $node : (\is_scalar($node) ? (string) $node : '');
                            $node = new UTCDateTime(new NativeDateTime($nodeStr));
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
            }
            unset($node);
            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }
        $rawIndexesAttr = $collection->getAttribute('indexes');
        /** @var array<mixed> $indexes */
        $indexes = \is_array($rawIndexesAttr) ? $rawIndexesAttr : [];
        /** @var array<string> $ttlIndexes */
        $ttlIndexes = array_filter($indexes, function ($index) {
            if ($index instanceof Document) {
                return $index->getAttribute('type') === IndexType::Ttl->value;
            }
            return false;
        });

        if (! $this->supports(Capability::DefinedAttributes)) {
            foreach ($document->getArrayCopy() as $key => $value) {
                $key = (string) $key;
                if (in_array($this->getInternalKeyForAttribute($key), Database::INTERNAL_ATTRIBUTE_KEYS)) {
                    continue;
                }
                if (is_string($value) && (in_array($key, $ttlIndexes) || $this->isExtendedISODatetime($value))) {
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
     * Returns the document after casting from
     */
    public function castingAfter(Document $collection, Document $document): Document
    {
        if ($document->isEmpty()) {
            return $document;
        }

        $rawCollectionAttributes = $collection->getAttribute('attributes', []);
        /** @var array<int, array<string, mixed>> $collectionAttributes */
        $collectionAttributes = \is_array($rawCollectionAttributes) ? $rawCollectionAttributes : [];

        $internalAttributeArrays = \array_map(
            fn (Attribute $a) => ['$id' => $a->key, 'type' => $a->type, 'array' => $a->array],
            Database::internalAttributes()
        );

        /** @var array<int, array<string, mixed>> $attributes */
        $attributes = \array_merge($collectionAttributes, $internalAttributeArrays);

        foreach ($attributes as $attribute) {
            /** @var array<string, mixed> $attribute */
            $rawId = $attribute['$id'] ?? null;
            $key = \is_string($rawId) ? $rawId : '';
            $rawType = $attribute['type'] ?? null;
            $type = $rawType instanceof ColumnType
                ? $rawType
                : (\is_string($rawType) ? ColumnType::tryFrom($rawType) : null);
            $array = (bool) ($attribute['array'] ?? false);
            $value = $document->getAttribute($key);
            if (is_null($value)) {
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
            foreach ($value as &$node) {
                switch ($type) {
                    case ColumnType::Integer:
                        $node = \is_int($node)
                            ? $node
                            : ($node instanceof Int64
                                ? (int) (string) $node
                                : (\is_numeric($node) ? (int) $node : 0));
                        break;
                    case ColumnType::String:
                    case ColumnType::Id:
                        $node = \is_string($node) ? $node : (\is_scalar($node) ? (string) $node : $node);
                        break;
                    case ColumnType::Double:
                        $node = \is_float($node) ? $node : (\is_numeric($node) ? (float) $node : 0.0);
                        break;
                    case ColumnType::Boolean:
                        $node = \is_scalar($node) ? (bool) $node : $node;
                        break;
                    case ColumnType::Datetime:
                        $node = $this->convertUTCDateToString($node);
                        break;
                    case ColumnType::Object:
                        // Convert stdClass objects to arrays for object attributes
                        if (is_object($node) && get_class($node) === stdClass::class) {
                            $node = $this->convertStdClassToArray($node);
                        }
                        break;
                    default:
                        break;
                }
            }
            unset($node);
            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }

        if (! $this->supports(Capability::DefinedAttributes)) {
            foreach ($document->getArrayCopy() as $key => $value) {
                // mongodb results out a stdclass for objects
                if (is_object($value) && get_class($value) === stdClass::class) {
                    $document->setAttribute($key, $this->convertStdClassToArray($value));
                } elseif ($value instanceof UTCDateTime) {
                    $document->setAttribute($key, $this->convertUTCDateToString($value));
                }
            }
        }

        return $document;
    }

    /**
     * Convert a datetime string to a MongoDB UTCDateTime object.
     *
     * @param string $value The datetime string
     * @return mixed
     */
    public function setUTCDatetime(string $value): mixed
    {
        return new UTCDateTime(new NativeDateTime($value));
    }

    /**
     * @return array<mixed>
     */
    public function decodePoint(string $wkb): array
    {
        return [];
    }

    /**
     * Decode a WKB or textual LINESTRING into [[x1, y1], [x2, y2], ...]
     *
     * @return float[][] Array of points, each as [x, y]
     */
    public function decodeLinestring(string $wkb): array
    {
        return [];
    }

    /**
     * Decode a WKB or textual POLYGON into [[[x1, y1], [x2, y2], ...], ...]
     *
     * @return float[][][] Array of rings, each ring is an array of points [x, y]
     */
    public function decodePolygon(string $wkb): array
    {
        return [];
    }

    /**
     * TODO Consider moving this to adapter.php
     */
    protected function getInternalKeyForAttribute(string $attribute): string
    {
        return match ($attribute) {
            '$id' => '_uid',
            '$sequence' => '_id',
            '$collection' => '_collection',
            '$tenant' => '_tenant',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            '$permissions' => '_permissions',
            '$version' => '_version',
            default => $attribute
        };
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
        $rawAttrs = $collection->getAttribute('attributes', []);
        /** @var array<array<string, mixed>> $attributes */
        $attributes = \is_array($rawAttrs) ? $rawAttrs : [];
        $dotAttributes = [];
        foreach ($attributes as $attribute) {
            /** @var array<string, mixed> $attribute */
            $rawKey = $attribute['$id'] ?? null;
            $key = \is_string($rawKey) ? $rawKey : (\is_scalar($rawKey) ? (string) $rawKey : '');
            if (\str_contains($key, '.') || \str_starts_with($key, '$')) {
                $dotAttributes[$key] = $this->escapeMongoFieldName($key);
            }
        }

        if (empty($dotAttributes)) {
            return;
        }

        foreach ($queries as $query) {
            $attr = $query->getAttribute();
            if (isset($dotAttributes[$attr])) {
                $query->setAttribute($dotAttributes[$attr]);
            }
        }
    }

    /**
     * Ensure relationship attributes have default null values in MongoDB documents.
     * MongoDB doesn't store null fields, so we need to add them for schema compatibility.
     */
    protected function ensureRelationshipDefaults(Document $collection, Document $document): void
    {
        $rawEnsureAttrs = $collection->getAttribute('attributes', []);
        /** @var array<array<string, mixed>> $attributes */
        $attributes = \is_array($rawEnsureAttrs) ? $rawEnsureAttrs : [];
        foreach ($attributes as $attribute) {
            /** @var array<string, mixed> $attribute */
            $rawEnsureKey = $attribute['$id'] ?? null;
            $key = \is_string($rawEnsureKey) ? $rawEnsureKey : (\is_scalar($rawEnsureKey) ? (string) $rawEnsureKey : '');
            $rawEnsureType = $attribute['type'] ?? null;
            $type = \is_string($rawEnsureType) ? $rawEnsureType : (\is_scalar($rawEnsureType) ? (string) $rawEnsureType : '');
            if ($type === ColumnType::Relationship->value && ! $document->offsetExists($key)) {
                $rawOptions = $attribute['options'] ?? [];
                /** @var array<string, mixed> $options */
                $options = \is_array($rawOptions) ? $rawOptions : [];
                $twoWay = (bool) ($options['twoWay'] ?? false);
                $rawSide = $options['side'] ?? null;
                $side = \is_string($rawSide) ? $rawSide : (\is_scalar($rawSide) ? (string) $rawSide : '');
                $rawRelationType = $options['relationType'] ?? null;
                $relationType = \is_string($rawRelationType) ? $rawRelationType : (\is_scalar($rawRelationType) ? (string) $rawRelationType : '');

                // Determine if this relationship stores data on this collection's documents
                // Only set null defaults for relationships that would have a column in SQL
                $storesData = match ($relationType) {
                    RelationType::OneToOne->value => $side === RelationSide::Parent->value || $twoWay,
                    RelationType::OneToMany->value => $side === RelationSide::Child->value,
                    RelationType::ManyToOne->value => $side === RelationSide::Parent->value,
                    RelationType::ManyToMany->value => false,
                    default => false,
                };

                if ($storesData) {
                    $document->setAttribute($key, null);
                }
            }
        }
    }

    /**
     * Keys cannot begin with $ in MongoDB
     * Convert $ prefix to _ on $id, $permissions, and $collection
     *
     * @param  array<string, mixed>  $array
     * @return array<string, mixed>
     */
    protected function replaceChars(string $from, string $to, array $array): array
    {
        $filter = [
            'permissions',
            'createdAt',
            'updatedAt',
            'collection',
            'version',
        ];

        // First pass: recursively process array values and collect keys to rename
        $keysToRename = [];
        foreach ($array as $k => $v) {
            if (is_array($v)) {
                /** @var array<string, mixed> $v */
                $array[$k] = $this->replaceChars($from, $to, $v);
            }

            $newKey = $k;

            // Handle key replacement for filtered attributes
            $clean_key = str_replace($from, '', $k);
            if (in_array($clean_key, $filter)) {
                $newKey = str_replace($from, $to, $k);
            } elseif (\str_starts_with($k, $from) && ! in_array($k, ['$id', '$sequence', '$tenant', '_uid', '_id', '_tenant'])) {
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
            if (isset($array['_id'])) {
                /** @var mixed $idVal */
                $idVal = $array['_id'];
                $array['$sequence'] = \is_string($idVal) ? $idVal : (\is_scalar($idVal) ? (string) $idVal : '');
                unset($array['_id']);
            }
            if (isset($array['_uid'])) {
                $array['$id'] = $array['_uid'];
                unset($array['_uid']);
            }
            if (isset($array['_tenant'])) {
                $array['$tenant'] = $array['_tenant'];
                unset($array['_tenant']);
            }
        } elseif ($from === '$') {
            if (isset($array['$id'])) {
                $array['_uid'] = $array['$id'];
                unset($array['$id']);
            }
            if (isset($array['$sequence'])) {
                $array['_id'] = $array['$sequence'];
                unset($array['$sequence']);
            }
            if (isset($array['$tenant'])) {
                $array['_tenant'] = $array['$tenant'];
                unset($array['$tenant']);
            }
        }

        return $array;
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
        $queries = Query::groupForDatabase($queries)['filters'];

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
        if (! $this->supports(Capability::DefinedAttributes) || \in_array($query->getAttribute(), ['$createdAt', '$updatedAt'], true)) {
            $values = $query->getValues();
            foreach ($values as $k => $value) {
                if (is_string($value) && $this->isExtendedISODatetime($value)) {
                    try {
                        $values[$k] = $this->toMongoDatetime($value);
                    } catch (Throwable $th) {
                        // Leave value as-is if it cannot be parsed as a datetime
                    }
                }
            }
            $query->setValues($values);
        }

        if ($query->getAttribute() === '$id') {
            $query->setAttribute('_uid');
        } elseif ($query->getAttribute() === '$sequence') {
            $query->setAttribute('_id');
            $values = $query->getValues();
            foreach ($values as $k => $v) {
                $values[$k] = $v;
            }
            $query->setValues($values);
        } elseif ($query->getAttribute() === '$createdAt') {
            $query->setAttribute('_createdAt');
        } elseif ($query->getAttribute() === '$updatedAt') {
            $query->setAttribute('_updatedAt');
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
                    $filter['$or'] = array_map(fn ($val) => [
                        $attribute => [
                            '$regex' => $this->createSafeRegex(
                                \is_string($val) ? $val : (\is_scalar($val) ? (string) $val : ''),
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
            Method::StartsWith => preg_quote(\is_string($value) ? $value : (\is_scalar($value) ? (string) $value : ''), '/').'.*',
            Method::EndsWith => '.*'.preg_quote(\is_string($value) ? $value : (\is_scalar($value) ? (string) $value : ''), '/'),
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
            default => throw new DatabaseException('Unknown sort order:'.$order->value.'. Must be one of '.OrderDirection::Asc->value.', '.OrderDirection::Desc->value),
        };
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
            $indexTypeVal = \is_string($rawIndexType) ? $rawIndexType : (\is_scalar($rawIndexType) ? (string) $rawIndexType : '');
            $indexType = IndexType::tryFrom($indexTypeVal) ?? IndexType::Key;
        } elseif ($indexOrType instanceof IndexType) {
            $indexType = $indexOrType;
        } else {
            $indexType = IndexType::tryFrom($indexOrType) ?? IndexType::Key;
        }

        return $indexType !== IndexType::Ttl;
    }

    /**
     * @param  array<string>  $selections
     */
    protected function getAttributeProjection(array $selections, string $prefix = ''): mixed
    {
        $projection = [];

        $internalKeys = \array_map(
            fn (Attribute $attr) => $attr->key,
            Database::internalAttributes()
        );

        foreach ($selections as $selection) {
            // Skip internal attributes since all are selected by default
            if (\in_array($selection, $internalKeys)) {
                continue;
            }

            $projection[$selection] = 1;
        }

        $projection['_uid'] = 1;
        $projection['_id'] = 1;
        $projection['_createdAt'] = 1;
        $projection['_updatedAt'] = 1;
        $projection['_permissions'] = 1;

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
            $message = $e->getMessage();
            if (! \str_contains($message, '_uid')) {
                return new DuplicateException('Document with the requested unique attributes already exists', $e->getCode(), $e);
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

        return $e;
    }

    protected function quote(string $string): string
    {
        return '';
    }

    protected function execute(mixed $stmt): bool
    {
        return true;
    }

    protected function isExtendedISODatetime(string $val): bool
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
        $len = strlen($val);

        // absolute minimum
        if ($len < 20) {
            return false;
        }

        // fixed datetime fingerprints
        if (
            ! isset($val[19]) ||
            $val[4] !== '-' ||
            $val[7] !== '-' ||
            $val[10] !== 'T' ||
            $val[13] !== ':' ||
            $val[16] !== ':'
        ) {
            return false;
        }

        // timezone detection
        $hasZ = ($val[$len - 1] === 'Z');

        $hasOffset = (
            $len >= 25 &&
            ($val[$len - 6] === '+' || $val[$len - 6] === '-') &&
            $val[$len - 3] === ':'
        );

        if (! $hasZ && ! $hasOffset) {
            return false;
        }

        if ($hasOffset && $len > 31) {
            return false;
        }

        if ($hasZ && $len > 26) {
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

        $timeEnd = $hasZ ? $len - 1 : $len - 6;

        // fractional seconds
        if ($timeEnd > 19) {
            if ($val[19] !== '.' || $timeEnd < 21) {
                return false;
            }
            for ($i = 20; $i < $timeEnd; $i++) {
                $digitPositions[] = $i;
            }
        }

        // timezone offset numeric digits
        if ($hasOffset) {
            foreach ([$len - 5, $len - 4, $len - 2, $len - 1] as $i) {
                $digitPositions[] = $i;
            }
        }

        foreach ($digitPositions as $i) {
            if (! ctype_digit($val[$i])) {
                return false;
            }
        }

        return true;
    }

    protected function convertUTCDateToString(mixed $node): mixed
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
     *
     * @throws DatabaseException
     */
    private function createSafeRegex(string $value, string $pattern = '%s', string $flags = 'i'): Regex
    {
        $escaped = preg_quote($value, '/');

        // Validate that the pattern doesn't contain injection vectors
        if (preg_match('/\$[a-z]+/i', $escaped)) {
            throw new DatabaseException('Invalid regex pattern: potential injection detected');
        }

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
            $filters = ['_uid' => $document['_uid']];

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
     * Converts Appwrite database type to MongoDB BSON type code.
     */
    private function getMongoTypeCode(ColumnType $type): string
    {
        return match ($type) {
            ColumnType::String,
            ColumnType::Varchar,
            ColumnType::Text,
            ColumnType::MediumText,
            ColumnType::LongText,
            ColumnType::Id,
            ColumnType::Uuid7 => 'string',
            ColumnType::Integer => 'int',
            ColumnType::Double => 'double',
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
            return array_map($this->convertStdClassToArray(...), get_object_vars($value));
        }

        if (is_array($value)) {
            return array_map(
                fn ($v) => $this->convertStdClassToArray($v),
                $value
            );
        }

        return $value;
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

        $protectedFields = ['_uid', '_id', '_createdAt', '_updatedAt', '_permissions', '_tenant', '_version'];

        foreach ($oldUserAttributes as $originalKey => $originalValue) {
            if (in_array($originalKey, $protectedFields) || array_key_exists($originalKey, $newUserAttributes)) {
                continue;
            }

            $transformed = $this->replaceChars('$', '_', [$originalKey => $originalValue]);
            $dbKey = array_key_first($transformed);

            if ($dbKey && ! array_key_exists($dbKey, $record) && ! in_array($dbKey, $protectedFields)) {
                $unsetFields[$dbKey] = '';
            }
        }

        return $unsetFields;
    }
}
