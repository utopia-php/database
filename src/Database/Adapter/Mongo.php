<?php

namespace Utopia\Database\Adapter;

use Exception;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Utopia\Database\Adapter;
use Utopia\Database\Change;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;

class Mongo extends Adapter
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
    ];

    protected Client $client;

    /**
     * Default batch size for cursor operations
     */
    private const DEFAULT_BATCH_SIZE = 1000;

    /**
     * Transaction/session state for MongoDB transactions
     * @var array<string, mixed>|null $session
     */
    private ?array $session = null; // Store session array from startSession
    protected int $inTransaction = 0;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param Client $client
     * @throws MongoException
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
        $this->client->connect();
    }

    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        if (!$this->getSupportForTimeouts()) {
            return;
        }

        $this->timeout = $milliseconds;
    }

    public function clearTimeout(string $event): void
    {
        parent::clearTimeout($event);

        $this->timeout = 0;
    }

    /**
     * @template T
     * @param callable(): T $callback
     * @return T
     * @throws \Throwable
     */
    public function withTransaction(callable $callback): mixed
    {
        // If the database is not a replica set, we can't use transactions
        if (!$this->client->isReplicaSet()) {
            $result = $callback();
            return $result;
        }

        try {
            $this->startTransaction();
            $result = $callback();
            $this->commitTransaction();
            return $result;
        } catch (\Throwable $action) {
            try {
                $this->rollbackTransaction();
            } catch (\Throwable) {
                // Throw the original exception, not the rollback one
                // Since if it's a duplicate key error, the rollback will fail,
                // and we want to throw the original exception.
            } finally {
                // Ensure state is cleaned up even if rollback fails
                $this->inTransaction = 0;
                $this->session = null;
            }

            throw $action;
        }
    }

    public function startTransaction(): bool
    {
        // If the database is not a replica set, we can't use transactions
        if (!$this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0) {
                if (!$this->session) {
                    $this->session = $this->client->startSession(); // Get session array
                    $this->client->startTransaction($this->session); // Start the transaction
                }
            }
            $this->inTransaction++;
            return true;
        } catch (\Throwable $e) {
            $this->session = null;
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to start transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    public function commitTransaction(): bool
    {
        // If the database is not a replica set, we can't use transactions
        if (!$this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0) {
                return false;
            }
            $this->inTransaction--;
            if ($this->inTransaction === 0) {
                if (!$this->session) {
                    return false;
                }
                try {
                    $result = $this->client->commitTransaction($this->session);
                } catch (MongoException $e) {
                    // If there's no active transaction, it may have been auto-aborted due to an error.
                    // This is not necessarily a failure, just return success since the transaction was already terminated.
                    $e = $this->processException($e);
                    if ($e instanceof TransactionException) {
                        $this->session = null;
                        $this->inTransaction = 0;  // Reset counter when transaction is already terminated
                        return true;
                    }
                    throw $e;
                } catch (\Throwable $e) {
                    throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                } finally {
                    $this->session = null;
                }

                return true;
            }
            return true;
        } catch (\Throwable $e) {
            // Ensure cleanup on any failure
            $this->session = null;
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to commit transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    public function rollbackTransaction(): bool
    {
        // If the database is not a replica set, we can't use transactions
        if (!$this->client->isReplicaSet()) {
            return true;
        }

        try {
            if ($this->inTransaction === 0) {
                return false;
            }
            $this->inTransaction--;
            if ($this->inTransaction === 0) {
                if (!$this->session) {
                    return false;
                }

                try {
                    $result = $this->client->abortTransaction($this->session);
                } catch (\Throwable $e) {
                    throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                } finally {
                    $this->session = null;
                }

                return true;
            }
            return true;
        } catch (\Throwable $e) {
            $this->session = null;
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to rollback transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Helper to add transaction/session context to command options if in transaction
     * Includes defensive check to ensure session is valid
     *
     * @param array<string, mixed> $options
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
     * @param string $value The user input to escape
     * @param string $pattern The pattern template (e.g., ".*%s.*" for contains)
     * @return Regex
     * @throws DatabaseException
     */
    private function createSafeRegex(string $value, string $pattern = '%s', string $flags = 'i'): Regex
    {
        $escaped = preg_quote($value, '/');

        // Additional MongoDB-specific escaping for $ and \ to prevent injection
        $escaped = str_replace(['\\', '$'], ['\\\\', '\\$'], $escaped);

        // Validate that the pattern doesn't contain injection vectors
        if (preg_match('/\$[a-z]+/i', $escaped)) {
            throw new DatabaseException('Invalid regex pattern: potential injection detected');
        }

        $finalPattern = sprintf($pattern, $escaped);

        return new Regex($finalPattern, $flags);
    }

    /**
     * Ping Database
     *
     * @return bool
     * @throws Exception
     * @throws MongoException
     */
    public function ping(): bool
    {
        return $this->getClient()->query([
            'ping' => 1,
            'skipReadConcern' => true
        ])->ok ?? false;
    }

    public function reconnect(): void
    {
        $this->client->connect();
    }

    /**
     * Create Database
     *
     * @param string $name
     *
     * @return bool
     */
    public function create(string $name): bool
    {
        return true;
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string $database database name
     * @param string|null $collection (optional) collection name
     *
     * @return bool
     * @throws Exception
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        if (!\is_null($collection)) {
            $collection = $this->getNamespace() . "_" . $collection;
            try {
                // Use listCollections command with filter for O(1) lookup
                $result = $this->getClient()->query([
                    'listCollections' => 1,
                    'filter' => ['name' => $collection]
                ]);

                return !empty($result->cursor->firstBatch);
            } catch (\Exception $e) {
                return false;
            }
        }

        return $this->getClient()->selectDatabase() != null;
    }

    /**
     * List Databases
     *
     * @return array<Document>
     * @throws Exception
     */
    public function list(): array
    {
        $list = [];

        foreach ((array)$this->getClient()->listDatabaseNames() as $value) {
            $list[] = $value;
        }

        return $list;
    }

    /**
     * Delete Database
     *
     * @param string $name
     *
     * @return bool
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
     * @param string $name
     * @param array<Document> $attributes
     * @param array<Document> $indexes
     * @return bool
     * @throws Exception
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->getNamespace() . '_' . $this->filter($name);

        // For metadata collections outside transactions, check if exists first
        if (!$this->inTransaction && $name === Database::METADATA && $this->exists($this->getNamespace(), $name)) {
            return true;
        }

        // Returns an array/object with the result document
        try {
            $options = $this->getTransactionOptions();
            $this->getClient()->createCollection($id, $options);

        } catch (MongoException $e) {
            $processed = $this->processException($e);
            if ($processed instanceof DuplicateException) {
                return true;
            }
            throw $processed;
        }

        $internalIndex = [
            [
                'key' => ['_uid' => $this->getOrder(Database::ORDER_ASC)],
                'name' => '_uid',
                'unique' => true,
                'collation' => [
                    'locale' => 'en',
                    'strength' => 1,
                ]
            ],
            [
                'key' => ['_createdAt' => $this->getOrder(Database::ORDER_ASC)],
                'name' => '_createdAt',
            ],
            [
                'key' => ['_updatedAt' => $this->getOrder(Database::ORDER_ASC)],
                'name' => '_updatedAt',
            ],
            [
                'key' => ['_permissions' => $this->getOrder(Database::ORDER_ASC)],
                'name' => '_permissions',
            ]
        ];

        if ($this->sharedTables) {
            foreach ($internalIndex as &$index) {
                $index['key'] = array_merge(['_tenant' => $this->getOrder(Database::ORDER_ASC)], $index['key']);
            }
            unset($index);
        }

        try {
            $options = $this->getTransactionOptions();
            $indexesCreated = $this->client->createIndexes($id, $internalIndex, $options);
        } catch (\Exception $e) {
            throw $this->processException($e);
        }

        if (!$indexesCreated) {
            return false;
        }

        // Since attributes are not used by this adapter
        // Only act when $indexes is provided

        if (!empty($indexes)) {
            /**
             * Each new index has format ['key' => [$attribute => $order], 'name' => $name, 'unique' => $unique]
             */
            $newIndexes = [];

            $collectionAttributes = $attributes;

            // using $i and $j as counters to distinguish from $key
            foreach ($indexes as $i => $index) {

                $key = [];
                $unique = false;
                $attributes = $index->getAttribute('attributes');
                $orders = $index->getAttribute('orders');

                // If sharedTables, always add _tenant as the first key
                if ($this->sharedTables) {
                    $key['_tenant'] = $this->getOrder(Database::ORDER_ASC);
                }

                foreach ($attributes as $j => $attribute) {
                    $attribute = $this->filter($this->getInternalKeyForAttribute($attribute));

                    switch ($index->getAttribute('type')) {
                        case Database::INDEX_KEY:
                            $order = $this->getOrder($this->filter($orders[$j] ?? Database::ORDER_ASC));
                            break;
                        case Database::INDEX_FULLTEXT:
                            // MongoDB fulltext index is just 'text'
                            // Not using Database::INDEX_KEY for clarity
                            $order = 'text';
                            break;
                        case Database::INDEX_UNIQUE:
                            $order = $this->getOrder($this->filter($orders[$j] ?? Database::ORDER_ASC));
                            $unique = true;
                            break;
                        default:
                            // index not supported
                            return false;
                    }

                    $key[$attribute] = $order;
                }

                $newIndexes[$i] = [
                    'key' => $key,
                    'name' => $this->filter($index->getId()),
                    'unique' => $unique
                ];

                if ($index->getAttribute('type') === Database::INDEX_FULLTEXT) {
                    $newIndexes[$i]['default_language'] = 'none';
                }

                // Add partial filter for indexes to avoid indexing null values
                if (in_array($index->getAttribute('type'), [
                    Database::INDEX_UNIQUE,
                    Database::INDEX_KEY
                ])) {
                    $partialFilter = [];
                    foreach ($attributes as $attr) {
                        // Find the matching attribute in collectionAttributes to get its type
                        $attrType = 'string'; // Default fallback
                        foreach ($collectionAttributes as $collectionAttr) {
                            if ($collectionAttr->getId() === $attr) {
                                $attrType = $this->getMongoTypeCode($collectionAttr->getAttribute('type'));
                                break;
                            }
                        }

                        $attr = $this->filter($this->getInternalKeyForAttribute($attr));

                        // Use both $exists: true and $type to exclude nulls and ensure correct type
                        $partialFilter[$attr] = [
                            '$exists' => true,
                            '$type' => $attrType
                        ];
                    }
                    if (!empty($partialFilter)) {
                        $newIndexes[$i]['partialFilterExpression'] = $partialFilter;
                    }
                }
            }

            try {
                $options = $this->getTransactionOptions();
                $indexesCreated = $this->getClient()->createIndexes($id, $newIndexes, $options);
            } catch (\Exception $e) {
                throw $this->processException($e);
            }

            if (!$indexesCreated) {
                return false;
            }
        }

        return true;
    }

    /**
     * List Collections
     *
     * @return array<Document>
     * @throws Exception
     */
    public function listCollections(): array
    {
        $list = [];

        // Note: listCollections is a metadata operation that should not run in transactions
        // to avoid transaction conflicts and readConcern issues
        foreach ((array)$this->getClient()->listCollectionNames() as $value) {
            $list[] = $value;
        }

        return $list;
    }

    /**
     * Get Collection Size on disk
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->getSizeOfCollection($collection);
    }

    /**
     * Get Collection Size of raw data
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollection(string $collection): int
    {
        $namespace = $this->getNamespace();
        $collection = $this->filter($collection);
        $collection = $namespace . '_' . $collection;

        $command = [
            'collStats' => $collection,
            'scale' => 1
        ];

        try {
            $result = $this->getClient()->query($command);
            if (is_object($result)) {
                return $result->totalSize;
            } else {
                throw new DatabaseException('No size found');
            }
        } catch (Exception $e) {
            throw new DatabaseException('Failed to get collection size: ' . $e->getMessage());
        }
    }

    /**
     * Delete Collection
     *
     * @param string $id
     * @return bool
     * @throws Exception
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->getNamespace() . '_' . $this->filter($id);
        return (!!$this->getClient()->dropCollection($id));
    }

    /**
     * Analyze a collection updating it's metadata on the database engine
     *
     * @param string $collection
     * @return bool
     */
    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

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
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        return true;
    }

    /**
     * Create Attributes
     *
     * @param string $collection
     * @param array<array<string, mixed>> $attributes
     * @return bool
     * @throws DatabaseException
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        return true;
    }

    /**
     * Delete Attribute
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     * @throws DatabaseException
     * @throws MongoException
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $collection = $this->getNamespace() . '_' . $this->filter($collection);

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
     * @param string $collection
     * @param string $id
     * @param string $name
     * @return bool
     * @throws DatabaseException
     * @throws MongoException
     */
    public function renameAttribute(string $collection, string $id, string $name): bool
    {
        $collection = $this->getNamespace() . '_' . $this->filter($collection);

        $from    = $this->filter($this->getInternalKeyForAttribute($id));
        $to      = $this->filter($this->getInternalKeyForAttribute($name));
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
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $id
     * @param string $twoWayKey
     * @return bool
     */
    public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool
    {
        return true;
    }

    /**
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
     * @throws DatabaseException
     * @throws MongoException
     */
    public function updateRelationship(
        string $collection,
        string $relatedCollection,
        string $type,
        bool $twoWay,
        string $key,
        string $twoWayKey,
        string $side,
        ?string $newKey = null,
        ?string $newTwoWayKey = null
    ): bool {
        $collection = $this->getNamespace() . '_' . $this->filter($collection);
        $relatedCollection = $this->getNamespace() . '_' . $this->filter($relatedCollection);

        $renameKey = [
            '$rename' => [
                $key => $newKey,
            ]
        ];

        $renameTwoWayKey = [
            '$rename' => [
                $twoWayKey => $newTwoWayKey,
            ]
        ];

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                if (!\is_null($newKey)) {
                    $this->getClient()->update($collection, updates: $renameKey, multi: true);
                }
                if ($twoWay && !\is_null($newTwoWayKey)) {
                    $this->getClient()->update($relatedCollection, updates: $renameTwoWayKey, multi: true);
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($twoWay && !\is_null($newTwoWayKey)) {
                    $this->getClient()->update($relatedCollection, updates: $renameTwoWayKey, multi: true);
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if (!\is_null($newKey)) {
                    $this->getClient()->update($collection, updates: $renameKey, multi: true);
                }
                break;
            case Database::RELATION_MANY_TO_MANY:
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collection = $this->getDocument($metadataCollection, $collection);
                $relatedCollection = $this->getDocument($metadataCollection, $relatedCollection);

                if ($collection->isEmpty() || $relatedCollection->isEmpty()) {
                    throw new DatabaseException('Collection or related collection not found');
                }

                $junction = $this->getNamespace() . '_' . $this->filter('_' . $collection->getSequence() . '_' . $relatedCollection->getSequence());

                if (!\is_null($newKey)) {
                    $this->getClient()->update($junction, updates: $renameKey, multi: true);
                }
                if ($twoWay && !\is_null($newTwoWayKey)) {
                    $this->getClient()->update($junction, updates: $renameTwoWayKey, multi: true);
                }
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    /**
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $key
     * @param string $twoWayKey
     * @param string $side
     * @return bool
     * @throws MongoException
     * @throws Exception
     */
    public function deleteRelationship(
        string $collection,
        string $relatedCollection,
        string $type,
        bool $twoWay,
        string $key,
        string $twoWayKey,
        string $side
    ): bool {
        $junction = $this->getNamespace() . '_' . $this->filter('_' . $collection . '_' . $relatedCollection);
        $collection = $this->getNamespace() . '_' . $this->filter($collection);
        $relatedCollection = $this->getNamespace() . '_' . $this->filter($relatedCollection);

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                $this->getClient()->update($collection, [], ['$unset' => [$key => '']], multi: true);
                if ($twoWay) {
                    $this->getClient()->update($relatedCollection, [], ['$unset' => [$twoWayKey => '']], multi: true);
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $this->getClient()->update($collection, [], ['$unset' => [$key => '']], multi: true);
                } else {
                    $this->getClient()->update($relatedCollection, [], ['$unset' => [$twoWayKey => '']], multi: true);
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    $this->getClient()->update($collection, [], ['$unset' => [$key => '']], multi: true);
                } else {
                    $this->getClient()->update($relatedCollection, [], ['$unset' => [$twoWayKey => '']], multi: true);
                }
                break;
            case Database::RELATION_MANY_TO_MANY:
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
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     * @param array<int> $lengths
     * @param array<string> $orders
     * @param array<string, string> $indexAttributeTypes
     * @param array<string, mixed> $collation
     * @return bool
     * @throws Exception
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = []): bool
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);
        $id = $this->filter($id);
        $indexes = [];
        $options = [];
        $indexes['name'] = $id;

        // If sharedTables, always add _tenant as the first key
        if ($this->sharedTables) {
            $indexes['key']['_tenant'] = $this->getOrder(Database::ORDER_ASC);
        }

        foreach ($attributes as $i => $attribute) {

            $attributes[$i] = $this->filter($this->getInternalKeyForAttribute($attribute));

            $orderType = $this->getOrder($this->filter($orders[$i] ?? Database::ORDER_ASC));
            $indexes['key'][$attributes[$i]] = $orderType;

            switch ($type) {
                case Database::INDEX_KEY:
                    break;
                case Database::INDEX_FULLTEXT:
                    $indexes['key'][$attributes[$i]] = 'text';
                    break;
                case Database::INDEX_UNIQUE:
                    $indexes['unique'] = true;
                    break;
                default:
                    return false;
            }
        }

        /**
         * Collation
         *  1.  Moved under $indexes.
         *  2.  Updated format.
         *  3.  Avoid adding collation to fulltext index
         */
        if (!empty($collation) &&
            $type !== Database::INDEX_FULLTEXT) {
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
        if ($type === Database::INDEX_FULLTEXT) {
            $indexes['default_language'] = 'none';
        }

        // Add partial filter for indexes to avoid indexing null values
        if (in_array($type, [Database::INDEX_UNIQUE, Database::INDEX_KEY])) {
            $partialFilter = [];
            foreach ($attributes as $i => $attr) {
                $attrType = $indexAttributeTypes[$i] ?? Database::VAR_STRING; // Default to string if type not provided
                $attrType = $this->getMongoTypeCode($attrType);
                $partialFilter[$attr] = ['$exists' => true, '$type' => $attrType];
            }
            if (!empty($partialFilter)) {
                $indexes['partialFilterExpression'] = $partialFilter;
            }
        }
        try {
            $result = $this->client->createIndexes($name, [$indexes], $options);

            // Wait for unique index to be fully built before returning
            // MongoDB builds indexes asynchronously, so we need to wait for completion
            // to ensure unique constraints are enforced immediately
            if ($type === Database::INDEX_UNIQUE) {
                $maxRetries = 10;
                $retryCount = 0;
                $baseDelay = 50000; // 50ms
                $maxDelay = 500000; // 500ms

                while ($retryCount < $maxRetries) {
                    try {
                        $indexList = $this->client->query([
                            'listIndexes' => $name
                        ]);

                        if (isset($indexList->cursor->firstBatch)) {
                            foreach ($indexList->cursor->firstBatch as $existingIndex) {
                                $indexArray = $this->client->toArray($existingIndex);

                                if (
                                    (isset($indexArray['name']) && $indexArray['name'] === $id) &&
                                    (!isset($indexArray['buildState']) || $indexArray['buildState'] === 'ready')
                                ) {
                                    return $result;
                                }
                            }
                        }
                    } catch (\Exception $e) {
                        if ($retryCount >= $maxRetries - 1) {
                            throw new DatabaseException(
                                'Timeout waiting for index creation: ' . $e->getMessage(),
                                $e->getCode(),
                                $e
                            );
                        }
                    }

                    $delay = \min($baseDelay * (2 ** $retryCount), $maxDelay);
                    \usleep((int)$delay);
                    $retryCount++;
                }

                throw new DatabaseException("Index {$id} creation timed out after {$maxRetries} retries");
            }

            return $result;
        } catch (\Exception $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Rename Index.
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     *
     * @return bool
     * @throws Exception
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $metadataCollection = new Document(['$id' => Database::METADATA]);
        $collectionDocument = $this->getDocument($metadataCollection, $collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $indexes = json_decode($collectionDocument['indexes'], true);
        $index = null;

        foreach ($indexes as $node) {
            if ($node['key'] === $old) {
                $index = $node;
                break;
            }
        }

        // Extract attribute types from the collection document
        $indexAttributeTypes = [];
        if (isset($collectionDocument['attributes'])) {
            $attributes = json_decode($collectionDocument['attributes'], true);
            if ($attributes && $index) {
                // Map index attributes to their types
                foreach ($index['attributes'] as $attrName) {
                    foreach ($attributes as $attr) {
                        if ($attr['key'] === $attrName) {
                            $indexAttributeTypes[$attrName] = $attr['type'];
                            break;
                        }
                    }
                }
            }
        }

        try {
            $deletedindex = $this->deleteIndex($collection, $old);
            $createdindex = $this->createIndex($collection, $new, $index['type'], $index['attributes'], $index['lengths'] ?? [], $index['orders'] ?? [], $indexAttributeTypes);
        } catch (\Exception $e) {
            throw $this->processException($e);
        }

        if ($index && $deletedindex && $createdindex) {
            return true;
        }

        return false;
    }

    /**
     * Delete Index
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     * @throws Exception
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);
        $id = $this->filter($id);
        $this->getClient()->dropIndexes($name, [$id]);

        return true;
    }

    /**
     * Get Document
     *
     * @param Document $collection
     * @param string $id
     * @param Query[] $queries
     * @param bool $forUpdate
     * @return Document
     * @throws DatabaseException
     */
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection->getId());

        $filters = ['_uid' => $id];

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection->getId());
        }


        $options = [];

        $selections = $this->getAttributeSelections($queries);

        if (!empty($selections) && !\in_array('*', $selections)) {
            $options['projection'] = $this->getAttributeProjection($selections);
        }

        try {
            $result = $this->client->find($name, $filters, $options)->cursor->firstBatch;
        } catch (MongoException $e) {
            throw $this->processException($e);
        }

        if (empty($result)) {
            return new Document([]);
        }

        $result = $this->replaceChars('_', '$', (array)$result[0]);

        return new Document($result);
    }

    /**
     * Create Document
     *
     * @param Document $collection
     * @param Document $document
     *
     * @return Document
     * @throws Exception
     */
    public function createDocument(Document $collection, Document $document): Document
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection->getId());

        $sequence = $document->getSequence();

        $document->removeAttribute('$sequence');

        if ($this->sharedTables) {
            $document->setAttribute('$tenant', $this->getTenant());
        }

        $record = $this->replaceChars('$', '_', (array)$document);

        // Insert manual id if set
        if (!empty($sequence)) {
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
     * Returns the document after casting from
     * @param Document $collection
     * @param Document $document
     * @return Document
     */
    public function castingAfter(Document $collection, Document $document): Document
    {
        if (!$this->getSupportForInternalCasting()) {
            return $document;
        }

        if ($document->isEmpty()) {
            return $document;
        }

        $attributes = $collection->getAttribute('attributes', []);

        $attributes = \array_merge($attributes, Database::INTERNAL_ATTRIBUTES);

        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? '';
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;
            $value = $document->getAttribute($key);
            if (is_null($value)) {
                continue;
            }

            if ($array) {
                if (is_string($value)) {
                    $decoded = json_decode($value, true);
                    if (json_last_error() !== JSON_ERROR_NONE) {
                        throw new DatabaseException('Failed to decode JSON for attribute ' . $key . ': ' . json_last_error_msg());
                    }
                    $value = $decoded;
                }
            } else {
                $value = [$value];
            }

            foreach ($value as &$node) {
                switch ($type) {
                    case Database::VAR_INTEGER:
                        $node = (int)$node;
                        break;
                    case Database::VAR_DATETIME :
                        if ($node instanceof UTCDateTime) {
                            $node = DateTime::format($node->toDateTime());
                        }
                        break;
                    default:
                        break;
                }
            }
            unset($node);
            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }

        return $document;
    }

    /**
     * Returns the document after casting to
     * @param Document $collection
     * @param Document $document
     * @return Document
     * @throws Exception
     */
    public function castingBefore(Document $collection, Document $document): Document
    {
        if (!$this->getSupportForInternalCasting()) {
            return $document;
        }

        if ($document->isEmpty()) {
            return $document;
        }

        $attributes = $collection->getAttribute('attributes', []);

        $attributes = \array_merge($attributes, Database::INTERNAL_ATTRIBUTES);

        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? '';
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;

            $value = $document->getAttribute($key);
            if (is_null($value)) {
                continue;
            }

            if ($array) {
                if (is_string($value)) {
                    $decoded = json_decode($value, true);
                    if (json_last_error() !== JSON_ERROR_NONE) {
                        throw new DatabaseException('Failed to decode JSON for attribute ' . $key . ': ' . json_last_error_msg());
                    }
                    $value = $decoded;
                }
            } else {
                $value = [$value];
            }

            foreach ($value as &$node) {
                switch ($type) {
                    case Database::VAR_DATETIME:
                        if (!($node instanceof UTCDateTime)) {
                            $node = new UTCDateTime(new \DateTime($node));
                        }
                        break;
                    default:
                        break;
                }
            }
            unset($node);
            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }

        return $document;
    }

    /**
     * Create Documents in batches
     *
     * @param Document $collection
     * @param array<Document> $documents
     *
     * @return array<Document>
     *
     * @throws DuplicateException
     * @throws DatabaseException
     */
    public function createDocuments(Document $collection, array $documents): array
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection->getId());

        $options = $this->getTransactionOptions();
        $records = [];
        $hasSequence = null;
        $documents = \array_map(fn ($doc) => clone $doc, $documents);

        foreach ($documents as $document) {
            $sequence = $document->getSequence();

            if ($hasSequence === null) {
                $hasSequence = !empty($sequence);
            } elseif ($hasSequence == empty($sequence)) {
                throw new DatabaseException('All documents must have an sequence if one is set');
            }

            $record = $this->replaceChars('$', '_', (array)$document);

            if (!empty($sequence)) {
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
            $documents[$index] = $this->replaceChars('_', '$', $this->client->toArray($document));
            $documents[$index] = new Document($documents[$index]);
        }

        return $documents;
    }

    /**
     *
     * @param string $name
     * @param array<string, mixed> $document
     * @param array<string, mixed> $options
     *
     * @return array<string, mixed>
     * @throws DuplicateException
     * @throws Exception
     */
    private function insertDocument(string $name, array $document, array $options = []): array
    {
        try {
            $result = $this->client->insert($name, $document, $options);
            $filters = [];
            $filters['_uid'] = $document['_uid'];

            if ($this->sharedTables) {
                $filters['_tenant'] = $this->getTenantFilters($name);
            }

            try {
                $result = $this->client->find(
                    $name,
                    $filters,
                    array_merge(['limit' => 1], $options)
                )->cursor->firstBatch[0];
            } catch (MongoException $e) {
                throw $this->processException($e);
            }

            return $this->client->toArray($result);
        } catch (MongoException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Update Document
     *
     * @param Document $collection
     * @param string $id
     * @param Document $document
     * @param bool $skipPermissions
     * @return Document
     * @throws DuplicateException
     * @throws DatabaseException
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection->getId());

        $record = $document->getArrayCopy();
        $record = $this->replaceChars('$', '_', $record);

        $filters = [];
        $filters['_uid'] = $id;

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection->getId());
        }

        try {
            unset($record['_id']); // Don't update _id

            $options = $this->getTransactionOptions();
            $this->client->update($name, $filters, $record, $options);
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
     * @param Document $collection
     * @param Document $updates
     * @param array<Document> $documents
     *
     * @return int
     *
     * @throws DatabaseException
     */
    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection->getId());

        $options = $this->getTransactionOptions();
        $queries = [
            Query::equal('$sequence', \array_map(fn ($document) => $document->getSequence(), $documents))
        ];

        $filters = $this->buildFilters($queries);

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection->getId());
        }

        $record = $updates->getArrayCopy();
        $record = $this->replaceChars('$', '_', $record);

        $updateQuery = [
            '$set' => $record,
        ];

        try {
            return $this->client->update(
                $name,
                $filters,
                $updateQuery,
                options: $options,
                multi: true,
            );
        } catch (MongoException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * @param Document $collection
     * @param string $attribute
     * @param array<Change> $changes
     * @return array<Document>
     * @throws DatabaseException
     */
    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        if (empty($changes)) {
            return $changes;
        }

        try {
            $name = $this->getNamespace() . '_' . $this->filter($collection->getId());
            $attribute = $this->filter($attribute);

            $operations = [];
            foreach ($changes as $change) {
                $document = $change->getNew();
                $attributes = $document->getAttributes();
                $attributes['_uid'] = $document->getId();
                $attributes['_createdAt'] = $document['$createdAt'];
                $attributes['_updatedAt'] = $document['$updatedAt'];
                $attributes['_permissions'] = $document->getPermissions();

                if (!empty($document->getSequence())) {
                    $attributes['_id'] = $document->getSequence();
                }

                if ($this->sharedTables) {
                    $attributes['_tenant'] = $document->getTenant();
                }

                $record = $this->replaceChars('$', '_', $attributes);

                // Build filter for upsert
                $filters = ['_uid' => $document->getId()];

                if ($this->sharedTables) {
                    $filters['_tenant'] = $this->getTenantFilters($collection->getId());
                }

                unset($record['_id']); // Don't update _id

                if (!empty($attribute)) {
                    // Get the attribute value before removing it from $set
                    $attributeValue = $record[$attribute] ?? 0;

                    // Remove the attribute from $set since we're incrementing it
                    // it is requierd to mimic the behaver of SQL on duplicate key update
                    unset($record[$attribute]);

                    // Increment the specific attribute and update all other fields
                    $update = [
                        '$inc' => [$attribute => $attributeValue],
                        '$set' => $record
                    ];
                } else {
                    // Update all fields
                    $update = [
                        '$set' => $record
                    ];

                    // Add UUID7 _id for new documents in upsert operations
                    if (empty($document->getSequence())) {
                        $update['$setOnInsert'] = [
                            '_id' => $this->client->createUuid()
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
     * Get sequences for documents that were created
     *
     * @param string $collection
     * @param array<Document> $documents
     * @return array<Document>
     * @throws DatabaseException
     * @throws MongoException
     */
    public function getSequences(string $collection, array $documents): array
    {
        $documentIds = [];
        $documentTenants = [];
        foreach ($documents as $document) {
            if (empty($document->getSequence())) {
                $documentIds[] = $document->getId();

                if ($this->sharedTables) {
                    $documentTenants[] = $document->getTenant();
                }
            }
        }

        if (empty($documentIds)) {
            return $documents;
        }

        $sequences = [];
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        $filters = ['_uid' => ['$in' => $documentIds]];

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection, $documentTenants);
        }
        try {
            // Use cursor paging for large result sets
            $options = [
                'projection' => ['_uid' => 1, '_id' => 1],
                'batchSize' => self::DEFAULT_BATCH_SIZE
            ];

            $options = $this->getTransactionOptions($options);
            $response = $this->client->find($name, $filters, $options);
            $results = $response->cursor->firstBatch ?? [];

            // Process first batch
            foreach ($results as $result) {
                $sequences[$result->_uid] = (string)$result->_id;
            }

            // Get cursor ID for subsequent batches
            $cursorId = $response->cursor->id ?? null;

            // Continue fetching with getMore
            while ($cursorId && $cursorId !== 0) {
                $moreResponse = $this->client->getMore((int)$cursorId, $name, self::DEFAULT_BATCH_SIZE);
                $moreResults = $moreResponse->cursor->nextBatch ?? [];

                if (empty($moreResults)) {
                    break;
                }

                foreach ($moreResults as $result) {
                    $sequences[$result->_uid] = (string)$result->_id;
                }

                // Update cursor ID for next iteration
                $cursorId = (int)($moreResponse->cursor->id ?? 0);
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
     * Increase or decrease an attribute value
     *
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param string $updatedAt
     * @param int|float|null $min
     * @param int|float|null $max
     * @return bool
     * @throws DatabaseException
     * @throws MongoException
     * @throws Exception
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, string $updatedAt, int|float|null $min = null, int|float|null $max = null): bool
    {
        $attribute = $this->filter($attribute);
        $filters = ['_uid' => $id];

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection);
        }

        if ($max) {
            $filters[$attribute] = ['$lte' => $max];
        }

        if ($min) {
            $filters[$attribute] = ['$gte' => $min];
        }

        $options = $this->getTransactionOptions();
        $this->client->update(
            $this->getNamespace() . '_' . $this->filter($collection),
            $filters,
            [
                '$inc' => [$attribute => $value],
                '$set' => ['_updatedAt' => $this->toMongoDatetime($updatedAt)],
            ],
            options: $options
        );

        return true;
    }

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     * @throws Exception
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        $filters = [];
        $filters['_uid'] = $id;

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection);
        }

        $options = $this->getTransactionOptions();
        $result = $this->client->delete($name, $filters, 1, [], $options);

        return (!!$result);
    }

    /**
     * Delete Documents
     *
     * @param string $collection
     * @param array<string> $sequences
     * @param array<string> $permissionIds
     * @return int
     * @throws DatabaseException
     */
    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        foreach ($sequences as $index => $sequence) {
            $sequences[$index] = $sequence;
        }

        $filters = $this->buildFilters([new Query(Query::TYPE_EQUAL, '_id', $sequences)]);

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection);
        }

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
     * Update Attribute.
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @param bool $array
     * @param string $newKey
     *
     * @return bool
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false): bool
    {
        if (!empty($newKey) && $newKey !== $id) {
            return $this->renameAttribute($collection, $id, $newKey);
        }
        return true;
    }

    /**
     * TODO Consider moving this to adapter.php
     * @param string $attribute
     * @return string
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
            default => $attribute
        };
    }


    /**
     * Find Documents
     *
     * Find data sets using chosen queries
     *
     * @param Document $collection
     * @param array<Query> $queries
     * @param int|null $limit
     * @param int|null $offset
     * @param array<string> $orderAttributes
     * @param array<string> $orderTypes
     * @param array<string, mixed> $cursor
     * @param string $cursorDirection
     * @param string $forPermission
     *
     * @return array<Document>
     * @throws Exception
     * @throws TimeoutException
     */
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection->getId());
        $queries = array_map(fn ($query) => clone $query, $queries);

        $filters = $this->buildFilters($queries);

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection->getId());
        }

        // permissions
        if (Authorization::$status) {
            $roles = \implode('|', Authorization::getRoles());
            $filters['_permissions']['$in'] = [new Regex("{$forPermission}\\(\".*(?:{$roles}).*\"\\)", 'i')];
        }

        $options = [];

        if (!\is_null($limit)) {
            $options['limit'] = $limit;
        }
        if (!\is_null($offset)) {
            $options['skip'] = $offset;
        }

        if ($this->timeout) {
            $options['maxTimeMS'] = $this->timeout;
        }

        $selections = $this->getAttributeSelections($queries);
        if (!empty($selections) && !\in_array('*', $selections)) {
            $options['projection'] = $this->getAttributeProjection($selections);
        }

        // Add transaction context to options
        $options = $this->getTransactionOptions($options);

        $orFilters = [];

        foreach ($orderAttributes as $i => $originalAttribute) {
            $attribute = $this->getInternalKeyForAttribute($originalAttribute);
            $attribute = $this->filter($attribute);

            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);
            $direction = $orderType;

            /** Get sort direction  ASC || DESC **/
            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $direction = ($direction === Database::ORDER_ASC)
                    ? Database::ORDER_DESC
                    : Database::ORDER_ASC;
            }

            $options['sort'][$attribute] = $this->getOrder($direction);

            /** Get operator sign  '$lt' ? '$gt' **/
            $operator = $cursorDirection === Database::CURSOR_AFTER
                ? ($orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER)
                : ($orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER);

            $operator = $this->getQueryOperator($operator);

            if (!empty($cursor)) {

                $andConditions = [];
                for ($j = 0; $j < $i; $j++) {
                    $originalPrev = $orderAttributes[$j];
                    $prevAttr = $this->filter($this->getInternalKeyForAttribute($originalPrev));
                    $tmp = $cursor[$originalPrev];
                    $andConditions[] = [
                        $prevAttr => $tmp
                    ];
                }

                $tmp = $cursor[$originalAttribute];

                if ($originalAttribute === '$sequence') {
                    /** If there is only $sequence attribute in $orderAttributes skip Or And  operators **/
                    if (count($orderAttributes) === 1) {
                        $filters[$attribute] = [
                            $operator => $tmp
                        ];
                        break;
                    }
                }

                $andConditions[] = [
                    $attribute => [
                        $operator => $tmp
                    ]
                ];

                $orFilters[] = [
                    '$and' => $andConditions
                ];
            }
        }

        if (!empty($orFilters)) {
            $filters['$or'] = $orFilters;
        }

        // Translate operators and handle time filters
        $filters = $this->replaceInternalIdsKeys($filters, '$', '_', $this->operators);

        $found = [];
        $cursorId = null;

        try {
            // Use proper cursor iteration with reasonable batch size
            $options['batchSize'] = self::DEFAULT_BATCH_SIZE;

            $response = $this->client->find($name, $filters, $options);
            $results = $response->cursor->firstBatch ?? [];
            // Process first batch
            foreach ($results as $result) {
                $record = $this->replaceChars('_', '$', (array)$result);
                $found[] = new Document($record);
            }

            // Get cursor ID for subsequent batches
            $cursorId = $response->cursor->id ?? null;

            // Continue fetching with getMore
            while ($cursorId && $cursorId !== 0) {
                $moreResponse = $this->client->getMore((int)$cursorId, $name, self::DEFAULT_BATCH_SIZE);
                $moreResults = $moreResponse->cursor->nextBatch ?? [];

                if (empty($moreResults)) {
                    break;
                }

                foreach ($moreResults as $result) {
                    $record = $this->replaceChars('_', '$', (array)$result);
                    $found[] = new Document($record);
                }

                $cursorId = (int)($moreResponse->cursor->id ?? 0);
            }

        } catch (MongoException $e) {
            throw $this->processException($e);
        } finally {
            // Ensure cursor is killed if still active to prevent resource leak
            if (isset($cursorId) && $cursorId !== 0) {
                try {
                    $this->client->query([
                        'killCursors' => $name,
                        'cursors' => [(int)$cursorId]
                    ]);
                } catch (\Exception $e) {
                    // Ignore errors during cursor cleanup
                }
            }
        }

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $found = array_reverse($found);
        }

        return $found;
    }


    /**
     * Converts Appwrite database type to MongoDB BSON type code.
     *
     * @param string $appwriteType
     * @return string
     */
    private function getMongoTypeCode(string $appwriteType): string
    {
        return match ($appwriteType) {
            Database::VAR_STRING => 'string',
            Database::VAR_INTEGER => 'int',
            Database::VAR_FLOAT => 'double',
            Database::VAR_BOOLEAN => 'bool',
            Database::VAR_DATETIME => 'date',
            Database::VAR_ID => 'string',
            Database::VAR_UUID7 => 'string',
            default => 'string'
        };
    }

    /**
     * Converts timestamp to Mongo\BSON datetime format.
     *
     * @param string $dt
     * @return UTCDateTime
     * @throws Exception
     */
    private function toMongoDatetime(string $dt): UTCDateTime
    {
        return new UTCDateTime(new \DateTime($dt));
    }

    /**
     * Recursive function to replace chars in array keys, while
     * skipping any that are explicitly excluded.
     *
     * @param array<string, mixed> $array
     * @param string $from
     * @param string $to
     * @param array<string> $exclude
     * @return array<string, mixed>
     */
    private function replaceInternalIdsKeys(array $array, string $from, string $to, array $exclude = []): array
    {
        $result = [];

        foreach ($array as $key => $value) {
            if (!in_array($key, $exclude)) {
                $key = str_replace($from, $to, $key);
            }

            $result[$key] = is_array($value)
                ? $this->replaceInternalIdsKeys($value, $from, $to, $exclude)
                : $value;
        }

        return $result;
    }


    /**
     * Count Documents
     *
     * @param Document $collection
     * @param array<Query> $queries
     * @param int|null $max
     * @return int
     * @throws Exception
     */
    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection->getId());

        $queries = array_map(fn ($query) => clone $query, $queries);

        $filters = [];
        $options = [];

        if (!\is_null($max) && $max > 0) {
            $options['limit'] = $max;
        }

        if ($this->timeout) {
            $options['maxTimeMS'] = $this->timeout;
        }

        // Build filters from queries
        $filters = $this->buildFilters($queries);

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection->getId());
        }

        // Add permissions filter if authorization is enabled
        if (Authorization::$status) {
            $roles = \implode('|', Authorization::getRoles());
            $filters['_permissions']['$in'] = [new Regex("read\\(\".*(?:{$roles}).*\"\\)", 'i')];
        }

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
        if (!empty($filters)) {
            $pipeline[] = ['$match' => $this->client->toObject($filters)];
        }

        // Add limit stage if specified
        if (!\is_null($max) && $max > 0) {
            $pipeline[] = ['$limit' => $max];
        }

        // Use $group and $sum when limit is specified, $count when no limit
        // Note: $count stage doesn't works well with $limit in the same pipeline
        // When limit is specified, we need to use $group + $sum to count the limited documents
        if (!\is_null($max) && $max > 0) {
            // When limit is specified, use $group and $sum to count limited documents
            $pipeline[] = [
                '$group' => [
                    '_id' => null,
                    'total' => ['$sum' => 1]]
            ];
        } else {
            // When no limit is passed, use $count for better performance
            $pipeline[] = [
                '$count' => 'total'
            ];
        }

        try {

            $result = $this->client->aggregate($name, $pipeline, $options);

            // Aggregation returns stdClass with cursor property containing firstBatch
            if (isset($result->cursor) && !empty($result->cursor->firstBatch)) {
                $firstResult = $result->cursor->firstBatch[0];

                // Handle both $count and $group response formats
                if (isset($firstResult->total)) {
                    return (int)$firstResult->total;
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
     * @param Document $collection
     * @param string $attribute
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int|float
     * @throws Exception
     */

    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection->getId());

        // queries
        $queries = array_map(fn ($query) => clone $query, $queries);
        $filters = $this->buildFilters($queries);

        if ($this->sharedTables) {
            $filters['_tenant'] = $this->getTenantFilters($collection->getId());
        }

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $roles = \implode('|', Authorization::getRoles());
            $filters['_permissions']['$in'] = [new Regex("read\\(\".*(?:{$roles}).*\"\\)", 'i')];
        }

        // using aggregation to get sum an attribute as described in
        // https://docs.mongodb.com/manual/reference/method/db.collection.aggregate/
        // Pipeline consists of stages to aggregation, so first we set $match
        // that will load only documents that matches the filters provided and passes to the next stage
        // then we set $limit (if $max is provided) so that only $max documents will be passed to the next stage
        // finally we use $group stage to sum the provided attribute that matches the given filters and max
        // We pass the $pipeline to the aggregate method, which returns a cursor, then we get
        // the array of results from the cursor, and we return the total sum of the attribute
        $pipeline = [];
        if (!empty($filters)) {
            $pipeline[] = ['$match' => $filters];
        }
        if (!empty($max)) {
            $pipeline[] = ['$limit' => $max];
        }
        $pipeline[] = [
            '$group' => [
                '_id' => null,
                'total' => ['$sum' => '$' . $attribute],
            ],
        ];

        $options = $this->getTransactionOptions();
        return $this->client->aggregate($name, $pipeline, $options)->cursor->firstBatch[0]->total ?? 0;
    }

    /**
     * @return Client
     *
     * @throws Exception
     */
    protected function getClient(): Client
    {
        return $this->client;
    }

    /**
     * Keys cannot begin with $ in MongoDB
     * Convert $ prefix to _ on $id, $permissions, and $collection
     *
     * @param string $from
     * @param string $to
     * @param array<string, mixed> $array
     * @return array<string, mixed>
     */
    protected function replaceChars(string $from, string $to, array $array): array
    {
        $filter = [
            'permissions',
            'createdAt',
            'updatedAt',
            'collection'
        ];

        // Process in-place with references to avoid array copies
        foreach ($array as $k => &$v) {
            if (is_array($v)) {
                $v = $this->replaceChars($from, $to, $v);
            }

            // Handle key replacement for filtered attributes
            $clean_key = str_replace($from, "", $k);
            if (in_array($clean_key, $filter)) {
                $new_key = str_replace($from, $to, $k);
                if ($new_key !== $k) {
                    $array[$new_key] = $v;
                    unset($array[$k]);
                }
            }
        }
        unset($v); // Break reference

        // Handle special attribute mappings
        if ($from === '_') {
            if (isset($array['_id'])) {
                $array['$sequence'] = (string)$array['_id'];
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
     * @param array<Query> $queries
     * @param string $separator
     * @return array<mixed>
     * @throws Exception
     */
    protected function buildFilters(array $queries, string $separator = '$and'): array
    {
        $filters = [];
        $queries = Query::groupByType($queries)['filters'];

        foreach ($queries as $query) {
            /* @var $query Query */
            if ($query->isNested()) {
                $operator = $this->getQueryOperator($query->getMethod());

                $filters[$separator][] = $this->buildFilters($query->getValues(), $operator);
            } else {
                $filters[$separator][] = $this->buildFilter($query);
            }
        }

        return $filters;
    }

    /**
     * @param Query $query
     * @return array<mixed>
     * @throws Exception
     */
    protected function buildFilter(Query $query): array
    {
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
        }

        $attribute = $query->getAttribute();
        $operator = $this->getQueryOperator($query->getMethod());

        $value = match ($query->getMethod()) {
            Query::TYPE_IS_NULL,
            Query::TYPE_IS_NOT_NULL => null,
            default => $this->getQueryValue(
                $query->getMethod(),
                count($query->getValues()) > 1
                    ? $query->getValues()
                    : $query->getValues()[0]
            ),
        };

        $filter = [];

        if ($operator == '$eq' && \is_array($value)) {
            $filter[$attribute]['$in'] = $value;
        } elseif ($operator == '$ne' && \is_array($value)) {
            $filter[$attribute]['$nin'] = $value;
        } elseif ($operator == '$in') {
            if ($query->getMethod() === Query::TYPE_CONTAINS && !$query->onArray()) {
                $filter[$attribute]['$regex'] = $this->createSafeRegex($value, '.*%s.*');
            } else {
                $filter[$attribute]['$in'] = $query->getValues();
            }
        } elseif ($operator === 'notContains') {
            if (!$query->onArray()) {
                $filter[$attribute] = ['$not' => $this->createSafeRegex($value, '.*%s.*')];
            } else {
                $filter[$attribute]['$nin'] = $query->getValues();
            }
        } elseif ($operator == '$search') {
            if ($query->getMethod() === Query::TYPE_NOT_SEARCH) {
                // MongoDB doesn't support negating $text expressions directly
                // Use regex as fallback for NOT search while keeping fulltext for positive search
                if (empty($value)) {
                    // If value is not passed, don't add any filter - this will match all documents
                } else {
                    $filter[$attribute] = ['$not' => $this->createSafeRegex($value, '.*%s.*')];
                }
            } else {
                $filter['$text'][$operator] = $value;
            }
        } elseif ($operator === Query::TYPE_BETWEEN) {
            $filter[$attribute]['$lte'] = $value[1];
            $filter[$attribute]['$gte'] = $value[0];
        } elseif ($operator === Query::TYPE_NOT_BETWEEN) {
            $filter['$or'] = [
                [$attribute => ['$lt' => $value[0]]],
                [$attribute => ['$gt' => $value[1]]]
            ];
        } elseif ($operator === '$regex' && $query->getMethod() === Query::TYPE_NOT_STARTS_WITH) {
            $filter[$attribute] = ['$not' => $this->createSafeRegex($value, '^%s')];
        } elseif ($operator === '$regex' && $query->getMethod() === Query::TYPE_NOT_ENDS_WITH) {
            $filter[$attribute] = ['$not' => $this->createSafeRegex($value, '%s$')];
        } else {
            $filter[$attribute][$operator] = $value;
        }

        return $filter;
    }

    /**
     * Get Query Operator
     *
     * @param string $operator
     *
     * @return string
     * @throws Exception
     */
    protected function getQueryOperator(string $operator): string
    {
        return match ($operator) {
            Query::TYPE_EQUAL,
            Query::TYPE_IS_NULL => '$eq',
            Query::TYPE_NOT_EQUAL,
            Query::TYPE_IS_NOT_NULL => '$ne',
            Query::TYPE_LESSER => '$lt',
            Query::TYPE_LESSER_EQUAL => '$lte',
            Query::TYPE_GREATER => '$gt',
            Query::TYPE_GREATER_EQUAL => '$gte',
            Query::TYPE_CONTAINS => '$in',
            Query::TYPE_NOT_CONTAINS => 'notContains',
            Query::TYPE_SEARCH => '$search',
            Query::TYPE_NOT_SEARCH => '$search',
            Query::TYPE_BETWEEN => 'between',
            Query::TYPE_NOT_BETWEEN => 'notBetween',
            Query::TYPE_STARTS_WITH,
            Query::TYPE_NOT_STARTS_WITH,
            Query::TYPE_ENDS_WITH,
            Query::TYPE_NOT_ENDS_WITH => '$regex',
            Query::TYPE_OR => '$or',
            Query::TYPE_AND => '$and',
            default => throw new DatabaseException('Unknown operator:' . $operator . '. Must be one of ' . Query::TYPE_EQUAL . ', ' . Query::TYPE_NOT_EQUAL . ', ' . Query::TYPE_LESSER . ', ' . Query::TYPE_LESSER_EQUAL . ', ' . Query::TYPE_GREATER . ', ' . Query::TYPE_GREATER_EQUAL . ', ' . Query::TYPE_IS_NULL . ', ' . Query::TYPE_IS_NOT_NULL . ', ' . Query::TYPE_BETWEEN . ', ' . Query::TYPE_NOT_BETWEEN . ', ' . Query::TYPE_STARTS_WITH . ', ' . Query::TYPE_NOT_STARTS_WITH . ', ' . Query::TYPE_ENDS_WITH . ', ' . Query::TYPE_NOT_ENDS_WITH . ', ' . Query::TYPE_CONTAINS . ', ' . Query::TYPE_NOT_CONTAINS . ', ' . Query::TYPE_SEARCH . ', ' . Query::TYPE_NOT_SEARCH . ', ' . Query::TYPE_SELECT),
        };
    }

    protected function getQueryValue(string $method, mixed $value): mixed
    {
        switch ($method) {
            case Query::TYPE_STARTS_WITH:
                $value = preg_quote($value, '/');
                $value = str_replace(['\\', '$'], ['\\\\', '\\$'], $value);
                return $value . '.*';
            case Query::TYPE_NOT_STARTS_WITH:
                return $value;
            case Query::TYPE_ENDS_WITH:
                $value = preg_quote($value, '/');
                $value = str_replace(['\\', '$'], ['\\\\', '\\$'], $value);
                return '.*' . $value;
            case Query::TYPE_NOT_ENDS_WITH:
                return $value;
            default:
                return $value;
        }
    }

    /**
     * Get Mongo Order
     *
     * @param string $order
     *
     * @return int
     * @throws Exception
     */
    protected function getOrder(string $order): int
    {
        return match ($order) {
            Database::ORDER_ASC => 1,
            Database::ORDER_DESC => -1,
            default => throw new DatabaseException('Unknown sort order:' . $order . '. Must be one of ' . Database::ORDER_ASC . ', ' . Database::ORDER_DESC),
        };
    }

    /**
     * @param array<string> $selections
     * @param string $prefix
     * @return mixed
     */
    protected function getAttributeProjection(array $selections, string $prefix = ''): mixed
    {
        $projection = [];

        $internalKeys = \array_map(
            fn ($attr) => $attr['$id'],
            Database::INTERNAL_ATTRIBUTES
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
     * Get max STRING limit
     *
     * @return int
     */
    public function getLimitForString(): int
    {
        return 2147483647;
    }

    /**
     * Get max INT limit
     *
     * @return int
     */
    public function getLimitForInt(): int
    {
        // Mongo does not handle integers directly, so using MariaDB limit for now
        return 4294967295;
    }

    /**
     * Get maximum column limit.
     * Returns 0 to indicate no limit
     *
     * @return int
     */
    public function getLimitForAttributes(): int
    {
        return 0;
    }

    /**
     * Get maximum index limit.
     * https://docs.mongodb.com/manual/reference/limits/#mongodb-limit-Number-of-Indexes-per-Collection
     *
     * @return int
     */
    public function getLimitForIndexes(): int
    {
        return 64;
    }

    public function getMinDateTime(): \DateTime
    {
        return new \DateTime('-9999-01-01 00:00:00');
    }

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    public function getSupportForSchemas(): bool
    {
        return false;
    }

    /**
     * Is index supported?
     *
     * @return bool
     */
    public function getSupportForIndex(): bool
    {
        return true;
    }

    public function getSupportForIndexArray(): bool
    {
        return true;
    }

    /**
     * Is internal casting supported?
     *
     * @return bool
     */
    public function getSupportForInternalCasting(): bool
    {
        return true;
    }

    public function getSupportForUTCCasting(): bool
    {
        return true;
    }

    public function setUTCDatetime(string $value): mixed
    {
        return new UTCDateTime(new \DateTime($value));
    }


    /**
     * Are attributes supported?
     *
     * @return bool
     */
    public function getSupportForAttributes(): bool
    {
        return true;
    }

    /**
     * Is unique index supported?
     *
     * @return bool
     */
    public function getSupportForUniqueIndex(): bool
    {
        return true;
    }

    /**
     * Is fulltext index supported?
     *
     * @return bool
     */
    public function getSupportForFulltextIndex(): bool
    {
        return true;
    }

    /**
     * Is fulltext Wildcard index supported?
     *
     * @return bool
     */
    public function getSupportForFulltextWildcardIndex(): bool
    {
        return false;
    }

    /**
     * Does the adapter handle Query Array Contains?
     *
     * @return bool
     */
    public function getSupportForQueryContains(): bool
    {
        return false;
    }

    /**
     * Are timeouts supported?
     *
     * @return bool
     */
    public function getSupportForTimeouts(): bool
    {
        return true;
    }

    public function getSupportForRelationships(): bool
    {
        return false;
    }

    public function getSupportForUpdateLock(): bool
    {
        return false;
    }

    public function getSupportForAttributeResizing(): bool
    {
        return false;
    }

    /**
     * Are batch operations supported?
     *
     * @return bool
     */
    public function getSupportForBatchOperations(): bool
    {
        return false;
    }

    /**
     * Is get connection id supported?
     *
     * @return bool
     */
    public function getSupportForGetConnectionId(): bool
    {
        return false;
    }

    /**
     * Is cache fallback supported?
     *
     * @return bool
     */
    public function getSupportForCacheSkipOnFailure(): bool
    {
        return false;
    }

    /**
     * Is hostname supported?
     *
     * @return bool
     */
    public function getSupportForHostname(): bool
    {
        return true;
    }

    /**
     * Is get schema attributes supported?
     *
     * @return bool
     */
    public function getSupportForSchemaAttributes(): bool
    {
        return false;
    }

    public function getSupportForCastIndexArray(): bool
    {
        return false;
    }

    public function getSupportForUpserts(): bool
    {
        return true;
    }

    public function getSupportForReconnection(): bool
    {
        return false;
    }

    public function getSupportForBatchCreateAttributes(): bool
    {
        return true;
    }

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfAttributes(Document $collection): int
    {
        $attributes = \count($collection->getAttribute('attributes') ?? []);

        return $attributes + static::getCountOfDefaultAttributes();
    }

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfIndexes(Document $collection): int
    {
        $indexes = \count($collection->getAttribute('indexes') ?? []);

        return $indexes + static::getCountOfDefaultIndexes();
    }

    /**
     * Returns number of attributes used by default.
     *p
     * @return int
     */
    public function getCountOfDefaultAttributes(): int
    {
        return \count(Database::INTERNAL_ATTRIBUTES);
    }

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    public function getCountOfDefaultIndexes(): int
    {
        return \count(Database::INTERNAL_INDEXES);
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
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
     *
     * @param Document $collection
     * @return int
     */
    public function getAttributeWidth(Document $collection): int
    {
        return 0;
    }

    /**
     * Is casting supported?
     *
     * @return bool
     */
    public function getSupportForCasting(): bool
    {
        return true;
    }

    /**
     * Is spatial attributes supported?
     *
     * @return bool
     */
    public function getSupportForSpatialAttributes(): bool
    {
        return false;
    }

    /**
     * Get Support for Null Values in Spatial Indexes
     *
     * @return bool
     */
    public function getSupportForSpatialIndexNull(): bool
    {
        return false;
    }

    /**
     * Does the adapter includes boundary during spatial contains?
     *
     * @return bool
     */

    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return false;
    }

    /**
     * Does the adapter support order attribute in spatial indexes?
     *
     * @return bool
     */
    public function getSupportForSpatialIndexOrder(): bool
    {
        return false;
    }


    /**
     * Does the adapter support spatial axis order specification?
     *
     * @return bool
     */
    public function getSupportForSpatialAxisOrder(): bool
    {
        return false;
    }

    /**
     * Does the adapter support calculating distance(in meters) between multidimension geometry(line, polygon,etc)?
     *
     * @return bool
     */
    public function getSupportForDistanceBetweenMultiDimensionGeometryInMeters(): bool
    {
        return false;
    }

    public function getSupportForOptionalSpatialAttributeWithExistingRows(): bool
    {
        return false;
    }

    /**
     * Does the adapter support multiple fulltext indexes?
     *
     * @return bool
     */
    public function getSupportForMultipleFulltextIndexes(): bool
    {
        return false;
    }

    /**
     * Does the adapter support identical indexes?
     *
     * @return bool
     */
    public function getSupportForIdenticalIndexes(): bool
    {
        return false;
    }

    /**
     * Does the adapter support random order for queries?
     *
     * @return bool
     */
    public function getSupportForOrderRandom(): bool
    {
        return false;
    }

    /**
     * Flattens the array.
     *
     * @param mixed $list
     * @return array<mixed>
     */
    protected function flattenArray(mixed $list): array
    {
        if (!is_array($list)) {
            // make sure the input is an array
            return array($list);
        }

        $newArray = [];

        foreach ($list as $value) {
            $newArray = array_merge($newArray, $this->flattenArray($value));
        }

        return $newArray;
    }

    /**
     * @param array<string, mixed>|Document $target
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

    public function getKeywords(): array
    {
        return [];
    }

    protected function processException(Exception $e): \Exception
    {
        // Timeout
        if ($e->getCode() === 50) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Duplicate key error
        if ($e->getCode() === 11000) {
            return new DuplicateException('Document already exists', $e->getCode(), $e);
        }

        // Duplicate key error for unique index
        if ($e->getCode() === 11001) {
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

        return $e;
    }

    protected function quote(string $string): string
    {
        return "";
    }

    /**
     * @param mixed $stmt
     * @return bool
     */
    protected function execute(mixed $stmt): bool
    {
        return true;
    }

    /**
     * @return string
     */
    public function getIdAttributeType(): string
    {
        return Database::VAR_UUID7;
    }

    /**
     * @return int
     */
    public function getMaxIndexLength(): int
    {
        return 1024;
    }

    public function getConnectionId(): string
    {
        return '0';
    }

    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    public function getSchemaAttributes(string $collection): array
    {
        return [];
    }

    /**
     * @param string $collection
     * @param array<int> $tenants
     * @return int|null|array<string, array<int>>
     */
    public function getTenantFilters(
        string $collection,
        array $tenants = [],
    ): int|null|array {
        $values = [];
        if (!$this->sharedTables) {
            return $values;
        }

        if (\count($tenants) === 0) {
            $values[] = $this->getTenant();
        } else {
            for ($index = 0; $index < \count($tenants); $index++) {
                $values[] = $tenants[$index];
            }
        }

        if ($collection === Database::METADATA) {
            $values[] = null;
        }

        if (\count($values) === 1) {
            return $values[0];
        }


        return ['$in' => $values];
    }

    public function decodePoint(string $wkb): array
    {
        return [];
    }

    /**
     * Decode a WKB or textual LINESTRING into [[x1, y1], [x2, y2], ...]
     *
     * @param string $wkb
     * @return float[][] Array of points, each as [x, y]
     */
    public function decodeLinestring(string $wkb): array
    {
        return [];
    }

    /**
     * Decode a WKB or textual POLYGON into [[[x1, y1], [x2, y2], ...], ...]
     *
     * @param string $wkb
     * @return float[][][] Array of rings, each ring is an array of points [x, y]
     */
    public function decodePolygon(string $wkb): array
    {
        return [];
    }

    /**
     * Get the query to check for tenant when in shared tables mode
     *
     * @param string $collection The collection being queried
     * @param string $alias The alias of the parent collection if in a subquery
     * @return string
     */
    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        return '';
    }


}
