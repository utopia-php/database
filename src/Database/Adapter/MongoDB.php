<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use MongoDB\Client as MongoClient;
use MongoDB\Database as MongoDatabase;

class MongoDB extends Adapter
{
    /**
     * @var MongoClient
     */
    protected $client;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param MongoClient $client
     */
    public function __construct(MongoClient $client)
    {
        $this->client = $client;
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
        $name = $this->filter($name);

        return (!!$this->getClient()->selectDatabase($name));
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string $database database name
     * @param string $collection (optional) collection name
     *
     * @return bool
     */
    public function exists(string $database, string $collection = null): bool
    {
        $database = $this->filter($database);

        if (!\is_null($collection)) {
            $collection = $this->filter($collection);

            $match = "{$this->getNamespace()}_{$collection}";
            $names = $this
                ->getClient()
                ->selectDatabase($database)
                ->listCollectionNames([
                    'filter' => [
                        'name' => $match
                    ]
                ]);
        } else {
            $match = $database;
            $names = $this->getClient()
                ->listDatabaseNames([
                    'filter' => [
                        'name' => $match
                    ]
                ]);
        }

        foreach ($names as $name) {
            if ($name === $match) {
                return true;
            }
        }

        return false;
    }

    /**
     * List Databases
     * 
     * @return array
     */
    public function list(): array
    {
        $list = [];

        foreach ($this->getClient()->listDatabaseNames() as $key => $value) {
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
     */
    public function delete(string $name): bool
    {
        $name = $this->filter($name);
        return (!!$this->getClient()->dropDatabase($name));
    }

    /**
     * Create Collection
     * 
     * @param string $name
     * @param Document[] $attributes (optional)
     * @param Document[] $indexes (optional)
     * @return bool
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->getNamespace() .'_'. $this->filter($name);

        $database = $this->getDatabase();

        // Returns an array/object with the result document
        if (empty($database->createCollection($id))) {
            return false;
        }

        $collection = $database->selectCollection($id);

        // Mongo creates an index for _id; _uid (unique, case insensitive) and _read index by default
        // Returns the name of the created index as a string.
        $uid = $collection->createIndex([
            '_uid' => $this->getOrder(Database::ORDER_DESC)],
            [
                'name' => '_uid',
                'unique' => true,
                'collation' => [ // https://docs.mongodb.com/manual/core/index-case-insensitive/#create-a-case-insensitive-index
                    'locale' => 'en',
                    'strength' => 1
                ],
            ]
        );
        $read = $collection->createIndex(['_read' => $this->getOrder(Database::ORDER_DESC)], ['name' => '_read_permissions']);


        if (!$uid || !$read) {
            return false;
        }

        // Since attributes are not used by this adapter
        // Only act when $indexes is provided
        if (!empty($indexes)) {
            /**
             * Each new index has format ['key' => [$attribute => $order], 'name' => $name, 'unique' => $unique]
             * @var array
             */
            $newIndexes = [];

            // using $i and $j as counters to distinguish from $key
            foreach ($indexes as $i => $index) {
                $key = [];
                $name = $this->filter($index->getId());
                $unique = false;

                $attributes = $index->getAttribute('attributes');
                $orders = $index->getAttribute('orders');

                foreach($attributes as $j => $attribute) {
                    $attribute = $this->filter($attribute);

                    switch ($index->getAttribute('type')) {
                        case Database::INDEX_KEY:
                            $order = $this->getOrder($this->filter($orders[$i] ?? Database::ORDER_ASC));
                            break;
                        case Database::INDEX_FULLTEXT:
                            // MongoDB fulltext index is just 'text'
                            // Not using Database::INDEX_KEY for clarity
                            $order = 'text';
                            break;
                        case Database::INDEX_UNIQUE:
                            $order = $this->getOrder($this->filter($orders[$i] ?? Database::ORDER_ASC));
                            $unique = true;
                            break;
                        default:
                            // index not supported
                            return false;
                    }

                    $key[$attribute] = $order;
                }

                $newIndexes[$i] = ['key' => $key, 'name' => $name, 'unique' => $unique];
            }

            if (!$collection->createIndexes($newIndexes)) {
                return false;
            }

        }

        return true;
    }

    /**
     * List Collections
     * 
     * @return array
     */
    public function listCollections(): array
    {
        $list = [];

        foreach ($this->getDatabase()->listCollectionNames() as $key => $value) {
            $list[] = $value;
        }

        return $list;
    }

    /**
     * Delete Collection
     * 
     * @param string $id
     * @return bool
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->getNamespace() .'_'. $this->filter($id);

        return (!!$this->getDatabase()->dropCollection($id));
    }

    /**
     * Create Attribute
     * 
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $array
     * 
     * @return bool
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
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
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        return true;
    }

    /**
     * Create Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array $attributes
     * @param array $lengths
     * @param array $orders
     *
     * @return bool
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool
    {
        $name = $this->getNamespace() .'_'.$this->filter($collection);
        $id = $this->filter($id);
        $collection = $this->getDatabase()->selectCollection($name);

        $indexes = [];
        $options = [];

        // pass in custom index name
        $options['name'] = $id;

        foreach($attributes as $i => $attribute) {
            $attribute = $this->filter($attribute);

            switch ($type) {
                case Database::INDEX_KEY:
                    // ordering for plain indexes
                    $orderType = $this->getOrder($this->filter($orders[$i] ?? Database::ORDER_ASC));
                    $indexes[$attribute] = $orderType;
                    break;
                case Database::INDEX_FULLTEXT:
                    // MongoDB fulltext index is just 'text'
                    // Not using Database::INDEX_KEY for clarity
                    $indexes[$attribute] = 'text';
                    break;
                case Database::INDEX_UNIQUE:
                    $orderType = $this->getOrder($this->filter($orders[$i] ?? Database::ORDER_ASC));
                    $indexes[$attribute] = $orderType;
                    $options['unique'] = true;
                    break;
                default:
                    // index not supported
                    // TODO@kodumbeats handle and test for this case
                    return false;
            }
        }

        return (!!$collection->createIndex($indexes, $options));
    }

    /**
     * Delete Index
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $name = $this->getNamespace() .'_'. $this->filter($collection);
        $id = $this->filter($id);
        $collection = $this->getDatabase()->selectCollection($name);

        return (!!$collection->dropIndex($id));
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return Document
     */
    public function getDocument(string $collection, string $id): Document
    {
        $name = $this->getNamespace() .'_'. $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        $result = $collection->findOne(['_uid' => $id]);

        if(empty($result)) {
            return new Document([]);
        }

        $result = $this->replaceChars('_', '$', $result);

        return new Document($result);

    }

    /**
     * Create Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $name = $this->getNamespace() .'_'. $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        try {
            $collection->insertOne($this->replaceChars('$', '_', $document->getArrayCopy()));
        } catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            switch ($e->getCode()) {
                case 11000:
                    throw new Duplicate('Duplicated document: '.$e->getMessage());
                    break;
                default:
                    throw $e;
                    break;
            }
        }

        return $document;
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     */
    public function updateDocument(string $collection, Document $document): Document
    {
        $name = $this->getNamespace() .'_'. $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        try {
            $result = $collection->findOneAndUpdate(
                ['_uid' => $document->getId()],
                ['$set' => $this->replaceChars('$', '_', $document->getArrayCopy())],
                ['returnDocument' => \MongoDB\Operation\FindOneAndUpdate::RETURN_DOCUMENT_AFTER]
            );
        } catch (\MongoDB\Driver\Exception\CommandException $e) {
            switch ($e->getCode()) {
                case 11000:
                    throw new Duplicate('Duplicated document: '.$e->getMessage());
                    break;
                default:
                    throw $e;
                    break;
            }
        }

        $result = $this->replaceChars('_', '$', $result);

        return new Document($result);
    }

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $name = $this->getNamespace() .'_'. $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        $result = $collection->findOneAndDelete(['_uid' => $id]);

        return (!!$result);
    }

    public function renameAttribute(string $collection, string $id, string $name): bool
    {
        return true;
    }

    /**
     * Find Documents
     *
     * Find data sets using chosen queries
     *
     * @param string $collection
     * @param array $queries
     * @param int $limit
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     * @param array $cursor
     * @param string $cursorDirection
     *
     * @return Document[]
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array
    {
        $name = $this->getNamespace() .'_'. $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        $filters = [];

        $options = ['sort' => [], 'limit' => $limit, 'skip' => $offset];

        // orders
        $orderAttributes = \array_map(function($orderAttribute) {
            return $orderAttribute === '$id' ? '_uid' : $orderAttribute;
        }, $orderAttributes);
        
        foreach($orderAttributes as $i => $attribute) {
            $attribute = $this->filter($attribute);
            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);
            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }
            $options['sort'][$attribute] = $this->getOrder($orderType);
        }

        $options['sort']['_id'] = $this->getOrder($cursorDirection === Database::CURSOR_AFTER ? Database::ORDER_ASC : Database::ORDER_DESC);

        // queries
        $filters = $this->buildFilters($queries);

        if (empty($orderAttributes)) {
            // Allow after pagination without any order
            if(!empty($cursor)) {
                $orderType = $orderTypes[0] ?? Database::ORDER_ASC;
                $orderOperator = $cursorDirection === Database::CURSOR_AFTER ? (
                    $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER
                ) : (
                    $orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER
                );

                $filters = array_merge($filters, [
                    '_id' => [
                        $this->getQueryOperator($orderOperator) => new \MongoDB\BSON\ObjectId($cursor['$internalId'])
                    ]
                ]);
            }
            // Allow order type without any order attribute, fallback to the natural order (_id)
            if(!empty($orderTypes)) {
                $orderType = $this->filter($orderTypes[0] ?? Database::ORDER_ASC);
                if ($cursorDirection === Database::CURSOR_BEFORE) {
                    $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                }
                $options['sort']['_id'] = $this->getOrder($orderType);
            }
        }

        if (!empty($cursor) && !empty($orderAttributes) && array_key_exists(0, $orderAttributes)) {
            $attribute = $orderAttributes[0];
            if (is_null($cursor[$attribute] ?? null)) {
                throw new Exception("Order attribute '{$attribute}' is empty.");
            }

            $orderOperatorInternalId = Query::TYPE_GREATER;
            $orderType = $this->filter($orderTypes[0] ?? Database::ORDER_ASC);
            $orderOperator = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;

            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                $orderOperatorInternalId = $orderType === Database::ORDER_ASC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
                $orderOperator = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
            }

            $filters = array_merge($filters, [
                '$or' => [
                    [
                        $attribute => [
                            $this->getQueryOperator($orderOperator) => $cursor[$attribute]
                        ]
                    ], [
                        $attribute => $cursor[$attribute],
                        '_id' => [
                            $this->getQueryOperator($orderOperatorInternalId) => new \MongoDB\BSON\ObjectId($cursor['$internalId'])
                        ]

                    ]
                ]
            ]);
        }

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $filters['_read']['$in'] = Authorization::getRoles();
        }

        /**
         * @var Document[]
         */
        $found = [];

        $results = $collection->find($filters, $options);

        foreach($results as $i => $result) {
            $found[] = new Document($this->replaceChars('_', '$', $result));
        }

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $found = array_reverse($found);
        }

        return $found;
    }

    /**
     * Count Documents
     * 
     * @param string $collection
     * @param Query[] $queries
     * @param int $max
     *
     * @return int
     */
    public function count(string $collection, array $queries = [], int $max = 0): int
    {
        $name = $this->getNamespace() .'_'. $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        $filters = [];

        $options = [];

        // set max limit
        $options['limit'] = ($max) ? $max : null;

        // queries
        $filters = $this->buildFilters($queries);

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $filters['_read']['$in'] = Authorization::getRoles();
        }

        return $collection->countDocuments($filters, $options);
    }

    /**
     * Sum an attribute
     * 
     * @param string $collection
     * @param string $attribute
     * @param Query[] $queries
     * @param int $max
     *
     * @return int|float
     */
    public function sum(string $collection, string $attribute, array $queries = [], int $max = 0)
    {
        $name = $this->getNamespace() .'_'. $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        $filters = [];

        // queries
        $filters = $this->buildFilters($queries);

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $filters['_read']['$in'] = Authorization::getRoles();
        }

        // using aggregation to get sum an attribute as described in
        // https://docs.mongodb.com/manual/reference/method/db.collection.aggregate/
        // Pipeline consists of stages to aggregation, so first we set $match
        // that will load only documents that matches the filters provided and passes to the next stage
        // then we set $limit (if $max is provided) so that only $max documents will be passed to the next stage
        // finally we use $group stage to sum the provided attribute that matches the given filters and max
        // We pass the $pipeline to the aggregate method, which returns a cursor, then we get
        // the array of results from the cursor and we return the total sum of the attribute
        $pipeline = [];
        if(!empty($filters)) {
            $pipeline[] = ['$match' => $filters];
        }
        if(!empty($max)) {
            $pipeline[] = ['$limit' => $max];
        }
        $pipeline[] = [
                '$group' => [
                    '_id' => null,
                    'total' => ['$sum' => '$' . $attribute],
                ],
        ];
        return ($collection->aggregate($pipeline)->toArray()[0] ?? [])['total'] ?? 0;
    }

    /**
     * @return MongoDatabase
     *
     * @throws Exception
     */
    protected function getDatabase(string $name = null)
    {
        $database = is_null($name) ? $this->getDefaultDatabase() : $name;

        return $this->getClient()->selectDatabase($database);
    }

    /**
     * @return MongoClient
     *
     * @throws Exception
     */
    protected function getClient()
    {
        return $this->client;
    }

    /**
     * Keys cannot begin with $ in MongoDB
     * Convert $ prefix to _ on $id, $read, $write, and $collection
     *
     * @param string $from
     * @param string $to
     * @param array $array
     * @return array
     */
    protected function replaceChars($from, $to, $array): array
    {
        $array[$to.'read'] = $array[$from.'read'];
        $array[$to.'write'] = $array[$from.'write'];
        $array[$to.'collection'] = $array[$from.'collection'];

        if ($from === '_') { // convert internal to document ID
            $array[$to.'id'] = $array[$from.'uid'];
            $array[$to.'internalId'] = (string) $array[$from.'id'];
            unset($array[$from.'uid']);
        } else if ($from === '$') { // convert document to internal ID
            $array[$to.'uid'] = $array[$from.'id'];

            if (array_key_exists($from.'internalId', $array)) {
                unset($array[$from.'internalId']); // remove unnecessary internal ID
            }
        }

        unset($array[$from.'id']);
        unset($array[$from.'read']);
        unset($array[$from.'write']);
        unset($array[$from.'collection']);

        return $array;
    }

    /**
     * Build mongo filters from array of $queries
     *
     * @param Query[] $queries
     *
     * @return array
     */
    protected function buildFilters($queries): array
    {
        $filters = [];

        foreach($queries as $i => $query) {
            if($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }
            $attribute = $query->getAttribute();
            $operator = $this->getQueryOperator($query->getOperator()); 
            $value = (count($query->getValues()) > 1) ? $query->getValues() : $query->getValues()[0]; 

            // TODO@kodumbeats Mongo recommends different methods depending on operator - implement the rest
            if (is_array($value) && $operator === '$eq') {
                $filters[$attribute]['$in'] = $value;
            } elseif ($operator === '$in') {
                $filters[$attribute]['$in'] = $query->getValues();
            } elseif ($operator === '$search') {
                // only one fulltext index per mongo collection, so attribute not necessary
                $filters['$text'][$operator] = $value;
            } else {
                $filters[$attribute][$operator] = $value;
            }
        }

        return $filters;
    }

    /**
     * Get Query Operator
     * 
     * @param string $operator
     * 
     * @return string
     */
    protected function getQueryOperator(string $operator): string
    {
        switch ($operator) {
            case Query::TYPE_EQUAL:
                return '$eq';
                break;

            case Query::TYPE_NOTEQUAL:
                return '$ne';
                break;

            case Query::TYPE_LESSER:
                return '$lt';
                break;

            case Query::TYPE_LESSEREQUAL:
                return '$lte';
                break;

            case Query::TYPE_GREATER:
                return '$gt';
                break;

            case Query::TYPE_GREATEREQUAL:
                return '$gte';
                break;

            case Query::TYPE_CONTAINS:
                return '$in';
                break;

            case Query::TYPE_SEARCH:
                return '$search';
                break;

            default:
                throw new Exception('Unknown Operator:' . $operator);
                break;
        }
    }

    /**
     * Get Mongo Order
     *
     * @param string $order
     *
     * @return int
     */
    protected function getOrder(string $order): int
    {
        switch ($order) {
            case Database::ORDER_ASC:
                return 1;
                break;
            case Database::ORDER_DESC:
                return -1;
                break;
            default:
                throw new Exception('Unknown sort order:' . $order);
                break;
        }
    }

    /**
     * Get max STRING limit
     * 
     * @return int
     */
    public function getStringLimit(): int
    {
        return 2147483647;
    }

    /**
     * Get max INT limit
     * 
     * @return int
     */
    public function getIntLimit(): int
    {
        // Mongo does not handle integers directly, so using MariaDB limit for now
        return 4294967295; 
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
     * Get current index count from collection document
     * 
     * @param Document $collection
     * @return int
     */
    public function getIndexCount(Document $collection): int
    {
        $indexes = \count((array) $collection->getAttribute('indexes') ?? []);
        return $indexes + static::getNumberOfDefaultIndexes();
    }

    /**
     * Get maximum index limit.
     * https://docs.mongodb.com/manual/reference/limits/#mongodb-limit-Number-of-Indexes-per-Collection
     * 
     * @return int
     */
    public function getIndexLimit(): int
    {
        return 64;
    }

    /**
     * Get current attribute count from collection document
     * 
     * @param Document $collection
     * @return int
     */
    public function getAttributeCount(Document $collection): int
    {
        $attributes = \count($collection->getAttribute('attributes') ?? []);
        return $attributes + static::getNumberOfDefaultAttributes();
    }

    /**
     * Get maximum column limit.
     * Returns 0 to indicate no limit
     * 
     * @return int
     */
    public function getAttributeLimit(): int
    {
        return 0;
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    public static function getRowLimit(): int
    {
        return 0;
    }

    public static function getNumberOfDefaultAttributes(): int
    {
        return 4;
    }

    public static function getNumberOfDefaultIndexes(): int
    {
        return 3;
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
}
