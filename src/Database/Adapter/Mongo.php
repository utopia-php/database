<?php

namespace Utopia\Database\Adapter;

use Exception;
use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Utopia\Database\Adapter;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Query;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Mongo\Client;

class Mongo extends Adapter
{
    /**
     * @var array
     */
    private array $operators = [
        '$eq',
        '$ne',
        '$lt',
        '$lte',
        '$gt',
        '$gte',
        '$in',
        '$text',
        '$search',
        '$or',
        '$and',
        '$match',
    ];

    protected Client $client;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
        $this->client->connect();
    }

    /**
     * @throws Exception
     */
    public function hello()
    {
        return $this->getClient()->query(['hello' => 1]);
    }

    /**
     * Ping Database
     *
     * @return bool
     */
    public function ping(): bool
    {
        return $this->getClient()->query(['ping' => 1])->ok ?? false;
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
    public function exists(string $database, string $collection = null): bool
    {
        if (!\is_null($collection)) {
            $collection = $this->getNamespace() . "_" . $collection;
            $list = $this->flattenArray($this->listCollections())[0]->firstBatch;
            foreach ($list as $obj) {
                if (\is_object($obj)) {
                    if ($obj->name == $collection) {
                        return true;
                    }
                }
            }

            return false;
        }

        return !\is_null($this->getClient()->selectDatabase());
    }

    /**
     * List Databases
     *
     * @return array
     * @throws Exception
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
     * @param Document[] $attributes (optional)
     * @param Document[] $indexes (optional)
     * @return bool
     * @throws Exception
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->getNamespace() . '_' . $this->filter($name);

        if($name === Database::METADATA && $this->exists($this->getNamespace(), $name)) {
            return true;
        }
        
        // Returns an array/object with the result document
        try {
            $this->getClient()->createCollection($id);
        } catch (MongoException $e) {
            throw $e;
        }

        $indexesCreated = $this->client->createIndexes($id, [
            [
                'key' => ['_uid' => $this->getOrder(Database::ORDER_DESC)],
                'name' => '_uid',
                'unique' => true,
                'collation' => [ // https://docs.mongodb.com/manual/core/index-case-insensitive/#create-a-case-insensitive-index
                    'locale' => 'en',
                    'strength' => 1,
                ]
            ],
            [
                'key' => ['_permissions' => $this->getOrder(Database::ORDER_DESC)],
                'name' => '_permissions',
            ]
        ]);

        if (!$indexesCreated) {
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
                $unique = false;
                $attributes = $index->getAttribute('attributes');
                $orders = $index->getAttribute('orders');

                foreach ($attributes as $attribute) {
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

                $newIndexes[$i] = ['key' => $key, 'name' => $this->filter($index->getId()), 'unique' => $unique];
            }

            if (!$this->getClient()->createIndexes($id, $newIndexes)) {
                return false;
            }
        }

        return true;
    }

    /**
     * List Collections
     *
     * @return array
     * @throws Exception
     */
    public function listCollections(): array
    {
        $list = [];

        foreach ($this->getClient()->listCollectionNames() as $key => $value) {
            $list[] = $value;
        }

        return $list;
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
     * Create Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $signed
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
     * Rename Attribute.
     *
     * @param string $collection
     * @param string $id
     * @param string $name
     * @return bool
     */
    public function renameAttribute(string $collection, string $id, string $name): bool
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
     * @param array $collation
     * @return bool
     * @throws Exception
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $collation = []): bool
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);
        $id = $this->filter($id);

        $indexes = [];
        $options = [];

        // pass in custom index name
        $indexes['name'] = $id;

        foreach ($attributes as $i => $attribute) {
            $attribute = $this->filter($attribute);

            $orderType = $this->getOrder($this->filter($orders[$i] ?? Database::ORDER_ASC));
            $indexes['key'][$attribute] = $orderType;

            switch ($type) {
                case Database::INDEX_KEY:
                    break;
                case Database::INDEX_FULLTEXT:
                    $indexes['key'][$attribute] = 'text';
                    break;
                case Database::INDEX_UNIQUE:
                    $indexes['unique'] = true;
                    break;
                default:
                    return false;
            }
        }

        if (!empty($collation)) {
            $options['collation'] = $collation;
        }

        return $this->client->createIndexes($name, [$indexes], $options);
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
        $collection = $this->getDatabase();

        return (!!$collection->dropIndexes($name, [$id]));
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return Document
     * @throws Exception
     */
    public function getDocument(string $collection, string $id): Document
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        $result = $this->client->find($name, ['_uid' => $id], ['limit' => 1])->cursor->firstBatch;

        if (empty($result)) {
            return new Document([]);
        }

        $result = $this->replaceChars('_', '$', (array)$result[0]);
        $result = $this->timeToDocument($result);

        return new Document($result);
    }

    /**
     * Create Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     * @throws Exception
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);
        $document->removeAttribute('$internalId');

        $record = $this->replaceChars('$', '_', (array)$document);
        $record = $this->timeToMongo($record);

        $result = $this->insertDocument($name, $this->removeNullKeys($record));
        $result = $this->replaceChars('_', '$', $result);
        $result = $this->timeToDocument($result);

        return new Document($result);
    }

    /**
     * @throws Duplicate
     */
    private function insertDocument(string $name, array $document): array
    {
        try {
            $this->client->insert($name, $document);

            $result = $this->client->find(
                $name,
                ['_uid' => $document['_uid']],
                ['limit' => 1]
            )->cursor->firstBatch[0];

            return $this->client->toArray($result);

        } catch (MongoException $e) {
            throw new Duplicate($e->getMessage());
        }
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     * @throws Exception
     */
    public function updateDocument(string $collection, Document $document): Document
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        $record = $document->getArrayCopy();
        $record = $this->replaceChars('$', '_', $record);
        $record = $this->timeToMongo($record);

        try {
            $this->client->update($name, ['_uid' => $document->getId()], $record);
        } catch (MongoException $e) {
            throw new Duplicate($e->getMessage());
        }

        return $document;
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

        $result = $this->client->delete($name, ['_uid' => $id]);

        return (!!$result);
    }

    /**
     * Rename Index.
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     *
     * @return bool
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        return true;
    }

    /**
     * Update Attribute.
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @param bool $array
     *
     * @return bool
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
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
     * @throws Exception
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        $filters = $this->buildFilters($queries);

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $roles = \implode('|', Authorization::getRoles());
            $filters['_permissions']['$in'] = [new Regex("read\(\".*(?:{$roles}).*\"\)", 'i')];
        }

        $options = ['limit' => $limit, 'skip' => $offset];

        // orders
        foreach ($orderAttributes as $i => $attribute) {
            $attribute = $this->filter($attribute);
            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);
            
            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }

            $attribute = $attribute == 'id' ? "_uid" : $attribute;
            $attribute = $attribute == 'createdAt' ? "_createdAt" : $attribute;
            $attribute = $attribute == 'updatedAt' ? "_updatedAt" : $attribute;

            $options['sort'][$attribute] = $this->getOrder($orderType);
        }

        $options['sort']['_id'] = $this->getOrder($cursorDirection === Database::CURSOR_AFTER ? Database::ORDER_ASC : Database::ORDER_DESC);

        // queries

        if (empty($orderAttributes)) {
            // Allow after pagination without any order
            if (!empty($cursor)) {
                $orderType = $orderTypes[0] ?? Database::ORDER_ASC;
                $orderOperator = $cursorDirection === Database::CURSOR_AFTER
                    ? ($orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER)
                    : ($orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER);

                $filters = array_merge($filters, [
                    '_id' => [
                        $this->getQueryOperator($orderOperator) => new ObjectId($cursor['$internalId'])
                    ]
                ]);
            }
            // Allow order type without any order attribute, fallback to the natural order (_id)
            if (!empty($orderTypes)) {
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

            $filter_ext = [
                [
                    $attribute => [
                        $this->getQueryOperator($orderOperator) => $cursor[$attribute]
                    ]
                ],
                [
                    $attribute => $cursor[$attribute],
                    '_id' => [
                        $this->getQueryOperator($orderOperatorInternalId) => new ObjectId($cursor['$internalId'])
                    ]

                ],
            ];

            $filters = [
                '$and' => [$filters, ['$or' => $filter_ext]]
            ];
        }

        $filters = $this->recursiveReplace($filters, '$', '_',  $this->operators);
        $filters = $this->timeFilter($filters);

        /**
         * @var Document[]
         */
        $found = [];
        $results = $this->client->find($name, $filters, $options)->cursor->firstBatch ?? [];

        foreach ($this->client->toArray($results) as $i => $result) {
            $record = $this->replaceChars('_', '$', (array)$result);
            $record = $this->timeToDocument($record);
    
            $found[] = new Document($record);
        }

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $found = array_reverse($found);
        }

        return $found;
    }

    /**
     * Recursive function to convert timestamps/datetime
     * to BSON based UTCDatetime type for Mongo filter/query.
     *
     * @param array $filters
     *
     * @return array
     * @throws Exception
     */
    private function timeFilter(array $filters):array 
    {
        $results = $filters;

        foreach($filters as $k=>$v) {
            if($k === '_createdAt' || $k == '_updatedAt') {
                if(is_array($v)) {
                    foreach($v as $sk=>$sv) {
                        $results[$k][$sk] = $this->toMongoDatetime($sv);
                    }
                } else {
                    $results[$k] = $this->toMongoDatetime($v);
                }
            } else {
                if(is_array($v)) {
                    $results[$k] = $this->timeFilter($v);
                }
            }
        }

        return $results;
    }

    /**
     * Converts timestamp base fields to Utopia\Document format.
     * 
     * @param array $record
     * 
     * @return array
     */
    private function timeToDocument(array $record):array
    {
        $record['$createdAt'] = DateTime::format($record['$createdAt']->toDateTime());
        $record['$updatedAt'] = DateTime::format($record['$updatedAt']->toDateTime());

        return $record;
    }

    /**
     * Converts timestamp base fields to Mongo\BSON datetime format.
     *
     * @param array $record
     *
     * @return array
     * @throws Exception
     */
    private function timeToMongo(array $record):array
    {
        $record['_createdAt'] = $this->toMongoDatetime($record['_createdAt']);
        $record['_updatedAt'] = $this->toMongoDatetime($record['_updatedAt']);

        return $record;
    }

    /**
     * Converts timestamp to Mongo\BSON datetime format.
     *
     * @param string $dt
     * @return UTCDateTime
     * @throws Exception
     */
    private function toMongoDatetime(string $dt): UTCDateTime {
        $dt = new \DateTime($dt);

        return new UTCDateTime($dt->getTimestamp() . $dt->format('v'));
    }

    /**
     * Recursive function to replace chars in array keys, while
     * skipping any that are explicitly excluded.
     *
     * @param array $array
     * @param string $from
     * @param string $to
     * @param array $exclude
     * @return array
     */
    private function recursiveReplace(array $array, string $from, string $to, array $exclude = []):array {
        $result = [];

        foreach ($array as $key => $value) {
            if (false == in_array($key, $exclude)) {
                $key = str_replace($from, $to, $key);
            }
            
            $result[$key] = is_array($value) 
                ? $this->recursiveReplace($value, $from, $to, $exclude) 
                : $value;
        }

        return $result;
    }


    /**
     * Count Documents
     *
     * @param string $collection
     * @param Query[] $queries
     * @param int $max
     *
     * @return int
     * @throws Exception
     */
    public function count(string $collection, array $queries = [], int $max = 0): int
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);
        // todo $collection is not used?
        $filters = [];

        $options = [];

        // set max limit
        if ($max > 0) {
            $options['limit'] = $max;
        }

        // queries
        $filters = $this->buildFilters($queries);

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $roles = \implode('|', Authorization::getRoles());
            $filters['_permissions']['$in'] = [new Regex("read\(\".*(?:{$roles}).*\"\)", 'i')];
        }

        return $this->client->count($name, $filters, $options);
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
     * @throws Exception
     */
    public function sum(string $collection, string $attribute, array $queries = [], int $max = 0): float|int
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);
// todo $collection is not used?
        $filters = [];

        // queries
        $filters = $this->buildFilters($queries);

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $roles = \implode('|', Authorization::getRoles());
            $filters['_permissions']['$in'] = [new Regex("read\(\".*(?:{$roles}).*\"\)", 'i')];
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

        return $this->client->aggregate($name, $pipeline)->cursor->firstBatch[0]->total ?? 0;
    }

    /**
     * @param string|null $name
     * @return Client
     *
     * @throws Exception
     */
    protected function getDatabase(string $name = null): Client
    {
        $database = is_null($name) ? $this->getDefaultDatabase() : $name;
        $selected = $this->getClient()->selectDatabase($database);

        return $selected;
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
     * @param array $array
     * @return array
     */
    protected function replaceChars(string $from, string $to, array $array): array
    {
        $filter = [
            'permissions',
            'createdAt',
            'updatedAt',
            'collection'
        ];

        $result = [];
        foreach ($array as $k => $v) {
            $clean_key = str_replace($from, "", $k);
            $key = in_array($clean_key, $filter) ? str_replace($from, $to, $k) : $k;

            $result[$key] = is_array($v) ? $this->replaceChars($from, $to, $v) : $v;
        }

        if ($from === '_') {
            if (array_key_exists('_id', $array)) {
                $result['$internalId'] = (string)$array['_id'];

                unset($result['_id']);
            }

            if (array_key_exists('_uid', $array)) {
                $result['$id'] = $array['_uid'];

                unset($result['_uid']);
            }
        } else if ($from === '$') {
            if (array_key_exists('$id', $array)) {
                $result['_uid'] = $array['$id'];

                unset($result['$id']);
            }

            if (array_key_exists('$internalId', $array)) {
                $result['_id'] = new ObjectId($array['$internalId']);

                unset($result['$internalId']);
            }
        }

        return $result;        
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

        foreach ($queries as $i => $query) {
            if ($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }

            if ($query->getAttribute() === '$createdAt') {
                $query->setAttribute('_createdAt');
            }

            if ($query->getAttribute() === '$updatedAt') {
                $query->setAttribute('_updatedAt');
            }
            
            $attribute = $query->getAttribute();
            $operator = $this->getQueryOperator($query->getMethod());
            $value = (count($query->getValues()) > 1) ? $query->getValues() : $query->getValues()[0];

            if ($operator === 'between' && isset($value[0]) && isset($value[1])) {
                $filters[$attribute]['$lte'] = $value[1];
                $filters[$attribute]['$gte'] = $value[0];
            } else if (is_array($value) && $operator === '$eq') {
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

            case Query::TYPE_NOTEQUAL:
                return '$ne';

            case Query::TYPE_LESSER:
                return '$lt';

            case Query::TYPE_LESSEREQUAL:
                return '$lte';

            case Query::TYPE_GREATER:
                return '$gt';

            case Query::TYPE_GREATEREQUAL:
                return '$gte';

            case Query::TYPE_CONTAINS:
                return '$in';

            case Query::TYPE_SEARCH:
                return '$search';

            case Query::TYPE_BETWEEN:
                return 'between'; // this is not an operator will be replaced with $gte/$lte

            default:
                throw new Exception('Unknown Operator:' . $operator);
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
            default => throw new Exception('Unknown sort order:' . $order),
        };
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

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    public function getSupportForSchemas(): bool
    {
        return true;
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
        $indexes = \count((array) $collection->getAttribute('indexes') ?? []);

        return $indexes + static::getCountOfDefaultIndexes();
    }

    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    public static function getCountOfDefaultAttributes(): int
    {
        return 6;
    }
    
    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    public static function getCountOfDefaultIndexes(): int
    {
        return 5;
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    public static function getDocumentSizeLimit(): int
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
     * Return set namespace.
     *
     * @return string
     * @throws \Exception
     */
    public function getNamespace(): string
    {
        if (empty($this->namespace)) {
            throw new Exception('Missing namespace');
        }

        return $this->namespace;
    }

    /**
     * Set's default database.
     *
     * @param string $name
     * @param bool $reset
     * @return bool
     * @throws \Exception
     */
    public function setDefaultDatabase(string $name, bool $reset = false): bool
    {
        if (empty($name) && $reset === false) {
            throw new Exception('Missing database');
        }

        $this->defaultDatabase = ($reset) ? '' : $this->filter($name);

        return true;
    }

    /**
     * Set's the namespace.
     *
     * @param string $namespace
     * @return bool
     * @throws \Exception
     */
    public function setNamespace(string $namespace): bool
    {
        if (empty($namespace)) {
            throw new Exception('Missing namespace');
        }

        $this->namespace = $this->filter($namespace);

        return true;
    }

    /**
     * Flattens the array.
     *
     * @param mixed $list
     * @return array
     */
    protected function flattenArray(mixed $list): array
    {
        if (!is_array($list)) {
            // make sure the input is an array
            return array($list);
        }

        $new_array = [];

        foreach ($list as $value) {
            $new_array = array_merge($new_array, $this->flattenArray($value));
        }

        return $new_array;
    }

    protected function removeNullKeys(array|Document $target): array
    {
        $target = is_array($target) ? $target : $target->getArrayCopy();
        $cleaned = [];

        foreach ($target as $key => $value) {
            if (\is_null($value)) continue;

            $cleaned[$key] = $value;
        }


        return $cleaned;
    }

    public function getKeywords(): array 
    {
        return [];
    }
}
