<?php

namespace Utopia\Database\Adapter\Mongo;

use Exception;

use MongoDB\BSON\Regex;
use Utopia\Database\Adapter;
use Utopia\Database\Document;
use Utopia\Database\Database;
use Utopia\Database\Exception\Timeout;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Query;

class MongoDBAdapter extends Adapter
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
        $this->client->connect();
    }

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
        $name = $this->filter($name);
        $this->getClient()->selectDatabase($name);

        return true;
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
        if (!\is_null($collection)) {
            $list = $this->flattenArray($this->list());

            foreach ($list as $obj) {
                if (\is_object($obj)) {
                    if ($obj->name == $collection) {
                        return true;
                    }
                }
            }

            return false;
        }

        return !\is_null($this->getClient()->selectDatabase($database));
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

        // Returns an array/object with the result document
        if (empty($this->getClient()->createCollection($id))) {
            return false;
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
                $name = $this->filter($index->getId());
                $unique = false;

                $attributes = $index->getAttribute('attributes');
                $orders = $index->getAttribute('orders');

                foreach ($attributes as $j => $attribute) {
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

            if (!$this->getClient()->createIndexes($name, $newIndexes)) {
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
     *
     * @return bool
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
     */
    public function getDocument(string $collection, string $id): Document
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        $result = $this->client->find($name, ['_uid' => $id])->cursor->firstBatch ?? [];

        if (empty($result)) {
            return new Document([]);
        }

        $result = $this->replaceChars('_', '$', $result[0]);
        $newDoc = new Document($this->client->toArray($result));

        return $newDoc;
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
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        $record =  $this->replaceChars('$', '_', $document->getArrayCopy());

        $this->client->insert($name, $record);

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
        $name = $this->getNamespace() . '_' . $this->filter($collection);


        $this->client->update(
            $name,
            ['_uid' => $document->getId()],
            $this->replaceChars('$', '_', $document->getArrayCopy()),
        );

        $newDoc = $document->getArrayCopy();
        $newDoc = $this->replaceChars('_', '$', $newDoc);

        return new Document($newDoc);
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
     * @param null $timeout
     * @return Document[]
     * @throws Exception
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, $timeout = null): array
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);



        $options = ['sort' => [], 'limit' => $limit, 'skip' => $offset];

        // Todo: Set max time execution in milliseconds
        $options['maxTimeMS'] = 99;

        // orders
        foreach ($orderAttributes as $i => $attribute) {
            $attribute = $this->filter($attribute);
            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);
            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }
            $options['sort'][$attribute] = $this->getOrder($orderType);
        }

        $options['sort']['_id'] = $this->getOrder($cursorDirection === Database::CURSOR_AFTER ? Database::ORDER_ASC : Database::ORDER_DESC);

        // queries
        $filters = $queries;

        if (empty($orderAttributes)) {
            // Allow after pagination without any order
            if (!empty($cursor)) {
                $orderType = $orderTypes[0] ?? Database::ORDER_ASC;
                $orderMethod = $cursorDirection === Database::CURSOR_AFTER ? ($orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER
                ) : ($orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER
                );

                $filters = array_merge($filters, [
                    '_id' => [
                        $this->getQueryOperator($orderMethod) => new \MongoDB\BSON\ObjectId($cursor['$internalId'])
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

            $orderMethodInternalId = Query::TYPE_GREATER;
            $orderType = $this->filter($orderTypes[0] ?? Database::ORDER_ASC);
            $orderMethod = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;

            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                $orderMethodInternalId = $orderType === Database::ORDER_ASC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
                $orderMethod = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
            }

            $filters = array_merge($filters, [
                '$or' => [
                    [
                        $attribute => [
                            $this->getQueryOperator($orderMethod) => $cursor[$attribute]
                        ]
                    ], [
                        $attribute => $cursor[$attribute],
                        '_id' => [
                            $this->getQueryOperator($orderMethodInternalId) => new \MongoDB\BSON\ObjectId($cursor['$internalId'])
                        ]

                    ]
                ]
            ]);
        }

        /**
         * @var Document[]
         */
        $found = [];

        $filters = $this->buildFilters($filters);

        // todo: comment mock exceed time limit
        $filters = array_merge($filters, ['$where' => "sleep(1000) || true"]);

        $results = $this->client->find($name, $filters, $options)->cursor->firstBatch ?? [];
        foreach ($this->client->toArray($results) as $i => $result) {
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
        $name = $this->getNamespace() . '_' . $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        $filters = [];

        $options = [];

        // set max limit
        if ($max > 0) {
            $options['limit'] = $max;
        }

        // queries
        $filters = $this->buildFilters($queries);

        $roles = \implode('|', Authorization::getRoles());

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
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
     */
    public function sum(string $collection, string $attribute, array $queries = [], int $max = 0)
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);
        $collection = $this->getDatabase()->selectCollection($name);

        $filters = [];

        // queries
        $filters = $this->buildFilters($queries);

        $roles = \implode('|', Authorization::getRoles());

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $filters['_permissions']['$in'] = [new Regex("read\(.*(?:{$roles}).*\)", 'i')];
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
     * @return MongoClient
     *
     * @throws Exception
     */
    protected function getDatabase(string $name = null): MongoClient
    {
        $database = is_null($name) ? $this->getDefaultDatabase() : $name;
        $selected = $this->getClient()->selectDatabase($database);

        return $selected;
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
     * Convert $ prefix to _ on $id, $permissions, and $collection
     *
     * @param string $from
     * @param string $to
     * @param array $array
     * @return array
     */
    protected function replaceChars($from, $to, $array): array
    {
        $array = (array) $array;

        if (array_key_exists($from . 'permissions', $array))
            $array[$to . 'permissions'] = $array[$from . 'permissions'];

        if (array_key_exists($from . 'createdAt', $array))
            $array[$to . 'createdAt'] = $array[$from . 'createdAt'];

        if (array_key_exists($from . 'updatedAt', $array))
            $array[$to . 'updatedAt'] = $array[$from . 'updatedAt'];

        if (array_key_exists($from . 'collection', $array))
            $array[$to . 'collection'] = $array[$from . 'collection'];

        if ($from === '_' && array_key_exists($from . 'uid', $array)) { // convert internal to document ID
            $array[$to . 'id'] = $array[$from . 'uid'];
            $array[$to . 'internalId'] = (string) $array[$from . 'id'];

            unset($array[$from . 'uid']);
        } else if ($from === '$' && array_key_exists($from . 'id', $array)) { // convert document to internal ID
            $array[$to . 'uid'] = $array[$from . 'id'];

            if (array_key_exists($from . 'internalId', $array)) {
                unset($array[$from . 'internalId']); // remove unnecessary internal ID
            }
        }

        unset($array[$from . 'id']);
        unset($array[$from . 'permissions']);
        unset($array[$from . 'collection']);
        unset($array[$from . 'createdAt']);
        unset($array[$from . 'updatedAt']);

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

        foreach ($queries as $i => $query) {
            if ($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }
            $attribute = $query->getAttribute();
            $operator = $this->getQueryOperator($query->getMethod());
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
     * @param string $method
     * 
     * @return string
     */
    protected function getQueryOperator(string $method): string
    {
        switch ($method) {
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
                throw new Exception('Unknown method:' . $method);
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
    public static function getRowLimit(): int
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

    /**
     * Get list of keywords that cannot be used
     * 
     * Mongo does not have concept of reserverd words.
     *  We put something here just to run the rests for this adapter too
     * 
     * @return string[]
     */
    public function getKeywords(): array
    {
        return ['mongodb'];
    }

    /**
     * @throws Timeout
     */
    protected function checkTimeoutException(Exception $e): void
    {
        if($e->getCode() === 50){
            Throw new Timeout($e->getMessage());
        }
    }

    /**
     * Force a query to throw a timeout exception
     *
     * @throws Timeout
     */
    public function forceTimeoutException(): void
    {

        var_dump("ininiiininnini------------ininininininin");
        die;

        try {
            $this->client->find(
                'movies',
                ['$where' => 'sleep(1000) || true'],
                ['maxTimeMS'=> 1]
            )->cursor->firstBatch ?? [];
        } catch (Exception $e){

            $this->checkTimeoutException($e);
        }

    }

}
