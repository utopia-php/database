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
use MongoDB\Collection as MongoCollection;
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
     * @return bool
     */
    public function create(): bool
    {
        $namespace = $this->getNamespace();
        return (!!$this->getClient()->$namespace);
    }

    /**
     * Check if database exists
     *
     * @return bool
     */
    public function exists(): bool
    {
        $name = $this->getNamespace();
        forEach ($this->getClient()->listDatabaseNames() as $key => $value) {
            if ($name === $value) {
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
     * @return bool
     */
    public function delete(): bool
    {
        return (!!$this->getClient()->dropDatabase($this->getNamespace()));
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
        $id = $this->filter($name);

        $database = $this->getDatabase();

        // Returns an array/object with the result document
        if (empty($database->createCollection($id))) {
            return false;
        }

        $collection = $database->$id;

        // Mongo creates an index for _id; index _read,_write by default
        // Returns the name of the created index as a string.
        // Update $this->getIndexCount when adding another default index
        $read = $collection->createIndex(['_read' => $this->getOrder(Database::ORDER_DESC)], ['name' => '_read_permissions']);
        $write = $collection->createIndex(['_write' => $this->getOrder(Database::ORDER_DESC)], ['name' => '_write_permissions']);


        if (!$read || !$write) {
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
        $id = $this->filter($id);

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
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $collection = $this->getDatabase()->$name;

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
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $collection = $this->getDatabase()->$name;

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
        $name = $this->filter($collection);
        $collection = $this->getDatabase()->$name;

        $result = $collection->findOne(['_id' => $id]);

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
    
        $name = $this->filter($collection);
        $collection = $this->getDatabase()->$name;

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
        $name = $this->filter($collection);
        $collection = $this->getDatabase()->$name;

        $result = $collection->findOneAndUpdate(
            ['_id' => $document->getId()],
            ['$set' => $this->replaceChars('$', '_', $document->getArrayCopy())],
            ['returnDocument' => \MongoDB\Operation\FindOneAndUpdate::RETURN_DOCUMENT_AFTER]
        );

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
        $name = $this->filter($collection);
        $collection = $this->getDatabase()->$name;

        $result = $collection->findOneAndDelete(['_id' => $id]);

        return (!!$result);
    }

    /**
     * Find Documents
     *
     * Find data sets using chosen queries
     *
     * @param string $collection
     * @param \Utopia\Database\Query[] $queries
     * @param int $limit
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     *
     * @return Document[]
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = []): array
    {
        $name = $this->filter($collection);
        $collection = $this->getDatabase()->$name;
        $roles = Authorization::getRoles();

        $filters = [];

        $options = ['sort' => [], 'limit' => $limit, 'skip' => $offset];

        // orders
        foreach($orderAttributes as $i => $attribute) {
            $attribute = $this->filter($attribute);
            $orderType = $this->getOrder($this->filter($orderTypes[$i] ?? Database::ORDER_ASC));
            $options['sort'][$attribute] = $orderType;
        }

        // queries
        $filters = $this->buildFilters($queries);

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
        $name = $this->filter($collection);
        $collection = $this->getDatabase()->$name;
        $roles = Authorization::getRoles();

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
     * @return MongoDatabase
     *
     * @throws Exception
     */
    protected function getDatabase()
    {
        $namespace = $this->getNamespace();
        
        return $this->getClient()->$namespace;
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
        $array[$to.'id'] = $array[$from.'id'];
        $array[$to.'read'] = $array[$from.'read'];
        $array[$to.'write'] = $array[$from.'write'];
        $array[$to.'collection'] = $array[$from.'collection'];

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
     * @param bool $strict (optional) Only count indexes in collection, ignoring queue count
     * @return int
     */
    public function getIndexCount(Document $collection, bool $strict = false): int
    {
        $indexes = \count((array) $collection->getAttribute('indexes') ?? []);
        $indexesInQueue = ($strict) ? 0 : \count((array) $collection->getAttribute('indexesInQueue') ?? []);

        // +3 ==> hardcoded number of default indexes from createCollection
        return $indexes + $indexesInQueue + 3;
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
        $attributes = $collection->getAttribute('attributes') ?? [];
        $attributesInQueue = $collection->getAttribute('attributesInQueue') ?? [];

        return \count($attributes) + \count($attributesInQueue);
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
