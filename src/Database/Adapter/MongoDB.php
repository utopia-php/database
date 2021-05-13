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
     * @param string $id
     * @return bool
     */
    public function createCollection(string $id): bool
    {
        $id = $this->filter($id);

        if ($this->getDatabase()->createCollection($id)) {
            return false;
        }

        /**
         * @var Database
         */
        $collection = $this->getDatabase();

        // Mongo creates an index for _id; index _read,_write by default
        $read = $collection->createIndex($id, '_read_permissions', Database::INDEX_KEY, ['_read'], [], [Database::ORDER_DESC]);
        $write = $collection->createIndex($id, '_write_permissions', Database::INDEX_KEY, ['_write'], [], [Database::ORDER_DESC]);

        if (!$read || !$write) {
            return false;
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
                default:
                    // index not supported
                    // TODO@kodumbeats handle and test for this case
                    return false;
            }
        }

        return (!!$collection->createIndex($indexes, ['name' => $id]));
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

        $options = ['sort', 'limit' => $limit, 'skip' => $offset];

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
        $options['limit'] = $max;

        // queries
        $filters = $this->buildFilters($queries);

        // permissions
        if (Authorization::$status) { // skip if authorization is disabled
            $filters['_read']['$in'] = Authorization::getRoles();
        }

        return $collection->count($filters, $options);
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
     * Convert $ to _
     *
     * @param string $from
     * @param string $to
     * @param array $array
     * @return array
     */
    protected function replaceChars($from, $to, $array): array
    {
        return array_combine(str_replace($from, $to, array_keys($array)), $array);
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

    public function getSupportForCasting(): bool
    {
        return true;
    }
}