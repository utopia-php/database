<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
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
        return (!!$this->getDatabase()->createCollection($id));
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
        
        // From MariaDB Adapter
        // $name = $this->filter($collection);
        // $id = $this->filter($id);
        // $type = $this->getSQLType($type, $size, $signed);

        // if($array) {
        //     $type = 'LONGTEXT';
        // }

        // return $this->getPDO()
        //     ->prepare("ALTER TABLE {$this->getNamespace()}.{$name}
        //         ADD COLUMN `{$id}` {$type};")
        //     ->execute();

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
        return true;
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
        return true;
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

        $result = $collection->findOne(
            ['+id' => $id],
            [
                'typemap' => [
                    'root' => 'array',
                    'document' => 'array',
                    'array' => 'array'
                ]
            ]
        );

        if(empty($result)) {
            return new Document([]);
        }

        // Remove internal Mongo ID
        unset($result['_id']);

        // Change back to $
        $result = $this->replaceChars('+', '$', $result);

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
        $namespace = $this->getNamespace();
        $collection = $this->getClient()->$namespace->$name;

        $collection->insertOne($this->replaceChars('$', '+', $document->getArrayCopy()));

        return $document;
    }

    /**
     * Keys cannot begin with $ in MongoDB
     * Convert $ to +
     *
     * @param string $from
     * @param string $to
     * @param array $array
     * @return array
     */
    protected function replaceChars($from, $to, $array) {
        return array_combine(str_replace($from, $to, array_keys($array)), $array);
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
            ['+id' => $document->getId()],
            ['$set' => $this->replaceChars('$', '+', $document->getArrayCopy())],
            [
                'typemap' => [
                    'root' => 'array',
                    'document' => 'array',
                    'array' => 'array'
                ],
                'returnDocument' => \MongoDB\Operation\FindOneAndUpdate::RETURN_DOCUMENT_AFTER,
            ]
        );

        // Remove internal Mongo ID
        unset($result['_id']);

        // Change back to $
        $result = $this->replaceChars('+', '$', $result);

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

        $result = $collection->findOneAndDelete(
            ['+id' => $id],
            [
                'typemap' => [
                    'root' => 'array',
                    'document' => 'array',
                    'array' => 'array'
                ]
            ]
        );

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

        /**
         * @var array
         */
        $filters = [];

        /**
         * @var array
         */
        $options = ['sort', 'limit' => $limit, 'skip' => $offset];

        // orders
        foreach($orderAttributes as $i => $attribute) {
            $attribute = $this->filter($attribute);
            $orderType = $this->getOrder($this->filter($orderTypes[$i] ?? Database::ORDER_ASC));
            $options['sort'][$attribute] = $orderType;
        }

        // queries
        foreach($queries as $i => $query) {
            $attribute = $query->getAttribute();
            $operator = $this->getQueryOperator($query->getOperator()); 
            $value = (count($query->getValues()) > 1) ? $query->getValues() : $query->getValues()[0]; 

            // TODO@kodumbeats Mongo recommends different methods depending on operator - implement the rest
            if (is_array($value) && $operator = '$eq') {
                $filters[$attribute]['$in'] = $value;
            } else {
                $filters[$attribute][$operator] = $value;
            }
        }

        /**
         * @var Document[]
         */
        $found = [];

        $results = $collection->find($filters, $options);

        foreach($results as $i => $result) {
            // Remove internal Mongo ID
            unset($result['_id']);

            // Change back to $
            $found[] = new Document($this->replaceChars('+', '$', $result));
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

        /**
         * @var array
         */
        $filters = [];

        /**
         * @var array
         */
        $options['limit'] = $max;


        // queries
        foreach($queries as $i => $query) {
            $attribute = $query->getAttribute();
            $operator = $this->getQueryOperator($query->getOperator()); 
            $value = (count($query->getValues()) > 1) ? $query->getValues() : $query->getValues()[0]; 

            // TODO@kodumbeats Mongo recommends different methods depending on operator - implement the rest
            if (is_array($value) && $operator = '$eq') {
                $filters[$attribute]['$in'] = $value;
            } else {
                $filters[$attribute][$operator] = $value;
            }
        }

        return $collection->count($filters, $options);
    }

    /**
     * @return Database
     *
     * @throws Exception
     */
    protected function getDatabase()
    {
        if($this->database) {
            return $this->database;
        }

        $namespace = $this->getNamespace();
        
        return $this->getClient()->$namespace;
    }

    /**
     * @return Client
     *
     * @throws Exception
     */
    protected function getClient()
    {
        return $this->client;
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

            default:
                throw new Exception('Unknown Operator:' . $operator);
                break;
        }
    }

    /**
     * Get Mongo Order
     *
     * @param string
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
}