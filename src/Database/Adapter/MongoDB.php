<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use MongoDB\Client;
use MongoDB\Database;

class MongoDB extends Adapter
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Database|null
     */
    protected $database;

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
    }

    /**
     * Create Database
     * 
     * @return bool
     */
    public function create(): bool
    {
        $namespace = $this->getNamespace();
        return (!!$this->client->$namespace);
    }

    /**
     * Check if database exists
     *
     * @return bool
     */
    public function exists(): bool
    {}

    /**
     * List Databases
     * 
     * @return array
     */
    public function list(): array
    {
        $list = [];

        foreach ($this->client->listDatabaseNames() as $key => $value) {
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
        return (!!$this->getDatabase()->drop($this->getNamespace()));
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
    {}

    /**
     * Delete Attribute
     * 
     * @param string $collection
     * @param string $id
     * 
     * @return bool
     */
    public function deleteAttribute(string $collection, string $id): bool
    {}

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
    {}

    /**
     * Delete Index
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function deleteIndex(string $collection, string $id): bool
    {}

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return Document
     */
    public function getDocument(string $collection, string $id): Document
    {}

    /**
     * Create Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     */
    public function createDocument(string $collection, Document $document): Document
    {}

    /**
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     */
    public function updateDocument(string $collection, Document $document): Document
    {}

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function deleteDocument(string $collection, string $id): bool
    {}

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
    {}
    
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
    {}

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
        
        return $this->client->$namespace;
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