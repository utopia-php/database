<?php

namespace Utopia\Database;

use Exception;

abstract class Adapter
{
    /**
     * @var string
     */
    protected $namespace = '';

    /**
     * @var array
     */
    protected $debug = [];

    /**
     * @param string $key
     * @param mixed $value
     *
     * @return $this
     */
    public function setDebug(string $key, $value): self
    {
        $this->debug[$key] = $value;

        return $this;
    }

    /**
     * @return array
     */
    public function getDebug(): array
    {
        return $this->debug;
    }

    /**
     * return $this
     */
    public function resetDebug(): self
    {
        $this->debug = [];

        return $this;
    }

    /**
     * Set Namespace.
     *
     * Set namespace to divide different scope of data sets
     *
     * @param $namespace
     *
     * @throws Exception
     *
     * @return bool
     */
    public function setNamespace(string $namespace): bool
    {
        if (empty($namespace)) {
            throw new Exception('Missing namespace');
        }

        $this->namespace = $namespace;

        return true;
    }

    /**
     * Get Namespace.
     *
     * Get namespace of current set scope
     *
     * @throws Exception
     *
     * @return string
     */
    public function getNamespace(): string
    {
        if (empty($this->namespace)) {
            throw new Exception('Missing namespace');
        }

        return $this->namespace;
    }

    /**
     * Create Database
     *
     * @return bool
     */
    abstract public function create(): bool;

    /**
     * List Databases
     *
     * @return array
     */
    abstract public function list(): array;

    /**
     * Delete Database
     *
     * @return bool
     */
    abstract public function delete(): bool;

    /**
     * Create Collection
     * 
     * @param string $name
     * 
     * @return bool
     */
    abstract public function createCollection(string $name): bool;

    /**
     * List Collections
     * 
     * @return array
     */
    abstract public function listCollections(): array;

    /**
     * Delete Collection
     * 
     * @param string $name
     * 
     * @return bool
     */
    abstract public function deleteCollection(string $name): bool;

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
    abstract public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool;

    /**
     * Delete Attribute
     * 
     * @param string $collection
     * @param string $id
     * 
     * @return bool
     */
    abstract public function deleteAttribute(string $collection, string $id): bool;

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
    abstract public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool;

    /**
     * Delete Index
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    abstract public function deleteIndex(string $collection, string $id): bool;

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return Document
     */
    abstract public function getDocument(string $collection, string $id): Document;

    /**
     * Create Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     */
    abstract public function createDocument(string $collection, Document $document): Document;

    /**
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     */
    abstract public function updateDocument(string $collection, Document $document): Document;

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    abstract public function deleteDocument(string $collection, string $id): bool;

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
    abstract public function find(string $collection, array $queries = [], $limit = 25, $offset = 0, $orderAttributes = [], $orderTypes = []): array;

    // /**
    //  * @param array $options
    //  *
    //  * @return int
    //  */
    // abstract public function count(array $options);

    /**
     * Get max STRING limit
     * 
     * @return int
     */
    abstract public function getStringLimit(): int;

    /**
     * Get max INT limit
     * 
     * @return int
     */
    abstract public function getIntLimit(): int;

    /**
     * Is index supported?
     * 
     * @return bool
     */
    abstract public function getSupportForIndex(): bool;

    /**
     * Is unique index supported?
     * 
     * @return bool
     */
    abstract public function getSupportForUniqueIndex(): bool;

    /**
     * Is fulltext index supported?
     * 
     * @return bool
     */
    abstract public function getSupportForFulltextIndex(): bool;

    /**
     * Filter Keys
     * 
     * @throws Exception
     * @return string
     */
    public function filter(string $value):string
    {
        $value = preg_replace("/[^A-Za-z0-9]_/", '', $value);

        if(\is_null($value)) {
            throw new Exception('Failed to filter key');
        }
        
        return $value;
    }
}
