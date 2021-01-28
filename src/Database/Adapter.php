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

        $this->namespace = $this->filter($namespace);

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
     * Create Database.
     *
     * @return bool
     */
    abstract public function create(): bool;

    /**
     * Delete Database.
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
     * Delete Collection
     * 
     * @param string $name
     * 
     * @return bool
     */
    abstract public function deleteCollection(string $name): bool;

    // /**
    //  * Create Attribute
    //  * 
    //  * @param Document $collection
    //  * @param string $id
    //  * @param string $type
    //  * @param bool $array
    //  * 
    //  * @return bool
    //  */
    // abstract public function createAttribute(Document $collection, string $id, string $type, bool $array = false): bool;

    // /**
    //  * Delete Attribute
    //  * 
    //  * @param Document $collection
    //  * @param string $id
    //  * @param bool $array
    //  * 
    //  * @return bool
    //  */
    // abstract public function deleteAttribute(Document $collection, string $id, bool $array = false): bool;

    // /**
    //  * Create Index
    //  *
    //  * @param Document $collection
    //  * @param string $id
    //  * @param string $type
    //  * @param array $attributes
    //  *
    //  * @return bool
    //  */
    // abstract public function createIndex(Document $collection, string $id, string $type, array $attributes): bool;

    // /**
    //  * Delete Index
    //  *
    //  * @param Document $collection
    //  * @param string $id
    //  *
    //  * @return bool
    //  */
    // abstract public function deleteIndex(Document $collection, string $id): bool;

    // /**
    //  * Get Document.
    //  *
    //  * @param Document $collection
    //  * @param string $id
    //  *
    //  * @return array
    //  */
    // abstract public function getDocument(Document $collection, $id);

    // /**
    //  * Create Document
    //  *
    //  * @param Document $collection
    //  * @param array $data
    //  * @param array $unique
    //  *
    //  * @return array
    //  */
    // abstract public function createDocument(Document $collection, array $data, array $unique = []);

    // /**
    //  * Update Document.
    //  *
    //  * @param Document $collection
    //  * @param array $data
    //  *
    //  * @return array
    //  */
    // abstract public function updateDocument(Document $collection, string $id, array $data);

    // /**
    //  * Delete Node.
    //  *
    //  * @param Document $collection
    //  * @param string $id
    //  *
    //  * @return array
    //  */
    // abstract public function deleteDocument(Document $collection, string $id);

    // /**
    //  * Find.
    //  *
    //  * Find data sets using chosen queries
    //  *
    //  * @param Document $collection
    //  * @param array $options
    //  *
    //  * @return array
    //  */
    // abstract public function find(Document $collection, array $options);

    // /**
    //  * @param array $options
    //  *
    //  * @return int
    //  */
    // abstract public function count(array $options);

    public function filter($value)
    {
        return preg_replace("/[^A-Za-z0-9 _]/", '', $value);
    }

    /**
     * Get Unique ID.
     */
    public function getId()
    {
        return \uniqid();
    }
}
