<?php

namespace Utopia\Database;

use Exception;

abstract class Adapter
{
    /**
     * @var string
     */
    protected string $namespace = '';

    /**
     * @var string
     */
    protected string $defaultDatabase = '';

    /**
     * @var array
     */
    protected array $debug = [];

    /**
     * @param string $key
     * @param mixed $value
     *
     * @return $this
     */
    public function setDebug(string $key, mixed $value): self
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
     * @return self
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
     * @param string $namespace
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
     * Set Database.
     *
     * Set database to use for current scope
     *
     * @param string $name
     * @param bool $reset
     *
     * @throws Exception
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
     * Get Database.
     *
     * Get Database from current scope
     *
     * @throws Exception
     *
     * @return string
     */
    public function getDefaultDatabase(): string
    {
        if (empty($this->defaultDatabase)) {
            throw new Exception('Missing database');
        }

        return $this->defaultDatabase;
    }

    /**
     * Create Database
     *
     * @param string $name
     *
     * @return bool
     */
    abstract public function create(string $name): bool;

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string $database database name
     * @param string $collection (optional) collection name
     *
     * @return bool
     */
    abstract public function exists(string $database, ?string $collection): bool;

    /**
     * List Databases
     *
     * @return array
     */
    abstract public function list(): array;

    /**
     * Delete Database
     *
     * @param string $name
     *
     * @return bool
     */
    abstract public function delete(string $name): bool;

    /**
     * Create Collection
     * 
     * @param string $name
     * @param Document[] $attributes (optional)
     * @param Document[] $indexes (optional)
     * 
     * @return bool
     */
    abstract public function createCollection(string $name, array $attributes = [], array $indexes = []): bool;

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
     * Rename Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $name
     * @return bool
     */
    abstract public function renameAttribute(string $collection, string $id, string $name): bool;

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
     * @param array $cursor Array copy of document used for before/after pagination
     * @param string $cursorDirection
     *
     * @return Document[]
     */
    abstract public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array;

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
    abstract public function sum(string $collection, string $attribute, array $queries = [], int $max = 0);

    /**
     * Count Documents
     * 
     * @param string $collection
     * @param Query[] $queries
     * @param int $max
     *
     * @return int
     */
    abstract public function count(string $collection, array $queries = [], int $max = 0): int;

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
     * Get current attribute count from collection document
     * 
     * @param Document $collection
     * @return int
     */
    abstract public function getAttributeCount(Document $collection): int;

    /**
     * Get maximum column limit.
     * 
     * @return int
     */
    abstract public function getAttributeLimit(): int;

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    abstract public static function getRowLimit(): int;

    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    abstract static public function getNumberOfDefaultAttributes(): int;

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    abstract static public function getNumberOfDefaultIndexes(): int;

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     * Return 0 when no restrictions apply to row width
     * 
     * @param Document $collection
     * @return int
     */
    abstract public function getAttributeWidth(Document $collection): int;

    /**
     * Get current index count from collection document
     * 
     * @param Document $collection
     * @return int
     */
    abstract public function getIndexCount(Document $collection): int;

    /**
     * Get maximum index limit.
     * 
     * @return int
     */
    abstract public function getIndexLimit(): int;

    /**
     * Does the adapter handle casting?
     * 
     * @return bool
     */
    abstract public function getSupportForCasting(): bool;

    /**
     * Filter Keys
     * 
     * @throws Exception
     * @return string
     */
    public function filter(string $value):string
    {
        $value = preg_replace("/[^A-Za-z0-9\_\-\.]/", '', $value);

        if(\is_null($value)) {
            throw new Exception('Failed to filter key');
        }

        return $value;
    }
}
