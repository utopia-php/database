<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;

class Mock extends Adapter
{
    protected array $data = [];
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
        $data[$name] = [];
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
    public function exists(string $database, ?string $collection): bool
    {
        $database = $this->filter($database);
        if(!is_null($collection)) {
            $collection = $this->filter($collection);
            return isset($this->data[$database]) && isset($this->data[$database][$collection]);
        }
        return isset($this->data[$database]);
    }

    /**
     * List Databases
     *
     * @return array
     */
    public function list(): array
    {
        return array_keys($this->data);
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
        unset($this->data[$name]);
        return true;
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
        return true;
    }

    /**
     * Delete Collection
     * 
     * @param string $name
     * 
     * @return bool
     */
    public function deleteCollection(string $name): bool
    {
        return true;
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
     * Update Attribute
     * 
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $array
     * 
     * @return bool
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
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
     * Rename Attribute
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        return true;
    }

    /**
     * Rename Index
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     */
    public function renameIndex(string $collection, string $old, string $new): bool
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
        return new Document();
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
        return new Document();
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
        return $document;
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
        return true;
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
     * @param array $cursor
     * @param string $cursorDirection
     *
     * @return Document[]
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array {
        return [];
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
        return 0;
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
        return 0;
    }

    /**
     * Get max STRING limit
     * 
     * @return int
     */
    public function getStringLimit(): int
    {
        return -1;
    }

    /**
     * Get max INT limit
     * 
     * @return int
     */
    public function getIntLimit(): int
    {
        return -1;
    }

    /**
     * Is index supported?
     * 
     * @return bool
     */
    public function getSupportForIndex(): bool
    {
        return false;
    }

    /**
     * Is unique index supported?
     * 
     * @return bool
     */
    public function getSupportForUniqueIndex(): bool
    {
        return false;
    }

    /**
     * Is fulltext index supported?
     * 
     * @return bool
     */
    public function getSupportForFulltextIndex(): bool
    {
        return false;
    }

    /**
     * Get current attribute count from collection document
     * 
     * @param Document $collection
     * @return int
     */
    public function getAttributeCount(Document $collection): int
    {
        return 0;
    }

    /**
     * Get maximum column limit.
     * 
     * @return int
     */
    public function getAttributeLimit(): int
    {
        return -1;
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    public static function getRowLimit(): int
    {
        return -1;
    }

    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    static public function getNumberOfDefaultAttributes(): int
    {
        return -1;
    }

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    static public function getNumberOfDefaultIndexes(): int
    {
        return -1;
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
        return -1;
    }

    /**
     * Get current index count from collection document
     * 
     * @param Document $collection
     * @return int
     */
    public function getIndexCount(Document $collection): int
    {
        return -1;
    }

    /**
     * Get maximum index limit.
     * 
     * @return int
     */
    public function getIndexLimit(): int
    {
        return -1;
    }

    /**
     * Does the adapter handle casting?
     * 
     * @return bool
     */
    public function getSupportForCasting(): bool
    {
        return false;
    }
}
