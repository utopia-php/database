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
     * @var array<string, mixed>
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
     * @return array<string, mixed>
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
     * @return bool
     * @throws Exception
     *
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
     * @return string
     * @throws Exception
     *
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
     * @return bool
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
     * @return string
     * @throws Exception
     *
     */
    public function getDefaultDatabase(): string
    {
        if (empty($this->defaultDatabase)) {
            throw new Exception('Missing default database');
        }

        return $this->defaultDatabase;
    }

    /**
     * Ping Database
     *
     * @return bool
     */
    abstract public function ping(): bool;

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
     * @return array<Document>
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
     * @param array<Document> $attributes (optional)
     * @param array<Document> $indexes (optional)
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
     * @param bool $signed
     * @param bool $array
     *
     * @return bool
     */
    abstract public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool;

    /**
     * Update Attribute
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
    abstract public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool;

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
     * @param string $old
     * @param string $new
     * @return bool
     */
    abstract public function renameAttribute(string $collection, string $old, string $new): bool;

    /**
     * Rename Index
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     */
    abstract public function renameIndex(string $collection, string $old, string $new): bool;

    /**
     * Create Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     * @param array<int> $lengths
     * @param array<string> $orders
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
     * @param array<Query> $queries
     * @return Document
     */
    abstract public function getDocument(string $collection, string $id, array $queries = []): Document;

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
     * @param array<Query> $queries
     * @param int|null $limit
     * @param int|null $offset
     * @param array<string> $orderAttributes
     * @param array<string> $orderTypes
     * @param array<string, mixed> $cursor
     * @param string $cursorDirection
     * @param int|null $timeout
     *
     * @return array<Document>
     */
    abstract public function find(string $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, ?int $timeout = null): array;

    /**
     * Sum an attribute
     *
     * @param string $collection
     * @param string $attribute
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int|float
     */
    abstract public function sum(string $collection, string $attribute, array $queries = [], ?int $max = null): float|int;

    /**
     * Count Documents
     *
     * @param string $collection
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int
     */
    abstract public function count(string $collection, array $queries = [], ?int $max = null): int;

    /**
     * Get max STRING limit
     *
     * @return int
     */
    abstract public function getLimitForString(): int;

    /**
     * Get max INT limit
     *
     * @return int
     */
    abstract public function getLimitForInt(): int;

    /**
     * Get maximum attributes limit.
     *
     * @return int
     */
    abstract public function getLimitForAttributes(): int;

    /**
     * Get maximum index limit.
     *
     * @return int
     */
    abstract public function getLimitForIndexes(): int;

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    abstract public function getSupportForSchemas(): bool;

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
     * Is fulltext wildcard supported?
     *
     * @return bool
     */
    abstract public function getSupportForFulltextWildcardIndex(): bool;


    /**
     * Does the adapter handle casting?
     *
     * @return bool
     */
    abstract public function getSupportForCasting(): bool;

    /**
     * Does the adapter handle array Contains?
     *
     * @return bool
     */
    abstract public function getSupportForQueryContains(): bool;

    /**
     * Are timeouts supported?
     *
     * @return bool
     */
    abstract public function getSupportForTimeouts(): bool;

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    abstract public function getCountOfAttributes(Document $collection): int;

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    abstract public function getCountOfIndexes(Document $collection): int;

    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    abstract public static function getCountOfDefaultAttributes(): int;

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    abstract public static function getCountOfDefaultIndexes(): int;

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    abstract public static function getDocumentSizeLimit(): int;

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
     * Get list of keywords that cannot be used
     *
     * @return array<string>
     */
    abstract public function getKeywords(): array;

    /**
     * Get an attribute projection given a list of selected attributes
     *
     * @param array<string> $selections
     * @param string $prefix
     * @return mixed
     */
    abstract protected function getAttributeProjection(array $selections, string $prefix = ''): mixed;

    /**
     * Get all selected attributes from queries
     *
     * @param Query[] $queries
     * @return string[]
     */
    protected function getAttributeSelections(array $queries): array
    {
        $selections = [];

        foreach ($queries as $query) {
            switch ($query->getMethod()) {
                case Query::TYPE_SELECT:
                    foreach ($query->getValues() as $value) {
                        $selections[] = $value;
                    }
                    break;
            }
        }

        return $selections;
    }

    /**
     * Filter Keys
     *
     * @param string $value
     * @return string
     * @throws Exception
     */
    public function filter(string $value): string
    {
        $value = preg_replace("/[^A-Za-z0-9\_\-]/", '', $value);

        if (\is_null($value)) {
            throw new Exception('Failed to filter key');
        }

        return $value;
    }

    public function escapeWildcards(string $value): string
    {
        $wildcards = [
            '%',
            '_',
            '[',
            ']',
            '^',
            '-',
            '.',
            '*',
            '+',
            '?',
            '(',
            ')',
            '{',
            '}',
            '|'
        ];

        foreach ($wildcards as $wildcard) {
            $value = \str_replace($wildcard, "\\$wildcard", $value);
        }

        return $value;
    }

    /**
     * Increase and Decrease Attribute Value
     *
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param int|float|null $min
     * @param int|float|null $max
     * @return bool
     * @throws Exception
     */
    abstract public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, int|float|null $min = null, int|float|null $max = null): bool;
}
