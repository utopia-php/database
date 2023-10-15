<?php

namespace Utopia\Database\Adapter;

use Exception;
use Throwable;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Fetch\Client;
use Utopia\Database\Query;
use Utopia\Database\Exception as DatabaseException;

abstract class Proxy extends Adapter
{
    protected string $endpoint;
    protected string $secret;
    protected string $database;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param string $endpoint
     * @param string $secret
     */
    public function __construct(string $endpoint, string $secret, string $database)
    {
        $this->endpoint = $endpoint;
        $this->secret = $secret;
        $this->database = $database;
    }

    private function query(string $action, mixed $body = []): mixed
    {
        $response = Client::fetch(
            url: $this->endpoint . '/queries/' . $action,
            method: 'POST',
            headers: [
                'x-utopia-secret' => $this->secret,
                'x-utopia-database' => $this->database,
                'x-utopia-namespace' => $this->getNamespace(),
                'x-utopia-default-database' => $this->defaultDatabase,
                'x-utopia-timeout' => self::$timeout,
                'content-type' => 'application/json'
            ],
            body: $body
        );

        if ($response->getStatusCode() >= 400) {
            if(empty($response->getBody())) {
                throw new Exception('Internal ' . $response->getBody() . ' HTTP error in database proxy.');
            }

            $error = \json_decode($response->getBody(), true);

            /**
             * @var \Utopia\Database\Exception $exception
             */
            try {
                $exception = new $error['type']($error['message'], $error['code']);
                $exception->setFile($error['file']);
                $exception->setLine($error['line']);
                // TODO: If possible in PHP, set trace too for better error reporting
                // $exception->setTrace($error['trace']);
            } catch(Throwable $err) {
                // Cannot find exception type
                throw new Exception($error['message'], $error['code']);
            }

            throw $exception;
        }

        $body = \json_decode($response->getBody(), true);
        return $body['output'] ?? '';
    }

    /**
     * Ping Database
     *
     * @return bool
     */
    public function ping(): bool
    {
        return $this->query('ping');
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
        return $this->query('create', ['name' => $name]);
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
        return $this->query('exists', ['database' => $database, 'collection' => $collection]);
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
        return $this->query('delete', ['name' => $name]);
    }

    /**
     * Create Collection
     *
     * @param string $name
     * @param array<Document> $attributes (optional)
     * @param array<Document> $indexes (optional)
     * @return bool
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        return $this->query('createCollection', ['name' => $name, 'attributes' => $attributes, 'indexes' => $indexes]);
    }

    /**
     * Delete Collection
     *
     * @param string $id
     *
     * @return bool
     */
    public function deleteCollection(string $id): bool
    {
        return $this->query('deleteCollection', ['id' => $id]);
    }

    /**
     * Create Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @param bool $array
     * @return bool
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        return $this->query('createAttribute', [
            'collection' => $collection,
            'id' => $id,
            'type' => $type,
            'size' => $size,
            'signed' => $signed,
            'array' => $array,
        ]);
    }

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
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        return $this->query('updateAttribute', [
            'collection' => $collection,
            'id' => $id,
            'type' => $type,
            'size' => $size,
            'signed' => $signed,
            'array' => $array,
        ]);
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
        return $this->query('deleteAttribute', ['collection' => $collection, 'id' => $id]);
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
        return $this->query('renameAttribute', ['collection' => $collection, 'old' => $old, 'new' => $new]);
    }

    /**
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $id
     * @param string $twoWayKey
     * @return bool
     */
    public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool
    {
        return $this->query('createRelationship', [
            'collection' => $collection,
            'relatedCollection' => $relatedCollection,
            'type' => $type,
            'twoWay' => $twoWay,
            'id' => $id,
            'twoWayKey' => $twoWayKey
        ]);
    }

    /**
     * Update Relationship
     *
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $key
     * @param string $twoWayKey
     * @param string|null $newKey
     * @param string|null $newTwoWayKey
     * @return bool
     */
    public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        return $this->query('updateRelationship', [
            'collection' => $collection,
            'relatedCollection' => $relatedCollection,
            'type' => $type,
            'twoWay' => $twoWay,
            'key' => $key,
            'twoWayKey' => $twoWayKey,
            'newKey' => $newKey,
            'newTwoWayKey' => $newTwoWayKey
        ]);
    }

    /**
     * Delete Relationship
     *
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $key
     * @param string $twoWayKey
     * @param string $side
     * @return bool
     */
    public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool
    {
        return $this->query('deleteRelationship', [
            'collection' => $collection,
            'relatedCollection' => $relatedCollection,
            'type' => $type,
            'twoWay' => $twoWay,
            'key' => $key,
            'twoWayKey' => $twoWayKey,
            'side' => $side,
        ]);
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
        return $this->query('renameIndex', ['collection' => $collection, 'old' => $old, 'new' => $new]);
    }

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
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool
    {
        return $this->query('createIndex', ['collection' => $collection, 'id' => $id, 'type' => $type, 'attributes' => $attributes, 'lengths' => $lengths, 'orders' => $orders]);
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
        return $this->query('deleteIndex', ['collection' => $collection, 'id' => $id]);
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     * @param array<Query> $queries
     * @return Document
     */
    public function getDocument(string $collection, string $id, array $queries = []): Document
    {
        return new Document($this->query('getDocument', ['collection' => $collection, 'id' => $id, 'queries' => $queries]));
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
        return new Document($this->query('createDocument', ['collection' => $collection, 'document' => $document]));
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
        return new Document($this->query('updateDocument', ['collection' => $collection, 'document' => $document]));
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
        return $this->query('deleteDocument', ['collection' => $collection, 'id' => $id]);
    }

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
    public function find(string $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, ?int $timeout = null): array
    {
        $results = $this->query('find', [
            'collection' => $collection,
            'queries' => $queries,
            'limit' => $limit,
            'offset' => $offset,
            'orderAttributes' => $orderAttributes,
            'orderTypes' => $orderTypes,
            'cursor' => $cursor,
            'cursorDirection' => $cursorDirection,
            'timeout' => $timeout
        ]);

        foreach ($results as $index => $document) {
            $results[$index] = new Document($results[$index]);
        }

        return $results;
    }

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
    public function sum(string $collection, string $attribute, array $queries = [], ?int $max = null, ?int $timeout = null): float|int
    {
        return $this->query('sum', [
            'collection' => $collection,
            'attribute' => $attribute,
            'queries' => $queries,
            'max' => $max,
            'timeout' => $timeout
        ]);
    }

    /**
     * Count Documents
     *
     * @param string $collection
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int
     */
    public function count(string $collection, array $queries = [], ?int $max = null, ?int $timeout = null): int
    {
        return $this->query('count', [
            'collection' => $collection,
            'queries' => $queries,
            'max' => $max,
            'timeout' => $timeout
        ]);
    }

    /**
     * Get Collection Size
     *
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollection(string $collection): int
    {
        return $this->query('getSizeOfCollection', ['collection' => $collection]);
    }

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfAttributes(Document $collection): int
    {
        return $this->query('getCountOfAttributes', ['collection' => $collection]);
    }

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfIndexes(Document $collection): int
    {
        return $this->query('getCountOfIndexes', ['collection' => $collection]);
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
        return $this->query('getAttributeWidth', ['collection' => $collection]);
    }

    /**
     * Get an attribute projection given a list of selected attributes
     *
     * @param array<string> $selections
     * @param string $prefix
     * @return mixed
     */
    protected function getAttributeProjection(array $selections, string $prefix = ''): mixed
    {
        // Not nessessary for this adapter
        return [];
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
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, int|float|null $min = null, int|float|null $max = null): bool
    {
        return $this->query('increaseDocumentAttribute', [
            'collection' => $collection,
            'id' => $id,
            'attribute' => $attribute,
            'value' => $value,
            'min' => $min,
            'max' => $max
        ]);
    }
}
