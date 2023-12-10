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
use Utopia\Database\Validator\Authorization;
use Utopia\Fetch\FetchException;

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

    /**
     * @throws FetchException
     * @throws DatabaseException
     * @throws Exception
     */
    private function query(string $method, string $path, mixed $body = []): mixed
    {
        $roles = \implode(',', Authorization::getRoles());
        $response = Client::fetch(
            url: $this->endpoint . $path,
            headers: [
                'x-utopia-secret' => $this->secret,
                'x-utopia-database' => $this->database,
                'x-utopia-namespace' => $this->getNamespace(),
                'x-utopia-default-database' => $this->defaultDatabase,
                'x-utopia-auth-roles' => $roles,
                'x-utopia-auth-status' => Authorization::$status ? 'true' : 'false',
                'x-utopia-auth-status-default' => Authorization::$statusDefault ? 'true' : 'false',
                'x-utopia-timeout' => self::$timeout ? \strval(self::$timeout) : '',
                'content-type' => 'application/json'
            ],
            method: $method,
            body: $body
        );

        if ($response->getStatusCode() >= 400) {
            if (empty($response->getBody())) {
                throw new Exception('Internal ' . $response->getStatusCode() . ' HTTP error in database proxy');
            }

            $error = \json_decode($response->getBody(), true);

            try {
                $exception = new $error['type']($error['message'], $error['code']);
                /**
                 * @var \Utopia\Database\Exception $exception
                 */

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
     * @throws DatabaseException|FetchException
     */
    public function ping(): bool
    {
        return $this->query('GET', '/ping', []);
    }

    /**
     * Create Database
     *
     * @param string $name
     *
     * @return bool
     * @throws DatabaseException|FetchException
     */
    public function create(string $name): bool
    {
        return $this->query('POST', '/databases', ['database' => $name]);
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
        $path = '/databases/' . $database;

        if(!empty($collection))
        {
            $path = '/collections/' . $collection . '?database=' . $database;
        }

        return $this->query('GET', $path, []);
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
        return $this->query('DELETE', '/databases/' . $name, []);
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
        return $this->query('POST', '/collections', [
            'collection' => $name,
            'attributes' => $attributes,
            'indexes' => $indexes
        ]);
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
        return $this->query('DELETE', '/collections/' . $id, []);
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
        return $this->query('POST', '/collections/' . $collection . '/attributes', [
            'attribute' => $id,
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
        return $this->query('PUT', '/collections/'.$collection.'/attributes/' . $id, [
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
        return $this->query('DELETE', '/collections/' . $collection . '/attributes/' . $id, []);
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
        return $this->query('PATCH', '/collections/' . $collection . '/attributes/' . $old . '/name', ['new' => $new]);
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
        return $this->query('POST', '/collections/' . $collection . '/relationships', [
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
        return $this->query('PUT', '/collections/'. $collection .'/relationships/' . $relatedCollection, [
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
        return $this->query('DELETE', '/collections/'. $collection .'/relationships/' . $relatedCollection, [
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
        return $this->query('PATCH', '/collections/'.$collection.'/indexes/'.$old.'/name',[
            'new' => $new
        ]);
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
        return $this->query('POST', '/collections/' . $collection . '/indexes', [
            'index' => $id,
            'type' => $type,
            'attributes' => $attributes,
            'lengths' => $lengths,
            'orders' => $orders
        ]);
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
        return $this->query('DELETE', '/collections/'. $collection .'/indexes/' . $id, []);
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     * @param array<Query> $queries
     * @return Document
     * @throws DatabaseException
     */
    public function getDocument(string $collection, string $id, array $queries = []): Document
    {
        $path = '/collections/' . $collection . '/documents/' . $id;

        $arr = [];
        foreach ($queries as $query){
            $arr[] = json_encode($query->jsonSerialize());
        }

        $path .= '?' . http_build_query(['queries'=> $arr]);

        return new Document($this->query('GET', $path, []));
    }

    /**
     * Create Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     * @throws DatabaseException|FetchException
     */
    public function createDocument(string $collection, Document $document): Document
    {
        return new Document($this->query('POST', '/collections/' . $collection . '/documents', ['document' => $document]));
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     * @throws DatabaseException|FetchException
     */
    public function updateDocument(string $collection, Document $document): Document
    {
        return new Document($this->query('PUT', '/collections/'. $collection .'/documents', ['document' => $document]));
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
        return $this->query('DELETE','/collections/'. $collection .'/documents/' . $id, []);
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
        $arr = [];
        foreach ($queries as $query){
            $arr[] = json_encode($query->jsonSerialize());
        }

        $body = [
            'queries'=> $arr,
            'limit' => $limit,
            'offset' => $offset,
            'orderAttributes' => $orderAttributes,
            'orderTypes' => $orderTypes,
            'cursor' => $cursor,
            'cursorDirection' => $cursorDirection,
            'timeout' => $timeout
        ];

        $path = '/collections/' . $collection . '/documents';
        $path .= '?' . http_build_query($body);

        $results = $this->query('GET', $path, []);

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
        return $this->query('GET', '/collections/'. $collection .'/documents-sum', [
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
        return $this->query('GET', '/collections/'. $collection .'/documents-count', [
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
     * @throws DatabaseException|FetchException
     */
    public function getSizeOfCollection(string $collection): int
    {
        return $this->query('GET', '/collections/' . $collection . '/size', []);
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
        return $this->query('PATCH', '/collections/'. $collection .'/documents/'. $id .'/increase', [
            'attribute' => $attribute,
            'value' => $value,
            'min' => $min,
            'max' => $max
        ]);
    }
}
