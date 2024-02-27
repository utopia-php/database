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

abstract class DataAPI extends Adapter
{
    protected string $endpoint;
    protected string $secret;
    protected string $database;

    /**
     * @var array<string, int> $timeouts Map of timeouts where key is event name and value is timeout in milliseconds
     */
    protected array $timeouts = [];

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
     * @param mixed[] $params
     *
     * @throws FetchException
     * @throws DatabaseException
     * @throws Exception
     */
    private function query(string $query, array $params = []): mixed
    {
        $roles = \implode(',', Authorization::getRoles());
        $response = Client::fetch(
            url: $this->endpoint . '/queries',
            headers: [
                'x-utopia-secret' => $this->secret,
                'x-utopia-database' => $this->database,
                'x-utopia-namespace' => $this->getNamespace(),
                'x-utopia-auth-roles' => $roles,
                'x-utopia-auth-status' => Authorization::$status ? 'true' : 'false',
                'x-utopia-auth-status-default' => Authorization::$statusDefault ? 'true' : 'false',
                'x-utopia-timeouts' => $this->timeouts,
                'content-type' => 'application/json'
            ],
            method: 'POST',
            body: [
                'query' => $query,
                'params' => $params
            ]
        );

        if ($response->getStatusCode() >= 400) {
            if (empty($response->getBody())) {
                throw new Exception('Internal ' . $response->getStatusCode() . ' HTTP error in data api');
            }

            $error = \json_decode($response->getBody(), true);

            try {
                $exception = new $error['type']($error['message'], $error['code'], $error['file'], $error['line']);
                /**
                 * @var DatabaseException $exception
                 */
            } catch(Throwable $err) {
                // Cannot find exception type
                throw new Exception($error['message'], $error['code']);
            }

            throw $exception;
        }

        $body = \json_decode($response->getBody(), false);

        $output = $body->output ?? '';

        $processArray = function (mixed $self, mixed $json) {
            $keys = [];

            foreach ($json as $param) {
                if(\is_object($param)) {
                    $keys[] = new Document((array) $param);
                } elseif(\is_array($param)) {
                    $keys[] = $self($self, $param);
                } else {
                    $keys[] = $param;
                }
            }

            return $keys;
        };

        if(\is_array($output)) {
            $output = $processArray($processArray, $output);
        } else {
            $output = $processArray($processArray, [$output])[0];
        }

        return $output;
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
        return $this->query('create', [$name]);
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string $database database name
     * @param string|null $collection (optional) collection name
     *
     * @return bool
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        return $this->query('exists', [$database, $collection]);
    }

    /**
     * List Databases
     *
     * @return array<Document>
     */
    public function list(): array
    {
        return $this->query('list');
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
        return $this->query('delete', [$name]);
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
        return $this->query('createCollection', [$name, $attributes, $indexes]);
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
        return $this->query('deleteCollection', [$id]);
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
        return $this->query('createAttribute', [$collection, $id, $type, $size, $signed, $array]);
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
        return $this->query('updateAttribute', [$collection, $id, $type, $size, $signed, $array]);
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
        return $this->query('deleteAttribute', [$collection, $id]);
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
        return $this->query('renameAttribute', [$collection, $old, $new]);
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
        return $this->query('createRelationship', [$collection, $relatedCollection, $type, $twoWay, $id, $twoWayKey]);
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
        return $this->query('updateRelationship', [$collection, $relatedCollection, $type, $twoWay, $key, $twoWayKey, $newKey, $newTwoWayKey]);
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
        return $this->query('deleteRelationship', [$collection, $relatedCollection, $type, $twoWay, $key, $twoWayKey, $side]);
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
        return $this->query('renameIndex', [$collection, $old, $new]);
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
        return $this->query('createIndex', [$collection, $id, $type, $attributes, $lengths, $orders]);
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
        return $this->query('deleteIndex', [$collection, $id]);
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
        return $this->query('getDocument', [$collection, $id, $queries]);
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
        return $this->query('createDocument', [$collection, $document]);
    }

    /**
     * Create Documents in batches
     *
     * @param string $collection
     * @param array<Document> $documents
     * @param int $batchSize
     *
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    public function createDocuments(string $collection, array $documents, int $batchSize): array
    {
        return $this->query('createDocuments', [$collection, $documents, $batchSize]);
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
        return $this->query('updateDocument', [$collection, $document]);
    }

    /**
     * Update Documents in batches
     *
     * @param string $collection
     * @param array<Document> $documents
     * @param int $batchSize
     *
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    public function updateDocuments(string $collection, array $documents, int $batchSize): array
    {
        return $this->query('updateDocuments', [$collection, $documents, $batchSize]);
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
        return $this->query('deleteDocument', [$collection, $id]);
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
     *
     * @return array<Document>
     */
    public function find(string $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array
    {
        return $this->query('find', [$collection, $queries, $limit, $offset, $orderAttributes, $orderTypes, $cursor, $cursorDirection]);
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
    public function sum(string $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        return $this->query('sum', [$collection, $attribute, $queries, $max]);
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
    public function count(string $collection, array $queries = [], ?int $max = null): int
    {
        return $this->query('count', [$collection, $queries, $max]);
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
        return $this->query('getSizeOfCollection', [$collection]);
    }

    /**
     * Get max STRING limit
     *
     * @return int
     */
    public function getLimitForString(): int
    {
        return $this->query('getLimitForString');
    }

    /**
     * Get max INT limit
     *
     * @return int
     */
    public function getLimitForInt(): int
    {
        return $this->query('getLimitForInt');
    }

    /**
     * Get maximum attributes limit.
     *
     * @return int
     */
    public function getLimitForAttributes(): int
    {
        return $this->query('getLimitForAttributes');
    }

    /**
     * Get maximum index limit.
     *
     * @return int
     */
    public function getLimitForIndexes(): int
    {
        return $this->query('getLimitForIndexes');
    }

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    public function getSupportForSchemas(): bool
    {
        return $this->query('getSupportForSchemas');
    }

    /**
     * Is index supported?
     *
     * @return bool
     */
    public function getSupportForIndex(): bool
    {
        return $this->query('getSupportForIndex');
    }

    /**
     * Is unique index supported?
     *
     * @return bool
     */
    public function getSupportForUniqueIndex(): bool
    {
        return $this->query('getSupportForUniqueIndex');
    }

    /**
     * Is fulltext index supported?
     *
     * @return bool
     */
    public function getSupportForFulltextIndex(): bool
    {
        return $this->query('getSupportForFulltextIndex');
    }

    /**
     * Is fulltext wildcard supported?
     *
     * @return bool
     */
    public function getSupportForFulltextWildcardIndex(): bool
    {
        return $this->query('getSupportForFulltextWildcardIndex');
    }


    /**
     * Does the adapter handle casting?
     *
     * @return bool
     */
    public function getSupportForCasting(): bool
    {
        return $this->query('getSupportForCasting');
    }

    /**
     * Does the adapter handle array Contains?
     *
     * @return bool
     */
    public function getSupportForQueryContains(): bool
    {
        return $this->query('getSupportForQueryContains');
    }

    /**
     * Are timeouts supported?
     *
     * @return bool
     */
    public function getSupportForTimeouts(): bool
    {
        return $this->query('getSupportForTimeouts');
    }

    /**
     * Are relationships supported?
     *
     * @return bool
     */
    public function getSupportForRelationships(): bool
    {
        return $this->query('getSupportForRelationships');
    }

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfAttributes(Document $collection): int
    {
        return $this->query('getCountOfAttributes', [$collection]);
    }

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfIndexes(Document $collection): int
    {
        return $this->query('getCountOfIndexes', [$collection]);
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
        return $this->query('getAttributeWidth', [$collection]);
    }

    /**
     * Get list of keywords that cannot be used
     *
     * @return array<string>
     */
    public function getKeywords(): array
    {
        return $this->query('getKeywords');
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
        return $this->query('getAttributeProjection', [$selections, $prefix]);
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
        return $this->query('increaseDocumentAttribute', [$collection, $id, $attribute, $value, $min, $max]);
    }

    /**
     * @return int
     */
    public function getMaxIndexLength(): int
    {
        return $this->query('getMaxIndexLength');
    }

    /**
     * Set a global timeout for database queries in milliseconds.
     *
     * This function allows you to set a maximum execution time for all database
     * queries executed using the library, or a specific event specified by the
     * event parameter. Once this timeout is set, any database query that takes
     * longer than the specified time will be automatically terminated by the library,
     * and an appropriate error or exception will be raised to handle the timeout condition.
     *
     * @param int $milliseconds The timeout value in milliseconds for database queries.
     * @param string $event     The event the timeout should fire fore
     * @return void
     *
     * @throws Exception The provided timeout value must be greater than or equal to 0.
     */
    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        // TODO: Use $event?
        $this->timeout = $milliseconds;
    }
}
