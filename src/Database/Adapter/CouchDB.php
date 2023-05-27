<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\CouchDB\Exception\DocumentNotFound;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Adapter\CouchDB\Request;
use Utopia\Database\Adapter\CouchDB\Exception\InvalidCase;
use Utopia\Database\Adapter\CouchDB\Exception\Notsupported;
use Utopia\Database\Adapter\CouchDB\Exception\NullException;
use Utopia\Database\Adapter\CouchDB\Response;
use Utopia\Database\Exception\Authorization;
use Utopia\Database\Exception\Conflict;
use Utopia\Database\Exception\Duplicate;



class CouchDB extends Adapter
{
    /**
     * @var array
     */
    public const operators = [
        Query::TYPE_EQUAL => '$eq',
        Query::TYPE_NOTEQUAL => '$ne',
        Query::TYPE_LESSER => '$lt',
        Query::TYPE_LESSEREQUAL => '$lte',
        Query::TYPE_GREATER => '$gt',
        Query::TYPE_GREATEREQUAL => '$gte',
        Query::TYPE_CONTAINS => '$in',
        '$text',
        Query::TYPE_SEARCH => '$exists',
        '$or',
        '$and',
        '$match',
        '$regex'
    ];

    /**
     * @var Client
     */
    private Request $req;

    public function __construct(string $namespace, string $host, int $port, string $username, string $password, ?string $database = null)
    {
        $this->setNamespace($namespace);

        $this->req = new Request($host, $port, $username, $password);
        
        if (!is_null($database) && isset($database)) {
            $this->setDefaultDatabase($this->generateDatabaseName($database));
            try {
                if (!$this->ping()) {
                    throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Database not found');
                }
            } catch (Exception $e) {
                throw $e;
            }
        }

    }

    /**
     * Generate database name
     * 
     * @param string $name
     * 
     * @return string
     */
    private function generateDatabaseName(string $name): string
    {
        return $this->filter($this->getNamespace() . '_' . $name);
    }

    /**
     * Ping Database
     * 
     * @return bool
     */
    public function ping(): bool
    {
        try {
            $database = $this->getDefaultDatabase();
        
            $response = $this->req->head(['uri' => "/$database", 'data' => null]);

            if ($response['code'] === Response::OK)
                return true;

            return false;
        } catch (Exception $e) {
            throw $e;
        }
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
        try {
            if (is_null($name) || !isset($name))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $name cannot be empty');

            if (!ctype_lower($name))
                throw new InvalidCase(__CLASS__ . '::' . __METHOD__ . ' $name must be lowercase');
        
            $name = $this->generateDatabaseName($name);
            $response = $this->req->put(['uri' => "/$name", 'data' => null]);

            if ($response['code'] === Response::CREATED)
                return true;
            elseif ($response['code'] === Response::BAD_REQUEST)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Invalid database name');
            elseif ($response['code'] === Response::UNAUTHORIZED)
                throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' CouchDB Server Administrator privileges required');
            elseif ($response['code'] === Response::PRECONDITION_FAILED)
                throw new Duplicate(__CLASS__ . '::' . __METHOD__ . ' Database already exists');

            return false;
        } catch (Exception $e) {
            throw $e;
        }
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
        try {
            if (is_null($database) || !isset($database))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $database cannot be empty');
            $database = $this->generateDatabaseName($database);

            if (!is_null($collection) && isset($collection)) {
                $collection = $this->filter($collection);
                $response = $this->req->head([
                    'uri' => "/$database/$collection",
                    'data' => null
                ]);

                if ($response['code'] === Response::OK)
                    return true;
                elseif ($response['code'] === Response::UNAUTHORIZED)
                    throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' Read privilege required');

                return false;
            }

            $response = $this->req->head(['uri' => "/$database", 'data' => null]);

            if ($response['code'] === Response::OK)
                return true;

            return false;
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * List Databases
     *
     * @return array<Document>
     */
    public function list(): array
    {
        try {
            $response = $this->req->get(['uri' => '/_all_dbs', 'data' => null]);
            return array(new Document((array)$response['body']));
        } catch (Exception $e) {
            throw $e;
        }
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
        try {
            if (is_null($name) || !isset($name))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $name cannot be empty');

            if (!ctype_lower($name))
                throw new InvalidCase(__CLASS__ . '::' . __METHOD__ . ' $name must be lowercase');
            
            $name = $this->generateDatabaseName($name);
            $response = $this->req->delete(['uri' => "/$name", 'data' => null]);

            if ($response['code'] === Response::OK)
                return true;
            elseif ($response['code'] === Response::BAD_REQUEST)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Invalid database name or forgotten document id by accident');
            elseif ($response['code'] === Response::UNAUTHORIZED)
                throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' CouchDB Server Administrator privileges required');
            elseif ($response['code'] === Response::NOT_FOUND)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Database doesn’t exist or invalid database name');

            return false;
        } catch (Exception $e) {
            throw $e;
        }
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
     * @param bool $signed
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
     * @param bool $signed
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
     * @param array<string> $attributes
     * @param array<int> $lengths
     * @param array<string> $orders
     *
     * @return bool
     */
    public function createIndex(string $collection = null, string $id, string $type = 'json', array $attributes, array $lengths, array $orders): bool
    {
        try {
            $database = $this->getDefaultDatabase();
          
            if (is_null($id) && !isset($id))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $id cannot be empty');

            $data = [
                'index' => [
                    'fields' => $attributes
                ],
                'name' => "$database-$id",
                'type' => $type
            ];
            $response = $this->req->post(['uri' => "$database/_index", 'data' => $data]);

            if ($response['code'] === Response::OK)
                return true;
            elseif ($response['code'] === Response::BAD_REQUEST)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Invalid request');
            elseif ($response['code'] === Response::UNAUTHORIZED)
                throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' Admin permission required');
            elseif ($response['code'] === Response::NOT_FOUND)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Database not found');

            return false;
        } catch (Exception $e) {
            throw $e;
        }
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
        try {
            $database = $this->getDefaultDatabase();

            if (is_null($id) && !isset($id))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $id cannot be empty');

            $res1 = $this->req->get(['uri' => "/$database/_index", 'data' => null]);
            if ($res1['code'] === Response::OK) {
                $foundIndex = false;
                $designDoc = '';
                $indexes = $res1->body['indexes'];
                foreach ($indexes as $index) {
                    if ($id === $index['name']) {
                        $foundIndex = true;
                        $designDoc .= $index['ddoc'];
                        break;
                    }
                }

                if ($foundIndex) {
                    $res2 = $this->req->delete(['uri' => "$database/_index/$designDoc/json/$id", 'data' => null]);

                    if ($res2['code'] === Response::OK)
                        return true;
                    elseif ($res2['code'] === Response::BAD_REQUEST)
                        throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Invalid request');
                    elseif ($res2['code'] === Response::UNAUTHORIZED)
                        throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' Writer permission required');
                    elseif ($res2['code'] === Response::NOT_FOUND)
                        throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Index not found');
                }
            }

            return false;
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     * @param array<Query> $queries
     * @return Document
     */
    public function getDocument(string $collection, string $id = null, array $queries = []): Document
    {
        try {
            $database = $this->getDefaultDatabase();

            if (is_null($collection) || !isset($collection))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $collection cannot be empty');

            $result = [];
            $collection = $this->filter($collection);
            
            $response = $this->req->get(['uri' => "/$database/$collection", 'data' => null]);
            if ($response['code'] === Response::OK || $response['code'] === Response::NOT_MODIFIED) {
                $result = $response['body'];

                $filteredResult = [];
                if (!empty($queries)) {
                    $attributes = $this->getAttributeSelections($queries);
                    foreach ($attributes as $attribute) {
                        if (array_key_exists($attribute, $result))
                            $filteredResult[$attribute] = $result[$attribute];
                        else
                            $filteredResult[$attribute] = null;
                    }
                    return new Document((array)$filteredResult);
                }

                return new Document((array)$result);
            }

            return new Document([]);
        } catch (Exception $e) {
            throw $e;
        }
            
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
        try {
            $database = $this->getDefaultDatabase();

            if (is_null($collection) || !isset($collection))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $collection cannot be empty');

            $collection = $this->filter($collection);
            $response = $this->req->put(['uri' => "/$database/$collection", 'data' => $document->getArrayCopy()]);

            if ($response['code'] === Response::OK)
                return new Document((array)$response['body']);
            elseif ($response['code'] === Response::BAD_REQUEST)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Invalid request body or parameters');
            elseif ($response['code'] === Response::UNAUTHORIZED)
                throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' Write privileges required');
            elseif ($response['code'] === Response::CONFLICT)
                throw new Conflict(__CLASS__ . '::' . __METHOD__ . ' Document with the specified ID already exists or specified revision is not latest for target document');
        } catch (Exception $e) {
            throw $e;
        }
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
        try {
            $database = $this->getDefaultDatabase();

            if (is_null($collection) || !isset($collection))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $collection cannot be empty');

            $collection = $this->filter($collection);
            $doc = $this->getDocument($collection);

            if ($doc->count() == 0)
                throw new DocumentNotFound(__CLASS__ . '::' . __METHOD__ . ' Document not found');

            $revisionNumber = $doc['_rev'];
            $response = $this->req->put(['uri' => "/$database/$collection?rev=$revisionNumber", 'data' => $document->getArrayCopy()]);

            if ($response['code'] === Response::OK || $response['code'] === Response::CREATED)
                return new Document((array)$response['body']);
            elseif ($response['code'] === Response::BAD_REQUEST)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Invalid request body or parameters');
            elseif ($response['code'] === Response::UNAUTHORIZED)
                throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' Write privileges required');
            elseif ($response['code'] === Response::CONFLICT)
                throw new Conflict(__CLASS__ . '::' . __METHOD__ . ' Document with the specified ID already exists or specified revision is not latest for target document');
            
            return new Document([]);
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function deleteDocument(string $collection, string $id = null): bool
    {
        try {
            $database = $this->getDefaultDatabase();

            if (is_null($collection) && !isset($collection))
                throw new NullException(__CLASS__ . '::' . __METHOD__ . ' $collection cannot be empty');

            $collection = $this->filter($collection);
            $doc = $this->getDocument($collection);

            if ($doc->count() == 0)
                throw new DocumentNotFound(__CLASS__ . '::' . __METHOD__ . ' Document not found');

            $revisionNumber = $doc['_rev'];
            $response = $this->req->delete(['uri' => "/$database/$collection?rev=$revisionNumber", 'data' => null]);

            if ($response['code'] === Response::OK)
                return true;
            elseif ($response['code'] === Response::BAD_REQUEST)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Invalid request body or parameters');
            elseif ($response['code'] === Response::UNAUTHORIZED)
                throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' Write privileges required');
            elseif ($response['code'] === Response::NOT_FOUND)
                throw new DocumentNotFound(__CLASS__ . '::' . __METHOD__ . ' Specified database or document ID doesn’t exists');
            elseif ($response['code'] === Response::CONFLICT)
                throw new Conflict(__CLASS__ . '::' . __METHOD__ . ' Specified revision is not the latest for target document');

            return false;
        } catch (Exception $e) {
            throw $e;
        }
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
    public function find(string $collection = null, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, ?int $timeout = null): array
    {
        try {
            $database = $this->getDefaultDatabase();

            $data = [
                'selector' => [],
                'limit' => $limit,
                'skip' => is_null($offset) ? 0 : $offset
            ];

            foreach ($queries as $query) {
                switch ($query->getMethod()) {
                    case Query::TYPE_SELECT:
                        $data['selector'][$query->getAttribute()] = $query->getValue();
                    case Query::TYPE_EQUAL:
                        $data['selector'][$query->getAttribute()] = [self::operators[Query::TYPE_EQUAL] => $query->getValue()];
                    case Query::TYPE_NOTEQUAL:
                        $data['selector'][$query->getAttribute()] = [self::operators[Query::TYPE_NOTEQUAL] => $query->getValue()];
                    case Query::TYPE_GREATER:
                        $data['selector'][$query->getAttribute()] = [self::operators[Query::TYPE_GREATER] => $query->getValue()];
                    case Query::TYPE_GREATEREQUAL:
                        $data['selector'][$query->getAttribute()] = [self::operators[Query::TYPE_GREATEREQUAL] => $query->getValue()];
                    case Query::TYPE_LESSER:
                        $data['selector'][$query->getAttribute()] = [self::operators[Query::TYPE_LESSER] => $query->getValue()];
                    case Query::TYPE_LESSEREQUAL:
                        $data['selector'][$query->getAttribute()] = [self::operators[Query::TYPE_LESSEREQUAL] => $query->getValue()];
                    case Query::TYPE_CONTAINS:
                        $data['selector'][$query->getAttribute()] = [self::operators[Query::TYPE_CONTAINS] => $query->getValue()];
                    case Query::TYPE_SEARCH:
                        $data['selector'][$query->getAttribute()] = [self::operators[Query::TYPE_SEARCH] => $query->getValue()];
                    default:
                        $data['selector'][$query->getAttribute()] = $query->getValue();
                }
            }

            $response = $this->req->post(['uri' => "/$database/_find", 'data' => $data]);

            $result = [];
            if ($response['code'] === Response::OK) {
                foreach ((array)$response['body']['docs'] as $doc)
                    $result[] = new Document((array)$doc);
                
                return $result;
            } elseif ($response['code'] === Response::BAD_REQUEST)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Invalid request');
            elseif ($response['code'] === Response::UNAUTHORIZED)
                throw new Authorization(__CLASS__ . '::' . __METHOD__ . ' Read permission required');
            elseif ($response['code'] === Response::NOT_FOUND)
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Requested database not found');

            return [new Document([])];
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * Find the first data set using chosen queries
     * 
     * @param array<Query> $queries
     * 
     * @return Document
     */
    public function findFirst(array $queries = []): Document
    {
        return $this->find(queries:$queries, limit:1)[0];
    }

    /**
     * Find the last data set using chosen queries
     * 
     * @param array<Query> $queries
     * 
     * @return Document
     */
    public function findLast(array $queries = []): Document
    {
        return end($this->find(queries:$queries));
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
        throw new Notsupported(__CLASS__ . '::' . __METHOD__ . 'CouchDB does not support fixed attributes/table columns');
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
    public function count(?string $collection, array $queries = [], ?int $max = null): int
    {
       try {
            $database = $this->getDefaultDatabase();
            
            $response = $this->req->get(['uri' => "/$database", 'data' => null]);

            if ($response['code'] === Response::OK) {
                if (!is_null($max) && $max < $response['body']['doc_count'])
                    return $max;

                return $response['body']['doc_count'];
            } else
                throw new Exception(__CLASS__ . '::' . __METHOD__ . ' Requested database not found');
       } catch (Exception $e) {
            throw $e;
       }
    }

    /**
     * Get max STRING limit
     *
     * @return int
     */
    public function getLimitForString(): int
    {
        return 4294967295;
    }

    /**
     * Get max INT limit
     *
     * @return int
     */
    public function getLimitForInt(): int
    {
        return 4294967295;
    }

    /**
     * Get maximum attributes limit.
     *
     * @return int
     */
    public function getLimitForAttributes(): int
    {
        throw new Notsupported(__CLASS__ . '::' . __METHOD__ . ' CouchDB does not support fixed attributes/table columns');
    }

    /**
     * Get maximum index limit.
     *
     * @return int
     */
    public function getLimitForIndexes(): int
    {
        return 4294967295;
    }

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    public function getSupportForSchemas(): bool
    {
        return false;
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
        return false;
    }

    /**
     * Is fulltext wildcard supported?
     *
     * @return bool
     */
    public function getSupportForFulltextWildcardIndex(): bool
    {
        return false;
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

    /**
     * Does the adapter handle array Contains?
     *
     * @return bool
     */
    public function getSupportForQueryContains(): bool
    {
        return false;
    }

    /**
     * Are timeouts supported?
     *
     * @return bool
     */
    public function getSupportForTimeouts(): bool
    {
        return true;
    }

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfAttributes(Document $collection): int
    {
        return $collection->count();
    }

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfIndexes(Document $collection): int
    {
        throw new Notsupported('');
    }

    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    public static function getCountOfDefaultAttributes(): int
    {
        return 0;
    }

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    public static function getCountOfDefaultIndexes(): int
    {
        throw new Notsupported('');
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    public static function getDocumentSizeLimit(): int
    {
        return 0;
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
        throw new Notsupported(__CLASS__ . '::' . __METHOD__ . ' ');
    }

    /**
     * Get list of keywords that cannot be used
     *
     * @return array<string>
     */
    public function getKeywords(): array
    {
        return [
            'ADVISE',
            'ALL',
            'ALTER',
            'ANALYZE',
            'AND',
            'ANY',
            'APPLY',
            'ARRAY',
            'AS',
            'ASC',
            'AT',
            'AUTOGENERATED',
            'BEGIN',
            'BETWEEN',
            'BINARY',
            'BOOLEAN',
            'BREAK',
            'BTREE',
            'BUCKET',
            'BUILD',
            'BY',
            'CALL',
            'CASE',
            'CAST',
            'CLOSED',
            'CLUSTER',
            'COLLATE',
            'COLLECTION',
            'COMMIT',
            'COMMITTED',
            'COMPACT',
            'COMPACTION',
            'CONNECT',
            'CONTINUE',
            'CORRELATE',
            'CORRELATED',
            'COVER',
            'CREATE',
            'CURRENT',
            'DATABASE',
            'DATASET',
            'DATASTORE',
            'DATAVERSE',
            'DECLARE',
            'DECREMENT',
            'DEFINITION',
            'DELETE',
            'DERIVED',
            'DESC',
            'DESCRIBE',
            'DISCONNECT',
            'DISTINCT',
            'DO',
            'DROP',
            'EACH',
            'ELEMENT',
            'ELSE',
            'END',
            'ENFORCED',
            'EVERY',
            'EXCEPT',
            'EXCLUDE',
            'EXECUTE',
            'EXISTS',
            'EXPLAIN',
            'EXTERNAL',
            'FALSE',
            'FEED',
            'FETCH',
            'FILTER',
            'FIRST',
            'FLATTEN',
            'FLUSH',
            'FOLLOWING',
            'FOR',
            'FORCE',
            'FROM',
            'FTS',
            'FULL',
            'FUNCTION',
            'GOLANG',
            'GRANT',
            'GROUP',
            'GROUPS',
            'GSI',
            'HASH',
            'HAVING',
            'HINTS',
            'IF',
            'IGNORE',
            'ILIKE',
            'IN',
            'INCLUDE',
            'INCREMENT',
            'INDEX',
            'INFER',
            'INGESTION',
            'INLINE',
            'INNER',
            'INSERT',
            'INTERNAL',
            'INTERSECT',
            'INTO',
            'IS',
            'ISOLATION',
            'JAVASCRIPT',
            'JOIN',
            'KEY',
            'KEYS',
            'KEYSPACE',
            'KEYWORD',
            'KNOWN',
            'LANGUAGE',
            'LAST',
            'LEFT',
            'LET',
            'LETTING',
            'LEVEL',
            'LIKE',
            'LIMIT',
            'LOAD',
            'LSM',
            'MAP',
            'MAPPING',
            'MATCHED',
            'MATERIALIZED',
            'MERGE',
            'MINUS',
            'MISSING',
            'NAMESPACE',
            'NEST',
            'NGRAM',
            'NL',
            'NO',
            'NODEGROUP',
            'NOT',
            'NTH_VALUE',
            'NULL',
            'NULLS',
            'NUMBER',
            'OBJECT',
            'OFFSET',
            'ON',
            'OPEN',
            'OPTION',
            'OPTIONS',
            'OR',
            'ORDER',
            'OTHERS',
            'OUTER',
            'OUTPUT',
            'OVER',
            'PARSE',
            'PARTITION',
            'PASSWORD',
            'PATH',
            'POLICY',
            'POOL',
            'PRE-SORTED',
            'PRECEDING',
            'PREPARE',
            'PRIMARY',
            'PRIVATE',
            'PRIVILEGE',
            'PROBE',
            'PROCEDURE',
            'PUBLIC',
            'RANGE',
            'RAW',
            'REALM',
            'REDUCE',
            'REFRESH',
            'RENAME',
            'RESPECT',
            'RETURN',
            'RETURNING',
            'REVOKE',
            'RIGHT',
            'ROLE',
            'ROLLBACK',
            'ROW',
            'ROWS',
            'RTREE',
            'RUN',
            'SATISFIES',
            'SAVEPOINT',
            'SCHEMA',
            'SCOPE',
            'SECONDARY',
            'SELECT',
            'SELF',
            'SEMI',
            'SET',
            'SHADOW',
            'SHOW',
            'SOME',
            'START',
            'STATISTICS',
            'STRING',
            'SYSTEM',
            'TEMPORARY',
            'THEN',
            'TIES',
            'TO',
            'TRAN',
            'TRANSACTION',
            'TRIGGER',
            'TRUE',
            'TRUNCATE',
            'TYPE',
            'UNBOUNDED',
            'UNDER',
            'UNION',
            'UNIQUE',
            'UNKNOWN',
            'UNNEST',
            'UNSET',
            'UPDATE',
            'UPSERT',
            'USE',
            'USER',
            'USING',
            'VALIDATE',
            'VALUE',
            'VALUED',
            'VALUES',
            'VIA',
            'VIEW',
            'WHEN',
            'WHERE',
            'WHILE',
            'WINDOW',
            'WITH',
            'WITHIN',
            'WORK',
            'WRITE',
            'XOR',
        ];
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
        throw new Notsupported(__CLASS__ . '::' . __METHOD__ . ' ');
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
        throw new Notsupported(__CLASS__ . '::' . __METHOD__ . ' ');
    }

    /**
     * Hash a string using md5 algorithm
     * 
     * @param string $text
     * 
     * @return string
     */
    public static function md5(string $text): string
    {
        return md5($text);
    }

    /**
     * Hash a string using bcrypt algorithm
     * 
     * @param string $text
     * @param int $cost
     * 
     * @return string
     */
    public static function bcrypt(string $text, int $cost = 12): string
    {
        $options = [
            'cost' => $cost
        ];

        return password_hash($text, PASSWORD_BCRYPT, $options);
    }
}