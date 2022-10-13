<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use GuzzleHttp\Client;
use PDOException;
use stdClass;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\ID;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class Tigris extends Adapter
{
    protected string $endpoint;
    protected Client $client;
    const DEFAULT_HEADERS = [
        'Accept' => 'application/json',
        'Content-Type' => 'application/json'
    ];
    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param PDO $pdo
     */
    public function __construct(string $endpoint, float $timeout = 2.0)
    {
        $this->endpoint = $endpoint;
        $this->client = new Client([
            'base_uri' => $endpoint,
            'timeout'  => $timeout,
            'http_errors' => false,
            'headers' => self::DEFAULT_HEADERS
        ]);
    }

    /**
     * Create Database
     *
     * @param string $name
     * @return bool
     * @throws Exception
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        $response = $this->client->post("/v1/databases/{$name}/create");

        return $response->getStatusCode() === 200;
    }

    /**
     * Check if Database exists
     * Optionally check if collection exists in Database
     *
     * @param string $database
     * @param string $collection
     * @return bool
     * @throws Exception
     */
    public function exists(string $database, ?string $collection): bool
    {
        $database = $this->filter($database);

        $response = \is_null($collection) ?
            $this->client->post("/v1/databases/{$database}/describe") :
            $this->client->post("/v1/databases/{$database}/collections/{$this->getNamespace()}_{$this->filter($collection)}/describe");

        return $response->getStatusCode() === 200;
    }

    /**
     * List Databases
     *
     * @return array
     */
    public function list(): array
    {
        $response = $this->client->post("/v1/databases/list");

        return json_decode((string) $response->getBody(), true)['databases'];
    }

    /**
     * Delete Database
     *
     * @param string $name
     * @return bool
     * @throws Exception
     */
    public function delete(string $name): bool
    {
        $name = $this->filter($name);

        $response = $this->client->delete("/v1/databases/{$name}/drop");

        return $response->getStatusCode() === 200;
    }

    /**
     * Create Collection
     *
     * @param string $name
     * @param Document[] $attributes
     * @param Document[] $indexes
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $database = $this->getDefaultDatabase();
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        $properties = [
            '$internalId' => [
                'type' => 'integer',
                'autoGenerate' => true
            ],
            '$id' => [
                'type' => 'string',
                'maxLength' => 255
            ],
            '$permissions' => [
                'type' => 'array',
                'items' => [
                    'type' => 'string'
                ]
            ]
        ];

        foreach ($attributes as $attribute) {
            $attrId = $this->filter($attribute->getId());
            $attrType = $this->getType($attribute->getAttribute('type'));
            $attrFormat = $this->getFormat($attribute->getAttribute('type'));

            $properties[$attrId] = [
                'type' => $attrType
            ];

            if (!is_null($attrFormat)) {
                $properties[$attrId]['format'] = $attrFormat;
            }
        }

        $response = $this->client->post("/v1/databases/{$database}/collections/{$namespace}_{$id}/createOrUpdate", [
            'body' => json_encode([
                'only_create' => true,
                'schema' => [
                    'title' => "{$namespace}_{$id}",
                    'properties' => $properties,
                    'primary_key' => ['$id', '$internalId']
                ]
            ])
        ]);

        return $response->getStatusCode() === 200;
    }

    /**
     * Delete Collection
     * @param string $id
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);
        $response = $this->client->delete("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$id}/drop");

        return $response->getStatusCode() === 200;
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
     * @throws Exception
     * @throws PDOException
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $type = $this->getType($type);
        $format = $this->getFormat($type);

        if ($array) {
            $attribute = [
                'type' => 'array',
                'items' => [
                    'type' => $type
                ]
            ];
            if (!is_null($format)) {
                $attribute['items']['format'] = $format;
            }
        } else {
            $attribute = [
                'type' => $type
            ];
            if (!is_null($format)) {
                $attribute['items']['format'] = $format;
            }
        }

        $response = $this->client->post("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/describe");
        $data = json_decode((string) $response->getBody(), true);

        $data['schema']['properties'][$id] = $attribute;

        $response = $this->client->post("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/createOrUpdate", [
            'body' => json_encode($data)
        ]);

        return $response->getStatusCode() === 200;
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
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        // $name = $this->filter($collection);
        // $id = $this->filter($id);
        // $type = $this->getSQLType($type, $size, $signed);

        // if ($array) {
        //     $type = 'LONGTEXT';
        // }

        // return $this->getPDO()
        //     ->prepare("ALTER TABLE {$this->getSQLTable($name)}
        //         MODIFY `{$id}` {$type};")
        //     ->execute();

        return true;
    }

    /**
     * Rename Attribute
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        // $collection = $this->filter($collection);
        // $old = $this->filter($old);
        // $new = $this->filter($new);

        // return $this->getPDO()
        //     ->prepare("ALTER TABLE {$this->getSQLTable($collection)} RENAME COLUMN `{$old}` TO `{$new}`;")
        //     ->execute();
        return true;
    }

    /**
     * Rename Index
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        // $collection = $this->filter($collection);
        // $old = $this->filter($old);
        // $new = $this->filter($new);

        // return $this->getPDO()
        //     ->prepare("ALTER TABLE {$this->getSQLTable($collection)} RENAME INDEX `{$old}` TO `{$new}`;")
        //     ->execute();
        return true;
    }

    /**
     * Delete Attribute
     *
     * @param string $collection
     * @param string $id
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        $response = $this->client->post("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/describe");
        $data = json_decode((string) $response->getBody(), true);

        unset($data['schema'][$id]);

        $response = $this->client->post("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/createOrUpdate", [
            'body' => json_encode($data)
        ]);

        return $response->getStatusCode() === 200;
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
     * @return bool
     * @throws Exception
     * @throws PDOException
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
     * @return bool
     * @throws Exception
     * @throws PDOException
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
     * @return Document
     * @throws Exception
     * @throws PDOException
     */
    public function getDocument(string $collection, string $id): Document
    {
        $name = $this->filter($collection);

        $response = $this->client->post("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/documents/read", [
            'body' => json_encode([
                'filter' => [
                    '$id' => $id
                ]
            ])
        ]);

        $document = json_decode((string) $response->getBody(), true);
        $document = new Document($document['result']['data'] ?? []);
        $document->setAttribute('$internalId', (string) $document->getAttribute('$internalId'));

        return $document;
    }

    /**
     * Create Document
     *
     * @param string $collection
     * @param Document $document
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws Duplicate
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $name = $this->filter($collection);
        $document = $document->getArrayCopy(disallow: [
            '$createdAt', '$updatedAt', '$collection'
        ]);

        $response = $this->client->post("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/documents/insert", [
            'body' => json_encode([
                'documents' => [
                    $document
                ]
            ])
        ]);

        $data = json_decode((string) $response->getBody(), true);

        $document['$internalId'] = (string) $data['keys'][0]['$internalId'];

        return new Document($document);
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws Duplicate
     */
    public function updateDocument(string $collection, Document $document): Document
    {
        $name = $this->filter($collection);
        $document = $document->getArrayCopy(disallow: [
            '$createdAt', '$updatedAt', '$collection'
        ]);

        $response = $this->client->put("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/documents/replace", [
            'body' => json_encode([
                'documents' => [
                    $document
                ]
            ])
        ]);

        $data = json_decode((string) $response->getBody(), true);

        $document['$internalId'] = (string) $data['keys'][0]['$internalId'];

        return new Document($document);


        return $document;
    }

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($id);

        $response = $this->client->delete("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$collection}/documents/delete", [
            'body' => json_encode([
                'filter' => [
                    '$id' => $id
                ]
            ])
        ]);

        return $response->getStatusCode() === 200;
    }

    /**
     * Find Documents
     *
     * @param string $collection
     * @param Query[] $queries
     * @param int $limit
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     * @param array $cursor
     * @param string $cursorDirection
     *
     * @return Document[]
     * @throws Exception
     * @throws PDOException
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array
    {
        $name = $this->getNamespace() . '_' . $this->filter($collection);

        $filter = [];
        $sort = [];
        $options =  [
            'limit' => $limit,
            'skip' => $offset
        ];

        // orders
        foreach ($orderAttributes as $i => $attribute) {
            $attribute = $this->filter($attribute);
            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);
            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }
            $sort[] = [$attribute => $this->getOrder($orderType)];
        }

        $sort[] = ['$internalId' => $this->getOrder($cursorDirection === Database::CURSOR_AFTER ? Database::ORDER_ASC : Database::ORDER_DESC)];

        if (empty($orderAttributes)) {
            // Allow after pagination without any order
            if (!empty($cursor)) {
                $orderType = $orderTypes[0] ?? Database::ORDER_ASC;
                $orderMethod = $cursorDirection === Database::CURSOR_AFTER ? ($orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER
                ) : ($orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER);

                // $filters = array_merge($filters, [
                //     '_id' => [
                //         $this->getQueryOperator($orderMethod) => new \MongoDB\BSON\ObjectId($cursor['$internalId'])
                //     ]
                // ]);
            }
            // Allow order type without any order attribute, fallback to the natural order (_id)
            if (!empty($orderTypes)) {
                $orderType = $this->filter($orderTypes[0] ?? Database::ORDER_ASC);
                if ($cursorDirection === Database::CURSOR_BEFORE) {
                    $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                }
                $sort[] = ['$internalId' => $this->getOrder($orderType)];
            }
        }

        if (!empty($cursor) && !empty($orderAttributes) && array_key_exists(0, $orderAttributes)) {
            $attribute = $orderAttributes[0];
            if (is_null($cursor[$attribute] ?? null)) {
                throw new Exception("Order attribute '{$attribute}' is empty.");
            }

            $orderMethodInternalId = Query::TYPE_GREATER;
            $orderType = $this->filter($orderTypes[0] ?? Database::ORDER_ASC);
            $orderMethod = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;

            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                $orderMethodInternalId = $orderType === Database::ORDER_ASC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
                $orderMethod = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
            }

            // $filters = array_merge($queries, [
            //     '$or' => [
            //         [
            //             $attribute => [
            //                 $this->getQueryOperator($orderMethod) => $cursor[$attribute]
            //             ]
            //         ], [
            //             $attribute => $cursor[$attribute],
            //             '_id' => [
            //                 $this->getQueryOperator($orderMethodInternalId) => new \MongoDB\BSON\ObjectId($cursor['$internalId'])
            //             ]

            //         ]
            //     ]
            // ]);
        }
        if (empty($filter)) {
            $filter = new stdClass;
        }
        $response = $this->client->post("/v1/databases/{$this->getDefaultDatabase()}/collections/{$name}/documents/search", [
            'body' => json_encode([
                'filter' => $filter,
                'options' => $options,
                'sort' => $sort
            ])
        ]);

        $response = json_decode((string) $response->getBody(), true);

        $found = array_map(function ($n) {
            $n = new Document($n['data']);
            $n->setAttribute('$internalId', (string) $n->getAttribute('$internalId'));

            return $n;
        }, $response['result']['hits']);

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $found = array_reverse($found);
        }

        return $found;
    }

    /**
     * Count Documents
     *
     * @param string $collection
     * @param Query[] $queries
     * @param int $max
     * @return int
     * @throws Exception
     * @throws PDOException
     */
    public function count(string $collection, array $queries = [], int $max = 0): int
    {
        // $name = $this->filter($collection);
        // $roles = Authorization::getRoles();
        // $where = [];
        // $limit = ($max === 0) ? '' : 'LIMIT :max';

        // foreach ($queries as $i => $query) {
        //     $query->setAttribute(match ($query->getAttribute()) {
        //         '$id' => ID::custom('_uid'),
        //         '$createdAt' => '_createdAt',
        //         '$updatedAt' => '_updatedAt',
        //         default => $query->getAttribute()
        //     });

        //     $conditions = [];
        //     foreach ($query->getValues() as $key => $value) {
        //         $conditions[] = $this->getSQLCondition('table_main.`' . $query->getAttribute() . '`', $query->getMethod(), ':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value);
        //     }

        //     $condition = implode(' OR ', $conditions);
        //     $where[] = empty($condition) ? '' : '(' . $condition . ')';
        // }

        // if (Authorization::$status) {
        //     $where[] = $this->getSQLPermissionsCondition($name, $roles);
        // }

        // $sqlWhere = !empty($where) ? 'where ' . implode(' AND ', $where) : '';
        // $sql = "SELECT COUNT(1) as sum
        //     FROM
        //         (
        //             SELECT 1
        //             FROM {$this->getSQLTable($name)} table_main
        //             " . $sqlWhere . "
        //             {$limit}
        //         ) table_count
        // ";
        // $stmt = $this->getPDO()->prepare($sql);

        // foreach ($queries as $i => $query) {
        //     if ($query->getMethod() === Query::TYPE_SEARCH) continue;
        //     foreach ($query->getValues() as $key => $value) {
        //         $stmt->bindValue(':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value, $this->getPDOType($value));
        //     }
        // }

        // if ($max !== 0) {
        //     $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        // }

        // $stmt->execute();

        // /** @var array $result */
        // $result = $stmt->fetch();

        return $result['sum'] ?? 0;
    }

    /**
     * Sum an Attribute
     *
     * @param string $collection
     * @param string $attribute
     * @param Query[] $queries
     * @param int $max
     * @return int|float
     * @throws Exception
     * @throws PDOException
     */
    public function sum(string $collection, string $attribute, array $queries = [], int $max = 0): int|float
    {
        // $name = $this->filter($collection);
        // $roles = Authorization::getRoles();
        // $where = [];
        // $limit = ($max === 0) ? '' : 'LIMIT :max';

        // foreach ($queries as $i => $query) {
        //     $query->setAttribute(match ($query->getAttribute()) {
        //         '$id' => ID::custom('_uid'),
        //         '$createdAt' => '_createdAt',
        //         '$updatedAt' => '_updatedAt',
        //         default => $query->getAttribute()
        //     });

        //     $conditions = [];
        //     foreach ($query->getValues() as $key => $value) {
        //         $conditions[] = $this->getSQLCondition('table_main.`' . $query->getAttribute() . '`', $query->getMethod(), ':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value);
        //     }

        //     $where[] = implode(' OR ', $conditions);
        // }

        // if (Authorization::$status) {
        //     $where[] = $this->getSQLPermissionsCondition($name, $roles);
        // }

        // $sqlWhere = !empty($where) ? 'where ' . implode(' AND ', $where) : '';

        // $stmt = $this->getPDO()->prepare("
        //     SELECT SUM({$attribute}) as sum
        //     FROM (
        //         SELECT {$attribute}
        //         FROM {$this->getSQLTable($name)} table_main
        //          " . $sqlWhere . "
        //         {$limit}
        //     ) table_count
        // ");

        // foreach ($queries as $i => $query) {
        //     if ($query->getMethod() === Query::TYPE_SEARCH) continue;
        //     foreach ($query->getValues() as $key => $value) {
        //         $stmt->bindValue(':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value, $this->getPDOType($value));
        //     }
        // }

        // if ($max !== 0) {
        //     $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        // }

        // $stmt->execute();

        // /** @var array $result */
        // $result = $stmt->fetch();

        return $result['sum'] ?? 0;
    }

    public function getLimitForString(): int
    {
        return 4294967295;
    }

    public function getLimitForInt(): int
    {
        return 4294967295;
    }

    public function getLimitForAttributes(): int
    {
        return 9999;
    }

    public function getLimitForIndexes(): int
    {
        return 9999;
    }

    public function getSupportForSchemas(): bool
    {
        return true;
    }

    public function getSupportForIndex(): bool
    {
        return true;
    }

    public function getSupportForUniqueIndex(): bool
    {
        return true;
    }

    public function getSupportForFulltextIndex(): bool
    {
        return true;
    }

    public function getCountOfAttributes(Document $collection): int
    {
        $attributes = \count($collection->getAttribute('attributes') ?? []);

        // +1 ==> virtual columns count as total, so add as buffer
        return $attributes + static::getCountOfDefaultAttributes() + 1;
    }

    public function getCountOfIndexes(Document $collection): int
    {
        $indexes = \count($collection->getAttribute('indexes') ?? []);

        return $indexes + static::getCountOfDefaultIndexes();
    }

    public static function getCountOfDefaultAttributes(): int
    {
        return 4;
    }

    public static function getCountOfDefaultIndexes(): int
    {
        return 0;
    }

    public static function getRowLimit(): int
    {
        return 0;
    }

    public function getAttributeWidth(Document $collection): int
    {
        return 9999;
    }

    /**
     * Get list of keywords that cannot be used
     *  Refference: https://mariadb.com/kb/en/reserved-words/
     * 
     * @return string[]
     */
    public function getKeywords(): array
    {
        return [
            'ACCESSIBLE',
            'ADD',
            'ALL',
            'ALTER',
            'ANALYZE',
            'AND',
            'AS',
            'ASC',
            'ASENSITIVE',
            'BEFORE',
            'BETWEEN',
            'BIGINT',
            'BINARY',
            'BLOB',
            'BOTH',
            'BY',
            'CALL',
            'CASCADE',
            'CASE',
            'CHANGE',
            'CHAR',
            'CHARACTER',
            'CHECK',
            'COLLATE',
            'COLUMN',
            'CONDITION',
            'CONSTRAINT',
            'CONTINUE',
            'CONVERT',
            'CREATE',
            'CROSS',
            'CURRENT_DATE',
            'CURRENT_ROLE',
            'CURRENT_TIME',
            'CURRENT_TIMESTAMP',
            'CURRENT_USER',
            'CURSOR',
            'DATABASE',
            'DATABASES',
            'DAY_HOUR',
            'DAY_MICROSECOND',
            'DAY_MINUTE',
            'DAY_SECOND',
            'DEC',
            'DECIMAL',
            'DECLARE',
            'DEFAULT',
            'DELAYED',
            'DELETE',
            'DELETE_DOMAIN_ID',
            'DESC',
            'DESCRIBE',
            'DETERMINISTIC',
            'DISTINCT',
            'DISTINCTROW',
            'DIV',
            'DO_DOMAIN_IDS',
            'DOUBLE',
            'DROP',
            'DUAL',
            'EACH',
            'ELSE',
            'ELSEIF',
            'ENCLOSED',
            'ESCAPED',
            'EXCEPT',
            'EXISTS',
            'EXIT',
            'EXPLAIN',
            'FALSE',
            'FETCH',
            'FLOAT',
            'FLOAT4',
            'FLOAT8',
            'FOR',
            'FORCE',
            'FOREIGN',
            'FROM',
            'FULLTEXT',
            'GENERAL',
            'GRANT',
            'GROUP',
            'HAVING',
            'HIGH_PRIORITY',
            'HOUR_MICROSECOND',
            'HOUR_MINUTE',
            'HOUR_SECOND',
            'IF',
            'IGNORE',
            'IGNORE_DOMAIN_IDS',
            'IGNORE_SERVER_IDS',
            'IN',
            'INDEX',
            'INFILE',
            'INNER',
            'INOUT',
            'INSENSITIVE',
            'INSERT',
            'INT',
            'INT1',
            'INT2',
            'INT3',
            'INT4',
            'INT8',
            'INTEGER',
            'INTERSECT',
            'INTERVAL',
            'INTO',
            'IS',
            'ITERATE',
            'JOIN',
            'KEY',
            'KEYS',
            'KILL',
            'LEADING',
            'LEAVE',
            'LEFT',
            'LIKE',
            'LIMIT',
            'LINEAR',
            'LINES',
            'LOAD',
            'LOCALTIME',
            'LOCALTIMESTAMP',
            'LOCK',
            'LONG',
            'LONGBLOB',
            'LONGTEXT',
            'LOOP',
            'LOW_PRIORITY',
            'MASTER_HEARTBEAT_PERIOD',
            'MASTER_SSL_VERIFY_SERVER_CERT',
            'MATCH',
            'MAXVALUE',
            'MEDIUMBLOB',
            'MEDIUMINT',
            'MEDIUMTEXT',
            'MIDDLEINT',
            'MINUTE_MICROSECOND',
            'MINUTE_SECOND',
            'MOD',
            'MODIFIES',
            'NATURAL',
            'NOT',
            'NO_WRITE_TO_BINLOG',
            'NULL',
            'NUMERIC',
            'OFFSET',
            'ON',
            'OPTIMIZE',
            'OPTION',
            'OPTIONALLY',
            'OR',
            'ORDER',
            'OUT',
            'OUTER',
            'OUTFILE',
            'OVER',
            'PAGE_CHECKSUM',
            'PARSE_VCOL_EXPR',
            'PARTITION',
            'POSITION',
            'PRECISION',
            'PRIMARY',
            'PROCEDURE',
            'PURGE',
            'RANGE',
            'READ',
            'READS',
            'READ_WRITE',
            'REAL',
            'RECURSIVE',
            'REF_SYSTEM_ID',
            'REFERENCES',
            'REGEXP',
            'RELEASE',
            'RENAME',
            'REPEAT',
            'REPLACE',
            'REQUIRE',
            'RESIGNAL',
            'RESTRICT',
            'RETURN',
            'RETURNING',
            'REVOKE',
            'RIGHT',
            'RLIKE',
            'ROWS',
            'SCHEMA',
            'SCHEMAS',
            'SECOND_MICROSECOND',
            'SELECT',
            'SENSITIVE',
            'SEPARATOR',
            'SET',
            'SHOW',
            'SIGNAL',
            'SLOW',
            'SMALLINT',
            'SPATIAL',
            'SPECIFIC',
            'SQL',
            'SQLEXCEPTION',
            'SQLSTATE',
            'SQLWARNING',
            'SQL_BIG_RESULT',
            'SQL_CALC_FOUND_ROWS',
            'SQL_SMALL_RESULT',
            'SSL',
            'STARTING',
            'STATS_AUTO_RECALC',
            'STATS_PERSISTENT',
            'STATS_SAMPLE_PAGES',
            'STRAIGHT_JOIN',
            'TABLE',
            'TERMINATED',
            'THEN',
            'TINYBLOB',
            'TINYINT',
            'TINYTEXT',
            'TO',
            'TRAILING',
            'TRIGGER',
            'TRUE',
            'UNDO',
            'UNION',
            'UNIQUE',
            'UNLOCK',
            'UNSIGNED',
            'UPDATE',
            'USAGE',
            'USE',
            'USING',
            'UTC_DATE',
            'UTC_TIME',
            'UTC_TIMESTAMP',
            'VALUES',
            'VARBINARY',
            'VARCHAR',
            'VARCHARACTER',
            'VARYING',
            'WHEN',
            'WHERE',
            'WHILE',
            'WINDOW',
            'WITH',
            'WRITE',
            'XOR',
            'YEAR_MONTH',
            'ZEROFILL',
            'ACTION',
            'BIT',
            'DATE',
            'ENUM',
            'NO',
            'TEXT',
            'TIME',
            'TIMESTAMP',
            'BODY',
            'ELSIF',
            'GOTO',
            'HISTORY',
            'MINUS',
            'OTHERS',
            'PACKAGE',
            'PERIOD',
            'RAISE',
            'ROWNUM',
            'ROWTYPE',
            'SYSDATE',
            'SYSTEM',
            'SYSTEM_TIME',
            'VERSIONING',
            'WITHOUT'
        ];
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
     * Get Tigris Type
     *
     * @param string $type
     * @return string
     * @throws Exception
     */
    protected function getType(string $type): string
    {
        return match ($type) {
            Database::VAR_STRING, Database::VAR_DATETIME => 'string',
            Database::VAR_INTEGER => 'integer',
            Database::VAR_FLOAT => 'number',
            Database::VAR_BOOLEAN => 'boolean',
            Database::VAR_DOCUMENT => 'object',
            default => throw new Exception('Unknown Type')
        };
    }

    /**
     * Get Tigris Format
     *
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @return string
     * @throws Exception
     */
    protected function getFormat(string $type): ?string
    {
        return match ($type) {
            Database::VAR_DATETIME => 'date-time',
            default => null
        };
    }

    protected function getOrder(string $order): string
    {
        return match ($order) {
            Database::ORDER_ASC => '$asc',
            Database::ORDER_DESC => '$desc',
            default => throw new Exception('Unknown sort order:' . $order)
        };
    }
}
