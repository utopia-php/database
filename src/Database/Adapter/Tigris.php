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

    protected function request(string $method = 'POST', string $path, array $payload = []): array
    {
        $response = empty($payload) ? $this->client->request($method, $path) : $this->client->request($method, $path, [
            'body' => json_encode($payload)
        ]);

        $body = json_decode((string) $response->getBody(), true);

        if (array_key_exists('error', $body)) {
            throw new Exception($body['error']['message'], $response->getStatusCode());
        }

        return $body;
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

        try {
            \is_null($collection) ?
                $this->request(path: "/v1/databases/{$database}/describe") :
                $this->request(path: "/v1/databases/{$database}/collections/{$this->getNamespace()}_{$this->filter($collection)}/describe");
            return true;
        } catch (\Throwable $th) {
            return false;
        }
    }

    /**
     * List Databases
     *
     * @return array
     */
    public function list(): array
    {
        $response = $this->request(path: "/v1/databases/list");

        return $response['databases'];
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

        $this->request(method: 'delete', path: "/v1/databases/{$name}/drop");

        return true;
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

            if (in_array($attrId, $this->getKeywords())) {
                $attrId = "\$reserved_{$attrId}";
            }

            $properties[$attrId] = [
                'type' => $attrType
            ];

            if (!is_null($attrFormat)) {
                $properties[$attrId]['format'] = $attrFormat;
            }
        }

        $this->request(path: "/v1/databases/{$database}/collections/{$namespace}_{$id}/createOrUpdate", payload: [
            'only_create' => true,
            'schema' => [
                'title' => "{$namespace}_{$id}",
                'properties' => $properties,
                'primary_key' => ['$id', '$internalId']
            ]
        ]);

        return true;
    }

    /**
     * Delete Collection
     * @param string $id
     * @return bool
     * @throws Exception
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        $this->request(method: 'delete', path: "/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$id}/drop");

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
     * @return bool
     * @throws Exception
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $type = $this->getType($type);
        $format = $this->getFormat($type);

        $id = $this->removeKeyword($id);

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

        $response = $this->request(path: "/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/describe");

        $response['schema']['properties'][$id] = $attribute;

        $response = $this->request(path: "/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/createOrUpdate", payload: $response);

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
        $document = $this->revertRemovingDocumentKeywordsFromDocument($document);

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

        $new = $this->removeDocumentKeywordsFromDocument(clone $document);
        $new = $new->getArrayCopy(disallow: [
            '$createdAt', '$updatedAt', '$collection'
        ]);

        $data = $this->request(path: "/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/documents/insert", payload: [
            'documents' => [
                $new
            ]
        ]);

        $document->setAttribute('$internalId', (string) $data['keys'][0]['$internalId']);

        return $document;
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
        $new = $this->removeDocumentKeywordsFromDocument(clone $document);
        $new = $new->getArrayCopy(disallow: [
            '$createdAt', '$updatedAt', '$collection'
        ]);

        $response = $this->client->put("/v1/databases/{$this->getDefaultDatabase()}/collections/{$this->getNamespace()}_{$name}/documents/replace", [
            'body' => json_encode([
                'documents' => [
                    $new
                ]
            ])
        ]);

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

        if (Authorization::$status) { // skip if authorization is disabled
            $roles = Authorization::getRoles();
            $count = count($roles);
            if ($count > 1) {
                $filter['$or'] = array_map(fn ($role) => ['$permissions' => "read({$role})"], $roles);
            } else if ($count === 1) {
                $filter['$permissions'] = "read({$roles[0]})";
            }
        }

        if (empty($filter)) {
            $filter = new stdClass;
        }
        var_dump([
            'filter' => $filter,
            'options' => $options,
            'sort' => $sort
        ]);
        $response = $this->request(path: "/v1/databases/{$this->getDefaultDatabase()}/collections/{$name}/documents/search", payload: [
            'filter' => $filter,
            'options' => $options,
            'sort' => $sort
        ]);

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
        return false;
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
     *  Refference: https://docs.tigrisdata.com/documents/restrictions
     * 
     * @return string[]
     */
    public function getKeywords(): array
    {
        return [
            "abstract", "add", "alias", "and", "any", "args", "arguments", "array",
            "as", "as?", "ascending", "assert", "async", "await", "base", "bool", "boolean", "break", "by", "byte",
            "callable", "case", "catch", "chan", "char", "checked", "class", "clone", "const", "constructor", "continue",
            "debugger", "decimal", "declare", "def", "default", "defer", "del", "delegate", "delete", "descending", "die",
            "do", "double", "dynamic", "echo", "elif", "else", "elseif", "empty", "enddeclare", "endfor", "endforeach",
            "endif", "endswitch", "endwhile", "enum", "equals", "eval", "event", "except", "exception", "exit", "explicit",
            "export", "extends", "extern", "fallthrough", "false", "final", "finally", "fixed", "float", "fn", "for",
            "foreach", "from", "fun", "func", "function", "get", "global", "go", "goto", "group", "if", "implements",
            "implicit", "import", "in", "include", "include_once", "init", "instanceof", "insteadof", "int", "integer",
            "interface", "internal", "into", "is", "isset", "join", "lambda", "let", "list", "lock", "long", "managed",
            "map", "match", "module", "nameof", "namespace", "native", "new", "nint", "none", "nonlocal", "not", "notnull",
            "nuint", "null", "number", "object", "of", "on", "operator", "or", "orderby", "out", "override", "package",
            "params", "partial", "pass", "print", "private", "protected", "public", "raise", "range", "readonly", "record",
            "ref", "remove", "require", "require_once", "return", "sbyte", "sealed", "select", "set", "short", "sizeof",
            "stackalloc", "static", "strictfp", "string", "struct", "super", "switch", "symbol", "synchronized", "this",
            "throw", "throws", "trait", "transient", "true", "try", "type", "typealias", "typeof", "uint", "ulong",
            "unchecked", "unmanaged", "unsafe", "unset", "use", "ushort", "using", "val", "value", "var", "virtual", "void",
            "volatile", "when", "where", "while", "with", "xor", "yield"
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

    protected function removeDocumentKeywordsFromDocument(Document $document): Document
    {
        foreach (array_keys($document->getAttributes()) as $attribute) {
            if (in_array($attribute, $this->getKeywords())) {
                $new = $this->removeKeyword($attribute);
                $document->setAttribute($new, $document->getAttribute($attribute))->removeAttribute($attribute);
            } elseif (str_contains($attribute, '-')) {
                $new = $this->removeKeyword($attribute);
                $document->setAttribute($new, $document->getAttribute($attribute))->removeAttribute($attribute);
            }
        }

        return $document;
    }

    protected function revertRemovingDocumentKeywordsFromDocument(Document $document): Document
    {
        foreach (array_keys($document->getAttributes()) as $attribute) {
            if (str_starts_with($attribute, '$reserved_')) {
                $new = $this->revertRemovingKeyword($attribute);
                $document->setAttribute($new, $document->getAttribute($attribute))->removeAttribute($attribute);
            } elseif (str_starts_with($attribute, '$dash')) {
                $new = $this->revertRemovingKeyword($attribute);
                $document->setAttribute($new, $document->getAttribute($attribute))->removeAttribute($attribute);
            }
        }

        return $document;
    }

    protected function removeKeyword(string $value): string
    {
        if (in_array($value, $this->getKeywords())) {
            return "\$reserved_{$value}";
        } elseif (str_contains($value, '-')) {
            $new = str_replace('-', '__dash__', $value);
            return "\$dash_{$new}";
        } elseif (is_numeric($value[0])) {
            return "\$number_{$value}";
        }
        return $value;
    }

    protected function revertRemovingKeyword(string $value): string
    {
        if (str_starts_with($value, '$reserved_')) {
            return str_replace('$reserved_', '', $value);
        } elseif (str_starts_with($value, '$dash')) {
            $new = str_replace('$dash_', '', $value);
            $new = str_replace('__dash__', '-', $new);

            return $new;
        } elseif (str_starts_with($value, '$number')) {
            return str_replace('$number_', '', $value);
        }

        return $value;
    }
}
