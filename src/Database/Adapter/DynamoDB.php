<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Document;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Query;
use Utopia\Database\Exception as DatabaseException;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\DynamoDbClient as Client;

class DynamoDB extends Adapter
{
    // Data types
    public const VAR_STRING = 'S';
    public const VAR_NUMBER = 'N';
    public const VAR_BINARY = 'B';
    public const VAR_NULL = 'NULL';
    public const VAR_BOOL = 'BOOL';

    // Index types
    public const SIMPLE_INDEX = 'SIMPLE';
    public const COMPOSITE_INDEX = 'COMPOSITE';

    protected Client $client;

    /**
     * @return Client
     *
     * @throws Exception
     */
    protected function getClient(): Client
    {
        return $this->client;
    }



    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * Ping Database
     * 
     * DynamoDB is a managed database - If the DynamoDB client is successfully initialized, you can assume the service is accessible.
     * @return bool
     */
    public function ping(): bool
    {
        return true;
    }

    /**
     * Create Database
     *
     * @param string $name
     *
     * No concept of Database schemas in DynamoDb.
     * @return bool
     */
    public function create(string $name): bool
    {
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
        if (!\is_null($collection)) {
            $collection = $this->filter($collection);
            try {
                $this->client->describeTable([
                    'TableName' => "{$this->getNamespace()}_{$collection}",
                ]);
            } catch (DynamoDbException $e) {
                return false;
            }
        }
        return true;
    }

    /**
     * List Databases
     *
     * No concept of Database schemas in DynamoDb.
     * @return array<Document>
     */
    public function list(): array
    {
        return [];
    }

    /**
     * Delete Database
     *
     * @param string $name
     *
     * No concept of Database schemas in DynamoDb.
     * @return bool
     */
    public function delete(string $name): bool
    {
        return true;
    }

    /**
     * Get DynamoDb Type
     *
     * @param string $type
     *
     * @return string
     */
    protected function getDynamoDbType(string $type): string
    {
        if (in_array($type, array(Database::VAR_STRING, Database::VAR_RELATIONSHIP))) {
            return DynamoDB::VAR_STRING;
        } else if (in_array($type, array(Database::VAR_INTEGER, Database::VAR_FLOAT, Database::VAR_BOOLEAN, Database::VAR_DATETIME))) {
            return DynamoDB::VAR_NUMBER;
        } else {
            throw new DatabaseException('Unknown Type: ' . $type);
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
        $tableName = "{$this->getNamespace()}_{$this->filter($name)}";
        
        if ($name === Database::METADATA && $this->exists($this->getNamespace(), $name)) {
            return true;
        }

        $attributeDefinitions = [
            [
                'AttributeName' => '_id',
                'AttributeType' => DynamoDB::VAR_STRING,
            ],
            [
                'AttributeName' => '_uid',
                'AttributeType' => DynamoDB::VAR_STRING,
            ],
            [
                'AttributeName' => '_createdAt',
                'AttributeType' => DynamoDB::VAR_NUMBER,
            ],
            [
                'AttributeName' => '_updatedAt',
                'AttributeType' => DynamoDB::VAR_NUMBER,
            ],
            [
                'AttributeName' => '_permissions',
                'AttributeType' => DynamoDB::VAR_STRING,
            ]
        ];

        $globalIndexes = [
            [
                'IndexName' => '_uid',
                'KeySchema' => [
                    [
                        'AttributeName' => '_uid',
                        'KeyType' => 'HASH',
                    ],
                ],
                'Projection' => [
                    'ProjectionType' => 'ALL',
                ],
            ],
            [
                'IndexName' => '_createdAt',
                'KeySchema' => [
                    [
                        'AttributeName' => '_createdAt',
                        'KeyType' => 'HASH',
                    ],
                ],
                'Projection' => [
                    'ProjectionType' => 'ALL',
                ],
            ],
            [
                'IndexName' => '_updatedAt',
                'KeySchema' => [
                    [
                        'AttributeName' => '_updatedAt',
                        'KeyType' => 'HASH',
                    ],
                ],
                'Projection' => [
                    'ProjectionType' => 'ALL',
                ],
            ],
            [
                'IndexName' => '_permissions',
                'KeySchema' => [
                    [
                        'AttributeName' => '_permissions',
                        'KeyType' => 'HASH',
                    ],
                ],
                'Projection' => [
                    'ProjectionType' => 'ALL',
                ],
            ],
        ];

        $attributeDefMap = [];

        foreach ($attributes as $attribute) {
            $attrId = $this->filter($attribute->getId());
            $attrType = $this->getDynamoDbType($attribute->getAttribute('type'));

            $attributeDef = [
                'AttributeName' => $attrId,
                'AttributeType' => $attrType,
            ];

            $attributeDefMap[$attrId] = $attributeDef;
        }

        foreach ($indexes as $index) {
            $indexId = $this->filter($index->getId());
            $indexType = $index->getAttribute('type');
            $indexAttributes = $index->getAttribute('attributes');

            $globalIndex = [
                'IndexName' => $indexId,
                'Projection' => [
                    'ProjectionType' => 'ALL',
                ],
            ];
            
            $globalIndex['KeySchema'] = [
                [
                    'AttributeName' => $indexAttributes[0],
                    'KeyType' => 'HASH',
                    ]
            ];
            array_push($attributeDefinitions, $attributeDefMap[$indexAttributes[0]]);

            if ($indexType == DynamoDB::COMPOSITE_INDEX) {
                array_push($globalIndex['KeySchema'], [
                    'AttributeName' => $indexAttributes[1],
                    'KeyType' => 'RANGE',
                ]);
                array_push($attributeDefinitions, $attributeDefMap[$indexAttributes[1]]);
            }
            array_push($globalIndexes, $globalIndex);
        }

        $createTableParams = [
            'TableName' => $tableName,
            'BillingMode' => 'PAY_PER_REQUEST',
            'AttributeDefinitions' => $attributeDefinitions,
            'KeySchema' => [
                [
                    'AttributeName' => '_id',
                    'KeyType' => 'HASH',
                ],
            ],
            'GlobalSecondaryIndexes' => $globalIndexes,
        ];

        $this->client->createTable($createTableParams);
        
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
        $name = $this->filter($name);
        try {
            $this->client->deleteTable([
                'TableName' => "{$this->getNamespace()}_{$name}",
            ]);
        } catch (DynamoDbException $e) {
            return false;
        }
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
        $collection = $this->filter($collection);
        $id = $this->filter($id);
        $queryParams = [
            'TableName' => "{$this->getNamespace()}_{$collection}",
            'ProjectionExpression' => '#id',
            'ExpressionAttributeNames' => [ '#id' => '_id' ],
        ];
        $items = ($this->client->scan($queryParams))['Items'];
        foreach ($items as $item) {
            $updateParams = [
                'TableName' => "{$this->getNamespace()}_{$collection}",
                'Key' => ['_id' => [ 'S' => $item['_id']]], // Replace with your primary key
                'UpdateExpression' => "REMOVE {$id}",
            ];
            try {
                $this->client->updateItem($updateParams);
            } catch (DynamoDbException $e) {
                return false;
            }
        }
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
        $collection = $this->filter($collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $queryParams = [
            'TableName' => "{$this->getNamespace()}_{$collection}",
            'ProjectionExpression' => "#id, {$old}",
            'ExpressionAttributeNames' => [ '#id' => '_id' ],
        ];
        $items = ($this->client->scan($queryParams))['Items'];
        foreach ($items as $item) {
            $oldItemValues = $item[$old];
            $oldItemValue = null;
            foreach ($oldItemValues as $value) {
                $oldItemValue = $value;
            }
            $updateParams = [
                'TableName' => "{$this->getNamespace()}_{$collection}",
                'Key' => ['_id' => [ 'S' => $item['_id']['S']]], // Replace with your primary key
                'UpdateExpression' => "SET {$new} = {$oldItemValue} REMOVE {$old}",
            ];
            try {
                $this->client->updateItem($updateParams);
            } catch (DynamoDbException $e) {
                return false;
            }
        }
        return true;
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
        return true;
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
        return true;
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
     * @param array<Query> $queries
     * @return Document
     */
    public function getDocument(string $collection, string $id, array $queries = []): Document
    {
        $tableName = "{$this->getNamespace()}_{$collection}";
        $id = $this->filter($id);

        $selections = $this->getAttributeSelections($queries);

        $queryParams = [
            'TableName' => $tableName,
            'IndexName' => '_uid',
            'KeyConditionExpression' => '#uid = :uid',
            'ExpressionAttributeValues' => [ ':uid' => [ 'S' => $id ] ],
            'ExpressionAttributeNames' => [ '#uid' => '_uid' ],
        ];

        if (!empty($selections) && !\in_array('*', $selections)) {
            $queryParams['ProjectionExpression'] = $this->getAttributeProjection($selections);
            $queryParams['ExpressionAttributeNames'] = [
                '#uid' => '_uid',
                '#id' => '_id', 
                '#createdAt' => '_createdAt', 
                '#updatedAt'=> '_updatedAt', 
                '#permissions' => '_permissions' 
            ];
        }

        $result = $this->client->query($queryParams);

        $result = $result['Items'];

        if (empty($result)) {
            return new Document([]);
        }

        $document = [];

        foreach ($result[0] as $resultKey => $resultAttributes) {
            foreach ($resultAttributes as $attribute) {
                $document[$resultKey] = $attribute;
            }
        }

        if (\array_key_exists('_id', $document)) {
            $document['$internalId'] = $document['_id'];
            unset($document['_id']);
        }
        if (\array_key_exists('_uid', $document)) {
            $document['$id'] = $document['_uid'];
            unset($document['_uid']);
        }
        if (\array_key_exists('_createdAt', $document)) {
            $epoch = $document['_createdAt'];
            $document['$createdAt'] = DateTime::format(new \DateTime("@$epoch"));
            unset($document['_createdAt']);
        }
        if (\array_key_exists('_updatedAt', $document)) {
            $epoch = $document['_updatedAt'];
            $document['$updatedAt'] = DateTime::format(new \DateTime("@$epoch"));
            unset($document['_updatedAt']);
        }
        if (\array_key_exists('_permissions', $document)) {
            $document['$permissions'] = json_decode($document['_permissions'] ?? '[]', true);
            unset($document['_permissions']);
        }
        
        return new Document($document);
    }

    protected function getAttributeValueType(mixed $attributeValue): string
    {
        $dataType = gettype($attributeValue);
        if (\in_array($dataType, ['integer', 'double'])) {
            return DynamoDB::VAR_NUMBER;
        } else if (\in_array($dataType, ['boolean'])) {
            return DynamoDB::VAR_BOOL;
        } else if (\in_array($dataType, ['string', 'array', 'object'])) {
            return DynamoDB::VAR_STRING;
        } else if (\in_array($dataType, ['resource', 'unknown type'])) {
            return DynamoDB::VAR_BINARY;
        } else {
            return DynamoDB::VAR_NULL;
        }
    }

    protected function getGuidv4($data = null): string {
        // Generate 16 bytes (128 bits) of random data or use the data passed into the function.
        $data = $data ?? random_bytes(16);
        assert(strlen($data) == 16);
    
        // Set version to 0100
        $data[6] = chr(ord($data[6]) & 0x0f | 0x40);
        // Set bits 6-7 to 10
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80);
    
        // Output the 36 character UUID.
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
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
        $tableName = $this->getNamespace() . '_' . $this->filter($collection);
        $internalId = $document->getInternalId();
        $documentUid = $document->getId();
        $document->removeAttribute('$internalId');

        $documentAttributes = [];

        if (\array_key_exists('$id', (array)$document)) {
            $documentAttributes['_uid'] = [ 'S' => $document['$id'] ];
            unset($document['$id']);
        }
        if (\array_key_exists('$createdAt', (array)$document)) {
            $documentAttributes['_createdAt'] = [ 'N' => \strtotime($document['$createdAt']) ];
            unset($document['$createdAt']);
        }
        if (\array_key_exists('$updatedAt', (array)$document)) {
            $documentAttributes['_updatedAt'] = [ 'N' => \strtotime($document['$updatedAt']) ];
            unset($document['$updatedAt']);
        }
        if (\array_key_exists('$permissions', (array)$document)) {
            $documentAttributes['_permissions'] = [ 'S' => json_encode($document['$permissions'] ?? []) ];
            unset($document['$permissions']);
        }

        if (empty($internalId)) {
            $internalId = $this->getGuidv4();
        }
        $documentAttributes['_id'] = [ 'S' => $internalId ];

        foreach ((array)$document as $documentAttributeKey => $documentAttributeValue) {
            $documentAttributes[$documentAttributeKey] = [ $this->getAttributeValueType($documentAttributeValue) => $documentAttributeValue];
        }

        // echo(json_encode($documentAttributes));

        $this->client->putItem([
            'Item' => $documentAttributes,
            'TableName' => $tableName,
        ]);

        return $this->getDocument($collection, $documentUid);
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
        return $this->createDocument($collection, $document);
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
        $tableName = $this->getNamespace() . '_' . $this->filter($collection);
        $document = $this->getDocument($collection, $id, [Query::select(['$id'])]);
        $result = $this->client->deleteItem([
            'Key' => [ '_id' => [ 'S' => $document->getInternalId() ] ],
            'TableName' => $tableName,
        ]);
        return (!!$result);
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
        return [];
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
        return 0;
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
        return 0;
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
        return 0;
    }

    /**
     * Get max STRING limit
     * 
     * DynamoDb string limits are governed by item size limit at 400 Kb - using MariaDB Limit for now.
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
     * The actual limit is 9.9999999999999999999999999999999999999E+125 but the PHP Limit is obviously much less.
     *
     * @return int
     */
    public function getLimitForInt(): int
    {
        return PHP_INT_MAX;
    }

    /**
     * Get maximum attributes limit.
     *
     * @return int
     */
    public function getLimitForAttributes(): int
    {
        return 0;
    }


    /**
     * Get maximum index limit.
     *
     * DynamoDb limit for Global Secondary Indexes.
     * @return int
     */
    public function getLimitForIndexes(): int
    {
        return 20;
    }

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    public function getSupportForSchemas(): bool
    {
        return true;
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
        return true;
    }

    /**
     * Is fulltext wildcard supported?
     *
     * @return bool
     */
    public function getSupportForFulltextWildcardIndex(): bool
    {
        return true;
    }


    /**
     * Does the adapter handle casting?
     *
     * @return bool
     */
    public function getSupportForCasting(): bool
    {
        return true;
    }

    /**
     * Does the adapter handle array Contains?
     *
     * @return bool
     */
    public function getSupportForQueryContains(): bool
    {
        return true;
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
     * Are relationships supported?
     *
     * @return bool
     */
    public function getSupportForRelationships(): bool
    {
        return false;
    }

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfAttributes(Document $collection): int
    {
        return 0;
    }

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfIndexes(Document $collection): int
    {
        return 0;
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
        return 0;
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
        return 0;
    }

    /**
     * Get list of keywords that cannot be used
     *
     * @return array<string>
     */
    public function getKeywords(): array
    {
        return [];
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
        $projection = ['#uid', '#id', '#createdAt', '#updatedAt', '#permissions'];

        foreach ($selections as $selection) {
            // Skip internal attributes since all are selected by default
            if (\in_array($selection, Database::INTERNAL_ATTRIBUTES)) {
                continue;
            }

            \array_push($projection, $selection);
        }
        return \implode(", ", $projection);
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
        return true;
    }

    /**
     * @return int
     */
    public function getMaxIndexLength(): int
    {
        return 0;
    }
}