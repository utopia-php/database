<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class ScyllaDB extends SQL
{
    /**
     * Create Database
     *
     * @param string $name
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        if ($this->exists($name)) {
            return true;
        }

        $sql = "CREATE KEYSPACE {$name} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";

        $sql = $this->trigger(Database::EVENT_DATABASE_CREATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
    }

    /**
     * Delete Database
     *
     * @param string $name
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function delete(string $name): bool
    {
        $name = $this->filter($name);

        $sql = "DROP KEYSPACE IF EXISTS {$name}";

        $sql = $this->trigger(Database::EVENT_DATABASE_DELETE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
    }

    /**
     * Create Collection
     *
     * @param string $name
     * @param array<Document> $attributes
     * @param array<Document> $indexes
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->filter($name);

        /** @var array<string> $attributeStrings */
        $attributeStrings = [];

        foreach ($attributes as $key => $attribute) {
            $attrId = $this->filter($attribute->getId());

            $attrType = $this->getSQLType(
                $attribute->getAttribute('type'),
                $attribute->getAttribute('size', 0),
                $attribute->getAttribute('signed', true),
                $attribute->getAttribute('array', false)
            );

            $attributeStrings[$key] = "{$attrId} {$attrType}";
        }

        $collection = "
            CREATE TABLE {$this->getSQLTable($id)} (
                _id UUID PRIMARY KEY,
                _uid text,
                _createdAt timestamp,
                _updatedAt timestamp,
                _permissions text,
                " . \implode(', ', $attributeStrings) . "
            ) WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
            AND gc_grace_seconds = 86400";

        $collection = $this->trigger(Database::EVENT_COLLECTION_CREATE, $collection);

        try {
            $this->getPDO()
                ->prepare($collection)
                ->execute();

            // Create permissions table
            $permissions = "
                CREATE TABLE {$this->getSQLTable($id . '_perms')} (
                    _id UUID PRIMARY KEY,
                    _type text,
                    _permission text,
                    _document text,
                    _tenant int
                ) WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
                AND gc_grace_seconds = 86400";

            $permissions = $this->trigger(Database::EVENT_COLLECTION_CREATE, $permissions);

            $this->getPDO()
                ->prepare($permissions)
                ->execute();

            // Create indexes
            foreach ($indexes as $index) {
                $indexId = $this->filter($index->getId());
                $indexType = $index->getAttribute('type');
                $indexAttributes = $index->getAttribute('attributes');

                $this->createIndex($id, $indexId, $indexType, $indexAttributes);
            }

            return true;
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Delete collection
     *
     * @param string $id
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id)}; DROP TABLE IF EXISTS {$this->getSQLTable($id . '_perms')};";

        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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
        $type = $this->getSQLType($type, $size, $signed, $array);

        $sql = "ALTER TABLE {$this->getSQLTable($name)} ADD {$id} {$type};";
        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Get SQL type
     *
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @param bool $array
     * @return string
     */
    protected function getSQLType(string $type, int $size, bool $signed = true, bool $array = false): string
    {
        switch ($type) {
            case Database::VAR_STRING:
                return 'text';
            case Database::VAR_INTEGER:
                return 'int';
            case Database::VAR_FLOAT:
                return 'float';
            case Database::VAR_BOOLEAN:
                return 'boolean';
            case Database::VAR_DATETIME:
                return 'timestamp';
            default:
                throw new DatabaseException('Unknown type: ' . $type);
        }
    }

    /**
     * Get PDO Type
     *
     * @param mixed $value
     * @return int
     */
    protected function getPDOType(mixed $value): int
    {
        switch (gettype($value)) {
            case 'NULL':
                return PDO::PARAM_NULL;
            case 'boolean':
                return PDO::PARAM_BOOL;
            case 'integer':
                return PDO::PARAM_INT;
            case 'string':
                return PDO::PARAM_STR;
            default:
                return PDO::PARAM_STR;
        }
    }

    /**
     * Get max STRING limit
     *
     * @return int
     */
    public function getLimitForString(): int
    {
        return PHP_INT_MAX; // ScyllaDB has no practical limit for text fields
    }

    /**
     * Get max INT limit
     *
     * @return int
     */
    public function getLimitForInt(): int
    {
        return PHP_INT_MAX;
    }

    /**
     * Get maximum column limit
     *
     * @return int
     */
    public function getLimitForAttributes(): int
    {
        return PHP_INT_MAX; // ScyllaDB has no practical limit for columns
    }

    /**
     * Get maximum index limit
     *
     * @return int
     */
    public function getLimitForIndexes(): int
    {
        return PHP_INT_MAX;
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
        return false; // ScyllaDB doesn't support fulltext search natively
    }

    /**
     * Are FOR UPDATE locks supported?
     *
     * @return bool
     */
    public function getSupportForUpdateLock(): bool
    {
        return true;
    }

    /**
     * Is Attribute Resizing Supported?
     *
     * @return bool
     */
    public function getSupportForAttributeResizing(): bool
    {
        return true;
    }

    /**
     * Process PDO Exception
     *
     * @param PDOException $e
     * @return \Exception
     */
    protected function processException(PDOException $e): \Exception
    {
        // Map ScyllaDB error codes to our exceptions
        switch ($e->getCode()) {
            case 1000: // Timeout
                return new TimeoutException('Query timed out', $e->getCode(), $e);
            case 1050: // Duplicate table
                return new DuplicateException('Collection already exists', $e->getCode(), $e);
            case 1060: // Duplicate column
                return new DuplicateException('Attribute already exists', $e->getCode(), $e);
            case 1061: // Duplicate index
                return new DuplicateException('Index already exists', $e->getCode(), $e);
            case 1062: // Duplicate row
                return new DuplicateException('Document already exists', $e->getCode(), $e);
            case 1406: // Data truncation
                return new TruncateException('Resize would result in data truncation', $e->getCode(), $e);
            default:
                return $e;
        }
    }

    /**
     * Find Documents
     *
     * @param string $collection
     * @param array<Query> $queries
     * @param int $limit
     * @param int $offset
     * @param array<string, string> $orderAttributes
     * @param array<string, int> $orderTypes
     * @param array<string> $select
     * @param int $count
     * @return array<Document>
     * @throws Exception
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $select = [], int &$count = 0): array
    {
        $name = $this->filter($collection);

        $where = [];
        $having = [];
        $bind = [];

        foreach ($queries as $i => $query) {
            $key = $this->filter($query->getAttribute());
            $value = $query->getValue();
            $method = $query->getMethod();

            switch ($method) {
                case Query::TYPE_EQUAL:
                    $where[] = "`{$key}` = :value{$i}";
                    $bind[":value{$i}"] = $value;
                    break;
                case Query::TYPE_NOT_EQUAL:
                    $where[] = "`{$key}` != :value{$i}";
                    $bind[":value{$i}"] = $value;
                    break;
                case Query::TYPE_LESSER:
                    $where[] = "`{$key}` < :value{$i}";
                    $bind[":value{$i}"] = $value;
                    break;
                case Query::TYPE_LESSER_EQUAL:
                    $where[] = "`{$key}` <= :value{$i}";
                    $bind[":value{$i}"] = $value;
                    break;
                case Query::TYPE_GREATER:
                    $where[] = "`{$key}` > :value{$i}";
                    $bind[":value{$i}"] = $value;
                    break;
                case Query::TYPE_GREATER_EQUAL:
                    $where[] = "`{$key}` >= :value{$i}";
                    $bind[":value{$i}"] = $value;
                    break;
                case Query::TYPE_SEARCH:
                    $where[] = "`{$key}` LIKE :value{$i}";
                    $bind[":value{$i}"] = '%' . $value . '%';
                    break;
                case Query::TYPE_IS_NULL:
                    $where[] = "`{$key}` IS NULL";
                    break;
                case Query::TYPE_IS_NOT_NULL:
                    $where[] = "`{$key}` IS NOT NULL";
                    break;
                case Query::TYPE_CONTAINS:
                    $where[] = "`{$key}` LIKE :value{$i}";
                    $bind[":value{$i}"] = '%' . $value . '%';
                    break;
                default:
                    throw new DatabaseException('Unknown query method: ' . $method);
            }
        }

        $where = empty($where) ? '' : 'WHERE ' . implode(' AND ', $where);
        $having = empty($having) ? '' : 'HAVING ' . implode(' AND ', $having);

        $order = '';
        if (!empty($orderAttributes)) {
            $order = 'ORDER BY ';
            foreach ($orderAttributes as $i => $key) {
                $key = $this->filter($key);
                $order .= "`{$key}` " . ($orderTypes[$i] === Database::ORDER_DESC ? 'DESC' : 'ASC') . ',';
            }
            $order = rtrim($order, ',');
        }

        $selectString = empty($select) ? '*' : implode(', ', array_map(fn ($column) => "`{$column}`", $select));

        $sql = "SELECT {$selectString} FROM {$this->getSQLTable($name)} {$where} {$having} {$order} LIMIT {$limit} OFFSET {$offset}";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_FIND, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($bind as $key => $value) {
            $stmt->bindValue($key, $value, $this->getPDOType($value));
        }

        $stmt->execute();

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $documents = [];

        foreach ($results as $result) {
            $documents[] = new Document($result);
        }

        // Get total count if requested
        if ($count !== null) {
            $sql = "SELECT COUNT(*) as count FROM {$this->getSQLTable($name)} {$where} {$having}";
            $stmt = $this->getPDO()->prepare($sql);

            foreach ($bind as $key => $value) {
                $stmt->bindValue($key, $value, $this->getPDOType($value));
            }

            $stmt->execute();
            $count = (int)$stmt->fetchColumn();
        }

        return $documents;
    }

    /**
     * Create Documents
     *
     * @param string $collection
     * @param array<Document> $documents
     * @return array<Document>
     * @throws Exception
     */
    public function createDocuments(string $collection, array $documents): array
    {
        if (empty($documents)) {
            return [];
        }

        $name = $this->filter($collection);
        $columns = [];
        $values = [];
        $bind = [];
        $index = 0;

        foreach ($documents as $document) {
            $row = [];
            foreach ($document->getAttributes() as $key => $value) {
                if (!in_array($key, $columns)) {
                    $columns[] = $key;
                }
                $bindKey = "value_{$index}_{$key}";
                $row[] = ":{$bindKey}";
                $bind[$bindKey] = $value;
            }
            $values[] = '(' . implode(', ', $row) . ')';
            $index++;
        }

        $columnString = implode(', ', array_map(fn ($col) => "`{$col}`", $columns));
        $sql = "INSERT INTO {$this->getSQLTable($name)} ({$columnString}) VALUES " . implode(', ', $values);

        $sql = $this->trigger(Database::EVENT_DOCUMENT_CREATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($bind as $key => $value) {
            $stmt->bindValue(":{$key}", $value, $this->getPDOType($value));
        }

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $documents;
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param string $id
     * @param Document $document
     * @return Document
     * @throws Exception
     */
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        $name = $this->filter($collection);
        $attributes = $document->getAttributes();

        if (empty($attributes)) {
            return $document;
        }

        $sets = [];
        $bind = [];

        foreach ($attributes as $key => $value) {
            $key = $this->filter($key);
            $bindKey = "value_{$key}";
            $sets[] = "`{$key}` = :{$bindKey}";
            $bind[$bindKey] = $value;
        }

        $sql = "UPDATE {$this->getSQLTable($name)} SET " . implode(', ', $sets) . " WHERE _uid = :id";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_UPDATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);
        $stmt->bindValue(':id', $id);

        foreach ($bind as $key => $value) {
            $stmt->bindValue(":{$key}", $value, $this->getPDOType($value));
        }

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
    }

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     * @return bool
     * @throws Exception
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $name = $this->filter($collection);

        $sql = "DELETE FROM {$this->getSQLTable($name)} WHERE _uid = :id";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_DELETE, $sql);

        $stmt = $this->getPDO()->prepare($sql);
        $stmt->bindValue(':id', $id);

        try {
            return $stmt->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Create Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     * @return bool
     * @throws Exception
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        $sqlType = match ($type) {
            Database::INDEX_KEY => 'INDEX',
            Database::INDEX_UNIQUE => 'UNIQUE INDEX',
            default => throw new DatabaseException('Unknown index type: ' . $type),
        };

        $attributes = implode(', ', array_map(fn ($attr) => "`{$attr}`", $attributes));

        $sql = "CREATE {$sqlType} {$id} ON {$this->getSQLTable($name)} ({$attributes})";

        $sql = $this->trigger(Database::EVENT_INDEX_CREATE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Delete Index
     *
     * @param string $collection
     * @param string $id
     * @return bool
     * @throws Exception
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        $sql = "DROP INDEX IF EXISTS {$id} ON {$this->getSQLTable($name)}";

        $sql = $this->trigger(Database::EVENT_INDEX_DELETE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Get SQL table name
     *
     * @param string $collection
     * @return string
     */
    protected function getSQLTable(string $collection): string
    {
        return $this->getNamespace() . '_' . $collection;
    }

    /**
     * List Databases
     *
     * @return array<Document>
     */
    public function list(): array
    {
        $sql = "SELECT keyspace_name FROM system_schema.keyspaces";
        $stmt = $this->getPDO()->prepare($sql);
        $stmt->execute();

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $databases = [];

        foreach ($results as $result) {
            $databases[] = new Document([
                '$id' => $result['keyspace_name'],
                'name' => $result['keyspace_name']
            ]);
        }

        return $databases;
    }

    /**
     * Check if database exists
     *
     * @param string $database
     * @param string|null $collection
     * @return bool
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        if ($collection) {
            $sql = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?";
            $stmt = $this->getPDO()->prepare($sql);
            $stmt->execute([$database, $this->getNamespace() . '_' . $collection]);
        } else {
            $sql = "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?";
            $stmt = $this->getPDO()->prepare($sql);
            $stmt->execute([$database]);
        }

        return !empty($stmt->fetch(PDO::FETCH_ASSOC));
    }

    /**
     * Get collection size on disk
     *
     * @param string $collection
     * @return int
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        $name = $this->filter($collection);
        $table = $this->getSQLTable($name);

        $sql = "SELECT sum(total_bytes) as size FROM system.size_estimates WHERE keyspace_name = ? AND table_name = ?";
        $stmt = $this->getPDO()->prepare($sql);
        $stmt->execute([$this->getDatabase(), $table]);

        $result = $stmt->fetch(PDO::FETCH_ASSOC);
        return (int)($result['size'] ?? 0);
    }

    /**
     * Analyze a collection updating its metadata on the database engine
     *
     * @param string $collection
     * @return bool
     */
    public function analyzeCollection(string $collection): bool
    {
        $name = $this->filter($collection);
        $table = $this->getSQLTable($name);

        $sql = "REFRESH MATERIALIZED VIEW {$table}";
        $stmt = $this->getPDO()->prepare($sql);
        return $stmt->execute();
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
     * @param string|null $newKey
     * @return bool
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $type = $this->getSQLType($type, $size, $signed, $array);

        if ($newKey !== null) {
            $newKey = $this->filter($newKey);
            $sql = "ALTER TABLE {$this->getSQLTable($name)} RENAME {$id} TO {$newKey}";
        } else {
            $sql = "ALTER TABLE {$this->getSQLTable($name)} ALTER {$id} TYPE {$type}";
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Delete Attribute
     *
     * @param string $collection
     * @param string $id
     * @return bool
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        $sql = "ALTER TABLE {$this->getSQLTable($name)} DROP COLUMN {$id}";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Increase or decrease attribute value
     *
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param string $updatedAt
     * @param int|float|null $min
     * @param int|float|null $max
     * @return bool
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, string $updatedAt, int|float|null $min = null, int|float|null $max = null): bool
    {
        $name = $this->filter($collection);
        $attribute = $this->filter($attribute);

        $sql = "UPDATE {$this->getSQLTable($name)} SET {$attribute} = {$attribute} + ?, _updatedAt = ? WHERE _uid = ?";

        if ($min !== null) {
            $sql = "UPDATE {$this->getSQLTable($name)} SET {$attribute} = GREATEST(? + {$attribute}, ?), _updatedAt = ? WHERE _uid = ?";
        }

        if ($max !== null) {
            $sql = "UPDATE {$this->getSQLTable($name)} SET {$attribute} = LEAST(? + {$attribute}, ?), _updatedAt = ? WHERE _uid = ?";
        }

        $sql = $this->trigger(Database::EVENT_DOCUMENT_UPDATE, $sql);

        try {
            $stmt = $this->getPDO()->prepare($sql);

            if ($min !== null) {
                $stmt->execute([$value, $min, $updatedAt, $id]);
            } elseif ($max !== null) {
                $stmt->execute([$value, $max, $updatedAt, $id]);
            } else {
                $stmt->execute([$value, $updatedAt, $id]);
            }

            return true;
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Get connection ID
     *
     * @return string
     */
    public function getConnectionId(): string
    {
        $stmt = $this->getPDO()->query("SELECT uuid() as connection_id");
        return $stmt->fetchColumn();
    }

    /**
     * Get internal index keys
     *
     * @return array<string>
     */
    public function getInternalIndexesKeys(): array
    {
        return ['primary', '_created_at', '_updated_at', '_tenant_id'];
    }

    /**
     * Get Schema Attributes
     *
     * @param string $collection
     * @return array<Document>
     */
    public function getSchemaAttributes(string $collection): array
    {
        $name = $this->filter($collection);
        $table = $this->getSQLTable($name);

        $sql = "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?";
        $stmt = $this->getPDO()->prepare($sql);
        $stmt->execute([$this->getDatabase(), $table]);

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $attributes = [];

        foreach ($results as $result) {
            $attributes[] = new Document([
                '$id' => $result['column_name'],
                'type' => $this->mapScyllaTypeToUtopia($result['type']),
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false
            ]);
        }

        return $attributes;
    }

    /**
     * Map ScyllaDB type to Utopia type
     *
     * @param string $type
     * @return string
     */
    protected function mapScyllaTypeToUtopia(string $type): string
    {
        return match ($type) {
            'text', 'varchar' => Database::VAR_STRING,
            'int', 'bigint', 'smallint', 'tinyint' => Database::VAR_INTEGER,
            'float', 'double', 'decimal' => Database::VAR_FLOAT,
            'boolean' => Database::VAR_BOOLEAN,
            'timestamp', 'date', 'time' => Database::VAR_DATETIME,
            default => Database::VAR_STRING,
        };
    }

    /**
     * Get PDO Attributes
     *
     * @return array
     */
    public static function getPDOAttributes(): array
    {
        return [
            PDO::ATTR_TIMEOUT => 3, // Seconds
            PDO::ATTR_PERSISTENT => true,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => true,
            PDO::ATTR_STRINGIFY_FETCHES => true,
        ];
    }
} 