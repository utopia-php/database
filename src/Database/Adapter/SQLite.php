<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Swoole\Database\PDOStatementProxy;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Index;
use Utopia\Database\Capability;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Query\Schema\IndexType;

/**
 * Main differences from MariaDB and MySQL:
 *
 * 1. No concept of a schema. All tables are in the same schema.
 * 2. AUTO_INCREMENT is AUTOINCREAMENT.
 * 3. Can't create indexes in the same statement as creating a table.
 * 4. Can't use SET to bind values on INSERT
 * 5. last_insert_id is last_insert_rowid
 * 6. Can only drop one table at a time
 * 7. no index length support?
 * 8. DELETE doesn't support ORDER BY or LIMIT
 * 9. MODIFY COLUMN is not supported
 * 10. Can't rename an index directly
 */
class SQLite extends MariaDB
{
    public function capabilities(): array
    {
        $remove = [
            Capability::Schemas,
            Capability::Fulltext,
            Capability::MultipleFulltextIndexes,
            Capability::Regex,
            Capability::PCRE,
            Capability::UpdateLock,
            Capability::AlterLock,
            Capability::BatchCreateAttributes,
            Capability::QueryContains,
            Capability::Hostname,
            Capability::AttributeResizing,
            Capability::SpatialIndexOrder,
            Capability::OptionalSpatial,
            Capability::SchemaAttributes,
            Capability::Spatial,
            Capability::Relationships,
            Capability::Upserts,
            Capability::Timeouts,
            Capability::ConnectionId,
        ];

        return array_values(array_filter(
            parent::capabilities(),
            fn (Capability $c) => !in_array($c, $remove, true)
        ));
    }

    protected function createBuilder(): \Utopia\Query\Builder\SQL
    {
        return new \Utopia\Query\Builder\SQLite();
    }

    /**
     * @inheritDoc
     */
    public function startTransaction(): bool
    {
        try {
            if ($this->inTransaction === 0) {
                if ($this->getPDO()->inTransaction()) {
                    $this->getPDO()->rollBack();
                }

                $result = $this->getPDO()->beginTransaction();
            } else {
                $result = $this->getPDO()
                    ->prepare('SAVEPOINT transaction' . $this->inTransaction)
                    ->execute();
            }
        } catch (PDOException $e) {
            throw new TransactionException('Failed to start transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        if (!$result) {
            throw new TransactionException('Failed to start transaction');
        }

        $this->inTransaction++;

        return $result;
    }

    /**
     * Check if Database exists
     * Optionally check if collection exists in Database
     *
     * @param string $database
     * @param string|null $collection
     * @return bool
     * @throws DatabaseException
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        $database = $this->filter($database);

        if (\is_null($collection)) {
            return false;
        }

        $collection = $this->filter($collection);

        $sql = "
			SELECT name FROM sqlite_master 
			WHERE type='table' AND name = :table
		";

        $sql = $this->trigger(Database::EVENT_DATABASE_CREATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", PDO::PARAM_STR);

        $stmt->execute();

        $document = $stmt->fetchAll();
        $stmt->closeCursor();
        if (!empty($document)) {
            $document = $document[0];
        }

        return (($document['name'] ?? '') === "{$this->getNamespace()}_{$collection}");
    }

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
        return true;
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
        return true;
    }

    /**
     * Create Collection
     *
     * @param string $name
     * @param array<Attribute> $attributes
     * @param array<Index> $indexes
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
            $attrId = $this->filter($attribute->key);

            $attrType = $this->getSQLType(
                $attribute->type->value,
                $attribute->size,
                $attribute->signed,
                $attribute->array,
                $attribute->required
            );

            $attributeStrings[$key] = "`{$attrId}` {$attrType}, ";
        }

        $tenantQuery = $this->sharedTables ? '`_tenant` INTEGER DEFAULT NULL,' : '';

        $collection = "
			CREATE TABLE {$this->getSQLTable($id)} (
				`_id` INTEGER PRIMARY KEY AUTOINCREMENT,
				`_uid` VARCHAR(36) NOT NULL,
				{$tenantQuery}
				`_createdAt` DATETIME(3) DEFAULT NULL,
				`_updatedAt` DATETIME(3) DEFAULT NULL,
				`_permissions` MEDIUMTEXT DEFAULT NULL".(!empty($attributes) ? ',' : '')."
				" . \substr(\implode(' ', $attributeStrings), 0, -2) . "
			)
		";

        $collection = $this->trigger(Database::EVENT_COLLECTION_CREATE, $collection);

        $permissions = "
			CREATE TABLE {$this->getSQLTable($id . '_perms')} (
				`_id` INTEGER PRIMARY KEY AUTOINCREMENT,
				{$tenantQuery}
				`_type` VARCHAR(12) NOT NULL,
				`_permission` VARCHAR(255) NOT NULL,
				`_document` VARCHAR(255) NOT NULL
			)
		";

        $permissions = $this->trigger(Database::EVENT_COLLECTION_CREATE, $permissions);

        try {
            $this->getPDO()
                ->prepare($collection)
                ->execute();

            $this->getPDO()
                ->prepare($permissions)
                ->execute();

            $this->createIndex($id, new Index(key: '_index1', type: IndexType::Unique, attributes: ['_uid']));
            $this->createIndex($id, new Index(key: '_created_at', type: IndexType::Key, attributes: ['_createdAt']));
            $this->createIndex($id, new Index(key: '_updated_at', type: IndexType::Key, attributes: ['_updatedAt']));

            $this->createIndex("{$id}_perms", new Index(key: '_index_1', type: IndexType::Unique, attributes: ['_document', '_type', '_permission']));
            $this->createIndex("{$id}_perms", new Index(key: '_index_2', type: IndexType::Key, attributes: ['_permission', '_type']));

            if ($this->sharedTables) {
                $this->createIndex($id, new Index(key: '_tenant_id', type: IndexType::Key, attributes: ['_id']));
            }

            foreach ($indexes as $index) {
                $this->createIndex($id, new Index(
                    key: $this->filter($index->key),
                    type: $index->type,
                    attributes: $index->attributes,
                    lengths: $index->lengths,
                    orders: $index->orders,
                    ttl: $index->ttl,
                ));
            }

            $this->createIndex("{$id}_perms", new Index(key: '_index_1', type: IndexType::Unique, attributes: ['_document', '_type', '_permission']));
            $this->createIndex("{$id}_perms", new Index(key: '_index_2', type: IndexType::Key, attributes: ['_permission', '_type']));

        } catch (PDOException $e) {
            throw $this->processException($e);
        }
        return true;
    }


    /**
     * Get Collection Size of raw data
     * @param string $collection
     * @return int
     * @throws DatabaseException
     *
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->filter($collection);
        $namespace = $this->getNamespace();
        $name = $namespace . '_' . $collection;
        $permissions = $namespace . '_' . $collection . '_perms';

        $collectionSize = $this->getPDO()->prepare("
             SELECT SUM(\"pgsize\") 
             FROM \"dbstat\" 
             WHERE name = :name;
        ");

        $permissionsSize = $this->getPDO()->prepare("
             SELECT SUM(\"pgsize\") 
             FROM \"dbstat\"
             WHERE name = :name;
        ");

        $collectionSize->bindParam(':name', $name);
        $permissionsSize->bindParam(':name', $permissions);

        try {
            $collectionSize->execute();
            $permissionsSize->execute();
            $size = $collectionSize->fetchColumn() + $permissionsSize->fetchColumn();
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: ' . $e->getMessage());
        }

        return $size;
    }

    /**
     * Get Collection Size on disk
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->getSizeOfCollection($collection);
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

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id)}";
        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        $this->getPDO()
            ->prepare($sql)
            ->execute();

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id . '_perms')}";
        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        $this->getPDO()
            ->prepare($sql)
            ->execute();

        return true;
    }

    /**
     * Analyze a collection updating it's metadata on the database engine
     *
     * @param string $collection
     * @return bool
     */
    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

    /**
     * Update Attribute
     *
     * @param string $collection
     * @param Attribute $attribute
     * @param string|null $newKey
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
    {
        if (!empty($newKey) && $newKey !== $attribute->key) {
            return $this->renameAttribute($collection, $attribute->key, $newKey);
        }

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
        $metadataCollection = new Document(['$id' => Database::METADATA]);
        $collection = $this->getDocument($metadataCollection, $name);

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $indexes = \json_decode($collection->getAttribute('indexes', []), true);

        foreach ($indexes as $index) {
            $attributes = $index['attributes'];
            if ($attributes === [$id]) {
                $this->deleteIndex($name, $index['$id']);
            } elseif (\in_array($id, $attributes)) {
                $this->deleteIndex($name, $index['$id']);
                $this->createIndex($name, new Index(
                    key: $index['$id'],
                    type: IndexType::from($index['type']),
                    attributes: \array_values(\array_diff($attributes, [$id])),
                    lengths: $index['lengths'],
                    orders: $index['orders'],
                ));
            }
        }

        $sql = "ALTER TABLE {$this->getSQLTable($name)} DROP COLUMN `{$id}`";

        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            if (str_contains($e->getMessage(), 'no such column')) {
                return true;
            }

            throw $e;
        }
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
        $metadataCollection = new Document(['$id' => Database::METADATA]);
        $collection = $this->getDocument($metadataCollection, $collection);

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $old = $this->filter($old);
        $new = $this->filter($new);
        $indexes = \json_decode($collection->getAttribute('indexes', []), true);
        $index = null;

        foreach ($indexes as $node) {
            if ($node['key'] === $old) {
                $index = $node;
                break;
            }
        }

        if ($index
            && $this->deleteIndex($collection->getId(), $old)
            && $this->createIndex(
                $collection->getId(),
                new Index(
                    key: $new,
                    type: IndexType::from($index['type']),
                    attributes: $index['attributes'],
                    lengths: $index['lengths'],
                    orders: $index['orders'],
                ),
            )) {
            return true;
        }

        return false;
    }

    /**
     * Create Index
     *
     * @param string $collection
     * @param Index $index
     * @param array<string,string> $indexAttributeTypes
     * @param array<string, mixed> $collation
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($index->key);
        $type = $index->type;
        $attributes = $index->attributes;

        // Workaround for no support for CREATE INDEX IF NOT EXISTS
        $stmt = $this->getPDO()->prepare("
			SELECT name
			FROM sqlite_master
			WHERE type='index' AND name=:_index;
		");
        $stmt->bindValue(':_index', "{$this->getNamespace()}_{$this->tenant}_{$name}_{$id}");
        $stmt->execute();
        $existingIndex = $stmt->fetch();
        if (!empty($existingIndex)) {
            return true;
        }

        $sql = $this->getSQLIndex($name, $id, $type->value, $attributes);

        $sql = $this->trigger(Database::EVENT_INDEX_CREATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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
        $name = $this->filter($collection);
        $id = $this->filter($id);

        $sql = "DROP INDEX `{$this->getNamespace()}_{$this->tenant}_{$name}_{$id}`";
        $sql = $this->trigger(Database::EVENT_INDEX_DELETE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            if (str_contains($e->getMessage(), 'no such index')) {
                return true;
            }

            throw $e;
        }
    }

    /**
     * Create Document
     *
     * @param Document $collection
     * @param Document $document
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws DuplicateException
     */
    public function createDocument(Document $collection, Document $document): Document
    {
        try {
            $this->syncWriteHooks();

            $collection = $collection->getId();
            $attributes = $document->getAttributes();
            $attributes['_createdAt'] = $document->getCreatedAt();
            $attributes['_updatedAt'] = $document->getUpdatedAt();
            $attributes['_permissions'] = json_encode($document->getPermissions());

            $name = $this->filter($collection);

            $builder = $this->createBuilder()->into($this->getSQLTableRaw($name));
            $row = ['_uid' => $document->getId()];

            if (!empty($document->getSequence())) {
                $row['_id'] = $document->getSequence();
            }

            foreach ($attributes as $attr => $value) {
                $column = $this->filter($attr);

                if (is_array($value)) {
                    $value = json_encode($value);
                }
                $value = (is_bool($value)) ? (int)$value : $value;
                $row[$column] = $value;
            }

            $row = $this->decorateRow($row, $this->documentMetadata($document));
            $builder->set($row);
            $result = $builder->insert();
            $stmt = $this->executeResult($result, Database::EVENT_DOCUMENT_CREATE);

            $stmt->execute();

            $statment = $this->getPDO()->prepare("SELECT last_insert_rowid() AS id");
            $statment->execute();
            $last = $statment->fetch();

            $document['$sequence'] = $last['id'];

            $ctx = $this->buildWriteContext($name);
            foreach ($this->writeHooks as $hook) {
                $hook->afterDocumentCreate($name, [$document], $ctx);
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
    }

    /**
     * Update Document
     *
     * @param Document $collection
     * @param string $id
     * @param Document $document
     * @param bool $skipPermissions
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws DuplicateException
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        try {
            $this->syncWriteHooks();

            $spatialAttributes = $this->getSpatialAttributes($collection);
            $collection = $collection->getId();
            $attributes = $document->getAttributes();
            $attributes['_createdAt'] = $document->getCreatedAt();
            $attributes['_updatedAt'] = $document->getUpdatedAt();
            $attributes['_permissions'] = json_encode($document->getPermissions());

            $name = $this->filter($collection);

            $operators = [];
            foreach ($attributes as $attribute => $value) {
                if (Operator::isOperator($value)) {
                    $operators[$attribute] = $value;
                }
            }

            $builder = $this->newBuilder($name);
            $regularRow = ['_uid' => $document->getId()];

            foreach ($attributes as $attribute => $value) {
                $column = $this->filter($attribute);

                if (isset($operators[$attribute])) {
                    $opResult = $this->getOperatorBuilderExpression($column, $operators[$attribute]);
                    $builder->setRaw($column, $opResult['expression'], $opResult['bindings']);
                } elseif ($this->supports(Capability::Spatial) && \in_array($attribute, $spatialAttributes, true)) {
                    if (\is_array($value)) {
                        $value = $this->convertArrayToWKT($value);
                    }
                    $value = (is_bool($value)) ? (int)$value : $value;
                    $builder->setRaw($column, $this->getSpatialGeomFromText('?'), [$value]);
                } else {
                    if (is_array($value)) {
                        $value = json_encode($value);
                    }
                    $value = (is_bool($value)) ? (int)$value : $value;
                    $regularRow[$column] = $value;
                }
            }

            $builder->set($regularRow);
            $builder->filter([\Utopia\Query\Query::equal('_uid', [$id])]);
            $result = $builder->update();
            $stmt = $this->executeResult($result, Database::EVENT_DOCUMENT_UPDATE);

            $stmt->execute();

            $ctx = $this->buildWriteContext($name);
            foreach ($this->writeHooks as $hook) {
                $hook->afterDocumentUpdate($name, $document, $skipPermissions, $ctx);
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
    }


    /**
     * Override getSpatialGeomFromText to return placeholder unchanged for SQLite
     * SQLite does not support ST_GeomFromText, so we return the raw placeholder
     *
     * @param string $wktPlaceholder
     * @param int|null $srid
     * @return string
     */
    protected function getSpatialGeomFromText(string $wktPlaceholder, ?int $srid = null): string
    {
        return $wktPlaceholder;
    }

    /**
     * Get SQL Index Type
     *
     * @param string $type
     * @return string
     * @throws Exception
     */
    protected function getSQLIndexType(string $type): string
    {
        return match ($type) {
            IndexType::Key->value => 'INDEX',
            IndexType::Unique->value => 'UNIQUE INDEX',
            default => throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . IndexType::Key->value . ', ' . IndexType::Unique->value . ', ' . IndexType::Fulltext->value),
        };
    }

    /**
     * Get SQL Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     * @return string
     * @throws Exception
     */
    protected function getSQLIndex(string $collection, string $id, string $type, array $attributes): string
    {
        $postfix = '';

        switch ($type) {
            case IndexType::Key->value:
                $type = 'INDEX';
                break;

            case IndexType::Unique->value:
                $type = 'UNIQUE INDEX';
                $postfix = 'COLLATE NOCASE';

                break;

            default:
                throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . IndexType::Key->value . ', ' . IndexType::Unique->value . ', ' . IndexType::Fulltext->value);
        }

        $attributes = \array_map(fn ($attribute) => match ($attribute) {
            '$id' => ID::custom('_uid'),
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $attribute
        }, $attributes);

        foreach ($attributes as $key => $attribute) {
            $attribute = $this->filter($attribute);

            $attributes[$key] = "`{$attribute}` {$postfix}";
        }

        $key = "`{$this->getNamespace()}_{$this->tenant}_{$collection}_{$id}`";
        $attributes = implode(', ', $attributes);

        if ($this->sharedTables) {
            $attributes = "`_tenant` {$postfix}, {$attributes}";
        }

        return "CREATE {$type} {$key} ON `{$this->getNamespace()}_{$collection}` ({$attributes})";
    }

    /**
     * Get SQL table
     *
     * @param string $name
     * @return string
     */
    protected function getSQLTable(string $name): string
    {
        return $this->quote("{$this->getNamespace()}_{$this->filter($name)}");
    }

    /**
     * SQLite doesn't use database-qualified table names.
     */
    protected function getSQLTableRaw(string $name): string
    {
        return $this->getNamespace() . '_' . $this->filter($name);
    }

    /**
     * Get list of keywords that cannot be used
     *  Refference: https://www.sqlite.org/lang_keywords.html
     *
     * @return array<string>
     */
    public function getKeywords(): array
    {
        return [
            'ABORT',
            'ACTION',
            'ADD',
            'AFTER',
            'ALL',
            'ALTER',
            'ALWAYS',
            'ANALYZE',
            'AND',
            'AS',
            'ASC',
            'ATTACH',
            'AUTOINCREMENT',
            'BEFORE',
            'BEGIN',
            'BETWEEN',
            'BY',
            'CASCADE',
            'CASE',
            'CAST',
            'CHECK',
            'COLLATE',
            'COLUMN',
            'COMMIT',
            'CONFLICT',
            'CONSTRAINT',
            'CREATE',
            'CROSS',
            'CURRENT',
            'CURRENT_DATE',
            'CURRENT_TIME',
            'CURRENT_TIMESTAMP',
            'DATABASE',
            'DEFAULT',
            'DEFERRABLE',
            'DEFERRED',
            'DELETE',
            'DESC',
            'DETACH',
            'DISTINCT',
            'DO',
            'DROP',
            'EACH',
            'ELSE',
            'END',
            'ESCAPE',
            'EXCEPT',
            'EXCLUDE',
            'EXCLUSIVE',
            'EXISTS',
            'EXPLAIN',
            'FAIL',
            'FILTER',
            'FIRST',
            'FOLLOWING',
            'FOR',
            'FOREIGN',
            'FROM',
            'FULL',
            'GENERATED',
            'GLOB',
            'GROUP',
            'GROUPS',
            'HAVING',
            'IF',
            'IGNORE',
            'IMMEDIATE',
            'IN',
            'INDEX',
            'INDEXED',
            'INITIALLY',
            'INNER',
            'INSERT',
            'INSTEAD',
            'INTERSECT',
            'INTO',
            'IS',
            'ISNULL',
            'JOIN',
            'KEY',
            'LAST',
            'LEFT',
            'LIKE',
            'LIMIT',
            'MATCH',
            'MATERIALIZED',
            'NATURAL',
            'NO',
            'NOT',
            'NOTHING',
            'NOTNULL',
            'NULL',
            'NULLS',
            'OF',
            'OFFSET',
            'ON',
            'OR',
            'ORDER',
            'OTHERS',
            'OUTER',
            'OVER',
            'PARTITION',
            'PLAN',
            'PRAGMA',
            'PRECEDING',
            'PRIMARY',
            'QUERY',
            'RAISE',
            'RANGE',
            'RECURSIVE',
            'REFERENCES',
            'REGEXP',
            'REINDEX',
            'RELEASE',
            'RENAME',
            'REPLACE',
            'RESTRICT',
            'RETURNING',
            'RIGHT',
            'ROLLBACK',
            'ROW',
            'ROWS',
            'SAVEPOINT',
            'SELECT',
            'SET',
            'TABLE',
            'TEMP',
            'TEMPORARY',
            'THEN',
            'TIES',
            'TO',
            'TRANSACTION',
            'TRIGGER',
            'UNBOUNDED',
            'UNION',
            'UNIQUE',
            'UPDATE',
            'USING',
            'VACUUM',
            'VALUES',
            'VIEW',
            'VIRTUAL',
            'WHEN',
            'WHERE',
            'WINDOW',
            'WITH',
            'WITHOUT',
        ];
    }

    protected function processException(PDOException $e): \Exception
    {
        // Timeout
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Duplicate - SQLite uses various error codes for constraint violations:
        // - Error code 19 is SQLITE_CONSTRAINT (includes UNIQUE violations)
        // - Error code 1 is also used for some duplicate cases
        // - SQL state '23000' is integrity constraint violation
        if (
            ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && ($e->errorInfo[1] === 1 || $e->errorInfo[1] === 19)) ||
            $e->getCode() === '23000'
        ) {
            // Check if it's actually a duplicate/unique constraint violation
            $message = $e->getMessage();
            if (
                (isset($e->errorInfo[1]) && $e->errorInfo[1] === 19) ||
                $e->getCode() === '23000' ||
                stripos($message, 'unique') !== false ||
                stripos($message, 'duplicate') !== false
            ) {
                if (!\str_contains($message, '_uid')) {
                    return new DuplicateException('Document with the requested unique attributes already exists', $e->getCode(), $e);
                }
                return new DuplicateException('Document already exists', $e->getCode(), $e);
            }
        }

        // String or BLOB exceeds size limit
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 18) {
            return new LimitException('Value too large', $e->getCode(), $e);
        }

        return $e;
    }

    /**
     * Get the SQL function for random ordering
     *
     * @return string
     */
    protected function getRandomOrder(): string
    {
        return 'RANDOM()';
    }

    /**
     * Check if SQLite math functions (like POWER) are available
     * SQLite must be compiled with -DSQLITE_ENABLE_MATH_FUNCTIONS
     *
     * @return bool
     */
    private function getSupportForMathFunctions(): bool
    {
        static $available = null;

        if ($available !== null) {
            return $available;
        }

        try {
            // Test if POWER function exists by attempting to use it
            $stmt = $this->getPDO()->query('SELECT POWER(2, 3) as test');
            $result = $stmt->fetch();
            $available = ($result['test'] == 8);
            return $available;
        } catch (PDOException $e) {
            // Function doesn't exist
            $available = false;
            return false;
        }
    }

    /**
     * Bind operator parameters to statement
     * Override to handle SQLite-specific operator bindings
     *
     * @param \PDOStatement|PDOStatementProxy $stmt
     * @param Operator $operator
     * @param int &$bindIndex
     * @return void
     */
    protected function bindOperatorParams(\PDOStatement|PDOStatementProxy $stmt, Operator $operator, int &$bindIndex): void
    {
        $method = $operator->getMethod();

        // For operators that SQLite doesn't use bind parameters for, skip binding entirely
        // Note: The bindIndex increment happens in getOperatorSQL(), NOT here
        if (in_array($method, [OperatorType::Toggle->value, OperatorType::DateSetNow->value, OperatorType::ArrayUnique->value])) {
            // These operators don't bind any parameters - they're handled purely in SQL
            // DO NOT increment bindIndex here as it's already handled in getOperatorSQL()
            return;
        }

        // For ARRAY_FILTER, bind the filter value if present
        if ($method === OperatorType::ArrayFilter->value) {
            $values = $operator->getValues();
            if (!empty($values) && count($values) >= 2) {
                $filterType = $values[0];
                $filterValue = $values[1];

                // Only bind if we support this filter type (all comparison operators need binding)
                $comparisonTypes = ['equal', 'notEqual', 'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual'];
                if (in_array($filterType, $comparisonTypes)) {
                    $bindKey = "op_{$bindIndex}";
                    $value = (is_bool($filterValue)) ? (int)$filterValue : $filterValue;
                    $stmt->bindValue(":{$bindKey}", $value, $this->getPDOType($value));
                    $bindIndex++;
                }
            }
            return;
        }

        // For all other operators, use parent implementation
        parent::bindOperatorParams($stmt, $operator, $bindIndex);
    }

    /**
     * @inheritDoc
     */
    protected function getOperatorBuilderExpression(string $column, Operator $operator): array
    {
        if ($operator->getMethod() === OperatorType::ArrayFilter->value) {
            $bindIndex = 0;
            $fullExpression = $this->getOperatorSQL($column, $operator, $bindIndex);

            if ($fullExpression === null) {
                throw new DatabaseException('Operator cannot be expressed in SQL: ' . $operator->getMethod());
            }

            $quotedColumn = $this->quote($column);
            $prefix = $quotedColumn . ' = ';
            $expression = $fullExpression;
            if (str_starts_with($expression, $prefix)) {
                $expression = substr($expression, strlen($prefix));
            }

            // SQLite ArrayFilter only uses one binding (the filter value), not the condition string
            $values = $operator->getValues();
            $namedBindings = [];
            if (count($values) >= 2) {
                $filterType = $values[0];
                $comparisonTypes = ['equal', 'notEqual', 'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual'];
                if (in_array($filterType, $comparisonTypes)) {
                    $namedBindings['op_0'] = $values[1];
                }
            }

            // Replace named bindings with positional
            $positionalBindings = [];
            $replacements = [];
            foreach (array_keys($namedBindings) as $key) {
                $search = ':' . $key;
                $offset = 0;
                while (($pos = strpos($expression, $search, $offset)) !== false) {
                    $replacements[] = ['pos' => $pos, 'len' => strlen($search), 'key' => $key];
                    $offset = $pos + strlen($search);
                }
            }
            usort($replacements, fn ($a, $b) => $a['pos'] - $b['pos']);
            $result = $expression;
            for ($i = count($replacements) - 1; $i >= 0; $i--) {
                $r = $replacements[$i];
                $result = substr_replace($result, '?', $r['pos'], $r['len']);
            }
            foreach ($replacements as $r) {
                $positionalBindings[] = $namedBindings[$r['key']];
            }

            return ['expression' => $result, 'bindings' => $positionalBindings];
        }

        return parent::getOperatorBuilderExpression($column, $operator);
    }

    /**
     * Get SQL expression for operator
     *
     * IMPORTANT: SQLite JSON Limitations
     * Array operators using json_each() and json_group_array() have type conversion behavior:
     * - Numbers are preserved but may lose precision (e.g., 1.0 becomes 1)
     * - Booleans become integers (true→1, false→0)
     * - Strings remain strings
     * - Objects and nested arrays are converted to JSON strings
     *
     * This is inherent to SQLite's JSON implementation and affects: ARRAY_APPEND, ARRAY_PREPEND,
     * ARRAY_UNIQUE, ARRAY_INTERSECT, ARRAY_DIFF, ARRAY_INSERT, and ARRAY_REMOVE.
     *
     * @param string $column
     * @param Operator $operator
     * @param int &$bindIndex
     * @return ?string
     */
    protected function getOperatorSQL(string $column, Operator $operator, int &$bindIndex): ?string
    {
        $quotedColumn = $this->quote($column);
        $method = $operator->getMethod();

        switch ($method) {
            // Numeric operators
            case OperatorType::Increment->value:
                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN COALESCE({$quotedColumn}, 0) > :$maxKey - :$bindKey THEN :$maxKey
                        ELSE COALESCE({$quotedColumn}, 0) + :$bindKey
                    END";
                }
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) + :$bindKey";

            case OperatorType::Decrement->value:
                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $minKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) <= :$minKey THEN :$minKey
                        WHEN COALESCE({$quotedColumn}, 0) < :$minKey + :$bindKey THEN :$minKey
                        ELSE COALESCE({$quotedColumn}, 0) - :$bindKey
                    END";
                }
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) - :$bindKey";

            case OperatorType::Multiply->value:
                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN :$bindKey > 0 AND COALESCE({$quotedColumn}, 0) > :$maxKey / :$bindKey THEN :$maxKey
                        WHEN :$bindKey < 0 AND COALESCE({$quotedColumn}, 0) < :$maxKey / :$bindKey THEN :$maxKey
                        ELSE COALESCE({$quotedColumn}, 0) * :$bindKey
                    END";
                }
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) * :$bindKey";

            case OperatorType::Divide->value:
                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $minKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN :$bindKey != 0 AND COALESCE({$quotedColumn}, 0) / :$bindKey <= :$minKey THEN :$minKey
                        ELSE COALESCE({$quotedColumn}, 0) / :$bindKey
                    END";
                }
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) / :$bindKey";

            case OperatorType::Modulo->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) % :$bindKey";

            case OperatorType::Power->value:
                if (!$this->getSupportForMathFunctions()) {
                    throw new DatabaseException(
                        'SQLite POWER operator requires math functions. ' .
                        'Compile SQLite with -DSQLITE_ENABLE_MATH_FUNCTIONS or use multiply operators instead.'
                    );
                }

                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN COALESCE({$quotedColumn}, 0) <= 1 THEN COALESCE({$quotedColumn}, 0)
                        WHEN :$bindKey * LN(COALESCE({$quotedColumn}, 1)) > LN(:$maxKey) THEN :$maxKey
                        ELSE POWER(COALESCE({$quotedColumn}, 0), :$bindKey)
                    END";
                }
                return "{$quotedColumn} = POWER(COALESCE({$quotedColumn}, 0), :$bindKey)";

                // String operators
            case OperatorType::StringConcat->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                return "{$quotedColumn} = IFNULL({$quotedColumn}, '') || :$bindKey";

            case OperatorType::StringReplace->value:
                $searchKey = "op_{$bindIndex}";
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $bindIndex++;
                return "{$quotedColumn} = REPLACE({$quotedColumn}, :$searchKey, :$replaceKey)";

                // Boolean operators
            case OperatorType::Toggle->value:
                // SQLite: toggle boolean (0 or 1), treat NULL as 0
                return "{$quotedColumn} = CASE WHEN COALESCE({$quotedColumn}, 0) = 0 THEN 1 ELSE 0 END";

                // Array operators
            case OperatorType::ArrayAppend->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: merge arrays by using json_group_array on extracted elements
                // We use json_each to extract elements from both arrays and combine them
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value FROM json_each(IFNULL({$quotedColumn}, '[]'))
                        UNION ALL
                        SELECT value FROM json_each(:$bindKey)
                    )
                )";

            case OperatorType::ArrayPrepend->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: prepend by extracting and recombining with new elements first
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value FROM json_each(:$bindKey)
                        UNION ALL
                        SELECT value FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    )
                )";

            case OperatorType::ArrayUnique->value:
                // SQLite: get distinct values from JSON array
                return "{$quotedColumn} = (
                    SELECT json_group_array(DISTINCT value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                )";

            case OperatorType::ArrayRemove->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: remove specific value from array
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value != :$bindKey
                )";

            case OperatorType::ArrayInsert->value:
                $indexKey = "op_{$bindIndex}";
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: Insert element at specific index by:
                // 1. Take elements before index (0 to index-1)
                // 2. Add new element
                // 3. Take elements from index to end
                // The bound value is JSON-encoded by parent, json() parses it back to a value,
                // then we wrap it in json_array() and extract to get the same format as json_each()
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value, rownum
                        FROM (
                            SELECT value, (ROW_NUMBER() OVER ()) - 1 as rownum
                            FROM json_each(IFNULL({$quotedColumn}, '[]'))
                        )
                        WHERE rownum < :$indexKey
                        UNION ALL
                        SELECT value, :$indexKey as rownum
                        FROM json_each(json_array(json(:$valueKey)))
                        UNION ALL
                        SELECT value, rownum + 1 as rownum
                        FROM (
                            SELECT value, (ROW_NUMBER() OVER ()) - 1 as rownum
                            FROM json_each(IFNULL({$quotedColumn}, '[]'))
                        )
                        WHERE rownum >= :$indexKey
                        ORDER BY rownum
                    )
                )";

            case OperatorType::ArrayIntersect->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: keep only values that exist in both arrays
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value IN (SELECT value FROM json_each(:$bindKey))
                )";

            case OperatorType::ArrayDiff->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: remove values that exist in the comparison array
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value NOT IN (SELECT value FROM json_each(:$bindKey))
                )";

            case OperatorType::ArrayFilter->value:
                $values = $operator->getValues();
                if (empty($values)) {
                    // No filter criteria, return array unchanged
                    return "{$quotedColumn} = {$quotedColumn}";
                }

                $filterType = $values[0]; // 'equal', 'notEqual', 'isNull', 'isNotNull', 'greaterThan', etc.

                switch ($filterType) {
                    case 'isNull':
                        // Filter for null values - no bind parameter needed
                        return "{$quotedColumn} = (
                            SELECT json_group_array(value)
                            FROM json_each(IFNULL({$quotedColumn}, '[]'))
                            WHERE value IS NULL
                        )";

                    case 'isNotNull':
                        // Filter out null values - no bind parameter needed
                        return "{$quotedColumn} = (
                            SELECT json_group_array(value)
                            FROM json_each(IFNULL({$quotedColumn}, '[]'))
                            WHERE value IS NOT NULL
                        )";

                    case 'equal':
                    case 'notEqual':
                    case 'greaterThan':
                    case 'greaterThanEqual':
                    case 'lessThan':
                    case 'lessThanEqual':
                        if (\count($values) < 2) {
                            return "{$quotedColumn} = {$quotedColumn}";
                        }

                        $bindKey = "op_{$bindIndex}";
                        $bindIndex++;

                        $operator = match ($filterType) {
                            'equal' => '=',
                            'notEqual' => '!=',
                            'greaterThan' => '>',
                            'greaterThanEqual' => '>=',
                            'lessThan' => '<',
                            'lessThanEqual' => '<=',
                            default => throw new OperatorException('Unsupported filter type: ' . $filterType),
                        };

                        // For numeric comparisons, cast to REAL; for equal/notEqual, use text comparison
                        $isNumericComparison = \in_array($filterType, ['greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual']);
                        if ($isNumericComparison) {
                            return "{$quotedColumn} = (
                                SELECT json_group_array(value)
                                FROM json_each(IFNULL({$quotedColumn}, '[]'))
                                WHERE CAST(value AS REAL) $operator CAST(:$bindKey AS REAL)
                            )";
                        } else {
                            return "{$quotedColumn} = (
                                SELECT json_group_array(value)
                                FROM json_each(IFNULL({$quotedColumn}, '[]'))
                                WHERE value $operator :$bindKey
                            )";
                        }

                        // no break
                    default:
                        return "{$quotedColumn} = {$quotedColumn}";
                }

                // Date operators
                // no break
            case OperatorType::DateAddDays->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = datetime({$quotedColumn}, :$bindKey || ' days')";

            case OperatorType::DateSubDays->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = datetime({$quotedColumn}, '-' || abs(:$bindKey) || ' days')";

            case OperatorType::DateSetNow->value:
                return "{$quotedColumn} = datetime('now')";

            default:
                // Fall back to parent implementation for other operators
                return parent::getOperatorSQL($column, $operator, $bindIndex);
        }
    }

    /**
     * @inheritDoc
     */
    protected function getConflictTenantExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));
        return "CASE WHEN _tenant = excluded._tenant THEN excluded.{$quoted} ELSE {$quoted} END";
    }

    /**
     * @inheritDoc
     */
    protected function getConflictIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));
        return "{$quoted} + excluded.{$quoted}";
    }

    /**
     * @inheritDoc
     */
    protected function getConflictTenantIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));
        return "CASE WHEN _tenant = excluded._tenant THEN {$quoted} + excluded.{$quoted} ELSE {$quoted} END";
    }

    /**
     * Override executeUpsertBatch because SQLite uses ON CONFLICT syntax which
     * is not supported by the MySQL query builder that SQLite inherits.
     *
     * @param string $name The filtered collection name
     * @param array<\Utopia\Database\Change> $changes The changes to upsert
     * @param array<string> $spatialAttributes Spatial column names
     * @param string $attribute Increment attribute name (empty if none)
     * @param array<string, Operator> $operators Operator map keyed by attribute name
     * @param array<string, mixed> $attributeDefaults Attribute default values
     * @param bool $hasOperators Whether this batch contains operator expressions
     * @return void
     * @throws \Utopia\Database\Exception
     */
    protected function executeUpsertBatch(
        string $name,
        array $changes,
        array $spatialAttributes,
        string $attribute,
        array $operators,
        array $attributeDefaults,
        bool $hasOperators
    ): void {
        $bindIndex = 0;
        $batchKeys = [];
        $bindValues = [];
        $allColumnNames = [];
        $documentsData = [];

        foreach ($changes as $change) {
            $document = $change->getNew();

            if ($hasOperators) {
                $extracted = Operator::extractOperators($document->getAttributes());
                $currentRegularAttributes = $extracted['updates'];
                $extractedOperators = $extracted['operators'];

                if ($change->getOld()->isEmpty() && !empty($extractedOperators)) {
                    foreach ($extractedOperators as $operatorKey => $operator) {
                        $default = $attributeDefaults[$operatorKey] ?? null;
                        $currentRegularAttributes[$operatorKey] = $this->applyOperatorToValue($operator, $default);
                    }
                }

                $currentRegularAttributes['_uid'] = $document->getId();
                $currentRegularAttributes['_createdAt'] = $document->getCreatedAt() ? $document->getCreatedAt() : null;
                $currentRegularAttributes['_updatedAt'] = $document->getUpdatedAt() ? $document->getUpdatedAt() : null;
            } else {
                $currentRegularAttributes = $document->getAttributes();
                $currentRegularAttributes['_uid'] = $document->getId();
                $currentRegularAttributes['_createdAt'] = $document->getCreatedAt() ? DateTime::setTimezone($document->getCreatedAt()) : null;
                $currentRegularAttributes['_updatedAt'] = $document->getUpdatedAt() ? DateTime::setTimezone($document->getUpdatedAt()) : null;
            }

            $currentRegularAttributes['_permissions'] = \json_encode($document->getPermissions());

            if (!empty($document->getSequence())) {
                $currentRegularAttributes['_id'] = $document->getSequence();
            }

            if ($this->sharedTables) {
                $currentRegularAttributes['_tenant'] = $document->getTenant();
            }

            foreach (\array_keys($currentRegularAttributes) as $colName) {
                $allColumnNames[$colName] = true;
            }

            $documentsData[] = ['regularAttributes' => $currentRegularAttributes];
        }

        foreach (\array_keys($operators) as $colName) {
            $allColumnNames[$colName] = true;
        }

        $allColumnNames = \array_keys($allColumnNames);
        \sort($allColumnNames);

        $columnsArray = [];
        foreach ($allColumnNames as $attr) {
            $columnsArray[] = "{$this->quote($this->filter($attr))}";
        }
        $columns = '(' . \implode(', ', $columnsArray) . ')';

        foreach ($documentsData as $docData) {
            $currentRegularAttributes = $docData['regularAttributes'];
            $bindKeys = [];

            foreach ($allColumnNames as $attributeKey) {
                $attrValue = $currentRegularAttributes[$attributeKey] ?? null;

                if (\is_array($attrValue)) {
                    $attrValue = \json_encode($attrValue);
                }

                if (in_array($attributeKey, $spatialAttributes) && $attrValue !== null) {
                    $bindKey = 'key_' . $bindIndex;
                    $bindKeys[] = $this->getSpatialGeomFromText(":" . $bindKey);
                } else {
                    if ($this->supports(Capability::IntegerBooleans)) {
                        $attrValue = (\is_bool($attrValue)) ? (int)$attrValue : $attrValue;
                    }
                    $bindKey = 'key_' . $bindIndex;
                    $bindKeys[] = ':' . $bindKey;
                }
                $bindValues[$bindKey] = $attrValue;
                $bindIndex++;
            }

            $batchKeys[] = '(' . \implode(', ', $bindKeys) . ')';
        }

        $regularAttributes = [];
        foreach ($allColumnNames as $colName) {
            $regularAttributes[$colName] = null;
        }
        foreach ($documentsData[0]['regularAttributes'] as $key => $value) {
            $regularAttributes[$key] = $value;
        }

        // Build ON CONFLICT clause manually for SQLite
        $getUpdateClause = function (string $attribute, bool $increment = false): string {
            $attribute = $this->quote($this->filter($attribute));
            if ($increment) {
                $new = "{$attribute} + excluded.{$attribute}";
            } else {
                $new = "excluded.{$attribute}";
            }

            if ($this->sharedTables) {
                return "{$attribute} = CASE WHEN _tenant = excluded._tenant THEN {$new} ELSE {$attribute} END";
            }

            return "{$attribute} = {$new}";
        };

        $updateColumns = [];
        $opIndex = 0;

        if (!empty($attribute)) {
            $updateColumns = [
                $getUpdateClause($attribute, increment: true),
                $getUpdateClause('_updatedAt'),
            ];
        } else {
            foreach (\array_keys($regularAttributes) as $attr) {
                /** @var string $attr */
                $filteredAttr = $this->filter($attr);

                if (isset($operators[$attr])) {
                    $operatorSQL = $this->getOperatorSQL($filteredAttr, $operators[$attr], $opIndex);
                    if ($operatorSQL !== null) {
                        $updateColumns[] = $operatorSQL;
                    }
                } else {
                    if (!in_array($attr, ['_uid', '_id', '_createdAt', '_tenant'])) {
                        $updateColumns[] = $getUpdateClause($filteredAttr);
                    }
                }
            }
        }

        $conflictKeys = $this->sharedTables ? '(_uid, _tenant)' : '(_uid)';

        $stmt = $this->getPDO()->prepare(
            "INSERT INTO {$this->getSQLTable($name)} {$columns}
            VALUES " . \implode(', ', $batchKeys) . "
            ON CONFLICT {$conflictKeys} DO UPDATE
                SET " . \implode(', ', $updateColumns)
        );

        foreach ($bindValues as $key => $binding) {
            $stmt->bindValue($key, $binding, $this->getPDOType($binding));
        }

        $opIndexForBinding = 0;
        foreach (array_keys($regularAttributes) as $attr) {
            if (isset($operators[$attr])) {
                $this->bindOperatorParams($stmt, $operators[$attr], $opIndexForBinding);
            }
        }

        $stmt->execute();
        $stmt->closeCursor();
    }

    public function getSupportNonUtfCharacters(): bool
    {
        return false;
    }

}
