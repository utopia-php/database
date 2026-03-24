<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Swoole\Database\PDOStatementProxy;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Query;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Builder\SQLite as SQLiteBuilder;
use Utopia\Query\Query as BaseQuery;
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
    /**
     * Get the list of capabilities supported by the SQLite adapter.
     *
     * @return array<Capability>
     */
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
            fn (Capability $c) => ! in_array($c, $remove, true)
        ));
    }

    protected function execute(mixed $stmt): bool
    {
        /** @var \PDOStatement|PDOStatementProxy $stmt */
        return $stmt->execute();
    }

    /**
     * Check whether the adapter supports storing non-UTF characters. SQLite does not.
     *
     * @return bool
     */
    public function getSupportNonUtfCharacters(): bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
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
                    ->prepare('SAVEPOINT transaction'.$this->inTransaction)
                    ->execute();
            }
        } catch (PDOException $e) {
            throw new TransactionException('Failed to start transaction: '.$e->getMessage(), $e->getCode(), $e);
        }

        if (! $result) {
            throw new TransactionException('Failed to start transaction');
        }

        $this->inTransaction++;

        return $result;
    }

    /**
     * Create Database
     *
     * @throws Exception
     * @throws PDOException
     */
    public function create(string $name): bool
    {
        return true;
    }

    /**
     * Check if Database exists
     * Optionally check if collection exists in Database
     *
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

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", PDO::PARAM_STR);

        $stmt->execute();

        $document = $stmt->fetchAll();
        $stmt->closeCursor();
        if (! empty($document)) {
            /** @var array<string, mixed> $firstDoc */
            $firstDoc = $document[0];
            $docName = $firstDoc['name'] ?? '';

            return (\is_string($docName) ? $docName : '') === "{$this->getNamespace()}_{$collection}";
        }

        return false;
    }

    /**
     * Delete Database
     *
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
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     *
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
                $attribute->type,
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
				`_permissions` MEDIUMTEXT DEFAULT NULL,
				`_version` INTEGER DEFAULT 1".(! empty($attributes) ? ',' : '').'
				'.\substr(\implode(' ', $attributeStrings), 0, -2).'
			)
		';

        $permissions = "
			CREATE TABLE {$this->getSQLTable($id.'_perms')} (
				`_id` INTEGER PRIMARY KEY AUTOINCREMENT,
				{$tenantQuery}
				`_type` VARCHAR(12) NOT NULL,
				`_permission` VARCHAR(255) NOT NULL,
				`_document` VARCHAR(255) NOT NULL
			)
		";

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
     * Delete Collection
     *
     * @throws Exception
     * @throws PDOException
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id)}";

        $this->getPDO()
            ->prepare($sql)
            ->execute();

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id.'_perms')}";

        $this->getPDO()
            ->prepare($sql)
            ->execute();

        return true;
    }

    /**
     * Analyze a collection updating it's metadata on the database engine
     */
    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

    /**
     * Get Collection Size of raw data
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->filter($collection);
        $namespace = $this->getNamespace();
        $name = $namespace.'_'.$collection;
        $permissions = $namespace.'_'.$collection.'_perms';

        $collectionSize = $this->getPDO()->prepare('
             SELECT SUM("pgsize")
             FROM "dbstat"
             WHERE name = :name;
        ');

        $permissionsSize = $this->getPDO()->prepare('
             SELECT SUM("pgsize")
             FROM "dbstat"
             WHERE name = :name;
        ');

        $collectionSize->bindParam(':name', $name);
        $permissionsSize->bindParam(':name', $permissions);

        try {
            $collectionSize->execute();
            $permissionsSize->execute();
            $collVal = $collectionSize->fetchColumn();
            $permVal = $permissionsSize->fetchColumn();
            $size = (int)(\is_numeric($collVal) ? $collVal : 0) + (int)(\is_numeric($permVal) ? $permVal : 0);
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }

        return $size;
    }

    /**
     * Get Collection Size on disk
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->getSizeOfCollection($collection);
    }

    /**
     * Update Attribute
     *
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
    {
        if (! empty($newKey) && $newKey !== $attribute->key) {
            return $this->renameAttribute($collection, $attribute->key, $newKey);
        }

        return true;
    }

    /**
     * Delete Attribute
     *
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

        $rawIndexes = $collection->getAttribute('indexes', '[]');
        /** @var array<int, array<string, mixed>> $indexes */
        $indexes = \json_decode(\is_string($rawIndexes) ? $rawIndexes : '[]', true) ?? [];

        foreach ($indexes as $index) {
            /** @var array<string, mixed> $index */
            $attributes = $index['attributes'] ?? [];
            $indexId = \is_string($index['$id'] ?? null) ? (string) $index['$id'] : '';
            $indexType = \is_string($index['type'] ?? null) ? (string) $index['type'] : '';
            if ($attributes === [$id]) {
                $this->deleteIndex($name, $indexId);
            } elseif (\in_array($id, \is_array($attributes) ? $attributes : [])) {
                $this->deleteIndex($name, $indexId);
                $this->createIndex($name, new Index(
                    key: $indexId,
                    type: IndexType::from($indexType),
                    attributes: \array_map(fn (mixed $v): string => \is_scalar($v) ? (string) $v : '', \is_array($attributes) ? \array_values(\array_diff($attributes, [$id])) : []),
                    lengths: \array_map(fn (mixed $v): int => \is_numeric($v) ? (int) $v : 0, \is_array($index['lengths'] ?? null) ? $index['lengths'] : []),
                    orders: \array_map(fn (mixed $v): string => \is_scalar($v) ? (string) $v : '', \is_array($index['orders'] ?? null) ? $index['orders'] : []),
                ));
            }
        }

        $sql = "ALTER TABLE {$this->getSQLTable($name)} DROP COLUMN `{$id}`";

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
     * Create Index
     *
     * @param  array<string,string>  $indexAttributeTypes
     * @param  array<string, mixed>  $collation
     *
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
        if (! empty($existingIndex)) {
            return true;
        }

        $sql = $this->getSQLIndex($name, $id, $type, $attributes);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
    }

    /**
     * Delete Index
     *
     * @throws Exception
     * @throws PDOException
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        $sql = "DROP INDEX `{$this->getNamespace()}_{$this->tenant}_{$name}_{$id}`";

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
     * Rename Index
     *
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
        $rawIdxs = $collection->getAttribute('indexes', '[]');
        /** @var array<int, array<string, mixed>> $indexes */
        $indexes = \json_decode(\is_string($rawIdxs) ? $rawIdxs : '[]', true) ?? [];
        /** @var array<string, mixed>|null $index */
        $index = null;

        foreach ($indexes as $node) {
            /** @var array<string, mixed> $node */
            if (($node['key'] ?? null) === $old) {
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
                    type: IndexType::from(\is_string($index['type'] ?? null) ? (string) $index['type'] : ''),
                    attributes: \array_map(fn (mixed $v): string => \is_scalar($v) ? (string) $v : '', \is_array($index['attributes'] ?? null) ? $index['attributes'] : []),
                    lengths: \array_map(fn (mixed $v): int => \is_numeric($v) ? (int) $v : 0, \is_array($index['lengths'] ?? null) ? $index['lengths'] : []),
                    orders: \array_map(fn (mixed $v): string => \is_scalar($v) ? (string) $v : '', \is_array($index['orders'] ?? null) ? $index['orders'] : []),
                ),
            )) {
            return true;
        }

        return false;
    }

    /**
     * Create Document
     *
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

            $version = $document->getVersion();
            if ($version !== null) {
                $attributes['_version'] = $version;
            }

            $name = $this->filter($collection);

            $builder = $this->createBuilder()->into($this->getSQLTableRaw($name));
            $row = ['_uid' => $document->getId()];

            if (! empty($document->getSequence())) {
                $row['_id'] = $document->getSequence();
            }

            foreach ($attributes as $attr => $value) {
                $column = $this->filter($attr);

                if (is_array($value)) {
                    $value = json_encode($value);
                }
                $value = (is_bool($value)) ? (int) $value : $value;
                $row[$column] = $value;
            }

            $row = $this->decorateRow($row, $this->documentMetadata($document));
            $builder->set($row);
            $result = $builder->insert();
            $stmt = $this->executeResult($result, Event::DocumentCreate);

            $stmt->execute();

            $statment = $this->getPDO()->prepare('SELECT last_insert_rowid() AS id');
            $statment->execute();
            $last = $statment->fetch();

            if (\is_array($last)) {
                /** @var array<string, mixed> $last */
                $document['$sequence'] = $last['id'] ?? null;
            }

            $ctx = $this->buildWriteContext($name);
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentCreate($name, [$document], $ctx));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
    }

    /**
     * Update Document
     *
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

            $version = $document->getVersion();
            if ($version !== null) {
                $attributes['_version'] = $version;
            }

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
                    $op = $operators[$attribute];
                    if ($op instanceof Operator) {
                        $opResult = $this->getOperatorBuilderExpression($column, $op);
                        $builder->setRaw($column, $opResult['expression'], $opResult['bindings']);
                    }
                } elseif ($this->supports(Capability::Spatial) && \in_array($attribute, $spatialAttributes, true)) {
                    if (\is_array($value)) {
                        $value = $this->convertArrayToWKT($value);
                    }
                    $value = (is_bool($value)) ? (int) $value : $value;
                    $builder->setRaw($column, $this->getSpatialGeomFromText('?'), [$value]);
                } else {
                    if (is_array($value)) {
                        $value = json_encode($value);
                    }
                    $value = (is_bool($value)) ? (int) $value : $value;
                    $regularRow[$column] = $value;
                }
            }

            $builder->set($regularRow);
            $builder->filter([BaseQuery::equal('_uid', [$id])]);
            $result = $builder->update();
            $stmt = $this->executeResult($result, Event::DocumentUpdate);

            $stmt->execute();

            $ctx = $this->buildWriteContext($name);
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentUpdate($name, $document, $skipPermissions, $ctx));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
    }

    public function getSupportForSchemaIndexes(): bool
    {
        return false;
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

    protected function createBuilder(): SQLBuilder
    {
        return new SQLiteBuilder();
    }

    /**
     * Override getSpatialGeomFromText to return placeholder unchanged for SQLite
     * SQLite does not support ST_GeomFromText, so we return the raw placeholder
     */
    protected function getSpatialGeomFromText(string $wktPlaceholder, ?int $srid = null): string
    {
        return $wktPlaceholder;
    }

    /**
     * Get SQL Index Type
     *
     * @throws Exception
     */
    protected function getSQLIndexType(IndexType $type): string
    {
        return match ($type) {
            IndexType::Key => 'INDEX',
            IndexType::Unique => 'UNIQUE INDEX',
            default => throw new DatabaseException('Unknown index type: '.$type->value.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value),
        };
    }

    /**
     * Get SQL Index
     *
     * @param  array<string>  $attributes
     *
     * @throws Exception
     */
    protected function getSQLIndex(string $collection, string $id, IndexType $type, array $attributes): string
    {
        [$sqlType, $postfix] = match ($type) {
            IndexType::Key => ['INDEX', ''],
            IndexType::Unique => ['UNIQUE INDEX', 'COLLATE NOCASE'],
            default => throw new DatabaseException('Unknown index type: '.$type->value.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value),
        };

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

        return "CREATE {$sqlType} {$key} ON `{$this->getNamespace()}_{$collection}` ({$attributes})";
    }

    /**
     * Get SQL table
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
        return $this->getNamespace().'_'.$this->filter($name);
    }

    /**
     * Get the SQL function for random ordering
     */
    protected function getRandomOrder(): string
    {
        return 'RANDOM()';
    }

    /**
     * Check if SQLite math functions (like POWER) are available
     * SQLite must be compiled with -DSQLITE_ENABLE_MATH_FUNCTIONS
     */
    private function getSupportForMathFunctions(): bool
    {
        static $available = null;

        if ($available !== null) {
            return (bool) $available;
        }

        try {
            // Test if POWER function exists by attempting to use it
            $stmt = $this->getPDO()->query('SELECT POWER(2, 3) as test');
            if ($stmt === false) {
                $available = false;

                return false;
            }
            $result = $stmt->fetch();
            /** @var array<string, mixed>|false $result */
            $testVal = \is_array($result) ? ($result['test'] ?? null) : null;
            $available = ($testVal == 8);

            return $available;
        } catch (PDOException $e) {
            // Function doesn't exist
            $available = false;

            return false;
        }
    }

    protected function getSearchRelevanceRaw(Query $query, string $alias): ?array
    {
        return null;
    }

    protected function processException(PDOException $e): Exception
    {
        // Timeout
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Table/index already exists (SQLITE_ERROR with "already exists" message)
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1 && stripos($e->getMessage(), 'already exists') !== false) {
            return new DuplicateException('Collection already exists', $e->getCode(), $e);
        }

        // Table not found (SQLITE_ERROR with "no such table" message)
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1 && stripos($e->getMessage(), 'no such table') !== false) {
            return new NotFoundException('Collection not found', $e->getCode(), $e);
        }

        // Duplicate - SQLite uses various error codes for constraint violations:
        // - Error code 19 is SQLITE_CONSTRAINT (includes UNIQUE violations)
        // - Error code 1 is also used for some duplicate cases
        // - SQL state '23000' is integrity constraint violation
        if (
            ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && ($e->errorInfo[1] === 1 || $e->errorInfo[1] === 19)) ||
            $e->getCode() === '23000'
        ) {
            $message = $e->getMessage();
            if (
                (isset($e->errorInfo[1]) && $e->errorInfo[1] === 19) ||
                $e->getCode() === '23000' ||
                stripos($message, 'unique') !== false ||
                stripos($message, 'duplicate') !== false
            ) {
                if (! \str_contains($message, '_uid')) {
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
     * Bind operator parameters to statement
     * Override to handle SQLite-specific operator bindings
     */
    protected function bindOperatorParams(PDOStatement|PDOStatementProxy $stmt, Operator $operator, int &$bindIndex): void
    {
        $method = $operator->getMethod();

        // For operators that SQLite doesn't use bind parameters for, skip binding entirely
        // Note: The bindIndex increment happens in getOperatorSQL(), NOT here
        if (in_array($method, [OperatorType::Toggle, OperatorType::DateSetNow, OperatorType::ArrayUnique])) {
            // These operators don't bind any parameters - they're handled purely in SQL
            // DO NOT increment bindIndex here as it's already handled in getOperatorSQL()
            return;
        }

        // For ARRAY_FILTER, bind the filter value if present
        if ($method === OperatorType::ArrayFilter) {
            $values = $operator->getValues();
            if (! empty($values) && count($values) >= 2) {
                $filterType = $values[0];
                $filterValue = $values[1];

                // Only bind if we support this filter type (all comparison operators need binding)
                $comparisonTypes = ['equal', 'notEqual', 'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual'];
                if (in_array($filterType, $comparisonTypes)) {
                    $bindKey = "op_{$bindIndex}";
                    $value = (is_bool($filterValue)) ? (int) $filterValue : $filterValue;
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
     * {@inheritDoc}
     */
    protected function getOperatorBuilderExpression(string $column, Operator $operator): array
    {
        if ($operator->getMethod() === OperatorType::ArrayFilter) {
            $bindIndex = 0;
            $fullExpression = $this->getOperatorSQL($column, $operator, $bindIndex);

            if ($fullExpression === null) {
                throw new DatabaseException('Operator cannot be expressed in SQL: '.$operator->getMethod()->value);
            }

            $quotedColumn = $this->quote($column);
            $prefix = $quotedColumn.' = ';
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
                $search = ':'.$key;
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
                $positionalBindings[] = $namedBindings[$r['key']] ?? null;
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
     */
    protected function getOperatorSQL(string $column, Operator $operator, int &$bindIndex): ?string
    {
        $quotedColumn = $this->quote($column);
        $method = $operator->getMethod();

        switch ($method) {
            // Numeric operators
            case OperatorType::Increment:
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

            case OperatorType::Decrement:
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

            case OperatorType::Multiply:
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

            case OperatorType::Divide:
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

            case OperatorType::Modulo:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) % :$bindKey";

            case OperatorType::Power:
                if (! $this->getSupportForMathFunctions()) {
                    throw new DatabaseException(
                        'SQLite POWER operator requires math functions. '.
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
            case OperatorType::StringConcat:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = IFNULL({$quotedColumn}, '') || :$bindKey";

            case OperatorType::StringReplace:
                $searchKey = "op_{$bindIndex}";
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = REPLACE({$quotedColumn}, :$searchKey, :$replaceKey)";

                // Boolean operators
            case OperatorType::Toggle:
                // SQLite: toggle boolean (0 or 1), treat NULL as 0
                return "{$quotedColumn} = CASE WHEN COALESCE({$quotedColumn}, 0) = 0 THEN 1 ELSE 0 END";

                // Array operators
            case OperatorType::ArrayAppend:
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

            case OperatorType::ArrayPrepend:
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

            case OperatorType::ArrayUnique:
                // SQLite: get distinct values from JSON array
                return "{$quotedColumn} = (
                    SELECT json_group_array(DISTINCT value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                )";

            case OperatorType::ArrayRemove:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                // SQLite: remove specific value from array
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value != :$bindKey
                )";

            case OperatorType::ArrayInsert:
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

            case OperatorType::ArrayIntersect:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                // SQLite: keep only values that exist in both arrays
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value IN (SELECT value FROM json_each(:$bindKey))
                )";

            case OperatorType::ArrayDiff:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                // SQLite: remove values that exist in the comparison array
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value NOT IN (SELECT value FROM json_each(:$bindKey))
                )";

            case OperatorType::ArrayFilter:
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
                            default => throw new OperatorException('Unsupported filter type: '.(\is_scalar($filterType) ? (string) $filterType : 'unknown')),
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
            case OperatorType::DateAddDays:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = datetime({$quotedColumn}, :$bindKey || ' days')";

            case OperatorType::DateSubDays:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = datetime({$quotedColumn}, '-' || abs(:$bindKey) || ' days')";

            case OperatorType::DateSetNow:
                return "{$quotedColumn} = datetime('now')";

            default:
                // Fall back to parent implementation for other operators
                return parent::getOperatorSQL($column, $operator, $bindIndex);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictTenantExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));

        return "CASE WHEN _tenant = excluded._tenant THEN excluded.{$quoted} ELSE {$quoted} END";
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));

        return "{$quoted} + excluded.{$quoted}";
    }

    /**
     * {@inheritDoc}
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
     * @param  string  $name  The filtered collection name
     * @param  array<Change>  $changes  The changes to upsert
     * @param  array<string>  $spatialAttributes  Spatial column names
     * @param  string  $attribute  Increment attribute name (empty if none)
     * @param  array<string, Operator>  $operators  Operator map keyed by attribute name
     * @param  array<string, mixed>  $attributeDefaults  Attribute default values
     * @param  bool  $hasOperators  Whether this batch contains operator expressions
     *
     * @throws DatabaseException
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

                if ($change->getOld()->isEmpty() && ! empty($extractedOperators)) {
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

            $version = $document->getVersion();
            if ($version !== null) {
                $currentRegularAttributes['_version'] = $version;
            }

            if (! empty($document->getSequence())) {
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
        $columns = '('.\implode(', ', $columnsArray).')';

        foreach ($documentsData as $docData) {
            $currentRegularAttributes = $docData['regularAttributes'];
            $bindKeys = [];

            foreach ($allColumnNames as $attributeKey) {
                $attrValue = $currentRegularAttributes[$attributeKey] ?? null;

                if (\is_array($attrValue)) {
                    $attrValue = \json_encode($attrValue);
                }

                if (in_array($attributeKey, $spatialAttributes) && $attrValue !== null) {
                    $bindKey = 'key_'.$bindIndex;
                    $bindKeys[] = $this->getSpatialGeomFromText(':'.$bindKey);
                } else {
                    if ($this->supports(Capability::IntegerBooleans)) {
                        $attrValue = (\is_bool($attrValue)) ? (int) $attrValue : $attrValue;
                    }
                    $bindKey = 'key_'.$bindIndex;
                    $bindKeys[] = ':'.$bindKey;
                }
                $bindValues[$bindKey] = $attrValue;
                $bindIndex++;
            }

            $batchKeys[] = '('.\implode(', ', $bindKeys).')';
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

        if (! empty($attribute)) {
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
                    if (! in_array($attr, ['_uid', '_id', '_createdAt', '_tenant'])) {
                        $updateColumns[] = $getUpdateClause($filteredAttr);
                    }
                }
            }
        }

        $conflictKeys = $this->sharedTables ? '(_uid, _tenant)' : '(_uid)';

        $stmt = $this->getPDO()->prepare(
            "INSERT INTO {$this->getSQLTable($name)} {$columns}
            VALUES ".\implode(', ', $batchKeys)."
            ON CONFLICT {$conflictKeys} DO UPDATE
                SET ".\implode(', ', $updateColumns)
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
}
