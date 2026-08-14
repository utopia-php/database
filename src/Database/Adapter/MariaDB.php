<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDOException;
use PDOStatement;
use Swoole\Database\PDOStatementProxy;
use Throwable;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\PDOStatement as DatabasePDOStatement;
use Utopia\Database\Query;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Storage;
use Utopia\Query\Builder\MariaDB as MariaDBBuilder;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\MySQL as MySQLSchema;

/**
 * Database adapter for MariaDB, extending the base SQL adapter with MariaDB-specific features.
 */
class MariaDB extends SQL implements Feature\ConnectionId, Feature\Relationships, Feature\SchemaAttributes, Feature\Spatial, Feature\Timeouts, Feature\Upserts
{
    /**
     * Get the list of capabilities supported by the MariaDB adapter.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        return array_merge(parent::capabilities(), [
            Capability::IntegerBooleans,
            Capability::NumericCasting,
            Capability::AlterLock,
            Capability::JSONOverlaps,
            Capability::FulltextWildcard,
            Capability::PCRE,
            Capability::SpatialIndexOrder,
            Capability::OptionalSpatial,
            Capability::Upserts,
            Capability::UpsertOnUniqueIndex,
            Capability::UnsignedBigInt,
        ]);
    }

    /**
     * Check whether the adapter supports storing non-UTF characters.
     *
     * @return bool
     */
    public function getSupportNonUtfCharacters(): bool
    {
        return true;
    }

    /**
     * Get the current database connection ID.
     *
     * @return string
     */
    public function getConnectionId(): string
    {
        $result = $this->createBuilder()->fromNone()->selectRaw('CONNECTION_ID()')->build();
        $stmt = $this->getPDO()->query($result->query);

        if ($stmt === false) {
            return '';
        }

        $col = $stmt->fetchColumn();

        return \is_scalar($col) ? (string) $col : '';
    }

    /**
     * Create Database
     *
     * @throws Exception
     * @throws PDOException
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        if ($this->exists($name)) {
            return true;
        }

        $result = $this->createSchemaBuilder()->createDatabase($name);
        $sql = $result->query;

        return $this->executeStatement($sql, Event::DatabaseCreate);
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
        $schema = $this->createSchemaBuilder();
        $sharedTables = $this->sharedTables;

        // Pre-build attribute hash for array lookups during index construction
        $hash = [];
        foreach ($attributes as $attribute) {
            $attrId = $this->filter($attribute->key);
            $hash[$attrId] = $attribute;
        }

        $table = $schema->table($this->getSQLTableRaw($id));
        $table->id(Storage::SEQUENCE);
        $table->string(Storage::UID, 255);
        $table->datetime(Storage::CREATED_AT, 3)->nullable()->default(null);
        $table->datetime(Storage::UPDATED_AT, 3)->nullable()->default(null);
        $table->mediumText(Storage::PERMISSIONS)->nullable()->default(null);
        $table->rawColumn('`'.Storage::VERSION.'` INT(11) UNSIGNED DEFAULT 1');

        foreach ($attributes as $attribute) {
            $attrId = $this->filter($attribute->key);

            if ($attribute->type === ColumnType::Relationship) {
                $options = $attribute->options ?? [];
                $relationType = $options['relationType'] ?? null;
                $twoWay = $options['twoWay'] ?? false;
                $side = $options['side'] ?? null;

                if (
                    $relationType === RelationType::ManyToMany->value
                    || ($relationType === RelationType::OneToOne->value && ! $twoWay && $side === RelationSide::Child->value)
                    || ($relationType === RelationType::OneToMany->value && $side === RelationSide::Parent->value)
                    || ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Child->value)
                ) {
                    continue;
                }
            }

            $attrType = $this->getSQLType(
                $attribute->type,
                $attribute->size,
                $attribute->signed,
                $attribute->array,
                $attribute->required
            );
            $table->rawColumn("`{$attrId}` {$attrType}");
        }

        foreach ($indexes as $index) {
            $indexId = $this->filter($index->key);
            $indexType = $index->type;
            $indexAttributes = $index->attributes;

            $regularColumns = [];
            $indexLengths = [];
            $indexOrders = [];
            $rawCastColumns = [];

            foreach ($indexAttributes as $nested => $attribute) {
                $indexLength = $index->lengths[$nested] ?? '';
                $indexOrder = $index->orders[$nested] ?? '';

                if ($indexType === IndexType::Spatial && ! $this->supports(Capability::SpatialIndexOrder) && ! empty($indexOrder)) {
                    throw new DatabaseException('Spatial indexes with explicit orders are not supported. Remove the orders to create this index.');
                }

                $indexAttribute = $this->filter($this->getInternalKeyForAttribute($attribute));

                if ($indexType === IndexType::Fulltext) {
                    $indexOrder = '';
                }

                if (! empty($hash[$indexAttribute]->array) && $this->supports(Capability::CastIndexArray)) {
                    $rawCastColumns[] = '(CAST(`'.$indexAttribute.'` AS char('.Database::MAX_ARRAY_INDEX_LENGTH.') ARRAY))';
                } else {
                    $regularColumns[] = $indexAttribute;
                    if (! empty($indexLength)) {
                        $indexLengths[$indexAttribute] = (int) $indexLength;
                    }
                    if (! empty($indexOrder)) {
                        $indexOrders[$indexAttribute] = $indexOrder;
                    }
                }
            }

            if ($sharedTables && $indexType !== IndexType::Fulltext && $indexType !== IndexType::Spatial) {
                \array_unshift($regularColumns, Storage::TENANT);
            }

            $table->addIndex(
                $indexId,
                $regularColumns,
                $indexType,
                $indexLengths,
                $indexOrders,
                rawColumns: $rawCastColumns,
            );
        }

        if ($sharedTables) {
            $table->rawColumn(Storage::TENANT.' INT(11) UNSIGNED DEFAULT NULL');
            $table->uniqueIndex([Storage::UID, Storage::TENANT], Storage::UID);
            $table->index([Storage::TENANT, Storage::CREATED_AT], Storage::INDEX_CREATED_AT);
            $table->index([Storage::TENANT, Storage::UPDATED_AT], Storage::INDEX_UPDATED_AT);
            $table->index([Storage::TENANT, Storage::SEQUENCE], Storage::INDEX_TENANT_ID);
        } else {
            $table->uniqueIndex([Storage::UID], Storage::UID);
            $table->index([Storage::CREATED_AT], Storage::INDEX_CREATED_AT);
            $table->index([Storage::UPDATED_AT], Storage::INDEX_UPDATED_AT);
        }

        $collectionResult = $table->create();
        $collection = $collectionResult->query;

        $permsTable = $schema->table($this->getSQLTableRaw(Storage::permissionsTable($id)));
        $permsTable->id(Storage::SEQUENCE);
        $permsTable->string(Storage::PERM_TYPE, 12);
        $permsTable->string(Storage::PERM_PERMISSION, 255);
        $permsTable->string(Storage::PERM_DOCUMENT, 255);

        if ($sharedTables) {
            $permsTable->integer(Storage::TENANT)->unsigned()->nullable()->default(null);
            $permsTable->uniqueIndex([Storage::PERM_DOCUMENT, Storage::TENANT, Storage::PERM_TYPE, Storage::PERM_PERMISSION], Storage::INDEX_1);
            $permsTable->index([Storage::TENANT, Storage::PERM_PERMISSION, Storage::PERM_TYPE], Storage::PERM_PERMISSION);
        } else {
            $permsTable->uniqueIndex([Storage::PERM_DOCUMENT, Storage::PERM_TYPE, Storage::PERM_PERMISSION], Storage::INDEX_1);
            $permsTable->index([Storage::PERM_PERMISSION, Storage::PERM_TYPE], Storage::PERM_PERMISSION);
        }

        $permsResult = $permsTable->create();
        $permissions = $permsResult->query;

        try {
            $this->executeStatement($collection, Event::CollectionCreate);
            $this->executeStatement($permissions, Event::CollectionCreate);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return true;
    }

    /**
     * Delete collection
     *
     * @throws Exception
     * @throws PDOException
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        $schema = $this->createSchemaBuilder();
        $mainResult = $schema->table($this->getSQLTableRaw($id))->drop();
        $permsResult = $schema->table($this->getSQLTableRaw(Storage::permissionsTable($id)))->drop();

        $sql = $mainResult->query.'; '.$permsResult->query;

        try {
            return $this->executeStatement($sql, Event::CollectionDelete);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Analyze a collection updating it's metadata on the database engine
     *
     * @throws DatabaseException
     */
    public function analyzeCollection(string $collection): bool
    {
        $name = $this->filter($collection);

        $result = $this->createSchemaBuilder()->analyzeTable($this->getSQLTableRaw($name));
        $sql = $result->query;

        return $this->executeStatement($sql, Event::CollectionUpdate);
    }

    /**
     * Get collection size on disk
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        $collection = $this->filter($collection);
        $collection = $this->getNamespace().'_'.$collection;
        $database = $this->getDatabase();
        $name = $database.'/'.$collection;
        $permissions = $database.'/'.Storage::permissionsTable($collection);

        $builder = $this->createBuilder();

        $collectionResult = $builder
            ->from('INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES')
            ->selectRaw('SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)')
            ->filter([BaseQuery::equal('NAME', [$name])])
            ->build();

        $permissionsResult = $builder->reset()
            ->from('INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES')
            ->selectRaw('SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)')
            ->filter([BaseQuery::equal('NAME', [$permissions])])
            ->build();

        $collectionSize = $this->executeResult($collectionResult, Event::CollectionRead);
        $permissionsSize = $this->executeResult($permissionsResult, Event::CollectionRead);

        foreach ($collectionResult->bindings as $i => $v) {
            $collectionSize->bindValue($i + 1, $v);
        }
        foreach ($permissionsResult->bindings as $i => $v) {
            $permissionsSize->bindValue($i + 1, $v);
        }

        try {
            $this->execute($collectionSize);
            $this->execute($permissionsSize);
            $collSizeVal = $collectionSize->fetchColumn();
            $permSizeVal = $permissionsSize->fetchColumn();
            $size = (int) (\is_numeric($collSizeVal) ? $collSizeVal : 0) + (int) (\is_numeric($permSizeVal) ? $permSizeVal : 0);
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }

        return $size;
    }

    /**
     * Get Collection Size of the raw data
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->filter($collection);
        $collection = $this->getNamespace().'_'.$collection;
        $database = $this->getDatabase();
        $permissions = Storage::permissionsTable($collection);

        $builder = $this->createBuilder();

        $collectionResult = $builder
            ->from('INFORMATION_SCHEMA.TABLES')
            ->selectRaw('SUM(data_length + index_length)')
            ->filter([
                BaseQuery::equal('table_name', [$collection]),
                BaseQuery::equal('table_schema', [$database]),
            ])
            ->build();

        $permissionsResult = $builder->reset()
            ->from('INFORMATION_SCHEMA.TABLES')
            ->selectRaw('SUM(data_length + index_length)')
            ->filter([
                BaseQuery::equal('table_name', [$permissions]),
                BaseQuery::equal('table_schema', [$database]),
            ])
            ->build();

        $collectionSize = $this->executeResult($collectionResult, Event::CollectionRead);
        $permissionsSize = $this->executeResult($permissionsResult, Event::CollectionRead);

        foreach ($collectionResult->bindings as $i => $v) {
            $collectionSize->bindValue($i + 1, $v);
        }
        foreach ($permissionsResult->bindings as $i => $v) {
            $permissionsSize->bindValue($i + 1, $v);
        }

        try {
            $this->execute($collectionSize);
            $this->execute($permissionsSize);
            $collVal = $collectionSize->fetchColumn();
            $permVal = $permissionsSize->fetchColumn();
            $size = (int) (\is_numeric($collVal) ? $collVal : 0) + (int) (\is_numeric($permVal) ? $permVal : 0);
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }

        return $size;
    }

    /**
     * Create a new attribute column, handling spatial types with MariaDB-specific syntax.
     *
     * @param string $collection The collection name
     * @param Attribute $attribute The attribute definition
     * @return bool
     *
     * @throws DatabaseException
     */
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        if (\in_array($attribute->type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon])) {
            $id = $this->filter($attribute->key);
            $table = $this->getSQLTableRaw($collection);
            $sqlType = $this->getSpatialSQLType($attribute->type->value, $attribute->required);
            $sql = "ALTER TABLE {$table} ADD COLUMN {$this->quote($id)} {$sqlType}";
            $lockType = $this->getLockType();
            if (! empty($lockType)) {
                $sql .= ' '.$lockType;
            }

            try {
                $ok = $this->executeStatement($sql, Event::AttributeCreate);
                $this->invalidateSpatialAttributesCache($collection);

                return $ok;
            } catch (PDOException $e) {
                throw $this->processException($e);
            }
        }

        return parent::createAttribute($collection, $attribute);
    }

    /**
     * Update Attribute
     *
     * @throws DatabaseException
     */
    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($attribute->key);
        $newKey = empty($newKey) ? null : $this->filter($newKey);
        $sqlType = $this->getSQLType($attribute->type, $attribute->size, $attribute->signed, $attribute->array, $attribute->required);
        $schema = $this->createSchemaBuilder();
        $tableRaw = $this->getSQLTableRaw($name);

        if (! empty($newKey)) {
            $result = $schema->changeColumn($tableRaw, $id, $newKey, $sqlType);
        } else {
            $result = $schema->modifyColumn($tableRaw, $id, $sqlType);
        }

        $sql = $result->query;

        try {
            $ok = $this->executeStatement($sql, Event::AttributeUpdate);
            $this->invalidateSpatialAttributesCache($collection);

            return $ok;
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Create Index
     *
     * @param  array<string,string>  $indexAttributeTypes
     * @param  array<string, mixed>  $collation
     *
     * @throws DatabaseException
     */
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
    {
        $metadataCollection = new Document([Document::ID => Database::METADATA]);
        $collection = $this->getDocument($metadataCollection, $collection);

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $rawAttrs = $collection->getAttribute('attributes', []);
        /** @var array<int, array<string, mixed>> $collectionAttributes */
        $collectionAttributes = \is_string($rawAttrs) ? (\json_decode($rawAttrs, true) ?? []) : [];
        $id = $this->filter($index->key);
        $type = $index->type;
        $attributes = $index->attributes;
        $lengths = $index->lengths;
        $orders = $index->orders;

        $schema = $this->createSchemaBuilder();
        $tableName = $this->getSQLTableRaw($collection->getId());

        // Build column lists, separating regular columns from raw CAST ARRAY expressions
        $schemaColumns = [];
        $schemaLengths = [];
        $schemaOrders = [];
        $rawExpressions = [];

        foreach ($attributes as $i => $attr) {
            $attribute = null;
            foreach ($collectionAttributes as $collectionAttribute) {
                $collAttrId = $collectionAttribute[Document::ID] ?? '';
                if (\strtolower(\is_string($collAttrId) ? $collAttrId : '') === \strtolower($attr)) {
                    $attribute = $collectionAttribute;
                    break;
                }
            }

            $attr = $this->filter($this->getInternalKeyForAttribute($attr));
            $order = empty($orders[$i]) || $type === IndexType::Fulltext ? '' : $orders[$i];
            $length = empty($lengths[$i]) ? 0 : (int) $lengths[$i];

            if ($this->supports(Capability::CastIndexArray) && ! empty($attribute['array'])) {
                $rawExpressions[] = '(CAST(`'.$attr.'` AS char('.Database::MAX_ARRAY_INDEX_LENGTH.') ARRAY))';
            } else {
                $schemaColumns[] = $attr;
                if ($length > 0) {
                    $schemaLengths[$attr] = $length;
                }
                if (! empty($order)) {
                    $schemaOrders[$attr] = $order;
                }
            }
        }

        if ($this->sharedTables && $type !== IndexType::Fulltext && $type !== IndexType::Spatial) {
            \array_unshift($schemaColumns, Storage::TENANT);
        }

        $unique = $type === IndexType::Unique;
        $schemaType = match ($type) {
            IndexType::Key, IndexType::Unique => '',
            IndexType::Fulltext => 'fulltext',
            IndexType::Spatial => 'spatial',
            default => throw new DatabaseException('Unknown index type: '.$type->value.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value.', '.IndexType::Spatial->value),
        };

        $result = $schema->createIndex(
            $tableName,
            $id,
            $schemaColumns,
            unique: $unique,
            type: $schemaType,
            lengths: $schemaLengths,
            orders: $schemaOrders,
            rawColumns: $rawExpressions,
        );
        $sql = $result->query;

        try {
            return $this->executeStatement($sql, Event::IndexCreate);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
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

        $schema = $this->createSchemaBuilder();
        $result = $schema->dropIndex($this->getSQLTableRaw($name), $id);

        $sql = $result->query;

        try {
            return $this->executeStatement($sql, Event::IndexDelete);
        } catch (PDOException $e) {
            if ($e->getCode() === '42000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1091) {
                return true;
            }

            throw $e;
        }
    }

    /**
     * Rename Index
     *
     * @throws Exception
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $old = $this->filter($old);
        $new = $this->filter($new);

        $result = $this->createSchemaBuilder()->renameIndex($this->getSQLTableRaw($collection), $old, $new);
        $sql = $result->query;

        return $this->executeStatement($sql, Event::IndexRename);
    }

    /**
     * Create Document
     *
     * @throws Exception
     * @throws PDOException
     * @throws DuplicateException
     * @throws \Throwable
     */
    public function createDocument(Document $collection, Document $document): Document
    {
        try {
            $this->syncWriteHooks();

            $spatialAttributes = $this->getSpatialAttributes($collection);
            $collection = $collection->getId();
            $attributes = $document->getAttributes();
            $attributes[Storage::CREATED_AT] = $document->getCreatedAt();
            $attributes[Storage::UPDATED_AT] = $document->getUpdatedAt();
            $attributes[Storage::PERMISSIONS] = \json_encode($document->getPermissions());
            $version = $document->getVersion();
            if ($version !== null) {
                $attributes[Storage::VERSION] = $version;
            }

            $name = $this->filter($collection);

            // Build document INSERT using query builder
            // Spatial columns use insertColumnExpression() for ST_GeomFromText() wrapping
            $builder = $this->createBuilder()->into($this->getSQLTableRaw($name));
            $row = [Storage::UID => $document->getId()];

            if (! empty($document->getSequence())) {
                $row[Storage::SEQUENCE] = $document->getSequence();
            }

            $spatialMap = \array_fill_keys($spatialAttributes, true);

            foreach ($attributes as $attr => $value) {
                $column = $this->filter($attr);

                if (isset($spatialMap[$attr])) {
                    if (\is_array($value)) {
                        $value = $this->convertArrayToWKT($value);
                    }
                    $value = (\is_bool($value)) ? (int) $value : $value;
                    $row[$column] = $value;
                    $builder->insertColumnExpression($column, $this->getSpatialGeomFromText('?'));
                } else {
                    if (\is_array($value)) {
                        $value = \json_encode($value);
                    }
                    $value = (\is_bool($value)) ? (int) $value : $value;
                    $row[$column] = $value;
                }
            }

            $row = $this->decorateRow($row, $this->documentMetadata($document));
            $builder->set($row);
            $result = $builder->insert();
            $stmt = $this->executeResult($result, Event::DocumentCreate);

            $this->execute($stmt);

            $document[Document::SEQUENCE] = $this->pdo->lastInsertId();

            if (empty($document[Document::SEQUENCE])) {
                throw new DatabaseException('Error creating document empty "'.Document::SEQUENCE.'"');
            }

            $ctx = $this->buildWriteContext($name);
            try {
                $this->runWriteHooks(fn ($hook) => $hook->afterDocumentCreate($name, [$document], $ctx));
            } catch (PDOException $e) {
                $isOrphanedPermission = $e->getCode() === '23000'
                    && isset($e->errorInfo[1])
                    && $e->errorInfo[1] === 1062
                    && \str_contains($e->getMessage(), Storage::INDEX_1);

                if (! $isOrphanedPermission) {
                    throw $e;
                }

                // Clean up orphaned permissions from a previous failed delete, then retry
                $cleanupBuilder = $this->newBuilder(Storage::permissionsTable($name));
                $cleanupBuilder->filter([BaseQuery::equal(Storage::PERM_DOCUMENT, [$document->getId()])]);
                $cleanupResult = $cleanupBuilder->delete();
                $cleanupStmt = $this->executeResult($cleanupResult, Event::PermissionsDelete);
                $this->execute($cleanupStmt);

                $this->runWriteHooks(fn ($hook) => $hook->afterDocumentCreate($name, [$document], $ctx));
            }
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
     * @throws \Throwable
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        try {
            $this->syncWriteHooks();

            $spatialAttributes = $this->getSpatialAttributes($collection);
            $collection = $collection->getId();
            $attributes = $document->getAttributes();
            $attributes[Storage::CREATED_AT] = $document->getCreatedAt();
            $attributes[Storage::UPDATED_AT] = $document->getUpdatedAt();
            $attributes[Storage::PERMISSIONS] = json_encode($document->getPermissions());

            $version = $document->getVersion();
            if ($version !== null) {
                $attributes[Storage::VERSION] = $version;
            }

            $name = $this->filter($collection);

            $operators = [];
            foreach ($attributes as $attribute => $value) {
                if (Operator::isOperator($value)) {
                    $operators[$attribute] = $value;
                }
            }

            $builder = $this->newBuilder($name);
            $regularRow = [Storage::UID => $document->getId()];

            $spatialMap = \array_fill_keys($spatialAttributes, true);

            foreach ($attributes as $attribute => $value) {
                $column = $this->filter($attribute);

                if (isset($operators[$attribute])) {
                    $op = $operators[$attribute];
                    if ($op instanceof Operator) {
                        $opResult = $this->getOperatorBuilderExpression($column, $op);
                        $builder->setRaw($column, $opResult['expression'], $opResult['bindings']);
                    }
                } elseif (isset($spatialMap[$attribute])) {
                    if (\is_array($value)) {
                        $value = $this->convertArrayToWKT($value);
                    }
                    $value = (\is_bool($value)) ? (int) $value : $value;
                    $builder->setRaw($column, $this->getSpatialGeomFromText('?'), [$value]);
                } else {
                    if (\is_array($value)) {
                        $value = \json_encode($value);
                    }
                    $value = (\is_bool($value)) ? (int) $value : $value;
                    $regularRow[$column] = $value;
                }
            }

            $builder->set($regularRow);
            $builder->filter([BaseQuery::equal(Storage::SEQUENCE, [$document->getSequence()])]);
            $result = $builder->update();
            $stmt = $this->executeResult($result, Event::DocumentUpdate);

            $this->execute($stmt);

            $ctx = $this->buildWriteContext($name);
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentUpdate($name, $document, $skipPermissions, $ctx));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
    }

    /**
     * Set max execution time
     *
     * @throws DatabaseException
     */
    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        if ($milliseconds <= 0) {
            throw new DatabaseException('Timeout must be greater than 0');
        }

        if ($event === Event::All) {
            $this->applyTimeout($milliseconds);
        }

        $this->setTimeoutState($milliseconds, $event);
    }

    public function clearTimeout(Event $event = Event::All): void
    {
        if ($event === Event::All) {
            $this->applyTimeout(0);
        }

        $this->clearTimeoutState($event);
    }

    /**
     * Size of POINT spatial type
     */
    protected function getMaxPointSize(): int
    {
        // https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html#gis-internal-format
        return 25;
    }

    /** Last value pushed to MariaDB session var max_statement_time, in seconds. */
    private float $appliedMaxStatementTime = 0.0;

    /**
     * @param  PDOStatement|DatabasePDOStatement|PDOStatementProxy  $stmt
     */
    protected function execute(mixed $stmt, ?Event $event = null): bool
    {
        $event ??= $this->getStatementEvent($stmt);
        $baseline = $this->getTimeout();
        $timeout = $event === null ? $baseline : $this->getTimeout($event);
        $this->applyTimeout($timeout);

        $exception = null;
        try {
            return parent::execute($stmt, $event);
        } catch (Throwable $error) {
            $exception = $error;
            throw $error;
        } finally {
            if ($timeout !== $baseline) {
                try {
                    $this->applyTimeout($baseline);
                } catch (Throwable $error) {
                    if ($exception === null) {
                        throw $error;
                    }
                }
            }
        }
    }

    private function applyTimeout(int $milliseconds): void
    {
        $seconds = $milliseconds > 0 ? $milliseconds / 1000.0 : 0.0;
        if ($seconds === $this->appliedMaxStatementTime) {
            return;
        }

        $this->getPDO()->exec('SET max_statement_time = '.\sprintf('%.6F', $seconds));
        $this->appliedMaxStatementTime = $seconds;
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictTenantExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));
        $tenant = Storage::TENANT;

        return "IF({$tenant} = VALUES({$tenant}), VALUES({$quoted}), {$quoted})";
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));

        return "{$quoted} + VALUES({$quoted})";
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictTenantIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));
        $tenant = Storage::TENANT;

        return "IF({$tenant} = VALUES({$tenant}), {$quoted} + VALUES({$quoted}), {$quoted})";
    }

    /**
     * Handle distance spatial queries
     *
     * @param  array<string, mixed>  $binds
     */
    protected function handleDistanceSpatialQueries(Query $query, array &$binds, string $attribute, string $type, string $alias, string $placeholder): string
    {
        /** @var array<mixed> $distanceParams */
        $distanceParams = $query->getValues()[0];
        /** @var array<mixed> $geomArray */
        $geomArray = \is_array($distanceParams[0]) ? $distanceParams[0] : [];
        $wkt = $this->convertArrayToWKT($geomArray);
        $binds[":{$placeholder}_0"] = $wkt;
        $binds[":{$placeholder}_1"] = $distanceParams[1];

        $useMeters = isset($distanceParams[2]) && $distanceParams[2] === true;

        $operator = match ($query->getMethod()) {
            Method::DistanceEqual => '=',
            Method::DistanceNotEqual => '!=',
            Method::DistanceGreaterThan => '>',
            Method::DistanceLessThan => '<',
            default => throw new DatabaseException('Unknown spatial query method: '.$query->getMethod()->value),
        };

        if ($useMeters) {
            $wktType = $this->getSpatialTypeFromWKT($wkt);
            $attrType = strtolower($type);
            if ($wktType != ColumnType::Point->value || $attrType != ColumnType::Point->value) {
                throw new QueryException('Distance in meters is not supported between '.$attrType.' and '.$wktType);
            }

            return "ST_DISTANCE_SPHERE({$alias}.{$attribute}, ".$this->getSpatialGeomFromText(":{$placeholder}_0", null).', '.Database::EARTH_RADIUS.") {$operator} :{$placeholder}_1";
        }

        return "ST_Distance({$alias}.{$attribute}, ".$this->getSpatialGeomFromText(":{$placeholder}_0", null).") {$operator} :{$placeholder}_1";
    }

    /**
     * Handle spatial queries
     *
     * @param  array<string, mixed>  $binds
     */
    protected function handleSpatialQueries(Query $query, array &$binds, string $attribute, string $type, string $alias, string $placeholder): string
    {
        /** @var array<mixed> $spatialGeomArr */
        $spatialGeomArr = \is_array($query->getValues()[0]) ? $query->getValues()[0] : [];
        $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($spatialGeomArr);
        $geom = $this->getSpatialGeomFromText(":{$placeholder}_0", null);

        return match ($query->getMethod()) {
            Method::Crosses => "ST_Crosses({$alias}.{$attribute}, {$geom})",
            Method::NotCrosses => "NOT ST_Crosses({$alias}.{$attribute}, {$geom})",
            Method::DistanceEqual,
            Method::DistanceNotEqual,
            Method::DistanceGreaterThan,
            Method::DistanceLessThan => $this->handleDistanceSpatialQueries($query, $binds, $attribute, $type, $alias, $placeholder),
            Method::Intersects => "ST_Intersects({$alias}.{$attribute}, {$geom})",
            Method::NotIntersects => "NOT ST_Intersects({$alias}.{$attribute}, {$geom})",
            Method::Overlaps => "ST_Overlaps({$alias}.{$attribute}, {$geom})",
            Method::NotOverlaps => "NOT ST_Overlaps({$alias}.{$attribute}, {$geom})",
            Method::Touches => "ST_Touches({$alias}.{$attribute}, {$geom})",
            Method::NotTouches => "NOT ST_Touches({$alias}.{$attribute}, {$geom})",
            Method::Equal => "ST_Equals({$alias}.{$attribute}, {$geom})",
            Method::NotEqual => "NOT ST_Equals({$alias}.{$attribute}, {$geom})",
            Method::Contains => "ST_Contains({$alias}.{$attribute}, {$geom})",
            Method::NotContains => "NOT ST_Contains({$alias}.{$attribute}, {$geom})",
            default => throw new DatabaseException('Unknown spatial query method: '.$query->getMethod()->value),
        };
    }

    protected function createBuilder(): SQLBuilder
    {
        return new MariaDBBuilder();
    }

    #[\Override]
    protected function createSchemaBuilder(): MySQLSchema
    {
        return new MySQLSchema();
    }

    /**
     * Get the SQL function for random ordering.
     */
    protected function getRandomOrder(): string
    {
        return 'RAND()';
    }

    /**
     * Get Schema Attributes
     *
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    public function getSchemaAttributes(string $collection): array
    {
        $schema = $this->getDatabase();
        $collection = $this->getNamespace().'_'.$this->filter($collection);

        try {
            $stmt = $this->prepareStatement('
                SELECT
                COLUMN_NAME as '.Storage::SEQUENCE.',
                COLUMN_DEFAULT as columnDefault,
                IS_NULLABLE as isNullable,
                DATA_TYPE as dataType,
                CHARACTER_MAXIMUM_LENGTH as characterMaximumLength,
                NUMERIC_PRECISION as numericPrecision,
                NUMERIC_SCALE as numericScale,
                DATETIME_PRECISION as datetimePrecision,
                COLUMN_TYPE as columnType,
                COLUMN_KEY as columnKey,
                EXTRA as extra
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            ', Event::CollectionRead);
            $stmt->bindParam(':schema', $schema);
            $stmt->bindParam(':table', $collection);
            $this->execute($stmt);
            $results = $stmt->fetchAll();
            $stmt->closeCursor();

            $docs = [];
            foreach ($results as $document) {
                /** @var array<string, mixed> $document */
                $document[Document::ID] = $document[Storage::SEQUENCE];
                unset($document[Storage::SEQUENCE]);

                $docs[] = new Document($document);
            }
            $results = $docs;

            return $results;

        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get schema attributes', $e->getCode(), $e);
        }
    }

    /**
     * Get operator SQL
     * Override to handle MariaDB/MySQL-specific operators
     */
    protected function getOperatorSQL(string $column, Operator $operator, int &$bindIndex): ?string
    {
        $quotedColumn = $this->quote($column);
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            // Numeric operators
            case OperatorType::Increment:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) > :$maxKey - :$bindKey THEN COALESCE({$quotedColumn}, 0)
                        ELSE COALESCE({$quotedColumn}, 0) + :$bindKey
                    END";
                }

                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) + :$bindKey";

            case OperatorType::Decrement:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $minKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) < :$minKey + :$bindKey THEN COALESCE({$quotedColumn}, 0)
                        ELSE COALESCE({$quotedColumn}, 0) - :$bindKey
                    END";
                }

                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) - :$bindKey";

            case OperatorType::Multiply:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN :$bindKey > 0 AND COALESCE({$quotedColumn}, 0) > :$maxKey / :$bindKey THEN COALESCE({$quotedColumn}, 0)
                        WHEN :$bindKey < 0 AND COALESCE({$quotedColumn}, 0) < :$maxKey / :$bindKey THEN COALESCE({$quotedColumn}, 0)
                        ELSE COALESCE({$quotedColumn}, 0) * :$bindKey
                    END";
                }

                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) * :$bindKey";

            case OperatorType::Divide:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $minKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN :$bindKey != 0 AND COALESCE({$quotedColumn}, 0) / :$bindKey < :$minKey THEN COALESCE({$quotedColumn}, 0)
                        ELSE COALESCE({$quotedColumn}, 0) / :$bindKey
                    END";
                }

                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) / :$bindKey";

            case OperatorType::Modulo:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = MOD(COALESCE({$quotedColumn}, 0), :$bindKey)";

            case OperatorType::Power:
                $exponent = $values[0] ?? 1;
                if (! \is_int($exponent) && ! \is_float($exponent)) {
                    throw new OperatorException('Power exponent must be numeric');
                }
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;

                    $columnValue = "COALESCE({$quotedColumn}, 0)";
                    $oddInteger = \floor($exponent) == $exponent && ((int) $exponent) % 2 !== 0;
                    $guards = [];

                    if ($exponent < 0) {
                        $guards[] = "WHEN {$columnValue} = 0 THEN {$columnValue}";
                    }
                    if (\floor($exponent) != $exponent) {
                        $guards[] = "WHEN {$columnValue} < 0 THEN {$columnValue}";
                    }
                    if ($exponent == 0) {
                        $guards[] = "WHEN LOG(:$maxKey) < 0 THEN {$columnValue}";
                    } elseif ($oddInteger) {
                        $guards[] = "WHEN {$columnValue} > 0 AND :$bindKey * LOG({$columnValue}) > LOG(:$maxKey) THEN {$columnValue}";
                    } else {
                        $guards[] = "WHEN {$columnValue} <> 0 AND :$bindKey * LOG(ABS({$columnValue})) > LOG(:$maxKey) THEN {$columnValue}";
                    }

                    return "{$quotedColumn} = CASE ".\implode(' ', $guards)." ELSE POWER({$columnValue}, :$bindKey) END";
                }

                return "{$quotedColumn} = POWER(COALESCE({$quotedColumn}, 0), :$bindKey)";

                // String operators
            case OperatorType::StringConcat:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = CONCAT(COALESCE({$quotedColumn}, ''), :$bindKey)";

            case OperatorType::StringReplace:
                $searchKey = "op_{$bindIndex}";
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = REPLACE({$quotedColumn}, :$searchKey, :$replaceKey)";

                // Boolean operators
            case OperatorType::Toggle:
                return "{$quotedColumn} = NOT COALESCE({$quotedColumn}, FALSE)";

                // Array operators
            case OperatorType::ArrayAppend:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = JSON_MERGE_PRESERVE(IFNULL({$quotedColumn}, JSON_ARRAY()), :$bindKey)";

            case OperatorType::ArrayPrepend:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = JSON_MERGE_PRESERVE(:$bindKey, IFNULL({$quotedColumn}, JSON_ARRAY()))";

            case OperatorType::ArrayInsert:
                $indexKey = "op_{$bindIndex}";
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = JSON_ARRAY_INSERT(
                    {$quotedColumn},
                    CONCAT('$[', :$indexKey, ']'),
                    JSON_EXTRACT(:$valueKey, '$')
                )";

            case OperatorType::ArrayRemove:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = IFNULL((
                    SELECT JSON_ARRAYAGG(value)
                    FROM JSON_TABLE({$quotedColumn}, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt
                    WHERE value != :$bindKey
                ), JSON_ARRAY())";

            case OperatorType::ArrayUnique:
                return "{$quotedColumn} = IFNULL((
                    SELECT JSON_ARRAYAGG(DISTINCT jt.value)
                    FROM JSON_TABLE({$quotedColumn}, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt
                ), JSON_ARRAY())";

            case OperatorType::ArrayIntersect:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = IFNULL((
                    SELECT JSON_ARRAYAGG(jt1.value)
                    FROM JSON_TABLE({$quotedColumn}, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt1
                    WHERE jt1.value IN (
                        SELECT value
                        FROM JSON_TABLE(:$bindKey, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt2
                    )
                ), JSON_ARRAY())";

            case OperatorType::ArrayDiff:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = IFNULL((
                    SELECT JSON_ARRAYAGG(jt1.value)
                    FROM JSON_TABLE({$quotedColumn}, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt1
                    WHERE jt1.value NOT IN (
                        SELECT value
                        FROM JSON_TABLE(:$bindKey, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt2
                    )
                ), JSON_ARRAY())";

            case OperatorType::ArrayFilter:
                $conditionKey = "op_{$bindIndex}";
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = IFNULL((
                    SELECT JSON_ARRAYAGG(value)
                    FROM JSON_TABLE({$quotedColumn}, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt
                    WHERE CASE :$conditionKey
                        WHEN 'equal' THEN value = JSON_UNQUOTE(:$valueKey)
                        WHEN 'notEqual' THEN value != JSON_UNQUOTE(:$valueKey)
                        WHEN 'greaterThan' THEN CAST(value AS DECIMAL(65,30)) > CAST(JSON_UNQUOTE(:$valueKey) AS DECIMAL(65,30))
                        WHEN 'greaterThanEqual' THEN CAST(value AS DECIMAL(65,30)) >= CAST(JSON_UNQUOTE(:$valueKey) AS DECIMAL(65,30))
                        WHEN 'lessThan' THEN CAST(value AS DECIMAL(65,30)) < CAST(JSON_UNQUOTE(:$valueKey) AS DECIMAL(65,30))
                        WHEN 'lessThanEqual' THEN CAST(value AS DECIMAL(65,30)) <= CAST(JSON_UNQUOTE(:$valueKey) AS DECIMAL(65,30))
                        WHEN 'isNull' THEN value IS NULL
                        WHEN 'isNotNull' THEN value IS NOT NULL
                        ELSE TRUE
                    END
                ), JSON_ARRAY())";

                // Date operators
            case OperatorType::DateAddDays:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = DATE_ADD({$quotedColumn}, INTERVAL :$bindKey DAY)";

            case OperatorType::DateSubDays:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = DATE_SUB({$quotedColumn}, INTERVAL :$bindKey DAY)";

            case OperatorType::DateSetNow:
                return "{$quotedColumn} = NOW()";

            default:
                throw new OperatorException('Invalid operator');
        }
    }

    protected function getSearchRelevanceRaw(Query $query, string $alias): ?array
    {
        $attribute = $this->filter($this->getInternalKeyForAttribute($query->getAttribute()));
        $attribute = $this->quote($attribute);
        $quotedAlias = $this->quote($alias);
        $searchVal = $query->getValue();
        $term = $this->getFulltextValue(\is_string($searchVal) ? $searchVal : '');

        return [
            'expression' => "MATCH({$quotedAlias}.{$attribute}) AGAINST (? IN BOOLEAN MODE) AS `_relevance`",
            'order' => '`_relevance` DESC',
            'bindings' => [$term],
        ];
    }

    public function getSupportForSchemaIndexes(): bool
    {
        return true;
    }

    public function getSchemaIndexes(string $collection): array
    {
        $schema = $this->getDatabase();
        $collection = $this->getNamespace() . '_' . $this->filter($collection);

        try {
            $stmt = $this->prepareStatement('
                SELECT
                    INDEX_NAME as indexName,
                    COLUMN_NAME as columnName,
                    NON_UNIQUE as nonUnique,
                    SEQ_IN_INDEX as seqInIndex,
                    INDEX_TYPE as indexType,
                    SUB_PART as subPart
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            ', Event::CollectionRead);
            $stmt->bindParam(':schema', $schema);
            $stmt->bindParam(':table', $collection);
            $this->execute($stmt);
            $rows = $stmt->fetchAll();
            $stmt->closeCursor();

            $grouped = [];
            foreach ($rows as $row) {
                $name = $row['indexName'];
                if (!isset($grouped[$name])) {
                    $grouped[$name] = [
                        Document::ID => $name,
                        'indexName' => $name,
                        'indexType' => $row['indexType'],
                        'nonUnique' => (int)$row['nonUnique'],
                        'columns' => [],
                        'lengths' => [],
                    ];
                }
                $grouped[$name]['columns'][] = $row['columnName'];
                $grouped[$name]['lengths'][] = $row['subPart'] !== null ? (int)$row['subPart'] : null;
            }

            return \array_map(fn ($idx) => new Document($idx), \array_values($grouped));
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get schema indexes', $e->getCode(), $e);
        }
    }

    protected function processException(PDOException $e): Exception
    {
        if ($e->getCode() === '22007' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1366) {
            return new CharacterException('Invalid character', $e->getCode(), $e);
        }

        // Timeout
        if ($e->getCode() === '70100' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1969) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Duplicate table
        if ($e->getCode() === '42S01' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1050) {
            return new DuplicateException('Collection already exists', $e->getCode(), $e);
        }

        // Duplicate column
        if ($e->getCode() === '42S21' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1060) {
            return new DuplicateException('Attribute already exists', $e->getCode(), $e);
        }

        // Duplicate index
        if ($e->getCode() === '42000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1061) {
            return new DuplicateException('Index already exists', $e->getCode(), $e);
        }

        // Duplicate row
        if ($e->getCode() === '23000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1062) {
            $key = $this->getViolatedKey($e->getMessage());
            if ($key === Storage::INDEX_1) {
                return new DuplicateException('Duplicate permissions for document', $e->getCode(), $e);
            }
            if ($key !== null && $key !== Storage::UID && $key !== 'PRIMARY') {
                return new UniqueException('Unique index violation', $e->getCode(), $e);
            }

            return new DuplicateException('Document already exists', $e->getCode(), $e);
        }

        // Data is too big for column resize
        if (($e->getCode() === '22001' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1406) ||
            ($e->getCode() === '01000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1265)) {
            return new TruncateException('Resize would result in data truncation', $e->getCode(), $e);
        }

        // Numeric value out of range
        if ($e->getCode() === '22003' && isset($e->errorInfo[1]) && ($e->errorInfo[1] === 1264 || $e->errorInfo[1] === 1690)) {
            return new LimitException('Value out of range', $e->getCode(), $e);
        }

        // Numeric value out of range
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1690) {
            return new LimitException('Value is out of range', $e->getCode(), $e);
        }

        // Unknown database
        if ($e->getCode() === '42000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1049) {
            return new NotFoundException('Database not found', $e->getCode(), $e);
        }

        // Unknown collection
        if ($e->getCode() === '42S02' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1049) {
            return new NotFoundException('Collection not found', $e->getCode(), $e);
        }

        // Unknown collection
        // We have two of same, because docs point to 1051.
        // Keeping previous 1049 (above) just in case it's for older versions
        if ($e->getCode() === '42S02' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1051) {
            return new NotFoundException('Collection not found', $e->getCode(), $e);
        }

        // Unknown column
        if ($e->getCode() === '42000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1091) {
            return new NotFoundException('Attribute not found', $e->getCode(), $e);
        }

        return $e;
    }

    protected function getViolatedKey(string $message): ?string
    {
        if (\preg_match("/for key '(?:[^'.]*\.)?([^']+)'/", $message, $matches) !== 1) {
            return null;
        }

        return $matches[1];
    }
}
