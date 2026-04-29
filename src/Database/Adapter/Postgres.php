<?php

namespace Utopia\Database\Adapter;

use DateTime;
use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Swoole\Database\PDOStatementProxy;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Query;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Builder\PostgreSQL as PostgreSQLBuilder;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\PostgreSQL as PostgreSQLSchema;
use Utopia\Query\Schema\Table;

/**
 * Differences between MariaDB and Postgres
 *
 * 1. Need to use CASCADE to DROP schema
 * 2. Quotes are different ` vs "
 * 3. DATETIME is TIMESTAMP
 * 4. Full-text search is different - to_tsvector() and to_tsquery()
 */
class Postgres extends SQL implements Feature\ConnectionId, Feature\Relationships, Feature\Spatial, Feature\Timeouts, Feature\Upserts
{
    public const MAX_IDENTIFIER_NAME = 63;

    /**
     * Get the list of capabilities supported by the PostgreSQL adapter.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        return array_merge(parent::capabilities(), [
            Capability::Vectors,
            Capability::Objects,
            Capability::SpatialIndexNull,
            Capability::MultiDimensionDistance,
            Capability::TrigramIndex,
            Capability::POSIX,
            Capability::ObjectIndexes,
        ]);
    }

    /**
     * Get the case-insensitive LIKE operator for PostgreSQL.
     *
     * @return string
     */
    public function getLikeOperator(): string
    {
        return 'ILIKE';
    }

    /**
     * Get the POSIX regex matching operator for PostgreSQL.
     *
     * @return string
     */
    public function getRegexOperator(): string
    {
        return '~';
    }

    /**
     * Get the PostgreSQL backend process ID as the connection identifier.
     *
     * @return string
     */
    public function getConnectionId(): string
    {
        $result = $this->createBuilder()->fromNone()->selectRaw('pg_backend_pid()')->build();
        $stmt = $this->getPDO()->query($result->query);
        if ($stmt === false) {
            return '';
        }
        $col = $stmt->fetchColumn();

        return \is_scalar($col) ? (string) $col : '';
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
                } else {
                    // If no active transaction, this has no effect.
                    $this->getPDO()->prepare('ROLLBACK')->execute();
                }

                $result = $this->getPDO()->beginTransaction();
            } else {
                $this->getPDO()->exec('SAVEPOINT transaction'.$this->inTransaction);
                $result = true;
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
     * {@inheritDoc}
     */
    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        try {
            if ($this->inTransaction > 1) {
                $this->getPDO()->exec('ROLLBACK TO transaction'.($this->inTransaction - 1));
                $this->inTransaction--;

                return true;
            }

            $result = $this->getPDO()->rollBack();
            $this->inTransaction = 0;
        } catch (PDOException $e) {
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to rollback transaction: '.$e->getMessage(), $e->getCode(), $e);
        }

        if (! $result) {
            throw new TransactionException('Failed to rollback transaction');
        }

        return $result;
    }

    /**
     * Create Database
     *
     *
     * @throws DatabaseException
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        if ($this->exists($name)) {
            return true;
        }

        $schema = $this->createSchemaBuilder();
        $sql = $schema->createDatabase($name)->query;

        $dbCreation = $this->getPDO()
            ->prepare($sql)
            ->execute();

        // Enable extensions — wrap in try-catch to handle concurrent creation race conditions
        foreach (['postgis', 'vector', 'pg_trgm'] as $ext) {
            try {
                $this->getPDO()->prepare($schema->createExtension($ext)->query)->execute();
            } catch (PDOException) {
                // Extension may already exist due to concurrent worker
            }
        }

        try {
            $collation = $schema->createCollation('utf8_ci_ai', [
                'provider' => 'icu',
                'locale' => 'und-u-ks-level1',
            ], deterministic: false);
            $this->getPDO()->prepare($collation->query)->execute();
        } catch (PDOException) {
            // Collation may already exist due to concurrent worker
        }

        return $dbCreation;
    }

    /**
     * Override to use lowercase catalog names for Postgres case sensitivity.
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        $database = $this->filter($database);

        if ($collection !== null) {
            $collection = $this->filter($collection);
            $sql = 'SELECT "table_name" FROM information_schema.tables WHERE "table_schema" = ? AND "table_name" = ?';
            $stmt = $this->getPDO()->prepare($sql);
            $stmt->bindValue(1, $database);
            $stmt->bindValue(2, "{$this->getNamespace()}_{$collection}");
        } else {
            $sql = 'SELECT "schema_name" FROM information_schema.schemata WHERE "schema_name" = ?';
            $stmt = $this->getPDO()->prepare($sql);
            $stmt->bindValue(1, $database);
        }

        try {
            $stmt->execute();
            $document = $stmt->fetchAll();
            $stmt->closeCursor();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return ! empty($document);
    }

    /**
     * Create Collection
     *
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     *
     * @throws DuplicateException
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $namespace = $this->getNamespace();
        $id = $this->filter($name);
        $tableRaw = $this->getSQLTableRaw($id);
        $permsTableRaw = $this->getSQLTableRaw($id.'_perms');

        $schema = $this->createSchemaBuilder();

        // Build main collection table using schema builder
        $collectionResult = $schema->create($tableRaw, function (Table $table) use ($attributes) {
            $table->id('_id');
            $table->string('_uid', 255);

            if ($this->sharedTables) {
                $table->integer('_tenant')->nullable()->default(null);
            }

            $table->datetime('_createdAt', 3)->nullable()->default(null);
            $table->datetime('_updatedAt', 3)->nullable()->default(null);

            foreach ($attributes as $attribute) {
                // Ignore relationships with virtual attributes
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

                $this->addBlueprintColumn(
                    $table,
                    $attribute->key,
                    $attribute->type,
                    $attribute->size,
                    $attribute->signed,
                    $attribute->array,
                    $attribute->required
                );
            }

            $table->text('_permissions')->nullable()->default(null);
            $table->integer('_version')->nullable()->default(1);
        });

        // Build default indexes using schema builder
        $indexStatements = [];

        if ($this->sharedTables) {
            $uidIndex = $this->getShortKey("{$namespace}_{$this->tenant}_{$id}_uid");
            $createdIndex = $this->getShortKey("{$namespace}_{$this->tenant}_{$id}_created");
            $updatedIndex = $this->getShortKey("{$namespace}_{$this->tenant}_{$id}_updated");
            $tenantIdIndex = $this->getShortKey("{$namespace}_{$this->tenant}_{$id}_tenant_id");
            $indexStatements[] = $schema->createIndex($tableRaw, $uidIndex, ['_uid', '_tenant'], unique: true, collations: ['_uid' => 'utf8_ci_ai'])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $createdIndex, ['_tenant', '_createdAt'])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $updatedIndex, ['_tenant', '_updatedAt'])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $tenantIdIndex, ['_tenant', '_id'])->query;
        } else {
            $uidIndex = $this->getShortKey("{$namespace}_{$id}_uid");
            $createdIndex = $this->getShortKey("{$namespace}_{$id}_created");
            $updatedIndex = $this->getShortKey("{$namespace}_{$id}_updated");
            $indexStatements[] = $schema->createIndex($tableRaw, $uidIndex, ['_uid'], unique: true, collations: ['_uid' => 'utf8_ci_ai'])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $createdIndex, ['_createdAt'])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $updatedIndex, ['_updatedAt'])->query;
        }

        $collectionSql = $collectionResult->query.'; '.implode('; ', $indexStatements);

        // Build permissions table using schema builder
        $permsResult = $schema->create($permsTableRaw, function (Table $table) {
            $table->id('_id');
            $table->integer('_tenant')->nullable()->default(null);
            $table->string('_type', 12);
            $table->string('_permission', 255);
            $table->string('_document', 255);
        });

        // Build permission indexes using schema builder
        $permsIndexStatements = [];

        if ($this->sharedTables) {
            $uniquePermissionIndex = $this->getShortKey("{$namespace}_{$this->tenant}_{$id}_ukey");
            $permissionIndex = $this->getShortKey("{$namespace}_{$this->tenant}_{$id}_permission");
            $permsIndexStatements[] = $schema->createIndex($permsTableRaw, $uniquePermissionIndex, ['_tenant', '_document', '_type', '_permission'], unique: true, method: 'btree')->query;
            $permsIndexStatements[] = $schema->createIndex($permsTableRaw, $permissionIndex, ['_tenant', '_permission', '_type'], method: 'btree')->query;
        } else {
            $uniquePermissionIndex = $this->getShortKey("{$namespace}_{$id}_ukey");
            $permissionIndex = $this->getShortKey("{$namespace}_{$id}_permission");
            $permsIndexStatements[] = $schema->createIndex($permsTableRaw, $uniquePermissionIndex, ['_document', '_type', '_permission'], unique: true, method: 'btree', collations: ['_document' => 'utf8_ci_ai'])->query;
            $permsIndexStatements[] = $schema->createIndex($permsTableRaw, $permissionIndex, ['_permission', '_type'], method: 'btree')->query;
        }

        $permsSql = $permsResult->query.'; '.implode('; ', $permsIndexStatements);

        try {
            $this->getPDO()->prepare($collectionSql)->execute();
            $this->getPDO()->prepare($permsSql)->execute();

            foreach ($indexes as $index) {
                $indexId = $this->filter($index->key);
                $indexType = $index->type;
                $indexAttributes = $index->attributes;
                $indexAttributesWithType = [];
                foreach ($indexAttributes as $indexAttribute) {
                    foreach ($attributes as $attribute) {
                        if ($attribute->key === $indexAttribute) {
                            $indexAttributesWithType[$indexAttribute] = $attribute->type->value;
                        }
                    }
                }
                $indexOrders = $index->orders;
                $indexTtl = $index->ttl;
                if ($indexType === IndexType::Spatial && count($indexOrders)) {
                    throw new DatabaseException('Spatial indexes with explicit orders are not supported. Remove the orders to create this index.');
                }
                $this->createIndex(
                    $id,
                    new Index(
                        key: $indexId,
                        type: $indexType,
                        attributes: $indexAttributes,
                        orders: $indexOrders,
                        ttl: $indexTtl,
                    ),
                    $indexAttributesWithType,
                );
            }
        } catch (DuplicateException $e) {
            throw $e;
        } catch (PDOException $e) {
            $e = $this->processException($e);

            if (! ($e instanceof DuplicateException)) {
                $dropSchema = $this->createSchemaBuilder();
                $dropSql = $dropSchema->dropIfExists($tableRaw)->query.'; '.$dropSchema->dropIfExists($permsTableRaw)->query;
                $this->execute($this->getPDO()->prepare($dropSql));
            }

            throw $e;
        }

        return true;
    }

    /**
     * Get Collection Size on disk
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        $collection = $this->filter($collection);
        $name = $this->getSQLTable($collection);
        $permissions = $this->getSQLTable($collection.'_perms');

        $builder = $this->createBuilder();

        $collectionResult = $builder->fromNone()->selectRaw('pg_total_relation_size(?)', [$name])->build();
        $permissionsResult = $builder->reset()->fromNone()->selectRaw('pg_total_relation_size(?)', [$permissions])->build();

        $collectionSize = $this->getPDO()->prepare($collectionResult->query);
        $permissionsSize = $this->getPDO()->prepare($permissionsResult->query);

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
            $size = (int)(\is_numeric($collVal) ? $collVal : 0) + (int)(\is_numeric($permVal) ? $permVal : 0);
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }

        return $size;
    }

    /**
     * Get Collection Size of raw data
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->filter($collection);
        $name = $this->getSQLTable($collection);
        $permissions = $this->getSQLTable($collection.'_perms');

        $builder = $this->createBuilder();

        $collectionResult = $builder->fromNone()->selectRaw('pg_relation_size(?)', [$name])->build();
        $permissionsResult = $builder->reset()->fromNone()->selectRaw('pg_relation_size(?)', [$permissions])->build();

        $collectionSize = $this->getPDO()->prepare($collectionResult->query);
        $permissionsSize = $this->getPDO()->prepare($permissionsResult->query);

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
            $size = (int)(\is_numeric($collVal) ? $collVal : 0) + (int)(\is_numeric($permVal) ? $permVal : 0);
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }

        return $size;
    }

    /**
     * Create Attribute
     *
     *
     * @throws DatabaseException
     */
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        // Ensure pgvector extension is installed for vector types
        if ($attribute->type === ColumnType::Vector) {
            if ($attribute->size <= 0) {
                throw new DatabaseException('Vector dimensions must be a positive integer');
            }
            if ($attribute->size > Database::MAX_VECTOR_DIMENSIONS) {
                throw new DatabaseException('Vector dimensions cannot exceed '.Database::MAX_VECTOR_DIMENSIONS);
            }
        }

        $schema = $this->createSchemaBuilder();
        $result = $schema->alter($this->getSQLTableRaw($collection), function (Table $table) use ($attribute) {
            $this->addBlueprintColumn($table, $attribute->key, $attribute->type, $attribute->size, $attribute->signed, $attribute->array, $attribute->required);
        });

        // Postgres does not support LOCK= on ALTER TABLE, so no lock type appended
        $sql = $result->query;

        try {
            return $this->execute($this->getPDO()
                ->prepare($sql));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Update Attribute
     *
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($attribute->key);
        $newKey = empty($newKey) ? null : $this->filter($newKey);

        if ($attribute->type === ColumnType::Vector) {
            if ($attribute->size <= 0) {
                throw new DatabaseException('Vector dimensions must be a positive integer');
            }
            if ($attribute->size > Database::MAX_VECTOR_DIMENSIONS) {
                throw new DatabaseException('Vector dimensions cannot exceed '.Database::MAX_VECTOR_DIMENSIONS);
            }
        }

        $schema = $this->createSchemaBuilder();

        // Rename column first if needed
        if (! empty($newKey) && $id !== $newKey) {
            $newKey = $this->filter($newKey);

            $renameResult = $schema->alter($this->getSQLTableRaw($collection), function (Table $table) use ($id, $newKey) {
                $table->renameColumn($id, $newKey);
            });

            $sql = $renameResult->query;

            $result = $this->execute($this->getPDO()
                ->prepare($sql));

            if (! $result) {
                return false;
            }

            $id = $newKey;
        }

        // Modify column type using schema builder's alterColumnType
        $sqlType = $this->getSQLType($attribute->type, $attribute->size, $attribute->signed, $attribute->array, $attribute->required);
        $tableRaw = $this->getSQLTableRaw($name);

        if ($sqlType == 'TIMESTAMP(3)') {
            $result = $schema->alterColumnType($tableRaw, $id, 'TIMESTAMP(3) without time zone', "TO_TIMESTAMP(\"{$id}\", 'YYYY-MM-DD HH24:MI:SS.MS')");
        } else {
            $result = $schema->alterColumnType($tableRaw, $id, $sqlType);
        }

        $sql = $result->query;

        try {
            return $this->execute($this->getPDO()
                ->prepare($sql));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Delete Attribute
     *
     *
     * @throws DatabaseException
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $schema = $this->createSchemaBuilder();
        $result = $schema->alter($this->getSQLTableRaw($collection), function (Table $table) use ($id) {
            $table->dropColumn($this->filter($id));
        });

        $sql = $result->query;

        try {
            return $this->execute($this->getPDO()
                ->prepare($sql));
        } catch (PDOException $e) {
            if ($e->getCode() === '42703' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
                return true;
            }

            throw $e;
        }
    }

    /**
     * Rename Attribute
     *
     * @throws Exception
     * @throws PDOException
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $schema = $this->createSchemaBuilder();
        $result = $schema->alter($this->getSQLTableRaw($collection), function (Table $table) use ($old, $new) {
            $table->renameColumn($this->filter($old), $this->filter($new));
        });

        $sql = $result->query;

        return $this->execute($this->getPDO()
            ->prepare($sql));
    }

    /**
     * Create Index
     *
     * @param  array<string,string>  $indexAttributeTypes
     * @param  array<string, mixed>  $collation
     */
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($index->key);
        $type = $index->type;
        $attributes = $index->attributes;
        $orders = $index->orders;

        // Validate index type
        match ($type) {
            IndexType::Key,
            IndexType::Fulltext,
            IndexType::Spatial,
            IndexType::HnswEuclidean,
            IndexType::HnswCosine,
            IndexType::HnswDot,
            IndexType::Object,
            IndexType::Trigram,
            IndexType::Unique => true,
            default => throw new DatabaseException('Unknown index type: '.$type->value.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value.', '.IndexType::Spatial->value.', '.IndexType::Object->value.', '.IndexType::HnswEuclidean->value.', '.IndexType::HnswCosine->value.', '.IndexType::HnswDot->value),
        };

        $keyName = $this->getShortKey("{$this->getNamespace()}_{$this->tenant}_{$collection}_{$id}");
        $tableRaw = $this->getSQLTableRaw($collection);
        $schema = $this->createSchemaBuilder();

        // Build column lists, separating regular columns from raw JSONB path expressions
        $columnNames = [];
        $columnOrders = [];
        $rawExpressions = [];

        foreach ($attributes as $i => $attr) {
            $order = empty($orders[$i]) || $type === IndexType::Fulltext ? '' : $orders[$i];
            $isNestedPath = isset($indexAttributeTypes[$attr]) && \str_contains($attr, '.') && $indexAttributeTypes[$attr] === ColumnType::Object->value;

            if ($isNestedPath) {
                $rawExpressions[] = $this->buildJsonbPath($attr, true).($order ? " {$order}" : '');
            } else {
                $attr = match ($attr) {
                    '$id' => '_uid',
                    '$createdAt' => '_createdAt',
                    '$updatedAt' => '_updatedAt',
                    default => $this->filter($attr),
                };
                $columnNames[] = $attr;
                if (! empty($order)) {
                    $columnOrders[$attr] = $order;
                }
            }
        }

        if ($this->sharedTables && \in_array($type, [IndexType::Key, IndexType::Unique])) {
            \array_unshift($columnNames, '_tenant');
        }

        $unique = $type === IndexType::Unique;

        $method = match ($type) {
            IndexType::Spatial => 'gist',
            IndexType::Object => 'gin',
            IndexType::Trigram => 'gin',
            IndexType::HnswEuclidean,
            IndexType::HnswCosine,
            IndexType::HnswDot => 'hnsw',
            default => '',
        };

        $operatorClass = match ($type) {
            IndexType::HnswEuclidean => 'vector_l2_ops',
            IndexType::HnswCosine => 'vector_cosine_ops',
            IndexType::HnswDot => 'vector_ip_ops',
            IndexType::Trigram => 'gin_trgm_ops',
            default => '',
        };

        $sql = $schema->createIndex(
            $tableRaw,
            $keyName,
            $columnNames,
            unique: $unique,
            method: $method,
            operatorClass: $operatorClass,
            orders: $columnOrders,
            rawColumns: $rawExpressions,
        )->query;


        try {
            return $this->getPDO()->prepare($sql)->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Delete Index
     *
     *
     * @throws Exception
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($id);

        $keyName = $this->getShortKey("{$this->getNamespace()}_{$this->tenant}_{$collection}_{$id}");
        $schemaQualifiedName = $this->getDatabase().'.'.$keyName;

        $schema = $this->createSchemaBuilder();
        $sql = $schema->dropIndex($this->getSQLTableRaw($collection), $schemaQualifiedName)->query;
        // Add IF EXISTS since the schema builder's dropIndex does not include it
        $sql = str_replace('DROP INDEX', 'DROP INDEX IF EXISTS', $sql);

        return $this->execute($this->getPDO()
            ->prepare($sql));
    }

    /**
     * Rename Index
     *
     * @throws Exception
     * @throws PDOException
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $namespace = $this->getNamespace();
        $old = $this->filter($old);
        $new = $this->filter($new);
        $schemaName = $this->getDatabase();
        $oldIndexName = $this->getShortKey("{$namespace}_{$this->tenant}_{$collection}_{$old}");
        $newIndexName = $this->getShortKey("{$namespace}_{$this->tenant}_{$collection}_{$new}");

        $schemaBuilder = $this->createSchemaBuilder();
        $schemaQualifiedOld = $schemaName.'.'.$oldIndexName;
        $sql = $schemaBuilder->renameIndex($this->getSQLTableRaw($collection), $schemaQualifiedOld, $newIndexName)->query;

        return $this->execute($this->getPDO()
            ->prepare($sql));
    }

    /**
     * Create Document
     */
    public function createDocument(Document $collection, Document $document): Document
    {
        try {
            $this->syncWriteHooks();

            $spatialAttributes = $this->getSpatialAttributes($collection);
            $collection = $collection->getId();
            $attributes = $document->getAttributes();
            $attributes['_createdAt'] = $document->getCreatedAt();
            $attributes['_updatedAt'] = $document->getUpdatedAt();
            $attributes['_permissions'] = \json_encode($document->getPermissions());

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

            foreach ($spatialAttributes as $spatialCol) {
                $builder->insertColumnExpression($spatialCol, $this->getSpatialGeomFromText('?'));
            }

            foreach ($attributes as $attr => $value) {
                $column = $this->filter($attr);

                if (\in_array($attr, $spatialAttributes, true)) {
                    if (\is_array($value)) {
                        $value = $this->convertArrayToWKT($value);
                    }
                    $row[$column] = $value;
                } else {
                    if (\is_array($value)) {
                        $value = \json_encode($value);
                    }
                    $row[$column] = $value;
                }
            }

            $row = $this->decorateRow($row, $this->documentMetadata($document));
            $builder->set($row);
            $result = $builder->insert();
            $stmt = $this->executeResult($result, Event::DocumentCreate);

            $this->execute($stmt);
            $lastInsertedId = $this->getPDO()->lastInsertId();
            $document['$sequence'] ??= $lastInsertedId;

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
     *
     * @throws DatabaseException
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
            $row = ['_uid' => $document->getId()];

            foreach ($attributes as $attribute => $value) {
                $column = $this->filter($attribute);

                if (isset($operators[$attribute])) {
                    $op = $operators[$attribute];
                    if ($op instanceof Operator) {
                        $opResult = $this->getOperatorBuilderExpression($column, $op);
                        $builder->setRaw($column, $opResult['expression'], $opResult['bindings']);
                    }
                } elseif (\in_array($attribute, $spatialAttributes, true)) {
                    if (\is_array($value)) {
                        $value = $this->convertArrayToWKT($value);
                    }
                    $builder->setRaw($column, $this->getSpatialGeomFromText('?'), [$value]);
                } else {
                    if (\is_array($value)) {
                        $value = \json_encode($value);
                    }
                    $row[$column] = $value;
                }
            }

            $builder->set($row);
            $builder->filter([BaseQuery::equal('_id', [$document->getSequence()])]);
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

    /**
     * Returns Max Execution Time
     *
     * @throws DatabaseException
     */
    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        if ($milliseconds <= 0) {
            throw new DatabaseException('Timeout must be greater than 0');
        }

        $this->timeout = $milliseconds;
    }

    /**
     * Get the minimum supported datetime value for PostgreSQL.
     *
     * @return DateTime
     */
    public function getMinDateTime(): DateTime
    {
        return new DateTime('-4713-01-01 00:00:00');
    }

    /**
     * Decode a WKB or WKT POINT into a coordinate array [x, y].
     *
     * @param string $wkb The WKB hex or WKT string
     * @return array<float>
     *
     * @throws DatabaseException If the input is invalid.
     */
    public function decodePoint(string $wkb): array
    {
        if (str_starts_with(strtoupper($wkb), 'POINT(')) {
            $start = strpos($wkb, '(') + 1;
            $end = strrpos($wkb, ')');
            $inside = substr($wkb, $start, $end - $start);

            $coords = explode(' ', trim($inside));

            return [(float) $coords[0], (float) $coords[1]];
        }

        $bin = hex2bin($wkb);
        if ($bin === false) {
            throw new DatabaseException('Invalid hex WKB string');
        }

        if (strlen($bin) < 13) { // 1 byte endian + 4 bytes type + 8 bytes for X
            throw new DatabaseException('WKB too short');
        }

        $isLE = ord($bin[0]) === 1;

        // Type (4 bytes)
        $typeBytes = substr($bin, 1, 4);
        if (strlen($typeBytes) !== 4) {
            throw new DatabaseException('Failed to extract type bytes from WKB');
        }

        $typeArr = unpack($isLE ? 'V' : 'N', $typeBytes);
        if ($typeArr === false || ! isset($typeArr[1])) {
            throw new DatabaseException('Failed to unpack type from WKB');
        }
        $type = \is_numeric($typeArr[1]) ? (int) $typeArr[1] : 0;

        // Offset to coordinates (skip SRID if present)
        $offset = 5 + (($type & 0x20000000) ? 4 : 0);

        if (strlen($bin) < $offset + 16) { // 16 bytes for X,Y
            throw new DatabaseException('WKB too short for coordinates');
        }

        $fmt = $isLE ? 'e' : 'E'; // little vs big endian double

        // X coordinate
        $xArr = unpack($fmt, substr($bin, $offset, 8));
        if ($xArr === false || ! isset($xArr[1])) {
            throw new DatabaseException('Failed to unpack X coordinate');
        }
        $x = \is_numeric($xArr[1]) ? (float) $xArr[1] : 0.0;

        // Y coordinate
        $yArr = unpack($fmt, substr($bin, $offset + 8, 8));
        if ($yArr === false || ! isset($yArr[1])) {
            throw new DatabaseException('Failed to unpack Y coordinate');
        }
        $y = \is_numeric($yArr[1]) ? (float) $yArr[1] : 0.0;

        return [$x, $y];
    }

    /**
     * Decode a WKB or WKT LINESTRING into an array of coordinate pairs.
     *
     * @param mixed $wkb The WKB binary or WKT string
     * @return array<array<float>>
     *
     * @throws DatabaseException If the input is invalid.
     */
    public function decodeLinestring(mixed $wkb): array
    {
        $wkb = \is_string($wkb) ? $wkb : '';
        if (str_starts_with(strtoupper($wkb), 'LINESTRING(')) {
            $start = strpos($wkb, '(') + 1;
            $end = strrpos($wkb, ')');
            $inside = substr($wkb, $start, (int) $end - $start);

            $points = explode(',', $inside);

            return array_map(function ($point) {
                $coords = explode(' ', trim($point));

                return [(float) $coords[0], (float) $coords[1]];
            }, $points);
        }

        if (ctype_xdigit($wkb)) {
            $wkb = hex2bin($wkb);
            if ($wkb === false) {
                throw new DatabaseException('Failed to convert hex WKB to binary.');
            }
        }

        if (strlen($wkb) < 9) {
            throw new DatabaseException('WKB too short to be a valid geometry');
        }

        $byteOrder = ord($wkb[0]);
        if ($byteOrder === 0) {
            throw new DatabaseException('Big-endian WKB not supported');
        } elseif ($byteOrder !== 1) {
            throw new DatabaseException('Invalid byte order in WKB');
        }

        // Type + SRID flag
        $typeField = unpack('V', substr($wkb, 1, 4));
        if ($typeField === false) {
            throw new DatabaseException('Failed to unpack the type field from WKB.');
        }

        $typeField = \is_numeric($typeField[1]) ? (int) $typeField[1] : 0;
        $geomType = $typeField & 0xFF;
        $hasSRID = ($typeField & 0x20000000) !== 0;

        if ($geomType !== 2) { // 2 = LINESTRING
            throw new DatabaseException("Not a LINESTRING geometry type, got {$geomType}");
        }

        $offset = 5;
        if ($hasSRID) {
            $offset += 4;
        }

        $numPoints = unpack('V', substr($wkb, $offset, 4));
        if ($numPoints === false) {
            throw new DatabaseException("Failed to unpack number of points at offset {$offset}.");
        }

        $numPoints = \is_numeric($numPoints[1]) ? (int) $numPoints[1] : 0;
        $offset += 4;

        $points = [];
        for ($i = 0; $i < $numPoints; $i++) {
            $x = unpack('e', substr($wkb, $offset, 8));
            if ($x === false) {
                throw new DatabaseException("Failed to unpack X coordinate at offset {$offset}.");
            }

            $x = \is_numeric($x[1]) ? (float) $x[1] : 0.0;

            $offset += 8;

            $y = unpack('e', substr($wkb, $offset, 8));
            if ($y === false) {
                throw new DatabaseException("Failed to unpack Y coordinate at offset {$offset}.");
            }

            $y = \is_numeric($y[1]) ? (float) $y[1] : 0.0;

            $offset += 8;
            $points[] = [$x, $y];
        }

        return $points;
    }

    /**
     * Decode a WKB or WKT POLYGON into an array of rings, each containing coordinate pairs.
     *
     * @param string $wkb The WKB hex or WKT string
     * @return array<array<array<float>>>
     *
     * @throws DatabaseException If the input is invalid.
     */
    public function decodePolygon(string $wkb): array
    {
        // POLYGON((x1,y1),(x2,y2))
        if (str_starts_with($wkb, 'POLYGON((')) {
            $start = strpos($wkb, '((') + 2;
            $end = strrpos($wkb, '))');
            $inside = substr($wkb, $start, $end - $start);

            $rings = explode('),(', $inside);

            return array_map(function ($ring) {
                $points = explode(',', $ring);

                return array_map(function ($point) {
                    $coords = explode(' ', trim($point));

                    return [(float) $coords[0], (float) $coords[1]];
                }, $points);
            }, $rings);
        }

        // Convert hex string to binary if needed
        if (preg_match('/^[0-9a-fA-F]+$/', $wkb)) {
            $wkb = hex2bin($wkb);
            if ($wkb === false) {
                throw new DatabaseException('Invalid hex WKB');
            }
        }

        if (strlen($wkb) < 9) {
            throw new DatabaseException('WKB too short');
        }

        $uInt32 = 'V'; // little-endian 32-bit unsigned
        $uDouble = 'd'; // little-endian double

        $typeInt = unpack($uInt32, substr($wkb, 1, 4));
        if ($typeInt === false) {
            throw new DatabaseException('Failed to unpack type field from WKB.');
        }

        $typeInt = \is_numeric($typeInt[1]) ? (int) $typeInt[1] : 0;
        $hasSrid = ($typeInt & 0x20000000) !== 0;
        $geomType = $typeInt & 0xFF;

        if ($geomType !== 3) { // 3 = POLYGON
            throw new DatabaseException("Not a POLYGON geometry type, got {$geomType}");
        }

        $offset = 5;
        if ($hasSrid) {
            $offset += 4;
        }

        // Number of rings
        $numRings = unpack($uInt32, substr($wkb, $offset, 4));
        if ($numRings === false) {
            throw new DatabaseException('Failed to unpack number of rings from WKB.');
        }

        $numRings = \is_numeric($numRings[1]) ? (int) $numRings[1] : 0;
        $offset += 4;

        $rings = [];
        for ($r = 0; $r < $numRings; $r++) {
            $numPoints = unpack($uInt32, substr($wkb, $offset, 4));
            if ($numPoints === false) {
                throw new DatabaseException('Failed to unpack number of points from WKB.');
            }

            $numPoints = \is_numeric($numPoints[1]) ? (int) $numPoints[1] : 0;
            $offset += 4;
            $points = [];
            for ($i = 0; $i < $numPoints; $i++) {
                $x = unpack($uDouble, substr($wkb, $offset, 8));
                if ($x === false) {
                    throw new DatabaseException('Failed to unpack X coordinate from WKB.');
                }

                $x = \is_numeric($x[1]) ? (float) $x[1] : 0.0;

                $y = unpack($uDouble, substr($wkb, $offset + 8, 8));
                if ($y === false) {
                    throw new DatabaseException('Failed to unpack Y coordinate from WKB.');
                }

                $y = \is_numeric($y[1]) ? (float) $y[1] : 0.0;

                $points[] = [$x, $y];
                $offset += 16;
            }
            $rings[] = $points;
        }

        return $rings; // array of rings, each ring is array of [x,y]
    }

    protected function execute(mixed $stmt): bool
    {
        $pdo = $this->getPDO();

        // Choose the right SET command based on transaction state
        $sql = $this->inTransaction === 0
            ? "SET statement_timeout = '{$this->timeout}ms'"
            : "SET LOCAL statement_timeout = '{$this->timeout}ms'";

        // Apply timeout
        $pdo->exec($sql);

        /** @var PDOStatement|PDOStatementProxy $stmt */
        try {
            return $stmt->execute();
        } finally {
            // Only reset the global timeout when not in a transaction
            if ($this->inTransaction === 0) {
                $pdo->exec('RESET statement_timeout');
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    protected function insertRequiresAlias(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictTenantExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));

        return "CASE WHEN target._tenant = EXCLUDED._tenant THEN EXCLUDED.{$quoted} ELSE target.{$quoted} END";
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));

        return "target.{$quoted} + EXCLUDED.{$quoted}";
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictTenantIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));

        return "CASE WHEN target._tenant = EXCLUDED._tenant THEN target.{$quoted} + EXCLUDED.{$quoted} ELSE target.{$quoted} END";
    }

    /**
     * Get a builder-compatible operator expression for upsert conflict resolution.
     *
     * Overrides the base implementation to use target-prefixed column references
     * so that ON CONFLICT DO UPDATE SET expressions correctly reference the
     * existing row via the target alias.
     *
     * @param  string  $column  The unquoted, filtered column name
     * @param  Operator  $operator  The operator to convert
     * @return array{expression: string, bindings: list<mixed>}
     */
    protected function getOperatorUpsertExpression(string $column, Operator $operator): array
    {
        $bindIndex = 0;
        $fullExpression = $this->getOperatorSQL($column, $operator, $bindIndex, useTargetPrefix: true);

        if ($fullExpression === null) {
            throw new DatabaseException('Operator cannot be expressed in SQL: '.$operator->getMethod()->value);
        }

        // Strip the "quotedColumn = " prefix to get just the RHS expression
        $quotedColumn = $this->quote($column);
        $prefix = $quotedColumn.' = ';
        $expression = $fullExpression;
        if (str_starts_with($expression, $prefix)) {
            $expression = substr($expression, strlen($prefix));
        }

        // Collect the named binding keys and their values in order
        /** @var array<string, mixed> $namedBindings */
        $namedBindings = [];
        $method = $operator->getMethod();
        $values = $operator->getValues();
        $idx = 0;

        switch ($method) {
            case OperatorType::Increment:
            case OperatorType::Decrement:
            case OperatorType::Multiply:
            case OperatorType::Divide:
                $namedBindings["op_{$idx}"] = $values[0] ?? 1;
                $idx++;
                if (isset($values[1])) {
                    $namedBindings["op_{$idx}"] = $values[1];
                    $idx++;
                }
                break;

            case OperatorType::Modulo:
                $namedBindings["op_{$idx}"] = $values[0] ?? 1;
                $idx++;
                break;

            case OperatorType::Power:
                $namedBindings["op_{$idx}"] = $values[0] ?? 1;
                $idx++;
                if (isset($values[1])) {
                    $namedBindings["op_{$idx}"] = $values[1];
                    $idx++;
                }
                break;

            case OperatorType::StringConcat:
                $namedBindings["op_{$idx}"] = $values[0] ?? '';
                $idx++;
                break;

            case OperatorType::StringReplace:
                $namedBindings["op_{$idx}"] = $values[0] ?? '';
                $idx++;
                $namedBindings["op_{$idx}"] = $values[1] ?? '';
                $idx++;
                break;

            case OperatorType::Toggle:
                // No bindings
                break;

            case OperatorType::DateAddDays:
            case OperatorType::DateSubDays:
                $namedBindings["op_{$idx}"] = $values[0] ?? 0;
                $idx++;
                break;

            case OperatorType::DateSetNow:
                // No bindings
                break;

            case OperatorType::ArrayAppend:
            case OperatorType::ArrayPrepend:
                $namedBindings["op_{$idx}"] = json_encode($values);
                $idx++;
                break;

            case OperatorType::ArrayRemove:
                $value = $values[0] ?? null;
                $namedBindings["op_{$idx}"] = json_encode($value);
                $idx++;
                break;

            case OperatorType::ArrayUnique:
                // No bindings
                break;

            case OperatorType::ArrayInsert:
                $namedBindings["op_{$idx}"] = $values[0] ?? 0;
                $idx++;
                $namedBindings["op_{$idx}"] = json_encode($values[1] ?? null);
                $idx++;
                break;

            case OperatorType::ArrayIntersect:
            case OperatorType::ArrayDiff:
                $namedBindings["op_{$idx}"] = json_encode($values);
                $idx++;
                break;

            case OperatorType::ArrayFilter:
                $condition = $values[0] ?? 'equal';
                $filterValue = $values[1] ?? null;
                $namedBindings["op_{$idx}"] = $condition;
                $idx++;
                $namedBindings["op_{$idx}"] = $filterValue !== null ? json_encode($filterValue) : null;
                $idx++;
                break;
        }

        // Replace each named binding occurrence with ? and collect positional bindings
        $positionalBindings = [];
        $keys = array_keys($namedBindings);
        usort($keys, fn ($a, $b) => strlen($b) - strlen($a));

        $replacements = [];
        foreach ($keys as $key) {
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
            $positionalBindings[] = $namedBindings[$r['key']];
        }

        return ['expression' => $result, 'bindings' => $positionalBindings];
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
        $geomArray = \is_array($distanceParams[0]) ? $distanceParams[0] : [];
        $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($geomArray);
        $binds[":{$placeholder}_1"] = $distanceParams[1];

        $meters = isset($distanceParams[2]) && $distanceParams[2] === true;

        $operator = match ($query->getMethod()) {
            Method::DistanceEqual => '=',
            Method::DistanceNotEqual => '!=',
            Method::DistanceGreaterThan => '>',
            Method::DistanceLessThan => '<',
            default => throw new DatabaseException('Unknown spatial query method: '.$query->getMethod()->value),
        };

        if ($meters) {
            $attr = "({$alias}.{$attribute}::geography)";
            $geom = 'ST_SetSRID('.$this->getSpatialGeomFromText(":{$placeholder}_0", null).', '.Database::DEFAULT_SRID.')::geography';

            return "ST_Distance({$attr}, {$geom}) {$operator} :{$placeholder}_1";
        }

        // Without meters, use the original SRID (e.g., 4326)
        return "ST_Distance({$alias}.{$attribute}, ".$this->getSpatialGeomFromText(":{$placeholder}_0").") {$operator} :{$placeholder}_1";
    }

    /**
     * Handle spatial queries
     *
     * @param  array<string, mixed>  $binds
     */
    protected function handleSpatialQueries(Query $query, array &$binds, string $attribute, string $type, string $alias, string $placeholder): string
    {
        $spatialGeomRaw = $query->getValues()[0];
        $binds[":{$placeholder}_0"] = $this->convertArrayToWKT(\is_array($spatialGeomRaw) ? $spatialGeomRaw : []);
        $geom = $this->getSpatialGeomFromText(":{$placeholder}_0");

        return match ($query->getMethod()) {
            Method::Crosses => "ST_Crosses({$alias}.{$attribute}, {$geom})",
            Method::NotCrosses => "NOT ST_Crosses({$alias}.{$attribute}, {$geom})",
            Method::DistanceEqual,
            Method::DistanceNotEqual,
            Method::DistanceGreaterThan,
            Method::DistanceLessThan => $this->handleDistanceSpatialQueries($query, $binds, $attribute, $type, $alias, $placeholder),
            Method::Equal => "ST_Equals({$alias}.{$attribute}, {$geom})",
            Method::NotEqual => "NOT ST_Equals({$alias}.{$attribute}, {$geom})",
            Method::Intersects => "ST_Intersects({$alias}.{$attribute}, {$geom})",
            Method::NotIntersects => "NOT ST_Intersects({$alias}.{$attribute}, {$geom})",
            Method::Overlaps => "ST_Overlaps({$alias}.{$attribute}, {$geom})",
            Method::NotOverlaps => "NOT ST_Overlaps({$alias}.{$attribute}, {$geom})",
            Method::Touches => "ST_Touches({$alias}.{$attribute}, {$geom})",
            Method::NotTouches => "NOT ST_Touches({$alias}.{$attribute}, {$geom})",
            // using st_cover instead of contains to match the boundary matching behaviour of the mariadb st_contains
            // postgis st_contains excludes matching the boundary
            Method::Contains => "ST_Covers({$alias}.{$attribute}, {$geom})",
            Method::NotContains => "NOT ST_Covers({$alias}.{$attribute}, {$geom})",
            default => throw new DatabaseException('Unknown spatial query method: '.$query->getMethod()->value),
        };
    }

    /**
     * Handle JSONB queries
     *
     * @param  array<string, mixed>  $binds
     */
    protected function handleObjectQueries(Query $query, array &$binds, string $attribute, string $alias, string $placeholder): string
    {
        switch ($query->getMethod()) {
            case Method::Equal:
            case Method::NotEqual:
                $isNot = $query->getMethod() === Method::NotEqual;
                $conditions = [];
                foreach ($query->getValues() as $key => $value) {
                    $binds[":{$placeholder}_{$key}"] = json_encode($value);
                    $fragment = "{$alias}.{$attribute} @> :{$placeholder}_{$key}::jsonb";
                    $conditions[] = $isNot ? 'NOT ('.$fragment.')' : $fragment;
                }
                $separator = $isNot ? ' AND ' : ' OR ';

                return empty($conditions) ? '' : '('.implode($separator, $conditions).')';

            case Method::Contains:
            case Method::ContainsAny:
            case Method::ContainsAll:
            case Method::NotContains:
                $isNot = $query->getMethod() === Method::NotContains;
                $conditions = [];
                foreach ($query->getValues() as $key => $value) {
                    if (\is_array($value) && count($value) === 1) {
                        $jsonKey = array_key_first($value);
                        $jsonValue = $value[$jsonKey];

                        // If scalar (e.g. "skills" => "typescript"),
                        // wrap it to express array containment: {"skills": ["typescript"]}
                        // If it's already an object/associative array (e.g. "config" => ["lang" => "en"]),
                        // keep as-is to express object containment.
                        if (! \is_array($jsonValue)) {
                            $value[$jsonKey] = [$jsonValue];
                        }
                    }
                    $binds[":{$placeholder}_{$key}"] = json_encode($value);
                    $fragment = "{$alias}.{$attribute} @> :{$placeholder}_{$key}::jsonb";
                    $conditions[] = $isNot ? 'NOT ('.$fragment.')' : $fragment;
                }
                $separator = $isNot ? ' AND ' : ' OR ';

                return empty($conditions) ? '' : '('.implode($separator, $conditions).')';

            default:
                throw new DatabaseException('Query method '.$query->getMethod()->value.' not supported for object attributes');
        }
    }

    /**
     * Get SQL Condition
     *
     * @param  array<string, mixed>  $binds
     *
     * @throws Exception
     */
    protected function getSQLCondition(Query $query, array &$binds): string
    {
        $query->setAttribute($this->getInternalKeyForAttribute($query->getAttribute()));
        $isNestedObjectAttribute = $query->isObjectAttribute() && \str_contains($query->getAttribute(), '.');
        if ($isNestedObjectAttribute) {
            $attribute = $this->buildJsonbPath($query->getAttribute());
        } else {
            $attribute = $this->filter($query->getAttribute());
            $attribute = $this->quote($attribute);
        }

        $alias = $this->quote(Query::DEFAULT_ALIAS);
        $placeholder = ID::unique();

        $operator = null;

        if ($query->isSpatialAttribute()) {
            return $this->handleSpatialQueries($query, $binds, $attribute, $query->getAttributeType(), $alias, $placeholder);
        }

        if ($query->isObjectAttribute() && ! $isNestedObjectAttribute) {
            return $this->handleObjectQueries($query, $binds, $attribute, $alias, $placeholder);
        }

        switch ($query->getMethod()) {
            case Method::Or:
            case Method::And:
                $conditions = [];
                /** @var iterable<Query> $nestedQueries */
                $nestedQueries = $query->getValue();
                foreach ($nestedQueries as $q) {
                    $conditions[] = $this->getSQLCondition($q, $binds);
                }

                $method = strtoupper($query->getMethod()->value);

                return empty($conditions) ? '' : ' '.$method.' ('.implode(' AND ', $conditions).')';

            case Method::Search:
                $searchVal = $query->getValue();
                $binds[":{$placeholder}_0"] = $this->getFulltextValue(\is_string($searchVal) ? $searchVal : '');

                return "to_tsvector(regexp_replace({$attribute}, '[^\w]+',' ','g')) @@ websearch_to_tsquery(:{$placeholder}_0)";

            case Method::NotSearch:
                $notSearchVal = $query->getValue();
                $binds[":{$placeholder}_0"] = $this->getFulltextValue(\is_string($notSearchVal) ? $notSearchVal : '');

                return "NOT (to_tsvector(regexp_replace({$attribute}, '[^\w]+',' ','g')) @@ websearch_to_tsquery(:{$placeholder}_0))";

            case Method::VectorDot:
            case Method::VectorCosine:
            case Method::VectorEuclidean:
                return ''; // Handled in ORDER BY clause

            case Method::Between:
                $binds[":{$placeholder}_0"] = $query->getValues()[0];
                $binds[":{$placeholder}_1"] = $query->getValues()[1];

                return "{$alias}.{$attribute} BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Method::NotBetween:
                $binds[":{$placeholder}_0"] = $query->getValues()[0];
                $binds[":{$placeholder}_1"] = $query->getValues()[1];

                return "{$alias}.{$attribute} NOT BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Method::IsNull:
            case Method::IsNotNull:
                return "{$alias}.{$attribute} {$this->getSQLOperator($query->getMethod())}";

            case Method::ContainsAll:
                if ($query->onArray()) {
                    // @> checks the array contains ALL specified values
                    $binds[":{$placeholder}_0"] = \json_encode($query->getValues());

                    return "{$alias}.{$attribute} @> :{$placeholder}_0::jsonb";
                }
                // no break
            case Method::Contains:
            case Method::ContainsAny:
            case Method::NotContains:
                if ($query->onArray()) {
                    $operator = '@>';
                }

                // no break
            default:
                $conditions = [];
                $operator = $operator ?? $this->getSQLOperator($query->getMethod());
                $isNotQuery = in_array($query->getMethod(), [
                    Method::NotStartsWith,
                    Method::NotEndsWith,
                    Method::NotContains,
                ]);

                foreach ($query->getValues() as $key => $value) {
                    $strValue = \is_string($value) ? $value : '';
                    $value = match ($query->getMethod()) {
                        Method::StartsWith => $this->escapeWildcards($strValue).'%',
                        Method::NotStartsWith => $this->escapeWildcards($strValue).'%',
                        Method::EndsWith => '%'.$this->escapeWildcards($strValue),
                        Method::NotEndsWith => '%'.$this->escapeWildcards($strValue),
                        Method::Contains, Method::ContainsAny => ($query->onArray()) ? \json_encode($value) : '%'.$this->escapeWildcards($strValue).'%',
                        Method::NotContains => ($query->onArray()) ? \json_encode($value) : '%'.$this->escapeWildcards($strValue).'%',
                        default => $value
                    };

                    $binds[":{$placeholder}_{$key}"] = $value;

                    if ($isNotQuery && $query->onArray()) {
                        // For array NOT queries, wrap the entire condition in NOT()
                        $conditions[] = "NOT ({$alias}.{$attribute} {$operator} :{$placeholder}_{$key})";
                    } elseif ($isNotQuery && ! $query->onArray()) {
                        $conditions[] = "{$alias}.{$attribute} NOT {$operator} :{$placeholder}_{$key}";
                    } else {
                        $conditions[] = "{$alias}.{$attribute} {$operator} :{$placeholder}_{$key}";
                    }
                }

                $separator = $isNotQuery ? ' AND ' : ' OR ';

                return empty($conditions) ? '' : '('.implode($separator, $conditions).')';
        }
    }

    /**
     * Get SQL Type
     */
    protected function createBuilder(): SQLBuilder
    {
        return new PostgreSQLBuilder();
    }

    protected function createSchemaBuilder(): PostgreSQLSchema
    {
        return new PostgreSQLSchema();
    }

    protected function getSQLType(ColumnType $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        if ($array === true) {
            return 'JSONB';
        }

        return match ($type) {
            ColumnType::Id => 'BIGINT',
            ColumnType::String => $size > $this->getMaxVarcharLength() ? 'TEXT' : "VARCHAR({$size})",
            ColumnType::Varchar => "VARCHAR({$size})",
            ColumnType::Text,
            ColumnType::MediumText,
            ColumnType::LongText => 'TEXT',
            ColumnType::Integer => $size >= 8 ? 'BIGINT' : 'INTEGER',
            ColumnType::Float, ColumnType::Double => 'DOUBLE PRECISION',
            ColumnType::Boolean => 'BOOLEAN',
            ColumnType::Relationship => 'VARCHAR(255)',
            ColumnType::Datetime => 'TIMESTAMP(3)',
            ColumnType::Object => 'JSONB',
            ColumnType::Point => 'GEOMETRY(POINT,'.Database::DEFAULT_SRID.')',
            ColumnType::Linestring => 'GEOMETRY(LINESTRING,'.Database::DEFAULT_SRID.')',
            ColumnType::Polygon => 'GEOMETRY(POLYGON,'.Database::DEFAULT_SRID.')',
            ColumnType::Vector => "VECTOR({$size})",
            default => throw new DatabaseException('Unknown Type: '.$type->value.'. Must be one of '.ColumnType::String->value.', '.ColumnType::Varchar->value.', '.ColumnType::Text->value.', '.ColumnType::MediumText->value.', '.ColumnType::LongText->value.', '.ColumnType::Integer->value.', '.ColumnType::Double->value.', '.ColumnType::Boolean->value.', '.ColumnType::Datetime->value.', '.ColumnType::Relationship->value.', '.ColumnType::Object->value.', '.ColumnType::Point->value.', '.ColumnType::Linestring->value.', '.ColumnType::Polygon->value),
        };
    }

    /**
     * Get SQL schema
     */
    protected function getSQLSchema(): string
    {
        if (! $this->supports(Capability::Schemas)) {
            return '';
        }

        return "\"{$this->getDatabase()}\".";
    }

    /**
     * Get PDO Type
     *
     *
     * @throws DatabaseException
     */
    protected function getPDOType(mixed $value): int
    {
        return match (\gettype($value)) {
            'string', 'double' => PDO::PARAM_STR,
            'boolean' => PDO::PARAM_BOOL,
            'integer' => PDO::PARAM_INT,
            'NULL' => PDO::PARAM_NULL,
            default => throw new DatabaseException('Unknown PDO Type for '.\gettype($value)),
        };
    }

    /**
     * Get vector distance calculation for ORDER BY clause
     *
     * @param  array<string, mixed>  $binds
     *
     * @throws DatabaseException
     */
    protected function getVectorDistanceOrder(Query $query, array &$binds, string $alias): ?string
    {
        $query->setAttribute($this->getInternalKeyForAttribute($query->getAttribute()));

        $attribute = $this->filter($query->getAttribute());
        $attribute = $this->quote($attribute);
        $alias = $this->quote($alias);
        $placeholder = ID::unique();

        $values = $query->getValues();
        $vectorArrayRaw = $values[0] ?? [];
        $vectorArray = \is_array($vectorArrayRaw) ? $vectorArrayRaw : [];
        $vector = \json_encode(\array_map(fn (mixed $v): float => \is_numeric($v) ? (float) $v : 0.0, $vectorArray));
        $binds[":vector_{$placeholder}"] = $vector;

        return match ($query->getMethod()) {
            Method::VectorDot => "({$alias}.{$attribute} <#> :vector_{$placeholder}::vector)",
            Method::VectorCosine => "({$alias}.{$attribute} <=> :vector_{$placeholder}::vector)",
            Method::VectorEuclidean => "({$alias}.{$attribute} <-> :vector_{$placeholder}::vector)",
            default => null,
        };
    }

    /**
     * {@inheritDoc}
     */
    protected function getVectorOrderRaw(Query $query, string $alias): ?array
    {
        $query->setAttribute($this->getInternalKeyForAttribute($query->getAttribute()));

        $attribute = $this->filter($query->getAttribute());
        $attribute = $this->quote($attribute);
        $quotedAlias = $this->quote($alias);

        $values = $query->getValues();
        $vectorArrayRaw2 = $values[0] ?? [];
        $vectorArray2 = \is_array($vectorArrayRaw2) ? $vectorArrayRaw2 : [];
        $vector = \json_encode(\array_map(fn (mixed $v): float => \is_numeric($v) ? (float) $v : 0.0, $vectorArray2));

        $expression = match ($query->getMethod()) {
            Method::VectorDot => "({$quotedAlias}.{$attribute} <#> ?::vector)",
            Method::VectorCosine => "({$quotedAlias}.{$attribute} <=> ?::vector)",
            Method::VectorEuclidean => "({$quotedAlias}.{$attribute} <-> ?::vector)",
            default => null,
        };

        if ($expression === null) {
            return null;
        }

        return ['expression' => $expression, 'bindings' => [$vector]];
    }

    /**
     * Size of POINT spatial type
     */
    protected function getMaxPointSize(): int
    {
        // https://stackoverflow.com/questions/30455025/size-of-data-type-geographypoint-4326-in-postgis
        return 32;
    }

    protected function getSearchRelevanceRaw(Query $query, string $alias): ?array
    {
        $attribute = $this->filter($this->getInternalKeyForAttribute($query->getAttribute()));
        $attribute = $this->quote($attribute);
        $searchVal = $query->getValue();
        $term = $this->getFulltextValue(\is_string($searchVal) ? $searchVal : '');

        return [
            'expression' => "ts_rank(to_tsvector(regexp_replace({$attribute}, '[^\w]+',' ','g')), websearch_to_tsquery(?)) AS \"_relevance\"",
            'order' => '"_relevance" DESC',
            'bindings' => [$term],
        ];
    }

    public function getSupportForSchemaIndexes(): bool
    {
        return false;
    }

    protected function processException(PDOException $e): Exception
    {
        // Timeout
        if ($e->getCode() === '57014' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Duplicate table
        if ($e->getCode() === '42P07' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new DuplicateException('Collection already exists', $e->getCode(), $e);
        }

        // Duplicate column
        if ($e->getCode() === '42701' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new DuplicateException('Attribute already exists', $e->getCode(), $e);
        }

        // Duplicate row
        if ($e->getCode() === '23505' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            $message = $e->getMessage();
            if (! \str_contains($message, '_uid')) {
                return new DuplicateException('Document with the requested unique attributes already exists', $e->getCode(), $e);
            }

            return new DuplicateException('Document already exists', $e->getCode(), $e);
        }

        // Data is too big for column resize
        if ($e->getCode() === '22001' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new TruncateException('Resize would result in data truncation', $e->getCode(), $e);
        }

        // Numeric value out of range (overflow/underflow from operators)
        if ($e->getCode() === '22003' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new LimitException('Numeric value out of range', $e->getCode(), $e);
        }

        // Datetime field overflow
        if ($e->getCode() === '22008' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new LimitException('Datetime field overflow', $e->getCode(), $e);
        }

        // Unknown table
        if ($e->getCode() === '42P01' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new NotFoundException('Collection not found', $e->getCode(), $e);
        }

        // Unknown column
        if ($e->getCode() === '42703' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new NotFoundException('Attribute not found', $e->getCode(), $e);
        }

        return $e;
    }

    protected function quote(string $string): string
    {
        return "\"{$string}\"";
    }

    protected function getIdentifierQuoteChar(): string
    {
        return '"';
    }

    protected function getInsertSuffix(string $table): string
    {
        if (! $this->skipDuplicates) {
            return '';
        }

        $conflictTarget = $this->sharedTables ? '("_uid", "_tenant")' : '("_uid")';

        return "ON CONFLICT {$conflictTarget} DO NOTHING";
    }

    protected function getInsertPermissionsSuffix(): string
    {
        if (! $this->skipDuplicates) {
            return '';
        }

        $conflictTarget = $this->sharedTables
            ? '("_type", "_permission", "_document", "_tenant")'
            : '("_type", "_permission", "_document")';

        return "ON CONFLICT {$conflictTarget} DO NOTHING";
    }


    /**
     * Get SQL expression for operator
     */
    protected function getOperatorSQL(string $column, Operator $operator, int &$bindIndex, bool $useTargetPrefix = false): ?string
    {
        $quotedColumn = $this->quote($column);
        $columnRef = $useTargetPrefix ? "target.{$quotedColumn}" : $quotedColumn;
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
                        WHEN COALESCE({$columnRef}, 0) >= CAST(:$maxKey AS NUMERIC) THEN CAST(:$maxKey AS NUMERIC)
                        WHEN COALESCE({$columnRef}, 0) > CAST(:$maxKey AS NUMERIC) - CAST(:$bindKey AS NUMERIC) THEN CAST(:$maxKey AS NUMERIC)
                        ELSE COALESCE({$columnRef}, 0) + CAST(:$bindKey AS NUMERIC)
                    END";
                }

                return "{$quotedColumn} = COALESCE({$columnRef}, 0) + :$bindKey";

            case OperatorType::Decrement:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $minKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$columnRef}, 0) <= CAST(:$minKey AS NUMERIC) THEN CAST(:$minKey AS NUMERIC)
                        WHEN COALESCE({$columnRef}, 0) < CAST(:$minKey AS NUMERIC) + CAST(:$bindKey AS NUMERIC) THEN CAST(:$minKey AS NUMERIC)
                        ELSE COALESCE({$columnRef}, 0) - CAST(:$bindKey AS NUMERIC)
                    END";
                }

                return "{$quotedColumn} = COALESCE({$columnRef}, 0) - :$bindKey";

            case OperatorType::Multiply:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$columnRef}, 0) >= CAST(:$maxKey AS NUMERIC) THEN CAST(:$maxKey AS NUMERIC)
                        WHEN CAST(:$bindKey AS NUMERIC) > 0 AND COALESCE({$columnRef}, 0) > CAST(:$maxKey AS NUMERIC) / CAST(:$bindKey AS NUMERIC) THEN CAST(:$maxKey AS NUMERIC)
                        WHEN CAST(:$bindKey AS NUMERIC) < 0 AND COALESCE({$columnRef}, 0) < CAST(:$maxKey AS NUMERIC) / CAST(:$bindKey AS NUMERIC) THEN CAST(:$maxKey AS NUMERIC)
                        ELSE COALESCE({$columnRef}, 0) * CAST(:$bindKey AS NUMERIC)
                    END";
                }

                return "{$quotedColumn} = COALESCE({$columnRef}, 0) * :$bindKey";

            case OperatorType::Divide:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $minKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN CAST(:$bindKey AS NUMERIC) != 0 AND COALESCE({$columnRef}, 0) / CAST(:$bindKey AS NUMERIC) <= CAST(:$minKey AS NUMERIC) THEN CAST(:$minKey AS NUMERIC)
                        ELSE COALESCE({$columnRef}, 0) / CAST(:$bindKey AS NUMERIC)
                    END";
                }

                return "{$quotedColumn} = COALESCE({$columnRef}, 0) / :$bindKey";

            case OperatorType::Modulo:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = MOD(COALESCE({$columnRef}::numeric, 0), :$bindKey::numeric)";

            case OperatorType::Power:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$columnRef}, 0) >= :$maxKey THEN :$maxKey
                        WHEN COALESCE({$columnRef}, 0) <= 1 THEN COALESCE({$columnRef}, 0)
                        WHEN :$bindKey * LN(COALESCE({$columnRef}, 1)) > LN(:$maxKey) THEN :$maxKey
                        ELSE POWER(COALESCE({$columnRef}, 0), :$bindKey)
                    END";
                }

                return "{$quotedColumn} = POWER(COALESCE({$columnRef}, 0), :$bindKey)";

                // String operators
            case OperatorType::StringConcat:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = CONCAT(COALESCE({$columnRef}, ''), :$bindKey)";

            case OperatorType::StringReplace:
                $searchKey = "op_{$bindIndex}";
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = REPLACE(COALESCE({$columnRef}, ''), :$searchKey, :$replaceKey)";

                // Boolean operators
            case OperatorType::Toggle:
                return "{$quotedColumn} = NOT COALESCE({$columnRef}, FALSE)";

                // Array operators
            case OperatorType::ArrayAppend:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE({$columnRef}, '[]'::jsonb) || :$bindKey::jsonb";

            case OperatorType::ArrayPrepend:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = :$bindKey::jsonb || COALESCE({$columnRef}, '[]'::jsonb)";

            case OperatorType::ArrayUnique:
                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(DISTINCT value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                ), '[]'::jsonb)";

            case OperatorType::ArrayRemove:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                    WHERE value != :$bindKey::jsonb
                ), '[]'::jsonb)";

            case OperatorType::ArrayInsert:
                $indexKey = "op_{$bindIndex}";
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = (
                    SELECT jsonb_agg(value ORDER BY idx)
                    FROM (
                        SELECT value, idx
                        FROM jsonb_array_elements({$columnRef}) WITH ORDINALITY AS t(value, idx)
                        WHERE idx - 1 < :$indexKey
                        UNION ALL
                        SELECT :$valueKey::jsonb AS value, :$indexKey + 1 AS idx
                        UNION ALL
                        SELECT value, idx + 1
                        FROM jsonb_array_elements({$columnRef}) WITH ORDINALITY AS t(value, idx)
                        WHERE idx - 1 >= :$indexKey
                    ) AS combined
                )";

            case OperatorType::ArrayIntersect:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                    WHERE value IN (SELECT jsonb_array_elements(:$bindKey::jsonb))
                ), '[]'::jsonb)";

            case OperatorType::ArrayDiff:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                    WHERE value NOT IN (SELECT jsonb_array_elements(:$bindKey::jsonb))
                ), '[]'::jsonb)";

            case OperatorType::ArrayFilter:
                $conditionKey = "op_{$bindIndex}";
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                    WHERE CASE :$conditionKey
                        WHEN 'equal' THEN value = :$valueKey::jsonb
                        WHEN 'notEqual' THEN value != :$valueKey::jsonb
                        WHEN 'greaterThan' THEN (value::text)::numeric > trim(both '\"' from :$valueKey::text)::numeric
                        WHEN 'greaterThanEqual' THEN (value::text)::numeric >= trim(both '\"' from :$valueKey::text)::numeric
                        WHEN 'lessThan' THEN (value::text)::numeric < trim(both '\"' from :$valueKey::text)::numeric
                        WHEN 'lessThanEqual' THEN (value::text)::numeric <= trim(both '\"' from :$valueKey::text)::numeric
                        WHEN 'isNull' THEN value = 'null'::jsonb
                        WHEN 'isNotNull' THEN value != 'null'::jsonb
                        ELSE TRUE
                    END
                ), '[]'::jsonb)";

                // Date operators
            case OperatorType::DateAddDays:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = {$columnRef} + (:$bindKey || ' days')::INTERVAL";

            case OperatorType::DateSubDays:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = {$columnRef} - (:$bindKey || ' days')::INTERVAL";

            case OperatorType::DateSetNow:
                return "{$quotedColumn} = NOW()";

            default:
                throw new OperatorException('Invalid operator');
        }
    }

    /**
     * Bind operator parameters to statement
     * Override to handle PostgreSQL-specific JSON binding
     */
    protected function bindOperatorParams(PDOStatement|PDOStatementProxy $stmt, Operator $operator, int &$bindIndex): void
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            case OperatorType::ArrayAppend:
            case OperatorType::ArrayPrepend:
                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $arrayValue, PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayRemove:
                $value = $values[0] ?? null;
                $bindKey = "op_{$bindIndex}";
                // Always JSON encode for PostgreSQL jsonb comparison
                $stmt->bindValue(':'.$bindKey, json_encode($value), PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayIntersect:
            case OperatorType::ArrayDiff:
                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $arrayValue, PDO::PARAM_STR);
                $bindIndex++;
                break;

            default:
                // Use parent implementation for other operators
                parent::bindOperatorParams($stmt, $operator, $bindIndex);
                break;
        }
    }

    protected function getFulltextValue(string $value): string
    {
        $exact = str_ends_with($value, '"') && str_starts_with($value, '"');
        $value = str_replace(['@', '+', '-', '*', '.', "'", '"'], ' ', $value);
        $value = preg_replace('/\s+/', ' ', $value); // Remove multiple whitespaces
        $value = trim($value ?? '');

        if (! $exact) {
            $value = str_replace(' ', ' or ', $value);
        }

        return "'".$value."'";
    }

    protected function getOperatorBuilderExpression(string $column, Operator $operator): array
    {
        if ($operator->getMethod() === OperatorType::ArrayRemove) {
            $result = parent::getOperatorBuilderExpression($column, $operator);
            $values = $operator->getValues();
            $value = $values[0] ?? null;
            if (! is_array($value)) {
                $result['bindings'] = [json_encode($value)];
            }

            return $result;
        }

        return parent::getOperatorBuilderExpression($column, $operator);
    }

    /**
     * Encode array
     *
     *
     * @return array<string>
     */
    protected function encodeArray(string $value): array
    {
        $string = substr($value, 1, -1);
        if (empty($string)) {
            return [];
        } else {
            return explode(',', $string);
        }
    }

    /**
     * Decode array
     *
     * @param  array<string>  $value
     */
    protected function decodeArray(array $value): string
    {
        if (empty($value)) {
            return '{}';
        }

        foreach ($value as &$item) {
            $item = '"'.str_replace(['"', '(', ')'], ['\"', '\(', '\)'], $item).'"';
        }

        return '{'.implode(',', $value).'}';
    }

    /**
     * Ensure index key length stays within PostgreSQL's 63 character limit.
     */
    protected function getShortKey(string $key): string
    {
        if (\strlen($key) <= self::MAX_IDENTIFIER_NAME) {
            return $key;
        }

        $suffix = '';
        $separatorPosition = strrpos($key, '_');
        if ($separatorPosition !== false) {
            $suffix = substr($key, $separatorPosition + 1);
        }

        $hash = md5($key);

        if ($suffix !== '') {
            $hashedKey = "{$hash}_{$suffix}";
            if (\strlen($hashedKey) <= self::MAX_IDENTIFIER_NAME) {
                return $hashedKey;
            }
        }

        return substr($hash, 0, self::MAX_IDENTIFIER_NAME);
    }

    protected function getSQLTable(string $name): string
    {
        $table = "{$this->getNamespace()}_{$this->filter($name)}";
        $table = $this->getShortKey($table);

        return "{$this->quote($this->getDatabase())}.{$this->quote($table)}";
    }

    protected function buildJsonbPath(string $path, bool $asText = false): string
    {
        $parts = \explode('.', $path);

        foreach ($parts as $part) {
            if (! preg_match('/^[a-zA-Z0-9_\-]+$/', $part)) {
                throw new DatabaseException('Invalid JSON key '.$part);
            }
        }
        if (\count($parts) === 1) {
            $column = $this->filter($parts[0]);

            return $this->quote($column);
        }

        $baseColumn = $this->quote($this->filter(\array_shift($parts)));
        $lastKey = \array_pop($parts);

        $chain = $baseColumn;
        foreach ($parts as $key) {
            $chain .= "->'{$key}'";
        }

        $result = "{$chain}->>'{$lastKey}'";

        return $asText ? "(({$result})::text)" : $result;
    }
}
