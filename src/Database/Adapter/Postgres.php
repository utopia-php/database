<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Swoole\Database\PDOStatementProxy;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
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
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * Differences between MariaDB and Postgres
 *
 * 1. Need to use CASCADE to DROP schema
 * 2. Quotes are different ` vs "
 * 3. DATETIME is TIMESTAMP
 * 4. Full-text search is different - to_tsvector() and to_tsquery()
 */
class Postgres extends SQL implements Feature\Timeouts
{
    public function capabilities(): array
    {
        $remove = [
            Capability::SchemaAttributes,
        ];

        return array_values(array_filter(
            array_merge(parent::capabilities(), [
                Capability::Vectors,
                Capability::Objects,
                Capability::SpatialIndexNull,
                Capability::MultiDimensionDistance,
                Capability::TrigramIndex,
                Capability::POSIX,
                Capability::ObjectIndexes,
                Capability::Timeouts,
            ]),
            fn (Capability $c) => ! in_array($c, $remove, true)
        ));
    }

    public const MAX_IDENTIFIER_NAME = 63;

    /**
     * Override to use lowercase catalog names for Postgres case sensitivity.
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        $database = $this->filter($database);

        if (! \is_null($collection)) {
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
        } catch (\PDOException $e) {
            throw $this->processException($e);
        }

        return ! empty($document);
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

    protected function execute(mixed $stmt): bool
    {
        $pdo = $this->getPDO();

        // Choose the right SET command based on transaction state
        $sql = $this->inTransaction === 0
            ? "SET statement_timeout = '{$this->timeout}ms'"
            : "SET LOCAL statement_timeout = '{$this->timeout}ms'";

        // Apply timeout
        $pdo->exec($sql);

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
     * Returns Max Execution Time
     *
     * @throws DatabaseException
     */
    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        if ($milliseconds <= 0) {
            throw new DatabaseException('Timeout must be greater than 0');
        }

        $this->timeout = $milliseconds;
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
        $sql = $this->trigger(Database::EVENT_DATABASE_CREATE, $sql);

        $dbCreation = $this->getPDO()
            ->prepare($sql)
            ->execute();

        // Enable extensions — wrap in try-catch to handle concurrent creation race conditions
        foreach (['postgis', 'vector', 'pg_trgm'] as $ext) {
            try {
                $this->getPDO()->prepare($schema->createExtension($ext)->query)->execute();
            } catch (\PDOException) {
                // Extension may already exist due to concurrent worker
            }
        }

        try {
            $collation = $schema->createCollation('utf8_ci_ai', [
                'provider' => 'icu',
                'locale' => 'und-u-ks-level1',
            ], deterministic: false);
            $this->getPDO()->prepare($collation->query)->execute();
        } catch (\PDOException) {
            // Collation may already exist due to concurrent worker
        }

        return $dbCreation;
    }

    /**
     * Delete Database
     *
     * @throws Exception
     * @throws PDOException
     */
    public function delete(string $name): bool
    {
        $name = $this->filter($name);

        $schema = $this->createSchemaBuilder();
        $sql = $schema->dropDatabase($name)->query;
        $sql = $this->trigger(Database::EVENT_DATABASE_DELETE, $sql);

        return $this->getPDO()->prepare($sql)->execute();
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
        $collectionResult = $schema->create($tableRaw, function (\Utopia\Query\Schema\Blueprint $table) use ($attributes) {
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
                    $attribute->type->value,
                    $attribute->size,
                    $attribute->signed,
                    $attribute->array,
                    $attribute->required
                );
            }

            $table->text('_permissions')->nullable()->default(null);
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
        $collectionSql = $this->trigger(Database::EVENT_COLLECTION_CREATE, $collectionSql);

        // Build permissions table using schema builder
        $permsResult = $schema->create($permsTableRaw, function (\Utopia\Query\Schema\Blueprint $table) {
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
        $permsSql = $this->trigger(Database::EVENT_COLLECTION_CREATE, $permsSql);

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
                            $indexAttributesWithType[$indexAttribute] = $attribute->type;
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
            $size = $collectionSize->fetchColumn() + $permissionsSize->fetchColumn();
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
            $size = $collectionSize->fetchColumn() + $permissionsSize->fetchColumn();
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }

        return $size;
    }

    /**
     * Delete Collection
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        $schema = $this->createSchemaBuilder();
        $mainResult = $schema->drop($this->getSQLTableRaw($id));
        $permsResult = $schema->drop($this->getSQLTableRaw($id.'_perms'));

        $sql = $mainResult->query.'; '.$permsResult->query;
        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        return $this->getPDO()->prepare($sql)->execute();
    }

    /**
     * Analyze a collection updating it's metadata on the database engine
     */
    public function analyzeCollection(string $collection): bool
    {
        return false;
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
        $result = $schema->alter($this->getSQLTableRaw($collection), function (\Utopia\Query\Schema\Blueprint $table) use ($attribute) {
            $this->addBlueprintColumn($table, $attribute->key, $attribute->type->value, $attribute->size, $attribute->signed, $attribute->array, $attribute->required);
        });

        // Postgres does not support LOCK= on ALTER TABLE, so no lock type appended
        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $result->query);

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
        $result = $schema->alter($this->getSQLTableRaw($collection), function (\Utopia\Query\Schema\Blueprint $table) use ($id) {
            $table->dropColumn($this->filter($id));
        });

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $result->query);

        try {
            return $this->execute($this->getPDO()
                ->prepare($sql));
        } catch (PDOException $e) {
            if ($e->getCode() === '42703' && $e->errorInfo[1] === 7) {
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
        $result = $schema->alter($this->getSQLTableRaw($collection), function (\Utopia\Query\Schema\Blueprint $table) use ($old, $new) {
            $table->renameColumn($this->filter($old), $this->filter($new));
        });

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $result->query);

        return $this->execute($this->getPDO()
            ->prepare($sql));
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

            $renameResult = $schema->alter($this->getSQLTableRaw($collection), function (\Utopia\Query\Schema\Blueprint $table) use ($id, $newKey) {
                $table->renameColumn($id, $newKey);
            });

            $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $renameResult->query);

            $result = $this->execute($this->getPDO()
                ->prepare($sql));

            if (! $result) {
                return false;
            }

            $id = $newKey;
        }

        // Modify column type using schema builder's alterColumnType
        $sqlType = $this->getSQLType($attribute->type->value, $attribute->size, $attribute->signed, $attribute->array, $attribute->required);
        $tableRaw = $this->getSQLTableRaw($name);

        if ($sqlType == 'TIMESTAMP(3)') {
            $result = $schema->alterColumnType($tableRaw, $id, 'TIMESTAMP(3) without time zone', "TO_TIMESTAMP(\"{$id}\", 'YYYY-MM-DD HH24:MI:SS.MS')");
        } else {
            $result = $schema->alterColumnType($tableRaw, $id, $sqlType);
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $result->query);

        try {
            return $this->execute($this->getPDO()
                ->prepare($sql));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * @throws Exception
     */
    public function createRelationship(Relationship $relationship): bool
    {
        $name = $this->filter($relationship->collection);
        $relatedName = $this->filter($relationship->relatedCollection);
        $id = $this->filter($relationship->key);
        $twoWayKey = $this->filter($relationship->twoWayKey);
        $type = $relationship->type;
        $twoWay = $relationship->twoWay;

        $schema = $this->createSchemaBuilder();
        $addRelColumn = function (string $tableName, string $columnId) use ($schema): string {
            $result = $schema->alter($this->getSQLTableRaw($tableName), function (\Utopia\Query\Schema\Blueprint $table) use ($columnId) {
                $table->string($columnId, 255)->nullable()->default(null);
            });

            return $result->query;
        };

        $sql = match ($type) {
            RelationType::OneToOne => $addRelColumn($name, $id).';'.($twoWay ? $addRelColumn($relatedName, $twoWayKey).';' : ''),
            RelationType::OneToMany => $addRelColumn($relatedName, $twoWayKey).';',
            RelationType::ManyToOne => $addRelColumn($name, $id).';',
            RelationType::ManyToMany => null,
        };

        if ($sql === null) {
            return true;
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

        return $this->execute($this->getPDO()
            ->prepare($sql));
    }

    /**
     * @throws DatabaseException
     */
    public function updateRelationship(
        Relationship $relationship,
        ?string $newKey = null,
        ?string $newTwoWayKey = null,
    ): bool {
        $collection = $relationship->collection;
        $relatedCollection = $relationship->relatedCollection;
        $name = $this->filter($collection);
        $relatedName = $this->filter($relatedCollection);
        $key = $this->filter($relationship->key);
        $twoWayKey = $this->filter($relationship->twoWayKey);
        $type = $relationship->type;
        $twoWay = $relationship->twoWay;
        $side = $relationship->side;

        if (! \is_null($newKey)) {
            $newKey = $this->filter($newKey);
        }
        if (! \is_null($newTwoWayKey)) {
            $newTwoWayKey = $this->filter($newTwoWayKey);
        }

        $schema = $this->createSchemaBuilder();
        $renameCol = function (string $tableName, string $from, string $to) use ($schema): string {
            $result = $schema->alter($this->getSQLTableRaw($tableName), function (\Utopia\Query\Schema\Blueprint $table) use ($from, $to) {
                $table->renameColumn($from, $to);
            });

            return $result->query;
        };

        $sql = '';

        switch ($type) {
            case RelationType::OneToOne:
                if ($key !== $newKey) {
                    $sql = $renameCol($name, $key, $newKey).';';
                }
                if ($twoWay && $twoWayKey !== $newTwoWayKey) {
                    $sql .= $renameCol($relatedName, $twoWayKey, $newTwoWayKey).';';
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    if ($twoWayKey !== $newTwoWayKey) {
                        $sql = $renameCol($relatedName, $twoWayKey, $newTwoWayKey).';';
                    }
                } else {
                    if ($key !== $newKey) {
                        $sql = $renameCol($name, $key, $newKey).';';
                    }
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Child) {
                    if ($twoWayKey !== $newTwoWayKey) {
                        $sql = $renameCol($relatedName, $twoWayKey, $newTwoWayKey).';';
                    }
                } else {
                    if ($key !== $newKey) {
                        $sql = $renameCol($name, $key, $newKey).';';
                    }
                }
                break;
            case RelationType::ManyToMany:
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collection = $this->getDocument($metadataCollection, $collection);
                $relatedCollection = $this->getDocument($metadataCollection, $relatedCollection);

                $junctionName = '_'.$collection->getSequence().'_'.$relatedCollection->getSequence();

                if (! \is_null($newKey)) {
                    $sql = $renameCol($junctionName, $key, $newKey).';';
                }
                if ($twoWay && ! \is_null($newTwoWayKey)) {
                    $sql .= $renameCol($junctionName, $twoWayKey, $newTwoWayKey).';';
                }
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        if (empty($sql)) {
            return true;
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

        return $this->execute($this->getPDO()
            ->prepare($sql));
    }

    /**
     * @throws DatabaseException
     */
    public function deleteRelationship(Relationship $relationship): bool
    {
        $collection = $relationship->collection;
        $relatedCollection = $relationship->relatedCollection;
        $name = $this->filter($collection);
        $relatedName = $this->filter($relatedCollection);
        $key = $this->filter($relationship->key);
        $twoWayKey = $this->filter($relationship->twoWayKey);
        $type = $relationship->type;
        $twoWay = $relationship->twoWay;
        $side = $relationship->side;

        $schema = $this->createSchemaBuilder();
        $dropCol = function (string $tableName, string $columnId) use ($schema): string {
            $result = $schema->alter($this->getSQLTableRaw($tableName), function (\Utopia\Query\Schema\Blueprint $table) use ($columnId) {
                $table->dropColumn($columnId);
            });

            return $result->query;
        };

        $sql = '';

        switch ($type) {
            case RelationType::OneToOne:
                if ($side === RelationSide::Parent) {
                    $sql = $dropCol($name, $key).';';
                    if ($twoWay) {
                        $sql .= $dropCol($relatedName, $twoWayKey).';';
                    }
                } elseif ($side === RelationSide::Child) {
                    $sql = $dropCol($relatedName, $twoWayKey).';';
                    if ($twoWay) {
                        $sql .= $dropCol($name, $key).';';
                    }
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    $sql = $dropCol($relatedName, $twoWayKey).';';
                } else {
                    $sql = $dropCol($name, $key).';';
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Child) {
                    $sql = $dropCol($relatedName, $twoWayKey).';';
                } else {
                    $sql = $dropCol($name, $key).';';
                }
                break;
            case RelationType::ManyToMany:
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collection = $this->getDocument($metadataCollection, $collection);
                $relatedCollection = $this->getDocument($metadataCollection, $relatedCollection);

                $junctionName = $side === RelationSide::Parent
                    ? '_'.$collection->getSequence().'_'.$relatedCollection->getSequence()
                    : '_'.$relatedCollection->getSequence().'_'.$collection->getSequence();

                $junctionResult = $schema->drop($this->getSQLTableRaw($junctionName));
                $permsResult = $schema->drop($this->getSQLTableRaw($junctionName.'_perms'));

                $sql = $junctionResult->query.'; '.$permsResult->query;
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        if (empty($sql)) {
            return true;
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $sql);

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

        $sql = $this->trigger(Database::EVENT_INDEX_CREATE, $sql);

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
        $sql = $this->trigger(Database::EVENT_INDEX_DELETE, $sql);

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
        $sql = $this->trigger(Database::EVENT_INDEX_RENAME, $sql);

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
            $stmt = $this->executeResult($result, Database::EVENT_DOCUMENT_CREATE);

            $this->execute($stmt);
            $lastInsertedId = $this->getPDO()->lastInsertId();
            $document['$sequence'] ??= $lastInsertedId;

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
                    $opResult = $this->getOperatorBuilderExpression($column, $operators[$attribute]);
                    $builder->setRaw($column, $opResult['expression'], $opResult['bindings']);
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
            $builder->filter([\Utopia\Query\Query::equal('_id', [$document->getSequence()])]);
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
            throw new DatabaseException('Operator cannot be expressed in SQL: '.$operator->getMethod());
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
            case OperatorType::Increment->value:
            case OperatorType::Decrement->value:
            case OperatorType::Multiply->value:
            case OperatorType::Divide->value:
                $namedBindings["op_{$idx}"] = $values[0] ?? 1;
                $idx++;
                if (isset($values[1])) {
                    $namedBindings["op_{$idx}"] = $values[1];
                    $idx++;
                }
                break;

            case OperatorType::Modulo->value:
                $namedBindings["op_{$idx}"] = $values[0] ?? 1;
                $idx++;
                break;

            case OperatorType::Power->value:
                $namedBindings["op_{$idx}"] = $values[0] ?? 1;
                $idx++;
                if (isset($values[1])) {
                    $namedBindings["op_{$idx}"] = $values[1];
                    $idx++;
                }
                break;

            case OperatorType::StringConcat->value:
                $namedBindings["op_{$idx}"] = $values[0] ?? '';
                $idx++;
                break;

            case OperatorType::StringReplace->value:
                $namedBindings["op_{$idx}"] = $values[0] ?? '';
                $idx++;
                $namedBindings["op_{$idx}"] = $values[1] ?? '';
                $idx++;
                break;

            case OperatorType::Toggle->value:
                // No bindings
                break;

            case OperatorType::DateAddDays->value:
            case OperatorType::DateSubDays->value:
                $namedBindings["op_{$idx}"] = $values[0] ?? 0;
                $idx++;
                break;

            case OperatorType::DateSetNow->value:
                // No bindings
                break;

            case OperatorType::ArrayAppend->value:
            case OperatorType::ArrayPrepend->value:
                $namedBindings["op_{$idx}"] = json_encode($values);
                $idx++;
                break;

            case OperatorType::ArrayRemove->value:
                $value = $values[0] ?? null;
                $namedBindings["op_{$idx}"] = json_encode($value);
                $idx++;
                break;

            case OperatorType::ArrayUnique->value:
                // No bindings
                break;

            case OperatorType::ArrayInsert->value:
                $namedBindings["op_{$idx}"] = $values[0] ?? 0;
                $idx++;
                $namedBindings["op_{$idx}"] = json_encode($values[1] ?? null);
                $idx++;
                break;

            case OperatorType::ArrayIntersect->value:
            case OperatorType::ArrayDiff->value:
                $namedBindings["op_{$idx}"] = json_encode($values);
                $idx++;
                break;

            case OperatorType::ArrayFilter->value:
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
     * Increase or decrease an attribute value
     *
     * @throws DatabaseException
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, string $updatedAt, int|float|null $min = null, int|float|null $max = null): bool
    {
        $name = $this->filter($collection);
        $attribute = $this->filter($attribute);

        $builder = $this->newBuilder($name);
        $builder->setRaw($attribute, $this->quote($attribute).' + ?', [$value]);
        $builder->set(['_updatedAt' => $updatedAt]);

        $filters = [\Utopia\Query\Query::equal('_uid', [$id])];
        if ($max !== null) {
            $filters[] = \Utopia\Query\Query::lessThanEqual($attribute, $max);
        }
        if ($min !== null) {
            $filters[] = \Utopia\Query\Query::greaterThanEqual($attribute, $min);
        }
        $builder->filter($filters);

        $result = $builder->update();
        $stmt = $this->executeResult($result, Database::EVENT_DOCUMENT_UPDATE);

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return true;
    }

    /**
     * Delete Document
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        try {
            $this->syncWriteHooks();

            $name = $this->filter($collection);

            $builder = $this->newBuilder($name);
            $builder->filter([\Utopia\Query\Query::equal('_uid', [$id])]);
            $result = $builder->delete();
            $stmt = $this->executeResult($result, Database::EVENT_DOCUMENT_DELETE);

            if (! $stmt->execute()) {
                throw new DatabaseException('Failed to delete document');
            }

            $deleted = $stmt->rowCount();

            $ctx = $this->buildWriteContext($name);
            foreach ($this->writeHooks as $hook) {
                $hook->afterDocumentDelete($name, [$id], $ctx);
            }
        } catch (\Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        return $deleted;
    }

    public function getConnectionId(): string
    {
        $result = $this->createBuilder()->fromNone()->selectRaw('pg_backend_pid()')->build();
        $stmt = $this->getPDO()->query($result->query);

        return $stmt->fetchColumn();
    }

    /**
     * Handle distance spatial queries
     *
     * @param  array<string, mixed>  $binds
     */
    protected function handleDistanceSpatialQueries(Query $query, array &$binds, string $attribute, string $alias, string $placeholder): string
    {
        $distanceParams = $query->getValues()[0];
        $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($distanceParams[0]);
        $binds[":{$placeholder}_1"] = $distanceParams[1];

        $meters = isset($distanceParams[2]) && $distanceParams[2] === true;

        $operator = match ($query->getMethod()) {
            Query::TYPE_DISTANCE_EQUAL => '=',
            Query::TYPE_DISTANCE_NOT_EQUAL => '!=',
            Query::TYPE_DISTANCE_GREATER_THAN => '>',
            Query::TYPE_DISTANCE_LESS_THAN => '<',
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
    protected function handleSpatialQueries(Query $query, array &$binds, string $attribute, string $alias, string $placeholder): string
    {
        $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
        $geom = $this->getSpatialGeomFromText(":{$placeholder}_0");

        return match ($query->getMethod()) {
            Query::TYPE_CROSSES => "ST_Crosses({$alias}.{$attribute}, {$geom})",
            Query::TYPE_NOT_CROSSES => "NOT ST_Crosses({$alias}.{$attribute}, {$geom})",
            Query::TYPE_DISTANCE_EQUAL,
            Query::TYPE_DISTANCE_NOT_EQUAL,
            Query::TYPE_DISTANCE_GREATER_THAN,
            Query::TYPE_DISTANCE_LESS_THAN => $this->handleDistanceSpatialQueries($query, $binds, $attribute, $alias, $placeholder),
            Query::TYPE_EQUAL => "ST_Equals({$alias}.{$attribute}, {$geom})",
            Query::TYPE_NOT_EQUAL => "NOT ST_Equals({$alias}.{$attribute}, {$geom})",
            Query::TYPE_INTERSECTS => "ST_Intersects({$alias}.{$attribute}, {$geom})",
            Query::TYPE_NOT_INTERSECTS => "NOT ST_Intersects({$alias}.{$attribute}, {$geom})",
            Query::TYPE_OVERLAPS => "ST_Overlaps({$alias}.{$attribute}, {$geom})",
            Query::TYPE_NOT_OVERLAPS => "NOT ST_Overlaps({$alias}.{$attribute}, {$geom})",
            Query::TYPE_TOUCHES => "ST_Touches({$alias}.{$attribute}, {$geom})",
            Query::TYPE_NOT_TOUCHES => "NOT ST_Touches({$alias}.{$attribute}, {$geom})",
            // using st_cover instead of contains to match the boundary matching behaviour of the mariadb st_contains
            // postgis st_contains excludes matching the boundary
            Query::TYPE_CONTAINS => "ST_Covers({$alias}.{$attribute}, {$geom})",
            Query::TYPE_NOT_CONTAINS => "NOT ST_Covers({$alias}.{$attribute}, {$geom})",
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
            case Query::TYPE_EQUAL:
            case Query::TYPE_NOT_EQUAL:
                $isNot = $query->getMethod() === Query::TYPE_NOT_EQUAL;
                $conditions = [];
                foreach ($query->getValues() as $key => $value) {
                    $binds[":{$placeholder}_{$key}"] = json_encode($value);
                    $fragment = "{$alias}.{$attribute} @> :{$placeholder}_{$key}::jsonb";
                    $conditions[] = $isNot ? 'NOT ('.$fragment.')' : $fragment;
                }
                $separator = $isNot ? ' AND ' : ' OR ';

                return empty($conditions) ? '' : '('.implode($separator, $conditions).')';

            case Query::TYPE_CONTAINS:
            case Query::TYPE_CONTAINS_ANY:
            case Query::TYPE_CONTAINS_ALL:
            case Query::TYPE_NOT_CONTAINS:
                $isNot = $query->getMethod() === Query::TYPE_NOT_CONTAINS;
                $conditions = [];
                foreach ($query->getValues() as $key => $value) {
                    if (count($value) === 1) {
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
            return $this->handleSpatialQueries($query, $binds, $attribute, $alias, $placeholder);
        }

        if ($query->isObjectAttribute() && ! $isNestedObjectAttribute) {
            return $this->handleObjectQueries($query, $binds, $attribute, $alias, $placeholder);
        }

        switch ($query->getMethod()) {
            case Query::TYPE_OR:
            case Query::TYPE_AND:
                $conditions = [];
                /* @var $q Query */
                foreach ($query->getValue() as $q) {
                    $conditions[] = $this->getSQLCondition($q, $binds);
                }

                $method = strtoupper($query->getMethod()->value);

                return empty($conditions) ? '' : ' '.$method.' ('.implode(' AND ', $conditions).')';

            case Query::TYPE_SEARCH:
                $binds[":{$placeholder}_0"] = $this->getFulltextValue($query->getValue());

                return "to_tsvector(regexp_replace({$attribute}, '[^\w]+',' ','g')) @@ websearch_to_tsquery(:{$placeholder}_0)";

            case Query::TYPE_NOT_SEARCH:
                $binds[":{$placeholder}_0"] = $this->getFulltextValue($query->getValue());

                return "NOT (to_tsvector(regexp_replace({$attribute}, '[^\w]+',' ','g')) @@ websearch_to_tsquery(:{$placeholder}_0))";

            case Query::TYPE_VECTOR_DOT:
            case Query::TYPE_VECTOR_COSINE:
            case Query::TYPE_VECTOR_EUCLIDEAN:
                return ''; // Handled in ORDER BY clause

            case Query::TYPE_BETWEEN:
                $binds[":{$placeholder}_0"] = $query->getValues()[0];
                $binds[":{$placeholder}_1"] = $query->getValues()[1];

                return "{$alias}.{$attribute} BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Query::TYPE_NOT_BETWEEN:
                $binds[":{$placeholder}_0"] = $query->getValues()[0];
                $binds[":{$placeholder}_1"] = $query->getValues()[1];

                return "{$alias}.{$attribute} NOT BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Query::TYPE_IS_NULL:
            case Query::TYPE_IS_NOT_NULL:
                return "{$alias}.{$attribute} {$this->getSQLOperator($query->getMethod())}";

            case Query::TYPE_CONTAINS_ALL:
                if ($query->onArray()) {
                    // @> checks the array contains ALL specified values
                    $binds[":{$placeholder}_0"] = \json_encode($query->getValues());

                    return "{$alias}.{$attribute} @> :{$placeholder}_0::jsonb";
                }
                // no break
            case Query::TYPE_CONTAINS:
            case Query::TYPE_CONTAINS_ANY:
            case Query::TYPE_NOT_CONTAINS:
                if ($query->onArray()) {
                    $operator = '@>';
                }

                // no break
            default:
                $conditions = [];
                $operator = $operator ?? $this->getSQLOperator($query->getMethod());
                $isNotQuery = in_array($query->getMethod(), [
                    Query::TYPE_NOT_STARTS_WITH,
                    Query::TYPE_NOT_ENDS_WITH,
                    Query::TYPE_NOT_CONTAINS,
                ]);

                foreach ($query->getValues() as $key => $value) {
                    $value = match ($query->getMethod()) {
                        Query::TYPE_STARTS_WITH => $this->escapeWildcards($value).'%',
                        Query::TYPE_NOT_STARTS_WITH => $this->escapeWildcards($value).'%',
                        Query::TYPE_ENDS_WITH => '%'.$this->escapeWildcards($value),
                        Query::TYPE_NOT_ENDS_WITH => '%'.$this->escapeWildcards($value),
                        Query::TYPE_CONTAINS, Query::TYPE_CONTAINS_ANY => ($query->onArray()) ? \json_encode($value) : '%'.$this->escapeWildcards($value).'%',
                        Query::TYPE_NOT_CONTAINS => ($query->onArray()) ? \json_encode($value) : '%'.$this->escapeWildcards($value).'%',
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
        $vectorArray = $values[0] ?? [];
        $vector = \json_encode(\array_map(\floatval(...), $vectorArray));
        $binds[":vector_{$placeholder}"] = $vector;

        return match ($query->getMethod()) {
            Query::TYPE_VECTOR_DOT => "({$alias}.{$attribute} <#> :vector_{$placeholder}::vector)",
            Query::TYPE_VECTOR_COSINE => "({$alias}.{$attribute} <=> :vector_{$placeholder}::vector)",
            Query::TYPE_VECTOR_EUCLIDEAN => "({$alias}.{$attribute} <-> :vector_{$placeholder}::vector)",
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
        $vectorArray = $values[0] ?? [];
        $vector = \json_encode(\array_map(\floatval(...), $vectorArray));

        $expression = match ($query->getMethod()) {
            \Utopia\Query\Method::VectorDot => "({$quotedAlias}.{$attribute} <#> ?::vector)",
            \Utopia\Query\Method::VectorCosine => "({$quotedAlias}.{$attribute} <=> ?::vector)",
            \Utopia\Query\Method::VectorEuclidean => "({$quotedAlias}.{$attribute} <-> ?::vector)",
            default => null,
        };

        if ($expression === null) {
            return null;
        }

        return ['expression' => $expression, 'bindings' => [$vector]];
    }

    protected function getFulltextValue(string $value): string
    {
        $exact = str_ends_with($value, '"') && str_starts_with($value, '"');
        $value = str_replace(['@', '+', '-', '*', '.', "'", '"'], ' ', $value);
        $value = preg_replace('/\s+/', ' ', $value); // Remove multiple whitespaces
        $value = trim($value);

        if (! $exact) {
            $value = str_replace(' ', ' or ', $value);
        }

        return "'".$value."'";
    }

    protected function getOperatorBuilderExpression(string $column, Operator $operator): array
    {
        if ($operator->getMethod() === OperatorType::ArrayRemove->value) {
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
     * Get SQL Type
     */
    protected function createBuilder(): \Utopia\Query\Builder\SQL
    {
        return new \Utopia\Query\Builder\PostgreSQL;
    }

    protected function createSchemaBuilder(): \Utopia\Query\Schema
    {
        return new \Utopia\Query\Schema\PostgreSQL;
    }

    protected function getSQLType(string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        if ($array === true) {
            return 'JSONB';
        }

        return match ($type) {
            ColumnType::Id->value => 'BIGINT',
            ColumnType::String->value => $size > $this->getMaxVarcharLength() ? 'TEXT' : "VARCHAR({$size})",
            ColumnType::Varchar->value => "VARCHAR({$size})",
            ColumnType::Text->value,
            ColumnType::MediumText->value,
            ColumnType::LongText->value => 'TEXT',
            ColumnType::Integer->value => $size >= 8 ? 'BIGINT' : 'INTEGER',
            ColumnType::Double->value => 'DOUBLE PRECISION',
            ColumnType::Boolean->value => 'BOOLEAN',
            ColumnType::Relationship->value => 'VARCHAR(255)',
            ColumnType::Datetime->value => 'TIMESTAMP(3)',
            ColumnType::Object->value => 'JSONB',
            ColumnType::Point->value => 'GEOMETRY(POINT,'.Database::DEFAULT_SRID.')',
            ColumnType::Linestring->value => 'GEOMETRY(LINESTRING,'.Database::DEFAULT_SRID.')',
            ColumnType::Polygon->value => 'GEOMETRY(POLYGON,'.Database::DEFAULT_SRID.')',
            ColumnType::Vector->value => "VECTOR({$size})",
            default => throw new DatabaseException('Unknown Type: '.$type.'. Must be one of '.ColumnType::String->value.', '.ColumnType::Varchar->value.', '.ColumnType::Text->value.', '.ColumnType::MediumText->value.', '.ColumnType::LongText->value.', '.ColumnType::Integer->value.', '.ColumnType::Double->value.', '.ColumnType::Boolean->value.', '.ColumnType::Datetime->value.', '.ColumnType::Relationship->value.', '.ColumnType::Object->value.', '.ColumnType::Point->value.', '.ColumnType::Linestring->value.', '.ColumnType::Polygon->value),
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
     * Get the SQL function for random ordering
     */
    protected function getRandomOrder(): string
    {
        return 'RANDOM()';
    }

    /**
     * Size of POINT spatial type
     */
    protected function getMaxPointSize(): int
    {
        // https://stackoverflow.com/questions/30455025/size-of-data-type-geographypoint-4326-in-postgis
        return 32;
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

    public function getMinDateTime(): \DateTime
    {
        return new \DateTime('-4713-01-01 00:00:00');
    }

    public function getLikeOperator(): string
    {
        return 'ILIKE';
    }

    public function getRegexOperator(): string
    {
        return '~';
    }

    protected function processException(PDOException $e): \Exception
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
        $type = $typeArr[1];

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
        $x = (float) $xArr[1];

        // Y coordinate
        $yArr = unpack($fmt, substr($bin, $offset + 8, 8));
        if ($yArr === false || ! isset($yArr[1])) {
            throw new DatabaseException('Failed to unpack Y coordinate');
        }
        $y = (float) $yArr[1];

        return [$x, $y];
    }

    public function decodeLinestring(mixed $wkb): array
    {
        if (str_starts_with(strtoupper($wkb), 'LINESTRING(')) {
            $start = strpos($wkb, '(') + 1;
            $end = strrpos($wkb, ')');
            $inside = substr($wkb, $start, $end - $start);

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

        $typeField = $typeField[1];
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

        $numPoints = $numPoints[1];
        $offset += 4;

        $points = [];
        for ($i = 0; $i < $numPoints; $i++) {
            $x = unpack('e', substr($wkb, $offset, 8));
            if ($x === false) {
                throw new DatabaseException("Failed to unpack X coordinate at offset {$offset}.");
            }

            $x = (float) $x[1];

            $offset += 8;

            $y = unpack('e', substr($wkb, $offset, 8));
            if ($y === false) {
                throw new DatabaseException("Failed to unpack Y coordinate at offset {$offset}.");
            }

            $y = (float) $y[1];

            $offset += 8;
            $points[] = [$x, $y];
        }

        return $points;
    }

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

        $typeInt = (int) $typeInt[1];
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

        $numRings = (int) $numRings[1];
        $offset += 4;

        $rings = [];
        for ($r = 0; $r < $numRings; $r++) {
            $numPoints = unpack($uInt32, substr($wkb, $offset, 4));
            if ($numPoints === false) {
                throw new DatabaseException('Failed to unpack number of points from WKB.');
            }

            $numPoints = (int) $numPoints[1];
            $offset += 4;
            $points = [];
            for ($i = 0; $i < $numPoints; $i++) {
                $x = unpack($uDouble, substr($wkb, $offset, 8));
                if ($x === false) {
                    throw new DatabaseException('Failed to unpack X coordinate from WKB.');
                }

                $x = (float) $x[1];

                $y = unpack($uDouble, substr($wkb, $offset + 8, 8));
                if ($y === false) {
                    throw new DatabaseException('Failed to unpack Y coordinate from WKB.');
                }

                $y = (float) $y[1];

                $points[] = [$x, $y];
                $offset += 16;
            }
            $rings[] = $points;
        }

        return $rings; // array of rings, each ring is array of [x,y]
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
            case OperatorType::Increment->value:
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

            case OperatorType::Decrement->value:
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

            case OperatorType::Multiply->value:
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

            case OperatorType::Divide->value:
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

            case OperatorType::Modulo->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = MOD(COALESCE({$columnRef}::numeric, 0), :$bindKey::numeric)";

            case OperatorType::Power->value:
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
            case OperatorType::StringConcat->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = CONCAT(COALESCE({$columnRef}, ''), :$bindKey)";

            case OperatorType::StringReplace->value:
                $searchKey = "op_{$bindIndex}";
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = REPLACE(COALESCE({$columnRef}, ''), :$searchKey, :$replaceKey)";

                // Boolean operators
            case OperatorType::Toggle->value:
                return "{$quotedColumn} = NOT COALESCE({$columnRef}, FALSE)";

                // Array operators
            case OperatorType::ArrayAppend->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE({$columnRef}, '[]'::jsonb) || :$bindKey::jsonb";

            case OperatorType::ArrayPrepend->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = :$bindKey::jsonb || COALESCE({$columnRef}, '[]'::jsonb)";

            case OperatorType::ArrayUnique->value:
                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(DISTINCT value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                ), '[]'::jsonb)";

            case OperatorType::ArrayRemove->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                    WHERE value != :$bindKey::jsonb
                ), '[]'::jsonb)";

            case OperatorType::ArrayInsert->value:
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

            case OperatorType::ArrayIntersect->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                    WHERE value IN (SELECT jsonb_array_elements(:$bindKey::jsonb))
                ), '[]'::jsonb)";

            case OperatorType::ArrayDiff->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = COALESCE((
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({$columnRef}) AS value
                    WHERE value NOT IN (SELECT jsonb_array_elements(:$bindKey::jsonb))
                ), '[]'::jsonb)";

            case OperatorType::ArrayFilter->value:
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
            case OperatorType::DateAddDays->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = {$columnRef} + (:$bindKey || ' days')::INTERVAL";

            case OperatorType::DateSubDays->value:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = {$columnRef} - (:$bindKey || ' days')::INTERVAL";

            case OperatorType::DateSetNow->value:
                return "{$quotedColumn} = NOW()";

            default:
                throw new OperatorException("Invalid operator: {$method}");
        }
    }

    /**
     * Bind operator parameters to statement
     * Override to handle PostgreSQL-specific JSON binding
     */
    protected function bindOperatorParams(\PDOStatement|PDOStatementProxy $stmt, Operator $operator, int &$bindIndex): void
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            case OperatorType::ArrayAppend->value:
            case OperatorType::ArrayPrepend->value:
                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $arrayValue, \PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayRemove->value:
                $value = $values[0] ?? null;
                $bindKey = "op_{$bindIndex}";
                // Always JSON encode for PostgreSQL jsonb comparison
                $stmt->bindValue(':'.$bindKey, json_encode($value), \PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayIntersect->value:
            case OperatorType::ArrayDiff->value:
                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $arrayValue, \PDO::PARAM_STR);
                $bindIndex++;
                break;

            default:
                // Use parent implementation for other operators
                parent::bindOperatorParams($stmt, $operator, $bindIndex);
                break;
        }
    }

    public function getSupportNonUtfCharacters(): bool
    {
        return false;
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
