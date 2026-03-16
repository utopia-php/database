<?php

namespace Utopia\Database\Adapter;

use DateTime;
use Exception;
use PDOException;
use Swoole\Database\PDOStatementProxy;
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
use Utopia\Database\Helpers\ID;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Builder\MariaDB as MariaDBBuilder;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema as BaseSchema;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\MySQL as MySQLSchema;

/**
 * Database adapter for MariaDB, extending the base SQL adapter with MariaDB-specific features.
 */
class MariaDB extends SQL implements Feature\Timeouts
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
            Capability::Timeouts,
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

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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

        $result = $this->createSchemaBuilder()->dropDatabase($name);
        $sql = $result->query;

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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

        // Build main collection table using schema builder
        $collectionResult = $schema->create($this->getSQLTableRaw($id), function (Blueprint $table) use ($attributes, $indexes, $hash, $sharedTables) {
            // System columns
            $table->id('_id');
            $table->string('_uid', 255);
            $table->datetime('_createdAt', 3)->nullable()->default(null);
            $table->datetime('_updatedAt', 3)->nullable()->default(null);
            $table->mediumText('_permissions')->nullable()->default(null);
            $table->rawColumn('`_version` INT(11) UNSIGNED DEFAULT 1');

            // User-defined attribute columns (raw SQL via getSQLType())
            foreach ($attributes as $attribute) {
                $attrId = $this->filter($attribute->key);

                // Skip virtual relationship attributes
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

            // User-defined indexes
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
                    \array_unshift($regularColumns, '_tenant');
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

            // Tenant column and system indexes
            if ($sharedTables) {
                $table->rawColumn('_tenant INT(11) UNSIGNED DEFAULT NULL');
                $table->uniqueIndex(['_uid', '_tenant'], '_uid');
                $table->index(['_tenant', '_createdAt'], '_created_at');
                $table->index(['_tenant', '_updatedAt'], '_updated_at');
                $table->index(['_tenant', '_id'], '_tenant_id');
            } else {
                $table->uniqueIndex(['_uid'], '_uid');
                $table->index(['_createdAt'], '_created_at');
                $table->index(['_updatedAt'], '_updated_at');
            }
        });
        $collection = $collectionResult->query;

        // Build permissions table using schema builder
        $permsResult = $schema->create($this->getSQLTableRaw($id.'_perms'), function (Blueprint $table) use ($sharedTables) {
            $table->id('_id');
            $table->string('_type', 12);
            $table->string('_permission', 255);
            $table->string('_document', 255);

            if ($sharedTables) {
                $table->integer('_tenant')->unsigned()->nullable()->default(null);
                $table->uniqueIndex(['_document', '_tenant', '_type', '_permission'], '_index1');
                $table->index(['_tenant', '_permission', '_type'], '_permission');
            } else {
                $table->uniqueIndex(['_document', '_type', '_permission'], '_index1');
                $table->index(['_permission', '_type'], '_permission');
            }
        });
        $permissions = $permsResult->query;

        try {
            $this->getPDO()->prepare($collection)->execute();
            $this->getPDO()->prepare($permissions)->execute();
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
        $mainResult = $schema->drop($this->getSQLTableRaw($id));
        $permsResult = $schema->drop($this->getSQLTableRaw($id.'_perms'));

        $sql = $mainResult->query.'; '.$permsResult->query;

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
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

        $stmt = $this->getPDO()->prepare($sql);

        return $stmt->execute();
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
        $permissions = $database.'/'.$collection.'_perms';

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

        $collectionSize = $this->getPDO()->prepare($collectionResult->query);
        $permissionsSize = $this->getPDO()->prepare($permissionsResult->query);

        foreach ($collectionResult->bindings as $i => $v) {
            $collectionSize->bindValue($i + 1, $v);
        }
        foreach ($permissionsResult->bindings as $i => $v) {
            $permissionsSize->bindValue($i + 1, $v);
        }

        try {
            $collectionSize->execute();
            $permissionsSize->execute();
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
        $permissions = $collection.'_perms';

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

        $collectionSize = $this->getPDO()->prepare($collectionResult->query);
        $permissionsSize = $this->getPDO()->prepare($permissionsResult->query);

        foreach ($collectionResult->bindings as $i => $v) {
            $collectionSize->bindValue($i + 1, $v);
        }
        foreach ($permissionsResult->bindings as $i => $v) {
            $permissionsSize->bindValue($i + 1, $v);
        }

        try {
            $collectionSize->execute();
            $permissionsSize->execute();
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
                return $this->getPDO()->prepare($sql)->execute();
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
        /** @var MySQLSchema $schema */
        $schema = $this->createSchemaBuilder();
        $tableRaw = $this->getSQLTableRaw($name);

        if (! empty($newKey)) {
            $result = $schema->changeColumn($tableRaw, $id, $newKey, $sqlType);
        } else {
            $result = $schema->modifyColumn($tableRaw, $id, $sqlType);
        }

        $sql = $result->query;

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * @throws DatabaseException
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
            $result = $schema->alter($this->getSQLTableRaw($tableName), function (Blueprint $table) use ($columnId) {
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

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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

        if ($newKey !== null) {
            $newKey = $this->filter($newKey);
        }
        if ($newTwoWayKey !== null) {
            $newTwoWayKey = $this->filter($newTwoWayKey);
        }

        $schema = $this->createSchemaBuilder();
        $renameCol = function (string $tableName, string $from, string $to) use ($schema): string {
            $result = $schema->alter($this->getSQLTableRaw($tableName), function (Blueprint $table) use ($from, $to) {
                $table->renameColumn($from, $to);
            });

            return $result->query;
        };

        $sql = '';

        switch ($type) {
            case RelationType::OneToOne:
                if ($key !== $newKey && \is_string($newKey)) {
                    $sql = $renameCol($name, $key, $newKey).';';
                }
                if ($twoWay && $twoWayKey !== $newTwoWayKey && \is_string($newTwoWayKey)) {
                    $sql .= $renameCol($relatedName, $twoWayKey, $newTwoWayKey).';';
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    if ($twoWayKey !== $newTwoWayKey && \is_string($newTwoWayKey)) {
                        $sql = $renameCol($relatedName, $twoWayKey, $newTwoWayKey).';';
                    }
                } else {
                    if ($key !== $newKey && \is_string($newKey)) {
                        $sql = $renameCol($name, $key, $newKey).';';
                    }
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Child) {
                    if ($twoWayKey !== $newTwoWayKey && \is_string($newTwoWayKey)) {
                        $sql = $renameCol($relatedName, $twoWayKey, $newTwoWayKey).';';
                    }
                } else {
                    if ($key !== $newKey && \is_string($newKey)) {
                        $sql = $renameCol($name, $key, $newKey).';';
                    }
                }
                break;
            case RelationType::ManyToMany:
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collection = $this->getDocument($metadataCollection, $collection);
                $relatedCollection = $this->getDocument($metadataCollection, $relatedCollection);

                $junctionName = '_'.$collection->getSequence().'_'.$relatedCollection->getSequence();

                if ($newKey !== null) {
                    $sql = $renameCol($junctionName, $key, $newKey).';';
                }
                if ($twoWay && $newTwoWayKey !== null) {
                    $sql .= $renameCol($junctionName, $twoWayKey, $newTwoWayKey).';';
                }
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        if ($sql === '') {
            return true;
        }

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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
            $result = $schema->alter($this->getSQLTableRaw($tableName), function (Blueprint $table) use ($columnId) {
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
                if ($side === RelationSide::Parent) {
                    $sql = $dropCol($name, $key).';';
                } else {
                    $sql = $dropCol($relatedName, $twoWayKey).';';
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

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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
        $metadataCollection = new Document(['$id' => Database::METADATA]);
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
                $collAttrId = $collectionAttribute['$id'] ?? '';
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
            \array_unshift($schemaColumns, '_tenant');
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
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
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

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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
            $attributes['_createdAt'] = $document->getCreatedAt();
            $attributes['_updatedAt'] = $document->getUpdatedAt();
            $attributes['_permissions'] = \json_encode($document->getPermissions());
            $version = $document->getVersion();
            if ($version !== null) {
                $attributes['_version'] = $version;
            }

            $name = $this->filter($collection);

            // Build document INSERT using query builder
            // Spatial columns use insertColumnExpression() for ST_GeomFromText() wrapping
            $builder = $this->createBuilder()->into($this->getSQLTableRaw($name));
            $row = ['_uid' => $document->getId()];

            if (! empty($document->getSequence())) {
                $row['_id'] = $document->getSequence();
            }

            foreach ($attributes as $attr => $value) {
                $column = $this->filter($attr);

                if (\in_array($attr, $spatialAttributes, true)) {
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

            $stmt->execute();

            $document['$sequence'] = $this->pdo->lastInsertId();

            if (empty($document['$sequence'])) {
                throw new DatabaseException('Error creating document empty "$sequence"');
            }

            $ctx = $this->buildWriteContext($name);
            try {
                $this->runWriteHooks(fn ($hook) => $hook->afterDocumentCreate($name, [$document], $ctx));
            } catch (PDOException $e) {
                $isOrphanedPermission = $e->getCode() === '23000'
                    && isset($e->errorInfo[1])
                    && $e->errorInfo[1] === 1062
                    && \str_contains($e->getMessage(), '_index1');

                if (! $isOrphanedPermission) {
                    throw $e;
                }

                // Clean up orphaned permissions from a previous failed delete, then retry
                $cleanupBuilder = $this->newBuilder($name.'_perms');
                $cleanupBuilder->filter([BaseQuery::equal('_document', [$document->getId()])]);
                $cleanupResult = $cleanupBuilder->delete();
                $cleanupStmt = $this->executeResult($cleanupResult);
                $cleanupStmt->execute();

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
                } elseif (\in_array($attribute, $spatialAttributes, true)) {
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
     * Increase or decrease an attribute value
     *
     * @throws DatabaseException
     */
    public function increaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float $value,
        string $updatedAt,
        int|float|null $min = null,
        int|float|null $max = null
    ): bool {
        $name = $this->filter($collection);
        $attribute = $this->filter($attribute);

        $builder = $this->newBuilder($name);
        $builder->setRaw($attribute, $this->quote($attribute).' + ?', [$value]);
        $builder->set(['_updatedAt' => $updatedAt]);

        $filters = [BaseQuery::equal('_uid', [$id])];
        if ($max !== null) {
            $filters[] = BaseQuery::lessThanEqual($attribute, $max);
        }
        if ($min !== null) {
            $filters[] = BaseQuery::greaterThanEqual($attribute, $min);
        }
        $builder->filter($filters);

        $result = $builder->update();
        $stmt = $this->executeResult($result, Event::DocumentUpdate);

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return true;
    }

    /**
     * Delete Document
     *
     * @throws Exception
     * @throws PDOException
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        try {
            $this->syncWriteHooks();

            $name = $this->filter($collection);

            $builder = $this->newBuilder($name);
            $builder->filter([BaseQuery::equal('_uid', [$id])]);
            $result = $builder->delete();
            $stmt = $this->executeResult($result, Event::DocumentDelete);

            if (! $stmt->execute()) {
                throw new DatabaseException('Failed to delete document');
            }

            $deleted = $stmt->rowCount();

            $ctx = $this->buildWriteContext($name);
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentDelete($name, [$id], $ctx));
        } catch (\Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        return $deleted > 0;
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

        $this->timeout = $milliseconds;
    }

    /**
     * Size of POINT spatial type
     */
    protected function getMaxPointSize(): int
    {
        // https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html#gis-internal-format
        return 25;
    }

    /**
     * Get the minimum supported datetime value for MariaDB.
     *
     * @return DateTime
     */
    public function getMinDateTime(): DateTime
    {
        return new DateTime('1000-01-01 00:00:00');
    }

    /**
     * Get the maximum supported datetime value for MariaDB.
     *
     * @return DateTime
     */
    public function getMaxDateTime(): DateTime
    {
        return new DateTime('9999-12-31 23:59:59');
    }

    /**
     * Get the keys of internally managed indexes for MariaDB.
     *
     * @return array<string>
     */
    public function getInternalIndexesKeys(): array
    {
        return ['primary', '_created_at', '_updated_at', '_tenant_id'];
    }

    protected function execute(mixed $stmt): bool
    {
        $seconds = $this->timeout > 0 ? $this->timeout / 1000 : 0;
        $this->getPDO()->exec("SET max_statement_time = " . (float) $seconds);

        /** @var \PDOStatement|PDOStatementProxy $stmt */
        return $stmt->execute();
    }

    /**
     * {@inheritDoc}
     */
    protected function insertRequiresAlias(): bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function getConflictTenantExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));

        return "IF(_tenant = VALUES(_tenant), VALUES({$quoted}), {$quoted})";
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

        return "IF(_tenant = VALUES(_tenant), {$quoted} + VALUES({$quoted}), {$quoted})";
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

        $attribute = $query->getAttribute();
        $attribute = $this->filter($attribute);
        $attribute = $this->quote($attribute);
        $alias = $this->quote(Query::DEFAULT_ALIAS);
        $placeholder = ID::unique();

        if ($query->isSpatialAttribute()) {
            return $this->handleSpatialQueries($query, $binds, $attribute, $query->getAttributeType(), $alias, $placeholder);
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

                return "MATCH({$alias}.{$attribute}) AGAINST (:{$placeholder}_0 IN BOOLEAN MODE)";

            case Method::NotSearch:
                $notSearchVal = $query->getValue();
                $binds[":{$placeholder}_0"] = $this->getFulltextValue(\is_string($notSearchVal) ? $notSearchVal : '');

                return "NOT (MATCH({$alias}.{$attribute}) AGAINST (:{$placeholder}_0 IN BOOLEAN MODE))";

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
                    $binds[":{$placeholder}_0"] = json_encode($query->getValues());

                    return "JSON_CONTAINS({$alias}.{$attribute}, :{$placeholder}_0)";
                }
                // no break
            case Method::Contains:
            case Method::ContainsAny:
            case Method::NotContains:
                if ($this->supports(Capability::JSONOverlaps) && $query->onArray()) {
                    $binds[":{$placeholder}_0"] = json_encode($query->getValues());
                    $isNot = $query->getMethod() === Method::NotContains;

                    return $isNot
                        ? "NOT (JSON_OVERLAPS({$alias}.{$attribute}, :{$placeholder}_0))"
                        : "JSON_OVERLAPS({$alias}.{$attribute}, :{$placeholder}_0)";
                }
                // no break
            default:
                $conditions = [];
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
                    if ($isNotQuery) {
                        $conditions[] = "{$alias}.{$attribute} NOT {$this->getSQLOperator($query->getMethod())} :{$placeholder}_{$key}";
                    } else {
                        $conditions[] = "{$alias}.{$attribute} {$this->getSQLOperator($query->getMethod())} :{$placeholder}_{$key}";
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
        return new MariaDBBuilder();
    }

    protected function createSchemaBuilder(): BaseSchema
    {
        return new MySQLSchema();
    }

    protected function getSQLType(ColumnType $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        if (in_array($type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
            return $this->getSpatialSQLType($type->value, $required);
        }
        if ($array === true) {
            return 'JSON';
        }

        if ($type === ColumnType::String) {
            // $size = $size * 4; // Convert utf8mb4 size to bytes
            if ($size > 16777215) {
                return 'LONGTEXT';
            }
            if ($size > 65535) {
                return 'MEDIUMTEXT';
            }
            if ($size > $this->getMaxVarcharLength()) {
                return 'TEXT';
            }

            return "VARCHAR({$size})";
        }

        if ($type === ColumnType::Varchar) {
            if ($size <= 0) {
                throw new DatabaseException('VARCHAR size '.$size.' is invalid; must be > 0. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.');
            }
            if ($size > $this->getMaxVarcharLength()) {
                throw new DatabaseException('VARCHAR size '.$size.' exceeds maximum varchar length '.$this->getMaxVarcharLength().'. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.');
            }

            return "VARCHAR({$size})";
        }

        if ($type === ColumnType::Integer) {
            // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554
            $suffix = $signed ? '' : ' UNSIGNED';

            return ($size >= 8 ? 'BIGINT' : 'INT').$suffix; // INT = 4 bytes, BIGINT = 8 bytes
        }

        if ($type === ColumnType::Double) {
            return 'DOUBLE'.($signed ? '' : ' UNSIGNED');
        }

        return match ($type) {
            ColumnType::Id => 'BIGINT UNSIGNED',
            ColumnType::Text => 'TEXT',
            ColumnType::MediumText => 'MEDIUMTEXT',
            ColumnType::LongText => 'LONGTEXT',
            ColumnType::Boolean => 'TINYINT(1)',
            ColumnType::Relationship => 'VARCHAR(255)',
            ColumnType::Datetime => 'DATETIME(3)',
            default => throw new DatabaseException('Unknown type: '.$type->value.'. Must be one of '.ColumnType::String->value.', '.ColumnType::Varchar->value.', '.ColumnType::Text->value.', '.ColumnType::MediumText->value.', '.ColumnType::LongText->value.', '.ColumnType::Integer->value.', '.ColumnType::Double->value.', '.ColumnType::Boolean->value.', '.ColumnType::Datetime->value.', '.ColumnType::Relationship->value.', '.ColumnType::Point->value.', '.ColumnType::Linestring->value.', '.ColumnType::Polygon->value),
        };
    }

    /**
     * Get the MariaDB SQL type definition for spatial column types.
     *
     * @param string $type The spatial type (point, linestring, polygon)
     * @param bool $required Whether the column is NOT NULL
     * @return string
     */
    public function getSpatialSQLType(string $type, bool $required): string
    {
        $srid = Database::DEFAULT_SRID;
        $nullability = '';

        if (! $this->supports(Capability::SpatialIndexNull)) {
            if ($required) {
                $nullability = ' NOT NULL';
            } else {
                $nullability = ' NULL';
            }
        }

        return match ($type) {
            ColumnType::Point->value => "POINT($srid)$nullability",
            ColumnType::Linestring->value => "LINESTRING($srid)$nullability",
            ColumnType::Polygon->value => "POLYGON($srid)$nullability",
            default => '',
        };
    }

    /**
     * Get PDO Type
     *
     * @throws Exception
     */
    protected function getPDOType(mixed $value): int
    {
        return match (gettype($value)) {
            'string','double' => \PDO::PARAM_STR,
            'integer', 'boolean' => \PDO::PARAM_INT,
            'NULL' => \PDO::PARAM_NULL,
            default => throw new DatabaseException('Unknown PDO Type for '.\gettype($value)),
        };
    }

    /**
     * Get the SQL function for random ordering
     */
    protected function getRandomOrder(): string
    {
        return 'RAND()';
    }

    protected function quote(string $string): string
    {
        return "`{$string}`";
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
            $stmt = $this->getPDO()->prepare('
                SELECT
                COLUMN_NAME as _id,
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
            ');
            $stmt->bindParam(':schema', $schema);
            $stmt->bindParam(':table', $collection);
            $stmt->execute();
            $results = $stmt->fetchAll();
            $stmt->closeCursor();

            $docs = [];
            foreach ($results as $document) {
                /** @var array<string, mixed> $document */
                $document['$id'] = $document['_id'];
                unset($document['_id']);

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
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN COALESCE({$quotedColumn}, 0) > :$maxKey - :$bindKey THEN :$maxKey
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
                        WHEN COALESCE({$quotedColumn}, 0) <= :$minKey THEN :$minKey
                        WHEN COALESCE({$quotedColumn}, 0) < :$minKey + :$bindKey THEN :$minKey
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
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN :$bindKey > 0 AND COALESCE({$quotedColumn}, 0) > :$maxKey / :$bindKey THEN :$maxKey
                        WHEN :$bindKey < 0 AND COALESCE({$quotedColumn}, 0) < :$maxKey / :$bindKey THEN :$maxKey
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
                        WHEN :$bindKey != 0 AND COALESCE({$quotedColumn}, 0) / :$bindKey <= :$minKey THEN :$minKey
                        ELSE COALESCE({$quotedColumn}, 0) / :$bindKey
                    END";
                }

                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) / :$bindKey";

            case OperatorType::Modulo:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = MOD(COALESCE({$quotedColumn}, 0), :$bindKey)";

            case OperatorType::Power:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;

                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN COALESCE({$quotedColumn}, 0) <= 1 THEN COALESCE({$quotedColumn}, 0)
                        WHEN :$bindKey * LOG(COALESCE({$quotedColumn}, 1)) > LOG(:$maxKey) THEN :$maxKey
                        ELSE POWER(COALESCE({$quotedColumn}, 0), :$bindKey)
                    END";
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
            $message = $e->getMessage();
            if (\str_contains($message, '_index1')) {
                return new DuplicateException('Duplicate permissions for document', $e->getCode(), $e);
            }
            if (! \str_contains($message, '_uid')) {
                return new DuplicateException('Document with the requested unique attributes already exists', $e->getCode(), $e);
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
}
