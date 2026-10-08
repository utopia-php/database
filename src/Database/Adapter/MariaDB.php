<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Swoole\Database\PDOProxy;
use Swoole\Database\PDOStatementProxy;
use Throwable;
use Utopia\Database\Adapter\SQL\Wkt;
use Utopia\Database\Attribute;
use Utopia\Database\Builder\MariaDB as MariaDBBuilder;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Contention as ContentionException;
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
use Utopia\Database\PDO as DatabasePDO;
use Utopia\Database\PDOStatement as DatabasePDOStatement;
use Utopia\Database\Query;
use Utopia\Database\Schema\Column as SchemaColumn;
use Utopia\Database\Schema\Index as SchemaIndex;
use Utopia\Database\Storage;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\MySQL as MySQLSchema;

/**
 * Database adapter for MariaDB, extending the base SQL adapter with MariaDB-specific features.
 */
class MariaDB extends SQL implements Feature\Spatial, Feature\Timeouts
{
    use Timeout;

    /**
     * Get the list of capabilities supported by the MariaDB adapter.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        return array_merge(parent::capabilities(), [
            Capability::IntegerBooleans,
            Capability::AlterLock,
            Capability::IndexFulltextWildcard,
            Capability::IndexSpatialOrder,
            Capability::NonUtfCharacters,
            Capability::SchemaIntrospection,
            Capability::UpsertOnUniqueIndex,
            Capability::UnsignedBigInt,
        ]);
    }

    public function id(): string
    {
        $result = $this->createBuilder()->fromNone()->selectRaw('CONNECTION_ID()')->build();
        $statement = $this->prepareStatement($result->query);

        if (! $statement->execute()) {
            return '';
        }

        $column = $statement->fetchColumn();

        return \is_scalar($column) ? (string) $column : '';
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

        $result = $this->schema()->createDatabase($name);
        $sql = $result->query;

        return $this->executeStatement($sql, Event::DatabaseCreate);
    }

    /**
     * MariaDB and MySQL cannot rename a database, so every table moves into a new one in a single atomic
     * `RENAME TABLE` and the emptied database is dropped. Grants on the old database do not move. A table
     * created in the old database during the rename keeps it from being dropped. Shared tables refuse the
     * rename: other tenants' rows share the database.
     *
     * @throws DatabaseException
     */
    public function update(string $name, string $new): bool
    {
        if ($this->hasSharedTables()) {
            throw new DatabaseException('Cannot rename a database while shared tables are enabled');
        }

        $name = $this->filter($name);
        $new = $this->filter($new);

        if (! $this->exists($name)) {
            throw new NotFoundException('Database not found');
        }

        if ($this->exists($new)) {
            throw new DuplicateException('Database already exists');
        }

        $tables = $this->getTables($name);
        $schema = $this->schema();

        $this->executeStatement($schema->createDatabase($new)->query, Event::DatabaseCreate);

        if ($tables !== []) {
            $moves = \array_map(
                fn (string $table): string => "{$this->quote($name)}.{$this->quote($table)} TO {$this->quote($new)}.{$this->quote($table)}",
                $tables,
            );

            try {
                $this->execute($this->prepareStatement('RENAME TABLE '.\implode(', ', $moves)));
            } catch (Throwable $error) {
                $this->executeStatement($schema->dropDatabase($new)->query, Event::DatabaseDelete);

                throw $error instanceof PDOException ? $this->processException($error) : $error;
            }
        }

        if ($this->getTables($name) !== []) {
            throw new DatabaseException("Database {$name} was renamed to {$new} but holds tables created during the rename, so it was not dropped");
        }

        return $this->executeStatement($schema->dropDatabase($name)->query, Event::DatabaseDelete);
    }

    /**
     * @return list<string>
     *
     * @throws DatabaseException
     */
    private function getTables(string $database): array
    {
        $result = $this->createBuilder()
            ->from('INFORMATION_SCHEMA.TABLES')
            ->selectRaw('TABLE_NAME')
            ->filter([BaseQuery::equal('TABLE_SCHEMA', [$database])])
            ->build();

        $statement = $this->executeResult($result, Event::DatabaseList);

        try {
            $this->execute($statement);
            $rows = $statement->fetchAll();
            $statement->closeCursor();
        } catch (PDOException $error) {
            throw $this->processException($error);
        }

        $tables = [];
        foreach ($rows as $row) {
            $table = \is_array($row) ? ($row['TABLE_NAME'] ?? $row['table_name'] ?? null) : null;
            if (\is_string($table)) {
                $tables[] = $table;
            }
        }

        return $tables;
    }

    /**
     * Create Collection
     *
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     *
     * @throws Exception
     * @throws PDOException
     */
    public function createCollection(string $collection, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->filter($collection);
        $schema = $this->schema();
        $sharedTables = $this->sharedTables;

        $hash = [];
        foreach ($attributes as $attribute) {
            $hash[$this->filter($attribute->key)] = $attribute;
        }

        $table = $schema->table($this->getTableRaw($id));
        $table->id(Storage::SEQUENCE);
        $table->string(Storage::UID, 255);
        $table->datetime(Storage::CREATED_AT, 3)->nullable()->default(null);
        $table->datetime(Storage::UPDATED_AT, 3)->nullable()->default(null);
        $table->mediumText(Storage::PERMISSIONS)->nullable()->default(null);

        foreach ($attributes as $attribute) {
            if (! self::storesColumn($attribute)) {
                continue;
            }

            $table->rawColumn('`'.$this->filter($attribute->key).'` '.$this->getAttributeSqlType($attribute));
        }

        foreach ($indexes as $index) {
            $indexId = $this->filter($index->key);
            $indexType = $index->type;
            $indexColumns = [];

            foreach ($index->attributes as $nested => $attribute) {
                $indexOrder = $index->orders[$nested]->value ?? '';

                if ($indexType === IndexType::Spatial && ! $this->supports(Capability::IndexSpatialOrder) && ! empty($indexOrder)) {
                    throw new DatabaseException('Spatial indexes with explicit orders are not supported. Remove the orders to create this index.');
                }

                $indexAttribute = $this->filter($this->getInternalKeyForAttribute($attribute));

                $indexColumns[] = $this->compileIndexColumn(
                    $indexAttribute,
                    isset($hash[$indexAttribute]) && $hash[$indexAttribute]->array,
                    $index->lengths[$nested] ?? 0,
                    $indexType === IndexType::Fulltext ? '' : $indexOrder,
                );
            }

            if ($sharedTables && $indexType !== IndexType::Fulltext && $indexType !== IndexType::Spatial) {
                \array_unshift($indexColumns, $this->quote(Storage::TENANT));
            }

            $table->addIndex($indexId, [], $indexType, rawColumns: $indexColumns);
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

        $permissionsTable = $schema->table($this->getTableRaw(Storage::permissionsTable($id)));
        $permissionsTable->id(Storage::SEQUENCE);
        $permissionsTable->string(Storage::PERMISSIONS_TYPE, 12);
        $permissionsTable->string(Storage::PERMISSIONS_PERMISSION, 255);
        $permissionsTable->string(Storage::PERMISSIONS_DOCUMENT, 255);

        if ($sharedTables) {
            $permissionsTable->integer(Storage::TENANT)->unsigned()->nullable()->default(null);
            $permissionsTable->uniqueIndex([Storage::PERMISSIONS_DOCUMENT, Storage::TENANT, Storage::PERMISSIONS_TYPE, Storage::PERMISSIONS_PERMISSION], Storage::INDEX_1);
            $permissionsTable->index([Storage::TENANT, Storage::PERMISSIONS_PERMISSION, Storage::PERMISSIONS_TYPE], Storage::PERMISSIONS_PERMISSION);
        } else {
            $permissionsTable->uniqueIndex([Storage::PERMISSIONS_DOCUMENT, Storage::PERMISSIONS_TYPE, Storage::PERMISSIONS_PERMISSION], Storage::INDEX_1);
            $permissionsTable->index([Storage::PERMISSIONS_PERMISSION, Storage::PERMISSIONS_TYPE], Storage::PERMISSIONS_PERMISSION);
        }

        $permissionsResult = $permissionsTable->create();
        $permissions = $permissionsResult->query;

        $created = false;

        try {
            $this->executeStatement($collection, Event::CollectionCreate);
            $created = true;
            $this->executeStatement($permissions, Event::CollectionCreate);
        } catch (PDOException $exception) {
            $error = $this->processException($exception);

            if ($created && ! $error instanceof DuplicateException) {
                $this->discardCreatedCollection($id);
            }

            throw $error;
        }

        return true;
    }

    /**
     * @throws Exception
     * @throws PDOException
     */
    public function deleteCollection(string $collection): bool
    {
        $id = $this->filter($collection);

        $schema = $this->schema();
        $main = $schema->table($this->getTableRaw($id))->drop();
        $permissions = $schema->table($this->getTableRaw(Storage::permissionsTable($id)))->dropIfExists();

        try {
            return $this->executeStatement($main->query.'; '.$permissions->query, Event::CollectionDelete);
        } catch (PDOException $e) {
            $error = $this->processException($e);
            if ($error instanceof NotFoundException) {
                $this->executeStatement($permissions->query, Event::CollectionDelete);
            }

            throw $error;
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

        $result = $this->schema()->analyzeTable($this->getTableRaw($name));
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

        $result = $this->createBuilder()
            ->fromNone()
            ->selectRaw(
                'SUM(size) FROM (
                    SELECT data_length + index_length AS size
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE table_name = ? AND
                    table_schema = ?
                    UNION ALL
                    SELECT data_length + index_length AS size
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE table_name = ? AND
                    table_schema = ?
                ) AS sizes',
                [$collection, $database, $permissions, $database]
            )
            ->build();

        $statement = $this->executeResult($result, Event::CollectionRead);

        try {
            $this->execute($statement);
            $size = $statement->fetchColumn();
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }

        return (int) (\is_numeric($size) ? $size : 0);
    }

    /**
     * MariaDB has no column SRID attribute: MySQL's `SRID n` column syntax is a parse error there.
     */
    #[\Override]
    protected function getSpatialColumnSrid(): ?int
    {
        return null;
    }

    /**
     * Update Attribute
     *
     * @throws DatabaseException
     */
    public function updateAttribute(string $collection, string $key, Attribute $attribute): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($key);
        $newKey = $attribute->key === $key ? null : $this->filter($attribute->key);
        $sqlType = $this->getAttributeSqlType($attribute);
        $schema = $this->schema();
        $tableRaw = $this->getTableRaw($name);

        if (! empty($newKey) && $this->isRenamed($collection, $id, $newKey)) {
            $id = $newKey;
            $newKey = null;
        }

        if (! empty($newKey)) {
            $result = $schema->changeColumn($tableRaw, $id, $newKey, $sqlType);
        } else {
            $result = $schema->modifyColumn($tableRaw, $id, $sqlType);
        }

        $sql = $result->query;

        try {
            return $this->executeStatement($sql, Event::AttributeUpdate);
        } catch (PDOException $error) {
            throw $this->processException($error);
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

        $collectionAttributes = self::collectionAttributes($collection);
        $id = $this->filter($index->key);
        $type = $index->type;

        $schema = $this->schema();
        $tableName = $this->getTableRaw($collection->getId());

        $columns = [];
        foreach ($index->attributes as $position => $key) {
            $array = false;
            foreach ($collectionAttributes as $collectionAttribute) {
                if (\strtolower($collectionAttribute->key) === \strtolower($key)) {
                    $array = $collectionAttribute->array;
                    break;
                }
            }

            $columns[] = $this->compileIndexColumn(
                $this->filter($this->getInternalKeyForAttribute($key)),
                $array,
                $index->lengths[$position] ?? 0,
                $type === IndexType::Fulltext ? '' : ($index->orders[$position]->value ?? ''),
            );
        }

        if ($this->sharedTables && $type !== IndexType::Fulltext && $type !== IndexType::Spatial) {
            \array_unshift($columns, $this->quote(Storage::TENANT));
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
            [],
            unique: $unique,
            type: $schemaType,
            rawColumns: $columns,
        );
        $sql = $result->query;

        try {
            return $this->executeStatement($sql, Event::IndexCreate);
        } catch (PDOException $error) {
            throw $this->processException($error);
        }
    }

    /**
     * Render one key part of an index. Parts are rendered in the caller's order and handed to the
     * schema builder as raw columns, because it places raw columns after all named ones.
     */
    private function compileIndexColumn(string $column, bool $array, int $length, string $order): string
    {
        if ($array && $this->supports(Capability::IndexArrayCast)) {
            return '(CAST('.$this->quote($column).' AS char('.Database::MAX_ARRAY_INDEX_LENGTH.') ARRAY))';
        }

        $part = $this->quote($column);
        if ($length > 0) {
            $part .= '('.$length.')';
        }
        if ($order !== '') {
            $part .= ' '.$order;
        }

        return $part;
    }

    /**
     * @throws Exception
     * @throws PDOException
     */
    public function deleteIndex(string $collection, string $key): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($key);

        $schema = $this->schema();
        $result = $schema->dropIndex($this->getTableRaw($name), $id);

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

        $result = $this->schema()->renameIndex($this->getTableRaw($collection), $old, $new);
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
            $name = $this->filter($collection);

            // Build document INSERT using query builder
            // Spatial columns use insertColumnExpression() for ST_GeomFromText() wrapping
            $builder = $this->createBuilder()->into($this->getTableRaw($name));
            $row = [Storage::UID => $document->getId()];

            if (! empty($document->getSequence())) {
                $row[Storage::SEQUENCE] = $document->getSequence();
            }

            $spatialMap = \array_fill_keys($spatialAttributes, true);

            foreach ($attributes as $attribute => $value) {
                $column = $this->filter($attribute);

                if (isset($spatialMap[$attribute])) {
                    $value = $this->encodeSpatialWriteValue($value);
                    $value = (\is_bool($value)) ? (int) $value : $value;
                    $row[$column] = $value;
                    $builder->insertColumnExpression($column, $this->getSpatialGeometryFromText('?'));
                } else {
                    if (\is_array($value)) {
                        $value = \json_encode($value);
                    }
                    $value = (\is_bool($value)) ? (int) $value : $value;
                    $row[$column] = $value;
                }
            }

            $row = $this->decorateRow($row, $document);
            $builder->set($row);
            $result = $builder->insert();
            $statement = $this->executeResult($result, Event::DocumentCreate);

            $this->execute($statement);

            $document[Document::SEQUENCE] = $this->getDriver()->lastInsertId();

            if (empty($document[Document::SEQUENCE])) {
                throw new DatabaseException('Error creating document empty "'.Document::SEQUENCE.'"');
            }

            $context = $this->writeContext();
            try {
                $this->runWriteHooks(fn ($hook) => $hook->afterDocumentCreate($name, [$document], $context));
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
                $cleanupBuilder->filter([BaseQuery::equal(Storage::PERMISSIONS_DOCUMENT, [$document->getId()])]);
                $cleanupResult = $cleanupBuilder->delete();
                $cleanupStmt = $this->executeResult($cleanupResult, Event::PermissionsDelete);
                $this->execute($cleanupStmt);

                $this->runWriteHooks(fn ($hook) => $hook->afterDocumentCreate($name, [$document], $context));
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

            $name = $this->filter($collection);

            $operators = [];
            foreach ($attributes as $attribute => $value) {
                if (Operator::isOperator($value)) {
                    $operators[$attribute] = $value;
                }
            }

            $builder = $this->newBuilder($name);
            $regularRow = [];
            if ($document->getId() !== $id) {
                $regularRow[Storage::UID] = $document->getId();
            }

            $spatialMap = \array_fill_keys($spatialAttributes, true);

            foreach ($attributes as $attribute => $value) {
                $column = $this->filter($attribute);

                if (isset($operators[$attribute])) {
                    $operation = $operators[$attribute];
                    if ($operation instanceof Operator) {
                        $expression = $this->getOperatorBuilderExpression($column, $operation);
                        $builder->setRaw($column, $expression->sql, $expression->bindings);
                    }
                } elseif (isset($spatialMap[$attribute])) {
                    $value = $this->encodeSpatialWriteValue($value);
                    $value = (\is_bool($value)) ? (int) $value : $value;
                    $builder->setRaw($column, $this->getSpatialGeometryFromText('?'), [$value]);
                } else {
                    if (\is_array($value)) {
                        $value = \json_encode($value);
                    }
                    $value = (\is_bool($value)) ? (int) $value : $value;
                    $regularRow[$column] = $value;
                }
            }

            $builder->set($regularRow);
            $filters = [BaseQuery::equal(Storage::SEQUENCE, [$document->getSequence()])];
            $builder->filter($filters);
            $result = $builder->update();
            $statement = $this->executeResult($result, Event::DocumentUpdate);

            $this->execute($statement);

            $context = $this->writeContext($skipPermissions ? [$document->getId() => true] : []);
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentUpdate($name, $id, $document, $context));
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

    public function encode(mixed $value, ColumnType $type): string
    {
        return Wkt::encode($value, $type);
    }

    /**
     * @return array<mixed>
     */
    public function decode(string $value, ColumnType $type): array
    {
        return match ($type) {
            ColumnType::Point => $this->decodePoint($value),
            ColumnType::Linestring => $this->decodeLinestring($value),
            ColumnType::Polygon => $this->decodePolygon($value),
            default => throw new DatabaseException('Unknown spatial type: '.$type->value),
        };
    }

    /**
     * Decode a WKB or WKT POINT into a coordinate array [x, y].
     *
     * @param string $wkb The WKB binary or WKT string
     * @return array<float>
     *
     * @throws DatabaseException If the input is invalid.
     */
    protected function decodePoint(string $wkb): array
    {
        if (str_starts_with(strtoupper($wkb), 'POINT(')) {
            $start = strpos($wkb, '(') + 1;
            $end = strrpos($wkb, ')');
            $inside = substr($wkb, $start, $end - $start);
            $coords = explode(' ', trim($inside));

            return [(float) $coords[0], (float) $coords[1]];
        }

        /**
         * [0..3]   SRID (4 bytes, little-endian)
         * [4]      Byte order (1 = little-endian, 0 = big-endian)
         * [5..8]   Geometry type (with SRID flag bit)
         * [9..]    Geometry payload (coordinates, etc.)
         */
        if (strlen($wkb) < 25) {
            throw new DatabaseException('Invalid WKB: too short for POINT');
        }

        // 4 bytes SRID first → skip to byteOrder at offset 4
        $byteOrder = ord($wkb[4]);
        $littleEndian = ($byteOrder === 1);

        if (! $littleEndian) {
            throw new DatabaseException('Only little-endian WKB supported');
        }

        // After SRID (4) + byteOrder (1) + type (4) = 9 bytes
        $coordsBin = substr($wkb, 9, 16);
        if (strlen($coordsBin) !== 16) {
            throw new DatabaseException('Invalid WKB: missing coordinate bytes');
        }

        // Unpack two doubles
        $coords = unpack('d2', $coordsBin);
        if ($coords === false || ! isset($coords[1], $coords[2])) {
            throw new DatabaseException('Invalid WKB: failed to unpack coordinates');
        }

        return [(float) (is_numeric($coords[1]) ? $coords[1] : 0), (float) (is_numeric($coords[2]) ? $coords[2] : 0)];
    }

    /**
     * Decode a WKB or WKT LINESTRING into an array of coordinate pairs.
     *
     * @param string $wkb The WKB binary or WKT string
     * @return array<array<float>>
     *
     * @throws DatabaseException If the input is invalid.
     */
    protected function decodeLinestring(string $wkb): array
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

        // Skip 1 byte (endianness) + 4 bytes (type) + 4 bytes (SRID)
        $offset = 9;

        // Number of points (4 bytes little-endian)
        $numPointsArr = unpack('V', substr($wkb, $offset, 4));
        if ($numPointsArr === false || ! isset($numPointsArr[1])) {
            throw new DatabaseException('Invalid WKB: cannot unpack number of points');
        }

        $numPoints = $numPointsArr[1];
        $offset += 4;

        $points = [];
        for ($i = 0; $i < $numPoints; $i++) {
            $xArr = unpack('d', substr($wkb, $offset, 8));
            $yArr = unpack('d', substr($wkb, $offset + 8, 8));

            if ($xArr === false || ! isset($xArr[1]) || $yArr === false || ! isset($yArr[1])) {
                throw new DatabaseException('Invalid WKB: cannot unpack point coordinates');
            }

            $points[] = [(float) (is_numeric($xArr[1]) ? $xArr[1] : 0), (float) (is_numeric($yArr[1]) ? $yArr[1] : 0)];
            $offset += 16;
        }

        return $points;
    }

    /**
     * Decode a WKB or WKT POLYGON into an array of rings, each containing coordinate pairs.
     *
     * @param string $wkb The WKB binary or WKT string
     * @return array<array<array<float>>>
     *
     * @throws DatabaseException If the input is invalid.
     */
    protected function decodePolygon(string $wkb): array
    {
        // POLYGON((x1,y1),(x2,y2))
        if (str_starts_with($wkb, 'POLYGON((')) {
            $start = strpos($wkb, '((') + 2;
            $end = strrpos($wkb, '))');
            $inside = substr($wkb, $start, $end - $start);

            $rings = \preg_split('/\)\s*,\s*\(/', $inside) ?: [$inside];

            return array_map(function ($ring) {
                $points = explode(',', $ring);

                return array_map(function ($point) {
                    $coords = explode(' ', trim($point));

                    return [(float) $coords[0], (float) $coords[1]];
                }, $points);
            }, $rings);
        }

        // Convert HEX string to binary if needed
        if (str_starts_with($wkb, '0x') || ctype_xdigit($wkb)) {
            $wkb = hex2bin(str_starts_with($wkb, '0x') ? substr($wkb, 2) : $wkb);
            if ($wkb === false) {
                throw new DatabaseException('Invalid hex WKB');
            }
        }

        if (strlen($wkb) < 21) {
            throw new DatabaseException('WKB too short to be a POLYGON');
        }

        // MySQL SRID-aware WKB layout: 4 bytes SRID prefix
        $offset = 4;

        $byteOrder = ord($wkb[$offset]);
        if ($byteOrder !== 1) {
            throw new DatabaseException('Only little-endian WKB supported');
        }
        $offset += 1;

        $typeArr = unpack('V', substr($wkb, $offset, 4));
        if ($typeArr === false || ! isset($typeArr[1])) {
            throw new DatabaseException('Invalid WKB: cannot unpack geometry type');
        }

        $type = \is_numeric($typeArr[1]) ? (int) $typeArr[1] : 0;
        $hasSRID = ($type & 0x20000000) === 0x20000000;
        $geomType = $type & 0xFF;
        $offset += 4;

        if ($geomType !== 3) { // 3 = POLYGON
            throw new DatabaseException("Not a POLYGON geometry type, got {$geomType}");
        }

        // Skip SRID in type flag if present
        if ($hasSRID) {
            $offset += 4;
        }

        $numRingsArr = unpack('V', substr($wkb, $offset, 4));

        if ($numRingsArr === false || ! isset($numRingsArr[1])) {
            throw new DatabaseException('Invalid WKB: cannot unpack number of rings');
        }

        $numRings = $numRingsArr[1];
        $offset += 4;

        $rings = [];

        for ($r = 0; $r < $numRings; $r++) {
            $numPointsArr = unpack('V', substr($wkb, $offset, 4));

            if ($numPointsArr === false || ! isset($numPointsArr[1])) {
                throw new DatabaseException('Invalid WKB: cannot unpack number of points');
            }

            $numPoints = $numPointsArr[1];
            $offset += 4;
            $ring = [];

            for ($p = 0; $p < $numPoints; $p++) {
                $xArr = unpack('d', substr($wkb, $offset, 8));
                if ($xArr === false) {
                    throw new DatabaseException('Failed to unpack X coordinate from WKB.');
                }

                $x = (float) (is_numeric($xArr[1]) ? $xArr[1] : 0);

                $yArr = unpack('d', substr($wkb, $offset + 8, 8));
                if ($yArr === false) {
                    throw new DatabaseException('Failed to unpack Y coordinate from WKB.');
                }

                $y = (float) (is_numeric($yArr[1]) ? $yArr[1] : 0);

                $ring[] = [$x, $y];
                $offset += 16;
            }

            $rings[] = $ring;
        }

        return $rings;
    }

    private const string TIMEOUT_SETTING = 'timeout';

    /** The session timeout last set, in milliseconds. */
    private int $appliedTimeout = 0;

    /**
     * The Swoole PDOProxy round the timeout was set in: the proxy's reconnects open
     * sessions at the server default, while Utopia\Database\PDO replays the timeout
     * on the sessions its reconnects open.
     */
    private int $appliedRound = 0;

    /**
     * @param  PDOStatement|DatabasePDOStatement|PDOStatementProxy  $statement
     */
    protected function execute(mixed $statement, ?Event $event = null): bool
    {
        $event ??= $this->getStatementEvent($statement);
        $baseline = $this->getTimeout();
        $timeout = $event === null ? $baseline : $this->getTimeout($event);
        $this->applyTimeout($timeout);

        $exception = null;
        try {
            return parent::execute($statement, $event);
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
        if ($milliseconds === 0 && $this->appliedTimeout === 0) {
            return;
        }

        $round = $this->getSessionRound();
        if ($round !== $this->appliedRound) {
            $this->appliedTimeout = 0;
            $this->appliedRound = $round;
        }

        if ($milliseconds === $this->appliedTimeout) {
            return;
        }

        $statement = $this->getTimeoutStatement($milliseconds);
        $driver = $this->getDriver();
        if ($driver instanceof DatabasePDO) {
            $driver->configure(self::TIMEOUT_SETTING, $statement);
        } else {
            $driver->exec($statement);
        }

        $this->appliedTimeout = $milliseconds;
        $this->appliedRound = $this->getSessionRound();
    }

    protected function getTimeoutStatement(int $milliseconds): string
    {
        return 'SET max_statement_time = '.\sprintf('%.6F', $milliseconds / 1000.0);
    }

    private function getSessionRound(): int
    {
        $driver = $this->getDriver();

        return $driver instanceof PDOProxy ? $driver->getRound() : 0;
    }

    protected function getConflictTenantExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));
        $tenant = Storage::TENANT;

        return "IF({$tenant} = VALUES({$tenant}), VALUES({$quoted}), {$quoted})";
    }

    protected function getConflictIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));

        return "{$quoted} + VALUES({$quoted})";
    }

    protected function getConflictTenantIncrementExpression(string $column): string
    {
        $quoted = $this->quote($this->filter($column));
        $tenant = Storage::TENANT;

        return "IF({$tenant} = VALUES({$tenant}), {$quoted} + VALUES({$quoted}), {$quoted})";
    }

    protected function createBuilder(): SQLBuilder
    {
        return new MariaDBBuilder();
    }

    #[\Override]
    protected function boundsJoinedSort(): bool
    {
        return true;
    }

    #[\Override]
    public function schema(): MySQLSchema
    {
        return new MySQLSchema();
    }

    /**
     * @return list<SchemaColumn>
     *
     * @throws DatabaseException
     */
    public function getSchemaAttributes(string $collection): array
    {
        $schema = $this->getDatabase();
        $table = $this->getNamespace().'_'.$this->filter($collection);

        try {
            $statement = $this->prepareStatement('
                SELECT
                    COLUMN_NAME AS name,
                    COLUMN_TYPE AS type,
                    CHARACTER_MAXIMUM_LENGTH AS length,
                    IS_NULLABLE AS nullable
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
                ORDER BY ORDINAL_POSITION
            ', Event::CollectionRead);
            $statement->bindParam(':schema', $schema);
            $statement->bindParam(':table', $table);
            $this->execute($statement);
            $rows = $statement->fetchAll(PDO::FETCH_ASSOC);
            $statement->closeCursor();
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get schema attributes', $e->getCode(), $e);
        }

        $columns = [];
        foreach ($rows as $row) {
            if (! \is_array($row) || ! \is_string($row['name'] ?? null)) {
                continue;
            }

            $type = $row['type'] ?? '';
            $length = $row['length'] ?? null;
            $columns[] = new SchemaColumn(
                name: $row['name'],
                type: $this->canonicalColumnType(\is_string($type) ? $type : ''),
                length: \is_numeric($length) ? (int) $length : null,
                nullable: ($row['nullable'] ?? '') === 'YES',
            );
        }

        return $columns;
    }

    /**
     * @return array<string>
     *
     * @throws DatabaseException
     */
    protected function getColumnNames(string $collection): array
    {
        return \array_map(
            static fn (SchemaColumn $column): string => $column->name,
            $this->getSchemaAttributes($collection),
        );
    }

    /**
     * Get operator SQL
     * Override to handle MariaDB/MySQL-specific operators
     */
    protected function getOperatorSql(string $column, Operator $operator, int &$bindIndex): ?string
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

    /**
     * @return list<SchemaIndex>
     *
     * @throws DatabaseException
     */
    public function getSchemaIndexes(string $collection): array
    {
        $schema = $this->getDatabase();
        $table = $this->getNamespace().'_'.$this->filter($collection);

        try {
            $statement = $this->prepareStatement('
                SELECT
                    INDEX_NAME AS name,
                    COLUMN_NAME AS columnName,
                    NON_UNIQUE AS nonUnique,
                    INDEX_TYPE AS indexType,
                    SUB_PART AS subPart
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            ', Event::CollectionRead);
            $statement->bindParam(':schema', $schema);
            $statement->bindParam(':table', $table);
            $this->execute($statement);
            $rows = $statement->fetchAll(PDO::FETCH_ASSOC);
            $statement->closeCursor();
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get schema indexes', $e->getCode(), $e);
        }

        $grouped = [];
        foreach ($rows as $row) {
            if (! \is_array($row) || ! \is_string($row['name'] ?? null) || $row['name'] === '') {
                continue;
            }

            $name = $row['name'];
            if (! isset($grouped[$name])) {
                $indexType = \is_string($row['indexType'] ?? null) ? \strtoupper($row['indexType']) : '';
                $nonUnique = \is_numeric($row['nonUnique'] ?? null) ? (int) $row['nonUnique'] : 1;
                $grouped[$name] = [
                    'type' => match (true) {
                        $indexType === 'FULLTEXT' => IndexType::Fulltext,
                        $indexType === 'SPATIAL' => IndexType::Spatial,
                        $nonUnique === 0 => IndexType::Unique,
                        default => IndexType::Key,
                    },
                    'columns' => [],
                    'lengths' => [],
                ];
            }

            $subPart = $row['subPart'] ?? null;
            $grouped[$name]['columns'][] = \is_string($row['columnName'] ?? null) ? $row['columnName'] : '';
            $grouped[$name]['lengths'][] = \is_numeric($subPart) ? (int) $subPart : null;
        }

        $indexes = [];
        foreach ($grouped as $name => $index) {
            $indexes[] = new SchemaIndex((string) $name, $index['type'], $index['columns'], $index['lengths']);
        }

        return $indexes;
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
                return new UniqueException(UniqueException::MESSAGE, $e->getCode(), $e);
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

        if ($e->getCode() === '42S02' && isset($e->errorInfo[1]) && ($e->errorInfo[1] === 1051 || $e->errorInfo[1] === 1146)) {
            return new NotFoundException('Collection not found', $e->getCode(), $e);
        }

        // Unknown column
        if ($e->getCode() === '42000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1091) {
            return new NotFoundException('Attribute not found', $e->getCode(), $e);
        }

        if ($e->getCode() === '42S22' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1054) {
            return new NotFoundException('Attribute not found', $e->getCode(), $e);
        }

        if ($e->getCode() === '42000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1072) {
            return new NotFoundException('Attribute not found', $e->getCode(), $e);
        }

        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1116) {
            return new QueryException('Too many tables in a join', $e->getCode(), $e);
        }

        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1191) {
            return new QueryException('Searching requires a fulltext index on the searched attributes', $e->getCode(), $e);
        }

        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3065) {
            return new QueryException('A distinct() query can only be ordered by a selected attribute on this database', $e->getCode(), $e);
        }

        if ($e->getCode() === '40001' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1213) {
            return new ContentionException('Deadlock detected', $e->getCode(), $e);
        }

        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1205) {
            return new ContentionException('Lock wait timeout exceeded', $e->getCode(), $e);
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
