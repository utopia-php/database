<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Swoole\Database\PDOStatementProxy;
use Throwable;
use Utopia\Database\Adapter\SQL\Wkt;
use Utopia\Database\Attribute;
use Utopia\Database\Builder\PostgreSQL as PostgreSQLBuilder;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Mismatch as MismatchException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Hook\PermissionFilter;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\PDOStatement as DatabasePDOStatement;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Schema\Column as SchemaColumn;
use Utopia\Database\Schema\Index as SchemaIndex;
use Utopia\Database\Storage;
use Utopia\Database\Validator\ObjectPath;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Builder\Statement;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\PostgreSQL as PostgreSQLSchema;

/**
 * Differences between MariaDB and Postgres
 *
 * 1. Need to use CASCADE to DROP schema
 * 2. Quotes are different ` vs "
 * 3. DATETIME is TIMESTAMP
 * 4. Full-text search is different - to_tsvector() and to_tsquery()
 */
class Postgres extends SQL implements Feature\Spatial, Feature\Timeouts
{
    use Timeout;

    public const MAX_IDENTIFIER_NAME = 63;

    protected const string MIN_DATETIME = '-4713-01-01 00:00:00';

    private const string QUOTED_IDENTIFIER = '/["\x{AB}\x{BB}\x{201C}\x{201D}\x{201E}\x{300C}\x{300D}][\s\x{A0}\x{202F}]*([^"\x{AB}\x{BB}\x{201C}\x{201D}\x{201E}\x{300C}\x{300D}]+?)[\s\x{A0}\x{202F}]*["\x{AB}\x{BB}\x{201C}\x{201D}\x{201E}\x{300C}\x{300D}]/u';

    private const string HASHED_IDENTIFIER = '/^[0-9a-f]{32}(?:_[A-Za-z0-9_-]+)?$/';

    /**
     * The catalog's format_type() spellings mapped onto getSqlType()'s.
     *
     * @var array<string, string>
     */
    private const array CATALOG_TYPE_SPELLINGS = [
        'CHARACTER VARYING' => 'VARCHAR',
        ' WITHOUT TIME ZONE' => '',
        ', ' => ',',
    ];

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
            Capability::IndexSpatialNull,
            Capability::IndexTrigram,
            Capability::IndexObject,
            Capability::SchemaIntrospection,
        ]);
    }

    public function id(): string
    {
        $result = $this->createBuilder()->fromNone()->selectRaw('pg_backend_pid()')->build();
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
     *
     * @throws DatabaseException
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        if ($this->exists($name)) {
            return true;
        }

        $schema = $this->schema();
        $sql = $schema->createDatabase($name)->query;

        $dbCreation = $this->executeStatement($sql, Event::DatabaseCreate);

        // Enable extensions — wrap in try-catch to handle concurrent creation race conditions
        foreach (['postgis', 'vector', 'pg_trgm'] as $ext) {
            try {
                $this->executeStatement($schema->createExtension($ext)->query, Event::DatabaseCreate);
            } catch (PDOException) {
                // Extension may already exist due to concurrent worker
            }
        }

        try {
            $collation = $schema->createCollation('utf8_ci_ai', [
                'provider' => 'icu',
                'locale' => 'und-u-ks-level1',
            ], deterministic: false);
            $this->executeStatement($collation->query, Event::DatabaseCreate);
        } catch (PDOException) {
            // Collation may already exist due to concurrent worker
        }

        return $dbCreation;
    }

    /**
     * A Postgres database is a schema, which renames in place with everything it holds. Shared tables refuse
     * the rename: other tenants' rows share the schema.
     *
     * @throws DatabaseException
     */
    public function update(string $name, string $new): bool
    {
        if ($this->getSharedTables()) {
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

        try {
            return $this->execute($this->prepareStatement("ALTER SCHEMA {$this->quote($name)} RENAME TO {$this->quote($new)}"));
        } catch (PDOException $error) {
            throw $this->processException($error);
        }
    }

    #[\Override]
    public function exists(string $database): bool
    {
        $statement = $this->prepareStatement('SELECT "schema_name" FROM information_schema.schemata WHERE "schema_name" = ?', Event::DatabaseList);
        $statement->bindValue(1, $this->filter($database));

        return $this->returnsRows($statement);
    }

    #[\Override]
    public function collectionExists(string $database, string $collection): bool
    {
        $statement = $this->prepareStatement('SELECT "table_name" FROM information_schema.tables WHERE "table_schema" = ? AND "table_name" = ?', Event::CollectionRead);
        $statement->bindValue(1, $this->filter($database));
        $statement->bindValue(2, $this->getPhysicalTableName($collection));

        return $this->returnsRows($statement);
    }

    /**
     * @throws DatabaseException
     */
    private function returnsRows(PDOStatement|DatabasePDOStatement|PDOStatementProxy $statement): bool
    {
        try {
            $this->execute($statement);
            $rows = $statement->fetchAll();
            $statement->closeCursor();
        } catch (PDOException $error) {
            throw $this->processException($error);
        }

        return ! empty($rows);
    }

    /**
     * Create Collection
     *
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     *
     * @throws DuplicateException
     */
    public function createCollection(string $collection, array $attributes = [], array $indexes = []): bool
    {
        $namespace = $this->getNamespace();
        $id = $this->filter($collection);
        $tableRaw = $this->getTableRaw($id);
        $permissionsTableRaw = $this->getTableRaw(Storage::permissionsTable($id));

        $schema = $this->schema();

        $table = $schema->table($tableRaw);
        $table->id(Storage::SEQUENCE);
        $table->string(Storage::UID, 255);

        if ($this->sharedTables) {
            $table->integer(Storage::TENANT)->nullable()->default(null);
        }

        $table->datetime(Storage::CREATED_AT, 3)->nullable()->default(null);
        $table->datetime(Storage::UPDATED_AT, 3)->nullable()->default(null);

        foreach ($attributes as $attribute) {
            if (self::storesColumn($attribute)) {
                $this->addAttributeColumn($table, $attribute);
            }
        }

        $table->json(Storage::PERMISSIONS)->nullable()->default(null);
        $collectionResult = $table->create();

        $indexStatements = [];

        if ($this->sharedTables) {
            $uidIndex = $this->getShortKey("{$namespace}_{$this->currentTenant()}_{$id}".Storage::UID);
            $createdIndex = $this->getShortKey("{$namespace}_{$this->currentTenant()}_{$id}_created");
            $updatedIndex = $this->getShortKey("{$namespace}_{$this->currentTenant()}_{$id}_updated");
            $tenantIdIndex = $this->getShortKey("{$namespace}_{$this->currentTenant()}_{$id}".Storage::INDEX_TENANT_ID);
            $permissionsIndex = $this->getShortKey("{$namespace}_{$this->currentTenant()}_{$id}".Storage::PERMISSIONS);
            $indexStatements[] = $schema->createIndex($tableRaw, $uidIndex, [Storage::UID, Storage::TENANT], unique: true, collations: [Storage::UID => 'utf8_ci_ai'])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $createdIndex, [Storage::TENANT, Storage::CREATED_AT])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $updatedIndex, [Storage::TENANT, Storage::UPDATED_AT])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $tenantIdIndex, [Storage::TENANT, Storage::SEQUENCE])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $permissionsIndex, [Storage::PERMISSIONS], method: 'gin')->query;
        } else {
            $uidIndex = $this->getShortKey("{$namespace}_{$id}".Storage::UID);
            $createdIndex = $this->getShortKey("{$namespace}_{$id}_created");
            $updatedIndex = $this->getShortKey("{$namespace}_{$id}_updated");
            $permissionsIndex = $this->getShortKey("{$namespace}_{$id}".Storage::PERMISSIONS);
            $indexStatements[] = $schema->createIndex($tableRaw, $uidIndex, [Storage::UID], unique: true, collations: [Storage::UID => 'utf8_ci_ai'])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $createdIndex, [Storage::CREATED_AT])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $updatedIndex, [Storage::UPDATED_AT])->query;
            $indexStatements[] = $schema->createIndex($tableRaw, $permissionsIndex, [Storage::PERMISSIONS], method: 'gin')->query;
        }

        $collectionSql = $collectionResult->query.'; '.implode('; ', $indexStatements);

        $permissionsTable = $schema->table($permissionsTableRaw);
        $permissionsTable->id(Storage::SEQUENCE);
        $permissionsTable->integer(Storage::TENANT)->nullable()->default(null);
        $permissionsTable->string(Storage::PERM_TYPE, 12);
        $permissionsTable->string(Storage::PERM_PERMISSION, 255);
        $permissionsTable->string(Storage::PERM_DOCUMENT, 255);
        $permissionsResult = $permissionsTable->create();

        $permissionsIndexStatements = [];

        if ($this->sharedTables) {
            $uniquePermissionIndex = $this->getShortKey("{$namespace}_{$this->currentTenant()}_{$id}_ukey");
            $permissionIndex = $this->getShortKey("{$namespace}_{$this->currentTenant()}_{$id}_permission");
            $permissionsIndexStatements[] = $schema->createIndex($permissionsTableRaw, $uniquePermissionIndex, [Storage::TENANT, Storage::PERM_DOCUMENT, Storage::PERM_TYPE, Storage::PERM_PERMISSION], unique: true, method: 'btree')->query;
            $permissionsIndexStatements[] = $schema->createIndex($permissionsTableRaw, $permissionIndex, [Storage::TENANT, Storage::PERM_PERMISSION, Storage::PERM_TYPE], method: 'btree')->query;
        } else {
            $uniquePermissionIndex = $this->getShortKey("{$namespace}_{$id}_ukey");
            $permissionIndex = $this->getShortKey("{$namespace}_{$id}_permission");
            $permissionsIndexStatements[] = $schema->createIndex($permissionsTableRaw, $uniquePermissionIndex, [Storage::PERM_DOCUMENT, Storage::PERM_TYPE, Storage::PERM_PERMISSION], unique: true, method: 'btree', collations: [Storage::PERM_DOCUMENT => 'utf8_ci_ai'])->query;
            $permissionsIndexStatements[] = $schema->createIndex($permissionsTableRaw, $permissionIndex, [Storage::PERM_PERMISSION, Storage::PERM_TYPE], method: 'btree')->query;
        }

        $permissionsSql = $permissionsResult->query.'; '.implode('; ', $permissionsIndexStatements);

        $created = false;

        try {
            $this->executeStatement($collectionSql, Event::CollectionCreate);
            $created = true;
            $this->executeStatement($permissionsSql, Event::CollectionCreate);

            foreach ($indexes as $index) {
                $indexAttributesWithType = [];
                foreach ($index->attributes as $indexAttribute) {
                    $baseAttribute = \explode('.', $indexAttribute, 2)[0];
                    foreach ($attributes as $attribute) {
                        if ($attribute->key === $baseAttribute) {
                            $indexAttributesWithType[$indexAttribute] = $attribute->type->value;
                        }
                    }
                }
                if ($index->type === IndexType::Spatial && $index->orders !== []) {
                    throw new DatabaseException('Spatial indexes with explicit orders are not supported. Remove the orders to create this index.');
                }
                $this->createIndex(
                    $id,
                    $index->withKey($this->filter($index->key)),
                    $indexAttributesWithType,
                    event: Event::CollectionCreate,
                );
            }
        } catch (Throwable $error) {
            if ($error instanceof PDOException) {
                $error = $this->processException($error);
            }

            if ($created && ! ($error instanceof DuplicateException)) {
                $this->discardCreatedCollection($id);
            }

            throw $error;
        }

        return true;
    }

    /**
     * Refresh the planner statistics of a collection's table and its permissions table.
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function analyzeCollection(string $collection): bool
    {
        $name = $this->filter($collection);
        $schema = $this->schema();

        $main = $schema->analyzeTable($this->getTableRaw($name));
        $permissions = $schema->analyzeTable($this->getTableRaw(Storage::permissionsTable($name)));

        try {
            return $this->executeStatement($main->query.'; '.$permissions->query, Event::CollectionUpdate);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Get Collection Size on disk
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        $collection = $this->filter($collection);
        $name = $this->getTable($collection);
        $permissions = $this->getTable(Storage::permissionsTable($collection));

        $builder = $this->createBuilder();

        $collectionResult = $builder->fromNone()->selectRaw('pg_total_relation_size(?)', [$name])->build();
        $permissionsResult = $builder->reset()->fromNone()->selectRaw('pg_total_relation_size(?)', [$permissions])->build();

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
        $name = $this->getTable($collection);
        $permissions = $this->getTable(Storage::permissionsTable($collection));

        $builder = $this->createBuilder();

        $collectionResult = $builder->fromNone()->selectRaw('pg_relation_size(?)', [$name])->build();
        $permissionsResult = $builder->reset()->fromNone()->selectRaw('pg_relation_size(?)', [$permissions])->build();

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
        self::assertVectorDimensions($attribute);

        $this->refuseSharedColumnsOfAnotherType($collection, [$attribute]);

        $schema = $this->schema();
        $table = $schema->table($this->getTableRaw($collection));
        $this->addAttributeColumn($table, $attribute);
        $result = $table->alter();

        // Postgres does not support LOCK= on ALTER TABLE, so no lock type appended
        $sql = $result->query;

        try {
            return $this->executeStatement($sql, Event::AttributeCreate);
        } catch (PDOException $error) {
            throw $this->processException($error);
        }
    }

    /**
     * @param  list<Attribute>  $attributes
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function createAttributes(string $collection, array $attributes): bool
    {
        $this->refuseSharedColumnsOfAnotherType($collection, $attributes);

        return parent::createAttributes($collection, $attributes);
    }

    /**
     * @param  array<Attribute>  $attributes
     *
     * @throws MismatchException
     * @throws DatabaseException
     */
    private function refuseSharedColumnsOfAnotherType(string $collection, array $attributes): void
    {
        if (! $this->sharedTables) {
            return;
        }

        $statement = $this->prepareStatement(
            'SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a WHERE a.attrelid = to_regclass(?) AND a.attnum > 0 AND NOT a.attisdropped',
            Event::CollectionRead,
        );
        $statement->bindValue(1, $this->getTable($this->filter($collection)));

        try {
            $this->execute($statement);
            /** @var array<string, string> $columns */
            $columns = $statement->fetchAll(PDO::FETCH_KEY_PAIR);
            $statement->closeCursor();
        } catch (PDOException $error) {
            throw $this->processException($error);
        }

        foreach ($attributes as $attribute) {
            $existing = $columns[$this->filter($attribute->key)] ?? null;
            if ($existing === null) {
                continue;
            }

            $requested = $this->getAttributeSqlType($attribute);
            if ($this->canonicalColumnType($existing) !== $this->canonicalColumnType($requested)) {
                throw new MismatchException('Attribute exists in the shared table with another type');
            }
        }
    }

    /**
     * Update Attribute
     *
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, string $key, Attribute $attribute): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($key);
        $newKey = $attribute->key === $key ? null : $this->filter($attribute->key);

        self::assertVectorDimensions($attribute);

        $schema = $this->schema();

        if (! empty($newKey) && $this->isRenamed($collection, $id, $newKey)) {
            $id = $newKey;
            $newKey = null;
        }

        if (! empty($newKey) && $id !== $newKey) {
            $newKey = $this->filter($newKey);

            $renameTable = $schema->table($this->getTableRaw($collection));
            $renameTable->renameColumn($id, $newKey);
            $renameResult = $renameTable->alter();

            $sql = $renameResult->query;

            try {
                $result = $this->executeStatement($sql, Event::AttributeUpdate);
            } catch (PDOException $error) {
                throw $this->processException($error);
            }

            if (! $result) {
                return false;
            }

            $id = $newKey;
        }

        $sqlType = $this->getAttributeSqlType($attribute);
        $tableRaw = $this->getTableRaw($name);

        if ($sqlType == 'TIMESTAMP(3)') {
            $result = $schema->alterColumnType($tableRaw, $id, 'TIMESTAMP(3)', $this->quote($id).'::TIMESTAMP(3)');
        } else {
            $result = $schema->alterColumnType($tableRaw, $id, $sqlType);
        }

        $sql = $result->query;

        try {
            $ok = $this->executeStatement($sql, Event::AttributeUpdate);

            // Postgres carries NOT NULL through ALTER COLUMN ... TYPE, so an
            // attribute that stops being required keeps a constraint its
            // definition no longer claims. Only the relaxing direction is
            // applied: tightening would fail against rows already holding
            // null, and MySQL does not tighten on update either.
            if ($ok && ! $attribute->required) {
                $nullable = $schema->alterColumnNullable($tableRaw, $id, true);
                $ok = $this->executeStatement($nullable->query, Event::AttributeUpdate);
            }

            return $ok;
        } catch (PDOException $error) {
            throw $this->processException($error);
        }
    }

    /**
     * @throws DatabaseException
     */
    private static function assertVectorDimensions(Attribute $attribute): void
    {
        if ($attribute->type !== ColumnType::Vector) {
            return;
        }

        $dimensions = $attribute->size ?? 0;
        if ($dimensions <= 0) {
            throw new DatabaseException('Vector dimensions must be a positive integer');
        }

        if ($dimensions > Database::MAX_VECTOR_DIMENSIONS) {
            throw new DatabaseException('Vector dimensions cannot exceed '.Database::MAX_VECTOR_DIMENSIONS);
        }
    }

    public function relaxAttributeRequired(string $collection, string $id): bool
    {
        $schema = $this->schema();
        $statement = $schema->alterColumnNullable(
            $this->getTableRaw($this->filter($collection)),
            $this->filter($id),
            true,
        );

        try {
            return $this->executeStatement($statement->query, Event::AttributeUpdate);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * @throws DatabaseException
     */
    public function deleteAttribute(string $collection, string $key): bool
    {
        $schema = $this->schema();
        $table = $schema->table($this->getTableRaw($collection));
        $table->dropColumn($this->filter($key));
        $result = $table->alter();

        $sql = $result->query;

        try {
            return $this->executeStatement($sql, Event::AttributeDelete);
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
        if ($this->isRenamed($collection, $old, $new)) {
            return true;
        }

        $schema = $this->schema();
        $table = $schema->table($this->getTableRaw($collection));
        $table->renameColumn($this->filter($old), $this->filter($new));
        $result = $table->alter();

        $sql = $result->query;

        try {
            return $this->executeStatement($sql, Event::AttributeUpdate);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * @return array<string>
     *
     * @throws DatabaseException
     */
    protected function getColumnNames(string $collection): array
    {
        $statement = $this->prepareStatement(
            'SELECT a.attname FROM pg_attribute a WHERE a.attrelid = to_regclass(?) AND a.attnum > 0 AND NOT a.attisdropped',
            Event::CollectionRead,
        );
        $statement->bindValue(1, $this->getTable($collection));

        try {
            $this->execute($statement);
            /** @var array<string> $columns */
            $columns = $statement->fetchAll(PDO::FETCH_COLUMN);
            $statement->closeCursor();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $columns;
    }

    /**
     * @return list<SchemaColumn>
     *
     * @throws DatabaseException
     */
    public function getSchemaAttributes(string $collection): array
    {
        $statement = $this->prepareStatement(
            'SELECT a.attname AS name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                CASE WHEN a.atttypid IN (1042, 1043) AND a.atttypmod > 4 THEN a.atttypmod - 4 END AS length,
                NOT a.attnotnull AS nullable
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = to_regclass(?) AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum',
            Event::CollectionRead,
        );
        $statement->bindValue(1, $this->getTable($collection));

        try {
            $this->execute($statement);
            $rows = $statement->fetchAll(PDO::FETCH_ASSOC);
            $statement->closeCursor();
        } catch (PDOException $e) {
            throw $this->processException($e);
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
                nullable: self::isTrue($row['nullable'] ?? false),
            );
        }

        return $columns;
    }

    /**
     * Under shared tables every tenant keeps its own copy of an index, named after it: the current tenant's are
     * reported under their keys, any other index under its physical name.
     *
     * @return list<SchemaIndex>
     *
     * @throws DatabaseException
     */
    public function getSchemaIndexes(string $collection): array
    {
        $statement = $this->prepareStatement(
            'SELECT i.relname AS name,
                x.indisunique AS "unique",
                am.amname AS method,
                (SELECT o.opcname FROM pg_catalog.pg_opclass o WHERE o.oid = x.indclass[k.position - 1]) AS operator,
                pg_catalog.pg_get_indexdef(x.indexrelid, k.position, true) AS "column"
            FROM pg_catalog.pg_index x
            JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
            JOIN pg_catalog.pg_am am ON am.oid = i.relam
            CROSS JOIN LATERAL pg_catalog.generate_series(1, x.indnkeyatts) AS k(position)
            WHERE x.indrelid = to_regclass(?)
            ORDER BY i.relname, k.position',
            Event::CollectionRead,
        );
        $statement->bindValue(1, $this->getTable($collection));

        try {
            $this->execute($statement);
            $rows = $statement->fetchAll(PDO::FETCH_ASSOC);
            $statement->closeCursor();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $prefix = "{$this->getNamespace()}_{$this->currentTenant()}_{$this->filter($collection)}_";

        $grouped = [];
        foreach ($rows as $row) {
            if (! \is_array($row) || ! \is_string($row['name'] ?? null)) {
                continue;
            }

            $name = $row['name'];
            if (! isset($grouped[$name])) {
                $operator = $row['operator'] ?? '';
                $grouped[$name] = [
                    'type' => match (true) {
                        self::isTrue($row['unique'] ?? false) => IndexType::Unique,
                        $operator === 'gin_trgm_ops' => IndexType::Trigram,
                        $operator === 'vector_l2_ops' => IndexType::HnswEuclidean,
                        $operator === 'vector_cosine_ops' => IndexType::HnswCosine,
                        $operator === 'vector_ip_ops' => IndexType::HnswDot,
                        ($row['method'] ?? '') === 'gin' => IndexType::Object,
                        ($row['method'] ?? '') === 'gist' => IndexType::Spatial,
                        default => IndexType::Key,
                    },
                    'columns' => [],
                ];
            }

            $column = \is_string($row['column'] ?? null) ? $row['column'] : '';
            if (\preg_match('/^"(.*)"$/s', $column, $matches) === 1) {
                $column = \str_replace('""', '"', $matches[1]);
            }
            $grouped[$name]['columns'][] = $column;
        }

        $indexes = [];
        foreach ($grouped as $name => $index) {
            $name = (string) $name;
            $indexes[] = new SchemaIndex(
                name: \str_starts_with($name, $prefix) ? \substr($name, \strlen($prefix)) : $name,
                type: $index['type'],
                columns: $index['columns'],
                lengths: \array_fill(0, \count($index['columns']), null),
            );
        }

        return $indexes;
    }

    #[\Override]
    protected function canonicalColumnType(string $type): string
    {
        return \strtr(\strtoupper(\trim($type)), self::CATALOG_TYPE_SPELLINGS);
    }

    private static function isTrue(mixed $value): bool
    {
        return $value === true || $value === 't' || $value === 1 || $value === '1';
    }

    /**
     * Create Index
     *
     * @param  array<string,string>  $indexAttributeTypes
     * @param  array<string, mixed>  $collation
     */
    public function createIndex(
        string $collection,
        Index $index,
        array $indexAttributeTypes = [],
        array $collation = [],
        Event $event = Event::IndexCreate,
    ): bool {
        $collection = $this->filter($collection);
        $id = $this->filter($index->key);
        $type = $index->type;

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

        $keyName = $this->getIndexName($collection, $id, $this->currentTenant());
        $tableRaw = $this->getTableRaw($collection);
        $schema = $this->schema();

        $operatorClass = match ($type) {
            IndexType::HnswEuclidean => 'vector_l2_ops',
            IndexType::HnswCosine => 'vector_cosine_ops',
            IndexType::HnswDot => 'vector_ip_ops',
            IndexType::Trigram => 'gin_trgm_ops',
            default => '',
        };

        $columns = [];
        foreach ($index->attributes as $position => $attribute) {
            $isNestedPath = isset($indexAttributeTypes[$attribute]) && \str_contains($attribute, '.') && $indexAttributeTypes[$attribute] === ColumnType::Object->value;
            $column = $isNestedPath
                ? $this->buildJsonbPath($attribute, true)
                : $this->quote($this->filter($this->getInternalKeyForAttribute($attribute)));
            $order = $type === IndexType::Fulltext ? '' : ($index->orders[$position]->value ?? '');

            $columns[] = $column
                .($operatorClass !== '' ? ' '.$operatorClass : '')
                .($order !== '' ? ' '.$order : '');
        }

        if ($this->sharedTables && \in_array($type, [IndexType::Key, IndexType::Unique])) {
            \array_unshift($columns, $this->quote(Storage::TENANT));
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

        $sql = $schema->createIndex(
            $tableRaw,
            $keyName,
            [],
            unique: $unique,
            method: $method,
            rawColumns: $columns,
        )->query;

        try {
            return $this->executeStatement($sql, $event);
        } catch (PDOException $error) {
            throw $this->processException($error);
        }
    }

    /**
     * @throws Exception
     */
    public function deleteIndex(string $collection, string $key): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($key);

        $keyName = $this->getIndexName($collection, $id, $this->currentTenant());
        $schemaQualifiedName = $this->getDatabase().'.'.$keyName;

        $schema = $this->schema();
        $sql = $schema->dropIndex($this->getTableRaw($collection), $schemaQualifiedName)->query;
        // Add IF EXISTS since the schema builder's dropIndex does not include it
        $sql = str_replace('DROP INDEX', 'DROP INDEX IF EXISTS', $sql);

        return $this->executeStatement($sql, Event::IndexDelete);
    }

    /**
     * Rename Index
     *
     * Reports the index renamed when the schema holds it under the new name afterwards. Under shared tables an
     * index is named after the tenant that created it, so a tenant without its own copy is renamed in its metadata
     * when another tenant's copy of the collection's index exists under the old or the new name.
     *
     * @throws Exception
     * @throws PDOException
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $name = $this->filter($collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $oldIndexName = $this->getIndexName($name, $old, $this->currentTenant());
        $newIndexName = $this->getIndexName($name, $new, $this->currentTenant());

        $schemaBuilder = $this->schema();
        $sql = $schemaBuilder->renameIndex($this->getTableRaw($name), $this->getDatabase().'.'.$oldIndexName, $newIndexName)->query;
        $sql = \str_replace('ALTER INDEX', 'ALTER INDEX IF EXISTS', $sql);

        $this->executeStatement($sql, Event::IndexRename);

        $names = [$newIndexName];
        if ($this->sharedTables) {
            foreach ($this->getCollectionTenants($collection) as $tenant) {
                \array_push($names, $this->getIndexName($name, $old, $tenant), $this->getIndexName($name, $new, $tenant));
            }
        }

        return $this->anyIndexExists($names);
    }

    private function getIndexName(string $collection, string $id, int|string|null $tenant): string
    {
        return $this->getShortKey("{$this->getNamespace()}_{$tenant}_{$collection}_{$id}");
    }

    /**
     * @return list<string>
     *
     * @throws DatabaseException
     */
    private function getCollectionTenants(string $collection): array
    {
        $statement = $this->prepareStatement(
            'SELECT DISTINCT '.$this->quote(Storage::TENANT).' FROM '.$this->getTable(Database::METADATA).' WHERE '.$this->quote(Storage::UID).' = ?',
            Event::IndexRename,
        );
        $statement->bindValue(1, $collection);

        try {
            $this->execute($statement);
            /** @var list<string> $tenants */
            $tenants = $statement->fetchAll(PDO::FETCH_COLUMN);
            $statement->closeCursor();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $tenants;
    }

    /**
     * @param  list<string>  $names
     *
     * @throws DatabaseException
     */
    private function anyIndexExists(array $names): bool
    {
        $names = \array_values(\array_unique($names));
        $placeholders = \implode(', ', \array_fill(0, \count($names), '?'));
        $statement = $this->prepareStatement(
            "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relkind = 'i' AND c.relname IN ({$placeholders})",
            Event::IndexRename,
        );
        $statement->bindValue(1, $this->getDatabase());
        foreach ($names as $position => $indexName) {
            $statement->bindValue($position + 2, $indexName);
        }

        try {
            $this->execute($statement);
            $found = $statement->fetchAll(PDO::FETCH_COLUMN);
            $statement->closeCursor();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $found !== [];
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
            $attributes[Storage::CREATED_AT] = $document->getCreatedAt();
            $attributes[Storage::UPDATED_AT] = $document->getUpdatedAt();
            $attributes[Storage::PERMISSIONS] = \json_encode($document->getPermissions());

            $name = $this->filter($collection);

            $builder = $this->createBuilder()->into($this->getTableRaw($name));

            $row = [Storage::UID => $document->getId()];
            if (! empty($document->getSequence())) {
                $row[Storage::SEQUENCE] = $document->getSequence();
            }

            foreach ($spatialAttributes as $spatialCol) {
                $builder->insertColumnExpression($spatialCol, $this->getSpatialGeometryFromText('?'));
            }

            $spatialMap = \array_fill_keys($spatialAttributes, true);

            foreach ($attributes as $attr => $value) {
                $column = $this->filter($attr);

                if (isset($spatialMap[$attr])) {
                    $row[$column] = $this->encodeSpatialWriteValue($value);
                    $builder->insertColumnExpression($column, $this->getSpatialGeometryFromText('?'));
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
            $statement = $this->executeResult($result, Event::DocumentCreate);

            $this->execute($statement);
            $lastInsertedId = $this->getDriver()->lastInsertId();
            $document[Document::SEQUENCE] ??= $lastInsertedId;

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
            $attributes[Storage::CREATED_AT] = $document->getCreatedAt();
            $attributes[Storage::UPDATED_AT] = $document->getUpdatedAt();
            $attributes[Storage::PERMISSIONS] = \json_encode($document->getPermissions());

            $name = $this->filter($collection);

            $operators = [];
            foreach ($attributes as $attribute => $value) {
                if (Operator::isOperator($value)) {
                    $operators[$attribute] = $value;
                }
            }

            $builder = $this->newBuilder($name);
            $row = [];
            if ($document->getId() !== $id) {
                $row[Storage::UID] = $document->getId();
            }

            $spatialMap = \array_fill_keys($spatialAttributes, true);

            foreach ($attributes as $attribute => $value) {
                $column = $this->filter($attribute);

                if (isset($operators[$attribute])) {
                    $operation = $operators[$attribute];
                    if ($operation instanceof Operator) {
                        $opResult = $this->getOperatorBuilderExpression($column, $operation);
                        $builder->setRaw($column, $opResult['expression'], $opResult['bindings']);
                    }
                } elseif (isset($spatialMap[$attribute])) {
                    $builder->setRaw($column, $this->getSpatialGeometryFromText('?'), [$this->encodeSpatialWriteValue($value)]);
                } else {
                    if (\is_array($value)) {
                        $value = \json_encode($value);
                    }
                    $row[$column] = $value;
                }
            }

            $builder->set($row);
            $filters = [BaseQuery::equal(Storage::SEQUENCE, [$document->getSequence()])];
            $builder->filter($filters);
            $result = $builder->update();
            $statement = $this->executeResult($result, Event::DocumentUpdate);

            $this->execute($statement);

            $ctx = $this->buildWriteContext($name, $id);
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

        $this->setTimeoutState($milliseconds, $event);
    }

    public function clearTimeout(Event $event = Event::All): void
    {
        $this->clearTimeoutState($event);
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

    public function encode(mixed $value, ColumnType $type): string
    {
        return Wkt::encode($value, $type);
    }

    /**
     * Decode a WKB or WKT POINT into a coordinate array [x, y].
     *
     * @param string $wkb The WKB hex or WKT string
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
    protected function decodeLinestring(mixed $wkb): array
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

    /**
     * The LOCAL statement timeout in force in the open transaction, in milliseconds:
     * 0 is the default, null is unknown after a rollback to a savepoint.
     */
    private ?int $localTimeout = 0;

    public function commitTransaction(): bool
    {
        try {
            return parent::commitTransaction();
        } finally {
            if ($this->inTransaction === 0) {
                $this->localTimeout = 0;
            }
        }
    }

    public function rollbackTransaction(): bool
    {
        try {
            return parent::rollbackTransaction();
        } finally {
            $this->localTimeout = $this->inTransaction === 0 ? 0 : null;
        }
    }

    public function reconnect(): void
    {
        $this->localTimeout = null;
        parent::reconnect();
        $this->localTimeout = 0;
    }

    /**
     * @param  PDOStatement|DatabasePDOStatement|PDOStatementProxy  $statement
     */
    protected function execute(mixed $statement, ?Event $event = null): bool
    {
        $event ??= $this->getStatementEvent($statement);
        $timeout = $event === null ? $this->getTimeout() : $this->getTimeout($event);

        if ($this->inTransaction > 0) {
            $this->applyLocalTimeout($timeout);

            return $this->executeAndProfile($statement);
        }

        $this->localTimeout = 0;

        if ($timeout === 0) {
            return $this->executeAndProfile($statement);
        }

        $pdo = $this->getDriver();
        $pdo->exec("SET statement_timeout = '{$timeout}ms'");

        $exception = null;
        try {
            return $this->executeAndProfile($statement);
        } catch (Throwable $error) {
            $exception = $error;
            throw $error;
        } finally {
            try {
                $pdo->exec('RESET statement_timeout');
            } catch (Throwable $error) {
                if ($exception === null) {
                    throw $error;
                }
            }
        }
    }

    private function applyLocalTimeout(int $milliseconds): void
    {
        if ($milliseconds === $this->localTimeout) {
            return;
        }

        $this->getDriver()->exec($milliseconds === 0
            ? 'SET LOCAL statement_timeout = DEFAULT'
            : "SET LOCAL statement_timeout = '{$milliseconds}ms'");

        $this->localTimeout = $milliseconds;
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

        return 'CASE WHEN target.'.Storage::TENANT.' = EXCLUDED.'.Storage::TENANT." THEN EXCLUDED.{$quoted} ELSE target.{$quoted} END";
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

        return 'CASE WHEN target.'.Storage::TENANT.' = EXCLUDED.'.Storage::TENANT." THEN target.{$quoted} + EXCLUDED.{$quoted} ELSE target.{$quoted} END";
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
        $fullExpression = $this->getOperatorSql($column, $operator, $bindIndex, useTargetPrefix: true);

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
                    $namedBindings["op_{$idx}"] = self::exactLimit($values[1]);
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
                    $namedBindings["op_{$idx}"] = self::exactLimit($values[1]);
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
     * Get SQL Type
     */
    protected function createBuilder(): SQLBuilder
    {
        return new PostgreSQLBuilder();
    }

    #[\Override]
    public function schema(): PostgreSQLSchema
    {
        return new PostgreSQLSchema();
    }

    protected function getSqlType(ColumnType $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        if ($array === true) {
            return 'JSONB';
        }

        return match ($type) {
            ColumnType::Id => 'BIGINT',
            ColumnType::String => $size <= 0 || $size > $this->limits()->varchar ? 'TEXT' : "VARCHAR({$size})",
            ColumnType::Varchar => "VARCHAR({$size})",
            ColumnType::Text,
            ColumnType::MediumText,
            ColumnType::LongText => 'TEXT',
            ColumnType::Integer => $size >= 8 ? 'BIGINT' : 'INTEGER',
            ColumnType::BigInteger => 'BIGINT',
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
     * Get PDO Type
     *
     *
     * @throws DatabaseException
     */
    protected function getPdoType(mixed $value): int
    {
        return match (\gettype($value)) {
            'string', 'double' => PDO::PARAM_STR,
            'boolean' => PDO::PARAM_BOOL,
            'integer' => PDO::PARAM_INT,
            'NULL' => PDO::PARAM_NULL,
            default => throw new DatabaseException('Unknown PDO Type for '.\gettype($value)),
        };
    }

    protected function getNullOrder(): OrderDirection
    {
        return OrderDirection::Desc;
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

    #[\Override]
    protected function getSqlReadableDistance(string $distance): string
    {
        return "{$distance}::text";
    }

    /**
     * Match read permissions against the JSONB copy stored on each row. This
     * keeps PostgreSQL free to combine the GIN permission index with ordering
     * indexes instead of resolving a permissions-table semi-join first.
     *
     * @param  array<string>  $roles
     */
    #[\Override]
    protected function newPermissionHook(string $collection, array $roles, string $type = PermissionType::Read->value, string $documentColumn = Storage::UID): PermissionFilter
    {
        return new class (\array_values($roles), $type, $documentColumn) extends PermissionFilter {
            /**
             * @param  list<string>  $roles
             */
            public function __construct(array $roles, string $type, string $documentColumn)
            {
                parent::__construct(
                    roles: $roles,
                    permissionsTable: static fn (string $table): string => $table,
                    type: $type,
                    documentColumn: $documentColumn,
                    quoteCharacter: '"',
                );
            }

            #[\Override]
            public function filter(string $table): Condition
            {
                if (empty($this->roles)) {
                    return new Condition('1 = 0');
                }

                $parts = \explode('.', $this->documentColumn);
                $parts[\array_key_last($parts)] = Storage::PERMISSIONS;
                $column = \implode('.', \array_map(
                    static fn (string $part): string => '"'.\str_replace('"', '""', $part).'"',
                    $parts
                ));

                $conditions = [];
                $bindings = [];
                foreach ($this->roles as $role) {
                    $conditions[] = "{$column} @> ?::jsonb";
                    $bindings[] = \json_encode(["{$this->type}(\"{$role}\")"]) ?: '[]';
                }

                return new Condition('('.\implode(' OR ', $conditions).')', $bindings);
            }
        };
    }

    /**
     * Size of POINT spatial type
     */
    protected function getMaxPointSize(): int
    {
        // https://stackoverflow.com/questions/30455025/size-of-data-type-geographypoint-4326-in-postgis
        return 32;
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
            $columns = $this->getViolatedColumns($e->getMessage());
            if ($columns !== null && $columns !== [Storage::UID] && $columns !== [Storage::TENANT, Storage::UID]) {
                return new UniqueException(UniqueException::MESSAGE, $e->getCode(), $e);
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

        // Invalid argument for power function
        if ($e->getCode() === '2201F' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new LimitException('Invalid argument for power function', $e->getCode(), $e);
        }

        // Datetime field overflow
        if ($e->getCode() === '22008' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new LimitException('Datetime field overflow', $e->getCode(), $e);
        }

        if ($e->getCode() === '42P01' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            if ($this->isUndefinedAlias($e->getMessage())) {
                return new QueryException('Query references an undefined table or alias', $e->getCode(), $e);
            }

            return new NotFoundException('Collection not found', $e->getCode(), $e);
        }

        // Unknown column
        if ($e->getCode() === '42703' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new NotFoundException('Attribute not found', $e->getCode(), $e);
        }

        if (
            $e->getCode() === '42P10'
            && isset($e->errorInfo[1])
            && $e->errorInfo[1] === 7
            && \str_contains($e->getMessage(), 'for SELECT DISTINCT, ORDER BY expressions must appear in select list')
        ) {
            return new QueryException('A distinct() query can only be ordered by a selected attribute on this database', $e->getCode(), $e);
        }

        if ($e->getCode() === '40P01' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new ContentionException('Deadlock detected', $e->getCode(), $e);
        }

        if ($e->getCode() === '40001' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new ContentionException('Could not serialize access due to a concurrent update', $e->getCode(), $e);
        }

        if ($e->getCode() === '55P03' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new ContentionException('Lock not available', $e->getCode(), $e);
        }

        if ($e->getCode() === '22021' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new CharacterException('Invalid character', $e->getCode(), $e);
        }

        if ($e->getCode() === '42883' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new QueryException('Query applies a function or operator the attribute type does not support', $e->getCode(), $e);
        }

        return $e;
    }

    #[\Override]
    protected function processSelectException(PDOException $e, Statement $statement): Exception
    {
        if (
            $e->getCode() === '42P10'
            && isset($e->errorInfo[1])
            && $e->errorInfo[1] === 7
            && \str_starts_with($statement->query, 'SELECT DISTINCT ')
        ) {
            return new QueryException('A distinct() query can only be ordered by a selected attribute on this database', $e->getCode(), $e);
        }

        return parent::processSelectException($e, $statement);
    }

    /**
     * Whether a 42P01 names something other than a table of this namespace, whatever the server's
     * language. A statement names a missing table with its schema and a DROP without one, but only a
     * statement reports a position, so an unqualified name followed by one is an alias.
     */
    protected function isUndefinedAlias(string $message): bool
    {
        $message = \rtrim($message);
        $firstLine = \explode("\n", $message, 2)[0];
        if (\preg_match(self::QUOTED_IDENTIFIER, $firstLine, $matches) !== 1) {
            return false;
        }

        $name = $matches[1];
        $separator = \strrpos($name, '.');
        $relation = $separator === false ? $name : \substr($name, $separator + 1);

        if (! \str_starts_with($relation, $this->getNamespace().'_') && \preg_match(self::HASHED_IDENTIFIER, $relation) !== 1) {
            return true;
        }

        return $separator === false && \str_contains($message, "\n");
    }

    /**
     * Extract the columns named by a PostgreSQL unique-violation DETAIL line.
     *
     * @return list<string>|null
     */
    protected function getViolatedColumns(string $message): ?array
    {
        if (\preg_match('/Key \(([^)]+)\)=/', $message, $matches) !== 1) {
            return null;
        }

        $columns = \array_map(
            static fn (string $column): string => \trim($column, " \t\"'"),
            \explode(',', $matches[1])
        );

        \sort($columns);

        return $columns;
    }

    protected function quote(string $string): string
    {
        return '"'.\str_replace('"', '""', $string).'"';
    }

    protected function getIdentifierQuote(): string
    {
        return '"';
    }

    /**
     * Only a stored id is skipped; a row colliding on another unique index still fails with
     * Unique, as a bare ON CONFLICT DO NOTHING would skip it silently.
     */
    #[\Override]
    protected function insertOrIgnore(SQLBuilder $builder): Statement
    {
        $insert = $builder->insert();
        $target = \implode(', ', \array_map($this->quote(...), $this->documentKeyColumns()));

        return new Statement($insert->query.' ON CONFLICT ('.$target.') DO NOTHING', $insert->bindings);
    }

    /**
     * Get SQL expression for operator
     */
    protected function getOperatorSql(string $column, Operator $operator, int &$bindIndex, bool $useTargetPrefix = false): ?string
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
                        WHEN COALESCE({$columnRef}, 0) + CAST(:$bindKey AS NUMERIC) > CAST(:$maxKey AS NUMERIC) THEN COALESCE({$columnRef}, 0)
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
                        WHEN COALESCE({$columnRef}, 0) - CAST(:$bindKey AS NUMERIC) < CAST(:$minKey AS NUMERIC) THEN COALESCE({$columnRef}, 0)
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
                        WHEN COALESCE({$columnRef}, 0) * CAST(:$bindKey AS NUMERIC) > CAST(:$maxKey AS NUMERIC) THEN COALESCE({$columnRef}, 0)
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
                        WHEN CAST(:$bindKey AS NUMERIC) != 0 AND COALESCE({$columnRef}, 0) / CAST(:$bindKey AS NUMERIC) < CAST(:$minKey AS NUMERIC) THEN COALESCE({$columnRef}, 0)
                        ELSE COALESCE({$columnRef}, 0) / CAST(:$bindKey AS NUMERIC)
                    END";
                }

                return "{$quotedColumn} = COALESCE({$columnRef}, 0) / :$bindKey";

            case OperatorType::Modulo:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = MOD(COALESCE({$columnRef}::numeric, 0), :$bindKey::numeric)";

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

                    $columnValue = "COALESCE({$columnRef}, 0)";
                    $oddInteger = \floor($exponent) == $exponent && ((int) $exponent) % 2 !== 0;
                    $guards = [];

                    if ($exponent < 0) {
                        $guards[] = "WHEN {$columnValue} = 0 THEN {$columnValue}";
                    }
                    if (\floor($exponent) != $exponent) {
                        $guards[] = "WHEN {$columnValue} < 0 THEN {$columnValue}";
                    }
                    if ($exponent == 0) {
                        $guards[] = "WHEN LN(:$maxKey) < 0 THEN {$columnValue}";
                    } elseif ($oddInteger) {
                        $guards[] = "WHEN {$columnValue} > 0 AND :$bindKey * LN({$columnValue}) > LN(:$maxKey) THEN {$columnValue}";
                    } else {
                        $guards[] = "WHEN {$columnValue} <> 0 AND :$bindKey * LN(ABS({$columnValue})) > LN(:$maxKey) THEN {$columnValue}";
                    }

                    return "{$quotedColumn} = CASE ".\implode(' ', $guards)." ELSE POWER({$columnValue}, :$bindKey) END";
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

    protected function getPhysicalTableName(string $name): string
    {
        return $this->getShortKey("{$this->getNamespace()}_{$this->filter($name)}");
    }

    #[\Override]
    protected function getTable(string $name): string
    {
        return "{$this->quote($this->getDatabase())}.{$this->quote($this->getPhysicalTableName($name))}";
    }

    #[\Override]
    protected function getTableRaw(string $name): string
    {
        return $this->getDatabase().'.'.$this->getPhysicalTableName($name);
    }

    protected function buildJsonbPath(string $path, bool $asText = false): string
    {
        $parts = \explode('.', $path);

        foreach ($parts as $part) {
            if (\preg_match(ObjectPath::KEY_PATTERN, $part) !== 1) {
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
