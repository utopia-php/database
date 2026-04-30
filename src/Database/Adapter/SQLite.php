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
use Utopia\Database\DateTime as DatabaseDateTime;
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
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Builder\SQLite as SQLiteBuilder;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;
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
class SQLite extends SQL
{
    /**
     * MariaDB byte ceilings for TEXT-family types, mirrored so PRAGMA-based
     * introspection produces the same characterMaximumLength values that
     * INFORMATION_SCHEMA.COLUMNS would on MariaDB.
     */
    private const MARIADB_TEXT_BYTES = '65535';
    private const MARIADB_MEDIUMTEXT_BYTES = '16777215';
    private const MARIADB_LONGTEXT_BYTES = '4294967295';

    /** Suffix appended to every FTS5 virtual table name created by this adapter. */
    private const FTS_TABLE_SUFFIX = '_fts';

    /** AFTER INSERT trigger suffix on the parent collection. */
    private const FTS_TRIGGER_INSERT = 'ai';

    /** AFTER DELETE trigger suffix on the parent collection. */
    private const FTS_TRIGGER_DELETE = 'ad';

    /** AFTER UPDATE trigger suffix on the parent collection. */
    private const FTS_TRIGGER_UPDATE = 'au';

    /**
     * Reject patterns over this size to bound ReDoS exposure — the UDF runs
     * once per candidate row, so a pathological pattern is amplified by
     * table cardinality.
     */
    private const REGEXP_MAX_PATTERN_LENGTH = 512;

    /**
     * Cap on cached delimited patterns. Long-lived adapters processing many
     * distinct user patterns would otherwise grow this map without bound.
     */
    private const REGEXP_PATTERN_CACHE_LIMIT = 256;

    /**
     * Per-collection attribute → FTS5 table memo. Populated in one pass
     * so multi-attribute SEARCH batches don't issue PRAGMA per attribute.
     *
     * @var array<string, array<string, ?string>>
     */
    private array $ftsTableCache = [];

    /**
     * When enabled, the adapter reports MariaDB-shaped column metadata,
     * advertises MariaDB-only capabilities (upserts, attribute resizing,
     * PCRE regex via the registered UDF), and declares schema-internal
     * columns (e.g. `_tenant`) using MariaDB-style types so callers that
     * inspect INFORMATION_SCHEMA-style results behave identically across
     * both adapters. Off by default — vanilla SQLite stays vanilla.
     */
    protected bool $emulateMySQL = false;

    /**
     * Whether the REGEXP UDF actually wired up. Pool/proxy PDOs may not
     * expose sqliteCreateFunction.
     */
    private bool $pcreRegistered = false;

    public function __construct(object $pdo)
    {
        parent::__construct($pdo);

        $this->registerUserFunctions();
    }

    /**
     * Return the underlying PDO with a narrowed type so static analysis
     * can resolve `prepare`, `execute`, `bindValue` etc. on every SQLite
     * call site without relying on object-typed property access.
     *
     * @return \PDO|\Utopia\Database\PDO
     */
    protected function getPDO(): object
    {
        /** @var \PDO|\Utopia\Database\PDO $pdo */
        $pdo = $this->pdo;

        return $pdo;
    }

    /**
     * Get the list of capabilities supported by the SQLite adapter.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        $remove = [
            Capability::Schemas,
            Capability::Regex,
            Capability::UpdateLock,
            Capability::QueryContains,
            Capability::Hostname,
            Capability::UpsertOnUniqueIndex,
        ];

        if (! $this->emulateMySQL) {
            $remove[] = Capability::AttributeResizing;
        }

        $extras = [
            Capability::IntegerBooleans,
            Capability::NumericCasting,
        ];

        if ($this->pcreRegistered) {
            $extras[] = Capability::PCRE;
        }

        return array_merge(
            array_values(array_filter(
                parent::capabilities(),
                fn (Capability $c) => ! in_array($c, $remove, true)
            )),
            $extras
        );
    }

    /**
     * Toggle MariaDB/MySQL emulation. See $emulateMySQL for what this
     * actually changes.
     */
    public function setEmulateMySQL(bool $emulate): static
    {
        $this->emulateMySQL = $emulate;
        // Capability set is computed from $emulateMySQL — invalidate the cache.
        $this->capabilitySet = null;

        return $this;
    }

    public function getEmulateMySQL(): bool
    {
        return $this->emulateMySQL;
    }

    public function setTenant(int|string|null $tenant): bool
    {
        $changed = $this->tenant !== $tenant;
        $result = parent::setTenant($tenant);
        if ($changed) {
            // Invalidate after the parent setter so a validation failure
            // doesn't leave us with a cleared cache against the prior tenant.
            $this->ftsTableCache = [];
        }

        return $result;
    }

    public function setNamespace(string $namespace): static
    {
        // Invalidate after the parent setter so a thrown validation
        // doesn't leave a cleared cache against the prior namespace.
        parent::setNamespace($namespace);
        $this->ftsTableCache = [];

        return $this;
    }

    public function setSharedTables(bool $sharedTables): bool
    {
        $changed = $this->sharedTables !== $sharedTables;
        $result = parent::setSharedTables($sharedTables);
        if ($changed) {
            $this->ftsTableCache = [];
        }

        return $result;
    }

    /**
     * Register a preg_match-backed REGEXP UDF so the inherited REGEXP
     * path resolves. Best-effort — non-SQLite PDOs simply skip it.
     */
    private function registerUserFunctions(): void
    {
        // SQLite invokes the UDF once per candidate row, so cache the
        // delimited pattern across rows. FIFO-evict at REGEXP_PATTERN_CACHE_LIMIT
        // entries so distinct user patterns can't grow this without bound.
        $delimitedCache = [];
        $pcre = static function (?string $pattern, ?string $value) use (&$delimitedCache): int {
            if ($pattern === null || $value === null) {
                return 0;
            }
            if (\strlen($pattern) > self::REGEXP_MAX_PATTERN_LENGTH) {
                return 0;
            }

            if (!isset($delimitedCache[$pattern])) {
                if (\count($delimitedCache) >= self::REGEXP_PATTERN_CACHE_LIMIT) {
                    \array_shift($delimitedCache);
                }
                // Use a delimiter unlikely to appear in user input (chr(1))
                // so we don't have to escape forward-slashes the user wrote
                // intentionally and risk double-escaping backslashes.
                $delimitedCache[$pattern] = "\x01" . $pattern . "\x01u";
            }
            $delimited = $delimitedCache[$pattern];

            // preg_match returns false on bad pattern / runtime error
            // (e.g. PREG_BACKTRACK_LIMIT_ERROR). Silently treat those as
            // no-match — REGEXP is a query predicate, not a validator.
            $result = @\preg_match($delimited, $value);

            return $result === 1 ? 1 : 0;
        };

        try {
            $this->getPDO()->sqliteCreateFunction('REGEXP', $pcre, 2);
            $this->pcreRegistered = true;
            // Capability::PCRE is conditional on UDF registration — invalidate cache.
            $this->capabilitySet = null;
        } catch (\Throwable) {
        }
    }

    protected function execute(mixed $stmt): bool
    {
        /** @var \PDOStatement|PDOStatementProxy $stmt */
        return $stmt->execute();
    }

    /**
     * {@inheritDoc}
     *
     * SQLite serialises writers through a single file lock. PDO's default
     * `BEGIN` is `DEFERRED`, which acquires the writer lock lazily on the
     * first write — if two transactions both started as readers and try to
     * promote to writer at the same time, one fails immediately with
     * SQLITE_BUSY without any busy_timeout retry (a real deadlock case).
     * `BEGIN IMMEDIATE` reserves the writer slot up-front so concurrent
     * writers queue behind it under busy_timeout instead.
     */
    public function startTransaction(): bool
    {
        try {
            if ($this->inTransaction === 0) {
                if ($this->getPDO()->inTransaction()) {
                    $this->getPDO()
                        ->prepare('ROLLBACK')
                        ->execute();
                }

                $result = $this->getPDO()
                    ->prepare('BEGIN IMMEDIATE')
                    ->execute();
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
     * @inheritDoc
     *
     * Overrides the inherited PDO-driven commit because startTransaction
     * issues a raw `BEGIN IMMEDIATE` (rather than PDO::beginTransaction),
     * so PDO's internal in-transaction flag is never set and PDO::commit()
     * would throw "no active transaction". Mirrors that with a raw COMMIT
     * and SAVEPOINT release for nested levels.
     */
    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        try {
            if ($this->inTransaction > 1) {
                $result = $this->getPDO()
                    ->prepare('RELEASE SAVEPOINT transaction' . ($this->inTransaction - 1))
                    ->execute();
                $this->inTransaction--;
                return $result;
            }

            $result = $this->getPDO()
                ->prepare('COMMIT')
                ->execute();
            $this->inTransaction = 0;
        } catch (PDOException $e) {
            throw new TransactionException('Failed to commit transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        return $result;
    }

    /**
     * @inheritDoc
     *
     * Counterpart to commitTransaction — uses a raw ROLLBACK for the same
     * reason (raw BEGIN IMMEDIATE bypasses PDO's transaction tracking).
     */
    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        try {
            if ($this->inTransaction > 1) {
                $this->getPDO()
                    ->prepare('ROLLBACK TO transaction' . ($this->inTransaction - 1))
                    ->execute();
                $this->inTransaction--;
            } else {
                $this->getPDO()
                    ->prepare('ROLLBACK')
                    ->execute();
                $this->inTransaction = 0;
            }
        } catch (PDOException $e) {
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to rollback transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        return true;
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

        // SQLite stores integers regardless of declared type, but
        // testSchemaAttributes asserts the columnType reads back as
        // `int(11) unsigned` to match MariaDB. Quote the declaration so
        // PRAGMA table_info echoes the exact string under emulation;
        // otherwise use INTEGER, the affinity-correct vanilla form.
        $tenantType = $this->emulateMySQL ? '"INT(11) UNSIGNED"' : 'INTEGER';
        $tenantQuery = $this->sharedTables ? "`_tenant` {$tenantType} DEFAULT NULL," : '';

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
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return true;
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
        $name = $namespace . '_' . $collection;
        $permissions = $namespace . '_' . $collection . '_perms';
        $ftsPrefix = $this->getFulltextTablePrefix($collection);

        // FTS5 storage lives in `<vtable>_data|_idx|_docsize|_config`
        // shadow tables; sum (pgsize - unused) over all of them.
        $ftsPattern = $this->escapeLikePattern($ftsPrefix) . '%' . $this->escapeLikePattern(self::FTS_TABLE_SUFFIX) . '%';

        $stmt = $this->getPDO()->prepare("
             SELECT COALESCE(SUM(\"pgsize\" - \"unused\"), 0)
             FROM \"dbstat\"
             WHERE name = :name OR name = :perms OR name LIKE :fts_pattern ESCAPE '\\';
        ");

        $stmt->bindParam(':name', $name);
        $stmt->bindParam(':perms', $permissions);
        $stmt->bindParam(':fts_pattern', $ftsPattern);

        try {
            $stmt->execute();
            $size = (int) $stmt->fetchColumn();
            $stmt->closeCursor();
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: ' . $e->getMessage());
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
     * Delete Collection
     *
     * @throws Exception
     * @throws PDOException
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        // FTS5 shadow tables don't drop with the parent.
        foreach ($this->findFulltextTables($id) as $ftsTable) {
            $sql = "DROP TABLE IF EXISTS `{$ftsTable}`";
            $this->getPDO()->prepare($sql)->execute();
        }

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id)}";

        $this->getPDO()
            ->prepare($sql)
            ->execute();

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id.'_perms')}";

        $this->getPDO()
            ->prepare($sql)
            ->execute();

        unset($this->ftsTableCache[$id]);

        return true;
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

        // SQLite is dynamically typed — `ALTER TABLE ... MODIFY COLUMN` is
        // not supported and a smaller declared size silently accepts
        // larger values. Under MySQL emulation, scan the column and
        // raise the same TruncateException MariaDB throws. Off-
        // emulation the declared size is metadata-only, so skip the
        // scan and let the rename branch (if any) handle the rest.
        if ($this->emulateMySQL && $attribute->type === ColumnType::String && $attribute->size > 0 && ! $attribute->array) {
            $name = $this->filter($collection);
            $column = $this->filter($attribute->key);

            // Under shared tables the underlying table is shared across
            // tenants; scoping the scan by `_tenant` keeps tenant A's
            // resize from being blocked (and tenant A's metadata from
            // leaking) by an oversized value owned by tenant B.
            $tenantClause = $this->sharedTables ? ' AND `_tenant` = :_tenant' : '';
            $sql = "SELECT 1 FROM {$this->getSQLTable($name)} WHERE LENGTH(`{$column}`) > :max{$tenantClause} LIMIT 1";

            $stmt = $this->getPDO()->prepare($sql);
            $stmt->bindValue(':max', $attribute->size, PDO::PARAM_INT);
            if ($this->sharedTables) {
                $stmt->bindValue(':_tenant', $this->tenant, \is_int($this->tenant) ? PDO::PARAM_INT : PDO::PARAM_STR);
            }

            try {
                $stmt->execute();
                $exceeds = $stmt->fetchColumn() !== false;
            } finally {
                $stmt->closeCursor();
            }

            if ($exceeds) {
                throw new TruncateException("Attribute '{$attribute->key}' has values exceeding new size {$attribute->size}");
            }
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
                    attributes: \array_map(fn (mixed $v): string => \is_scalar($v) ? (string) $v : '', \is_array($attributes) ? \array_values(\array_filter($attributes, fn ($v) => $v !== $id)) : []),
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

        if ($type === IndexType::Fulltext) {
            return $this->createFulltextIndex($name, $id, $attributes);
        }

        // Workaround for no support for CREATE INDEX IF NOT EXISTS
        $stmt = $this->getPDO()->prepare("
			SELECT name
			FROM sqlite_master
			WHERE type='index' AND name=:_index;
		");
        $stmt->bindValue(':_index', "{$this->getNamespace()}_{$this->getTenantSegment()}_{$name}_{$id}");
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
     * Create an FTS5 virtual table mirroring `$attributes` and the triggers
     * that keep it in sync with the parent collection.
     *
     * @param array<string> $attributes
     * @throws PDOException
     */
    protected function createFulltextIndex(string $collection, string $id, array $attributes): bool
    {
        if (empty($attributes)) {
            throw new DatabaseException('Fulltext index requires at least one attribute');
        }

        $attributes = \array_map(fn (string $a) => $this->getInternalKeyForAttribute($a), $attributes);
        $ftsTable = $this->getFulltextTableName($collection, $attributes);
        $parentTable = "{$this->getNamespace()}_{$collection}";

        $stmt = $this->getPDO()->prepare("
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name=:_table;
        ");
        $stmt->bindValue(':_table', $ftsTable);
        $stmt->execute();
        $exists = !empty($stmt->fetch());
        $stmt->closeCursor();
        if ($exists) {
            return true;
        }

        $columns = \array_map(fn (string $attr) => $this->filter($attr), $attributes);
        $ftsColumnList = \implode(', ', $columns);
        $columnList = \implode(', ', \array_map(fn (string $c) => "`{$c}`", $columns));
        $newColumnList = \implode(', ', \array_map(fn (string $c) => "NEW.`{$c}`", $columns));
        $oldColumnList = \implode(', ', \array_map(fn (string $c) => "OLD.`{$c}`", $columns));

        // Under shared tables every tenant has a distinct FTS vtable but the
        // parent table is shared, so triggers must filter by `_tenant`
        // literal — otherwise tenant A's vtable accumulates tenant B's
        // tokenized content. The same applies to the initial backfill.
        $tenantLiteral = $this->sharedTables ? $this->getTenantSqlLiteral() : null;
        $insertWhen = $tenantLiteral !== null ? " WHEN NEW.`_tenant` IS {$tenantLiteral}" : '';
        $deleteWhen = $tenantLiteral !== null ? " WHEN OLD.`_tenant` IS {$tenantLiteral}" : '';
        $updateWhen = $tenantLiteral !== null
            ? " WHEN OLD.`_tenant` IS {$tenantLiteral} OR NEW.`_tenant` IS {$tenantLiteral}"
            : '';
        $backfillWhere = $tenantLiteral !== null ? " WHERE `_tenant` IS {$tenantLiteral}" : '';

        $this->startTransaction();
        try {
            $createSql = "CREATE VIRTUAL TABLE `{$ftsTable}` USING fts5({$ftsColumnList}, content=\"{$parentTable}\", content_rowid=\"_id\")";
            $this->getPDO()->prepare($createSql)->execute();

            $insertSuffix = self::FTS_TRIGGER_INSERT;
            $insertTrigger = "
                CREATE TRIGGER `{$ftsTable}_{$insertSuffix}` AFTER INSERT ON `{$parentTable}`{$insertWhen} BEGIN
                    INSERT INTO `{$ftsTable}` (rowid, {$columnList}) VALUES (NEW.`_id`, {$newColumnList});
                END
            ";
            $this->getPDO()->prepare($insertTrigger)->execute();

            $deleteSuffix = self::FTS_TRIGGER_DELETE;
            $deleteTrigger = "
                CREATE TRIGGER `{$ftsTable}_{$deleteSuffix}` AFTER DELETE ON `{$parentTable}`{$deleteWhen} BEGIN
                    INSERT INTO `{$ftsTable}` (`{$ftsTable}`, rowid, {$columnList}) VALUES ('delete', OLD.`_id`, {$oldColumnList});
                END
            ";
            $this->getPDO()->prepare($deleteTrigger)->execute();

            $updateSuffix = self::FTS_TRIGGER_UPDATE;
            // OF <cols>: skip re-tokenise when only timestamps/permissions change.
            $updateTrigger = "
                CREATE TRIGGER `{$ftsTable}_{$updateSuffix}` AFTER UPDATE OF {$columnList} ON `{$parentTable}`{$updateWhen} BEGIN
                    INSERT INTO `{$ftsTable}` (`{$ftsTable}`, rowid, {$columnList}) VALUES ('delete', OLD.`_id`, {$oldColumnList});
                    INSERT INTO `{$ftsTable}` (rowid, {$columnList}) VALUES (NEW.`_id`, {$newColumnList});
                END
            ";
            $this->getPDO()->prepare($updateTrigger)->execute();

            $backfill = "INSERT INTO `{$ftsTable}` (rowid, {$columnList}) SELECT `_id`, {$columnList} FROM `{$parentTable}`{$backfillWhere}";
            $this->getPDO()->prepare($backfill)->execute();

            $this->commitTransaction();
        } catch (\Throwable $e) {
            // Swallow rollback failures so the original cause keeps its
            // place at the top of the stack — a "Failed to rollback"
            // wrapper hides what actually went wrong.
            try {
                $this->rollbackTransaction();
            } catch (\Throwable) {
            }
            throw $e;
        }

        unset($this->ftsTableCache[$collection]);

        return true;
    }

    /**
     * FTS5 table name keyed off the sorted attribute set. Hashed because
     * `_`-joining `['ab', 'cd_ef']` collides with `['ab_cd', 'ef']`.
     *
     * @param array<string>|string $attributes
     */
    protected function getFulltextTableName(string $collection, array|string $attributes): string
    {
        $attrs = \is_array($attributes) ? $attributes : [$attributes];
        $attrs = \array_map(fn (string $attr) => $this->filter($attr), $attrs);
        \sort($attrs);
        $key = \substr(\hash('sha1', \implode("\0", $attrs)), 0, 16);

        return $this->getFulltextTablePrefix($collection) . $key . self::FTS_TABLE_SUFFIX;
    }

    /**
     * Common LIKE prefix for FTS5 tables on `$collection`. Tenant-scoped
     * under sharedTables so per-tenant drops don't tear down a shared vtable.
     */
    protected function getFulltextTablePrefix(string $collection): string
    {
        if ($this->sharedTables) {
            return "{$this->getNamespace()}_{$this->getTenantSegment()}_{$this->filter($collection)}_";
        }

        return "{$this->getNamespace()}_{$this->filter($collection)}_";
    }

    /**
     * Filtered tenant for identifier interpolation (the base property is
     * `int|string|null`).
     */
    private function getTenantSegment(): string
    {
        return $this->filter((string) ($this->tenant ?? ''));
    }

    /**
     * Tenant rendered as a SQL literal for embedding in trigger bodies and
     * other places where parameter binding isn't available.
     */
    private function getTenantSqlLiteral(): string
    {
        if ($this->tenant === null) {
            return 'NULL';
        }
        if (\is_int($this->tenant)) {
            return (string) $this->tenant;
        }

        return $this->getPDO()->quote((string) $this->tenant);
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

        // If a regular SQLite index with this id exists, take the normal
        // DROP INDEX path. Otherwise the index is either an FTS5 virtual
        // table (whose name is keyed off attributes, not the id) or
        // already absent — try the FTS5 path before erroring.
        $regularIndex = "{$this->getNamespace()}_{$this->getTenantSegment()}_{$name}_{$id}";
        $stmt = $this->getPDO()->prepare("
            SELECT name FROM sqlite_master WHERE type='index' AND name=:_index
        ");
        $stmt->bindValue(':_index', $regularIndex);
        $stmt->execute();
        $hasRegular = $stmt->fetchColumn() !== false;
        // Free the read cursor before issuing DDL — SQLite holds a SHARED
        // lock on the database while a statement has unfetched rows, and
        // any subsequent DROP INDEX / ALTER TABLE under emulated prepares
        // will trip "database table is locked".
        $stmt->closeCursor();

        if (! $hasRegular && $this->dropFulltextIndexById($name, $id)) {
            return true;
        }

        $sql = "DROP INDEX `{$regularIndex}`";

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
     * Drop the FTS5 vtable backing index `$id` on `$collection`. Returns
     * false when no FTS5 table exists; throws when ambiguous.
     */
    protected function dropFulltextIndexById(string $collection, string $id): bool
    {
        $tables = $this->findFulltextTables($collection);

        if (empty($tables)) {
            return false;
        }

        // Resolve via metadata since the table name is hashed off the
        // attribute set, not the index id.
        $ftsTable = $this->resolveFulltextTableById($collection, $id, $tables);

        if ($ftsTable === null) {
            if (\count($tables) === 1) {
                $ftsTable = $tables[0];
            } else {
                // Returning false would let deleteIndex swallow the
                // resulting "no such index" and report success while
                // the FTS5 tables survived.
                throw new DatabaseException(
                    "Cannot resolve fulltext index '{$id}' on '{$collection}': "
                    . \count($tables) . ' FTS5 tables exist and metadata does not'
                    . ' match any of them.'
                );
            }
        }
        $triggerSuffixes = [
            self::FTS_TRIGGER_INSERT,
            self::FTS_TRIGGER_DELETE,
            self::FTS_TRIGGER_UPDATE,
        ];

        // Atomic teardown: orphaned triggers without their FTS5 table will
        // start failing on every parent write, so do not commit a partial
        // drop if any step fails midway.
        $this->startTransaction();
        try {
            foreach ($triggerSuffixes as $suffix) {
                $this->getPDO()->prepare("DROP TRIGGER IF EXISTS `{$ftsTable}_{$suffix}`")->execute();
            }
            $sql = "DROP TABLE IF EXISTS `{$ftsTable}`";
            $this->getPDO()->prepare($sql)->execute();
            $this->commitTransaction();
        } catch (\Throwable $e) {
            try {
                $this->rollbackTransaction();
            } catch (\Throwable) {
            }
            throw $e;
        }

        unset($this->ftsTableCache[$collection]);

        return true;
    }


    /**
     * Resolve the FTS5 table for index `$id` via metadata. Returns null
     * when metadata doesn't reach a candidate.
     *
     * @param array<string> $candidates
     */
    protected function resolveFulltextTableById(string $collection, string $id, array $candidates): ?string
    {
        try {
            $metadataCollection = new Document(['$id' => Database::METADATA]);
            $collectionDoc = $this->getDocument($metadataCollection, $collection);
        } catch (NotFoundException) {
            // Metadata not yet seeded (collection drop during bootstrap).
            // Anything else surfaces — masking PDO errors here would silently
            // fall through to the single-candidate drop path and tear down
            // the wrong table.
            return null;
        }

        if ($collectionDoc->isEmpty()) {
            return null;
        }

        $indexes = $collectionDoc->getAttribute('indexes', []);
        $filteredId = $this->filter($id);

        if (! \is_array($indexes)) {
            return null;
        }

        foreach ($indexes as $index) {
            $indexId = $index instanceof Document
                ? $index->getId()
                : (\is_array($index) ? ($index['$id'] ?? null) : null);

            if (! \is_scalar($indexId)) {
                continue;
            }
            if ($this->filter((string) $indexId) !== $filteredId) {
                continue;
            }

            $type = $index instanceof Document
                ? $index->getAttribute('type')
                : (\is_array($index) ? ($index['type'] ?? null) : null);

            if ($type !== IndexType::Fulltext->value) {
                return null;
            }

            if ($index instanceof Document) {
                $attributes = $index->getAttribute('attributes', []);
            } else {
                $attributes = $index['attributes'] ?? [];
            }

            /** @var array<mixed> $attributesArr */
            $attributesArr = \is_array($attributes) ? $attributes : [];
            $internal = \array_map(
                fn (mixed $a): string => \is_string($a) ? $this->getInternalKeyForAttribute($a) : '',
                $attributesArr
            );
            $candidate = $this->getFulltextTableName($collection, $internal);

            return \in_array($candidate, $candidates, true) ? $candidate : null;
        }

        return null;
    }

    /**
     * Every FTS5 vtable on `$collection`.
     *
     * @return array<string>
     */
    protected function findFulltextTables(string $collection): array
    {
        // ESCAPE '\\' so the literal `_` separators in the prefix don't
        // act as LIKE wildcards (e.g. `db_users_` matching `db_usersA_`).
        $stmt = $this->getPDO()->prepare("
            SELECT name FROM sqlite_master
            WHERE type='table'
              AND name LIKE :_prefix ESCAPE '\\'
              AND name LIKE :_suffix ESCAPE '\\'
        ");
        $stmt->bindValue(':_prefix', $this->escapeLikePattern($this->getFulltextTablePrefix($collection)) . '%');
        $stmt->bindValue(':_suffix', '%' . $this->escapeLikePattern(self::FTS_TABLE_SUFFIX));
        $stmt->execute();
        $tables = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $stmt->closeCursor();

        return \array_map(fn (mixed $t): string => \is_string($t) ? $t : '', $tables);
    }

    /**
     * Escape `_`, `%`, and `\` so a literal value can be embedded in a
     * LIKE pattern without acting as a wildcard. Pair every call site
     * with an explicit `ESCAPE '\\'` clause.
     */
    private function escapeLikePattern(string $value): string
    {
        return \str_replace(['\\', '_', '%'], ['\\\\', '\_', '\%'], $value);
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
                } elseif ($this instanceof Feature\Spatial && \in_array($attribute, $spatialAttributes, true)) {
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
        return true;
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

    protected function getSQLType(ColumnType $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        if (in_array($type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
            return '';
        }
        if ($array === true) {
            return 'JSON';
        }

        if ($type === ColumnType::String) {
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
            $suffix = $signed ? '' : ' UNSIGNED';

            return ($size >= 8 ? 'BIGINT' : 'INT').$suffix;
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
            default => throw new DatabaseException('Unknown type: '.$type->value.'. Must be one of '.ColumnType::String->value.', '.ColumnType::Varchar->value.', '.ColumnType::Text->value.', '.ColumnType::MediumText->value.', '.ColumnType::LongText->value.', '.ColumnType::Integer->value.', '.ColumnType::Double->value.', '.ColumnType::Boolean->value.', '.ColumnType::Datetime->value.', '.ColumnType::Relationship->value),
        };
    }

    protected function getMaxPointSize(): int
    {
        return 0;
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
                return null;
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
                $currentRegularAttributes['_createdAt'] = $document->getCreatedAt() ? DatabaseDateTime::setTimezone($document->getCreatedAt()) : null;
                $currentRegularAttributes['_updatedAt'] = $document->getUpdatedAt() ? DatabaseDateTime::setTimezone($document->getUpdatedAt()) : null;
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

        // getSQLIndex prepends `_tenant` to every index column list
        // under shared tables, so the actual UNIQUE on the documents
        // table is (_tenant, _uid). SQLite's ON CONFLICT clause needs
        // the same column order to match a UNIQUE constraint.
        $conflictKeys = $this->sharedTables ? '(_tenant, _uid)' : '(_uid)';

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

    public function getSupportNonUtfCharacters(): bool
    {
        return false;
    }

    protected function getInsertKeyword(): string
    {
        return $this->skipDuplicates ? 'INSERT OR IGNORE INTO' : 'INSERT INTO';
    }

    /**
     * SQLite's ALTER TABLE accepts a single column per statement, so the
     * shared SQL implementation that joins many ADD COLUMN clauses with
     * commas doesn't parse here. Loop over createAttribute instead.
     *
     * @param array<Attribute> $attributes
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        // The flag advertises atomic batch creation. SQLite has no
        // multi-column ADD, but DDL inside a transaction is still
        // rolled back on failure, so a mid-batch error doesn't leave
        // the table half-extended.
        $this->startTransaction();
        try {
            foreach ($attributes as $attribute) {
                $this->createAttribute($collection, $attribute);
            }
            $this->commitTransaction();
        } catch (\Throwable $e) {
            try {
                $this->rollbackTransaction();
            } catch (\Throwable) {
            }
            throw $e;
        }

        return true;
    }

    /**
     * SQL::createRelationship concatenates multiple ALTER TABLE statements
     * with `;` and runs them through a single prepare/execute, which only
     * works because MySQL accepts multi-statement queries. SQLite's PDO
     * driver runs the first statement and silently drops the rest, so
     * re-implement the dispatch with one statement per call.
     */
    public function createRelationship(Relationship $relationship): bool
    {
        $name = $this->filter($relationship->collection);
        $relatedName = $this->filter($relationship->relatedCollection);
        $table = $this->getSQLTable($name);
        $relatedTable = $this->getSQLTable($relatedName);
        $id = $this->filter($relationship->key);
        $twoWayKey = $this->filter($relationship->twoWayKey);
        $sqlType = $this->getSQLType(ColumnType::Relationship, 0, false, false, false);
        $twoWay = $relationship->twoWay;

        $statements = match ($relationship->type) {
            RelationType::OneToOne => $twoWay
                ? [
                    "ALTER TABLE {$table} ADD COLUMN `{$id}` {$sqlType} DEFAULT NULL",
                    "ALTER TABLE {$relatedTable} ADD COLUMN `{$twoWayKey}` {$sqlType} DEFAULT NULL",
                ]
                : ["ALTER TABLE {$table} ADD COLUMN `{$id}` {$sqlType} DEFAULT NULL"],
            RelationType::OneToMany => ["ALTER TABLE {$relatedTable} ADD COLUMN `{$twoWayKey}` {$sqlType} DEFAULT NULL"],
            RelationType::ManyToOne => ["ALTER TABLE {$table} ADD COLUMN `{$id}` {$sqlType} DEFAULT NULL"],
            RelationType::ManyToMany => [],
        };

        foreach ($statements as $stmt) {
            $this->getPDO()->prepare($stmt)->execute();
        }

        return true;
    }

    public function updateRelationship(
        Relationship $relationship,
        ?string $newKey = null,
        ?string $newTwoWayKey = null,
    ): bool {
        $collection = $relationship->collection;
        $relatedCollection = $relationship->relatedCollection;
        $name = $this->filter($collection);
        $relatedName = $this->filter($relatedCollection);
        $table = $this->getSQLTable($name);
        $relatedTable = $this->getSQLTable($relatedName);
        $key = $this->filter($relationship->key);
        $twoWayKey = $this->filter($relationship->twoWayKey);
        $twoWay = $relationship->twoWay;
        $side = $relationship->side;

        if ($newKey !== null) {
            $newKey = $this->filter($newKey);
        }
        if ($newTwoWayKey !== null) {
            $newTwoWayKey = $this->filter($newTwoWayKey);
        }

        $statements = [];

        switch ($relationship->type) {
            case RelationType::OneToOne:
                if ($newKey !== null && $key !== $newKey) {
                    $statements[] = "ALTER TABLE {$table} RENAME COLUMN `{$key}` TO `{$newKey}`";
                }
                if ($twoWay && $newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                    $statements[] = "ALTER TABLE {$relatedTable} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`";
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    if ($newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                        $statements[] = "ALTER TABLE {$relatedTable} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`";
                    }
                } else {
                    if ($newKey !== null && $key !== $newKey) {
                        $statements[] = "ALTER TABLE {$table} RENAME COLUMN `{$key}` TO `{$newKey}`";
                    }
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Child) {
                    if ($newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                        $statements[] = "ALTER TABLE {$relatedTable} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`";
                    }
                } else {
                    if ($newKey !== null && $key !== $newKey) {
                        $statements[] = "ALTER TABLE {$table} RENAME COLUMN `{$key}` TO `{$newKey}`";
                    }
                }
                break;
            case RelationType::ManyToMany:
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collectionDoc = $this->getDocument($metadataCollection, $collection);
                $relatedCollectionDoc = $this->getDocument($metadataCollection, $relatedCollection);

                $junction = $this->getSQLTable('_' . $collectionDoc->getSequence() . '_' . $relatedCollectionDoc->getSequence());

                if ($newKey !== null) {
                    $statements[] = "ALTER TABLE {$junction} RENAME COLUMN `{$key}` TO `{$newKey}`";
                }
                if ($twoWay && $newTwoWayKey !== null) {
                    $statements[] = "ALTER TABLE {$junction} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`";
                }
                break;
        }

        foreach ($statements as $stmt) {
            $this->getPDO()->prepare($stmt)->execute();
        }

        return true;
    }

    public function deleteRelationship(Relationship $relationship): bool
    {
        $collection = $relationship->collection;
        $relatedCollection = $relationship->relatedCollection;
        $name = $this->filter($collection);
        $relatedName = $this->filter($relatedCollection);
        $table = $this->getSQLTable($name);
        $relatedTable = $this->getSQLTable($relatedName);
        $key = $this->filter($relationship->key);
        $twoWayKey = $this->filter($relationship->twoWayKey);
        $twoWay = $relationship->twoWay;
        $side = $relationship->side;

        $statements = [];

        switch ($relationship->type) {
            case RelationType::OneToOne:
                if ($side === RelationSide::Parent) {
                    $statements[] = "ALTER TABLE {$table} DROP COLUMN `{$key}`";
                    if ($twoWay) {
                        $statements[] = "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`";
                    }
                } elseif ($side === RelationSide::Child) {
                    $statements[] = "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`";
                    if ($twoWay) {
                        $statements[] = "ALTER TABLE {$table} DROP COLUMN `{$key}`";
                    }
                }
                break;
            case RelationType::OneToMany:
                $statements[] = $side === RelationSide::Parent
                    ? "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`"
                    : "ALTER TABLE {$table} DROP COLUMN `{$key}`";
                break;
            case RelationType::ManyToOne:
                $statements[] = $side === RelationSide::Parent
                    ? "ALTER TABLE {$table} DROP COLUMN `{$key}`"
                    : "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`";
                break;
            case RelationType::ManyToMany:
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collectionDoc = $this->getDocument($metadataCollection, $collection);
                $relatedCollectionDoc = $this->getDocument($metadataCollection, $relatedCollection);

                $junctionBase = $side === RelationSide::Parent
                    ? '_' . $collectionDoc->getSequence() . '_' . $relatedCollectionDoc->getSequence()
                    : '_' . $relatedCollectionDoc->getSequence() . '_' . $collectionDoc->getSequence();

                $statements[] = "DROP TABLE {$this->getSQLTable($junctionBase)}";
                $statements[] = "DROP TABLE {$this->getSQLTable($junctionBase . '_perms')}";
                break;
        }

        foreach ($statements as $stmt) {
            $this->getPDO()->prepare($stmt)->execute();
        }

        return true;
    }

    /**
     * Introspect a collection's columns via PRAGMA table_info instead of
     * MariaDB's INFORMATION_SCHEMA.COLUMNS, which doesn't exist in SQLite.
     * Returned shape matches the MariaDB result enough that
     * Database::analyzeCollection() doesn't have to special-case the
     * adapter.
     *
     * @return array<Document>
     */
    public function getSchemaAttributes(string $collection): array
    {
        $table = "{$this->getNamespace()}_{$this->filter($collection)}";

        $stmt = $this->getPDO()->prepare("PRAGMA table_info(`{$table}`)");
        $stmt->execute();
        $rows = $stmt->fetchAll();
        $stmt->closeCursor();

        $results = [];
        foreach ($rows as $row) {
            if (! \is_array($row)) {
                continue;
            }
            $rawType = \is_scalar($row['type'] ?? null) ? (string) $row['type'] : '';
            $parsed = $this->parseSqliteColumnType($rawType);
            $name = \is_scalar($row['name'] ?? null) ? (string) $row['name'] : '';

            $results[] = new Document([
                '$id' => $name,
                'columnDefault' => $row['dflt_value'] ?? null,
                'isNullable' => empty($row['notnull']) ? 'YES' : 'NO',
                'dataType' => $parsed['dataType'],
                'characterMaximumLength' => $parsed['characterMaximumLength'],
                'numericPrecision' => $parsed['numericPrecision'],
                'numericScale' => $parsed['numericScale'],
                'datetimePrecision' => $parsed['datetimePrecision'],
                'columnType' => \strtolower($rawType),
                'columnKey' => ! empty($row['pk']) ? 'PRI' : '',
                'extra' => '',
            ]);
        }

        return $results;
    }

    /**
     * Introspect a collection's indexes via PRAGMA index_list +
     * PRAGMA index_info. Returns one Document per index with a `columns`
     * array, matching the grouped shape MariaDB::getSchemaIndexes returns
     * so Database::createIndex can compare `columns` against the requested
     * attributes without special-casing the adapter.
     *
     * @return array<Document>
     */
    public function getSchemaIndexes(string $collection): array
    {
        $table = "{$this->getNamespace()}_{$this->filter($collection)}";

        $stmt = $this->getPDO()->prepare("PRAGMA index_list(`{$table}`)");
        $stmt->execute();
        $indexes = $stmt->fetchAll();
        $stmt->closeCursor();

        $results = [];
        foreach ($indexes as $index) {
            if (! \is_array($index)) {
                continue;
            }
            $name = \is_scalar($index['name'] ?? null) ? (string) $index['name'] : '';
            $unique = ! empty($index['unique']);

            $colStmt = $this->getPDO()->prepare("PRAGMA index_info(`{$name}`)");
            $colStmt->execute();
            $cols = $colStmt->fetchAll();
            $colStmt->closeCursor();

            \usort(
                $cols,
                fn (mixed $a, mixed $b) => (
                    \is_array($a) && \is_scalar($a['seqno'] ?? null) ? (int) $a['seqno'] : 0
                ) <=> (
                    \is_array($b) && \is_scalar($b['seqno'] ?? null) ? (int) $b['seqno'] : 0
                )
            );

            $columns = [];
            $lengths = [];
            foreach ($cols as $col) {
                if (! \is_array($col)) {
                    continue;
                }
                $columns[] = \is_scalar($col['name'] ?? null) ? (string) $col['name'] : '';
                $lengths[] = null;
            }

            $results[] = new Document([
                '$id' => $name,
                'indexName' => $name,
                'indexType' => 'BTREE',
                'nonUnique' => $unique ? 0 : 1,
                'columns' => $columns,
                'lengths' => $lengths,
            ]);
        }

        // PRAGMA index_list misses FTS5 vtables.
        foreach ($this->getFulltextSchemaIndexes($collection) as $entry) {
            $results[] = new Document($entry);
        }

        return $results;
    }

    /**
     * Schema-index entries for FTS5 fulltext tables on `$collection`.
     * Maps each back to a metadata index id when possible.
     *
     * @return array<array{
     *     '$id': string,
     *     indexName: string,
     *     indexType: string,
     *     nonUnique: int,
     *     columns: array<string>,
     *     lengths: array<null>,
     * }>
     */
    protected function getFulltextSchemaIndexes(string $collection): array
    {
        $tables = $this->findFulltextTables($collection);

        if (empty($tables)) {
            return [];
        }

        $hashToId = [];
        try {
            $metadataCollection = new Document(['$id' => Database::METADATA]);
            $collectionDoc = $this->getDocument($metadataCollection, $collection);
            if (! $collectionDoc->isEmpty()) {
                $indexes = $collectionDoc->getAttribute('indexes', []);
                if (\is_array($indexes)) {
                    foreach ($indexes as $index) {
                        if ($index instanceof Document) {
                            $indexId = $index->getId();
                            $type = $index->getAttribute('type');
                            $attributes = $index->getAttribute('attributes', []);
                        } elseif (\is_array($index)) {
                            $indexId = $index['$id'] ?? null;
                            $type = $index['type'] ?? null;
                            $attributes = $index['attributes'] ?? [];
                        } else {
                            continue;
                        }

                        if (! \is_scalar($indexId) || $type !== IndexType::Fulltext->value) {
                            continue;
                        }

                        $internal = \array_map(
                            fn (mixed $a): string => \is_string($a) ? $this->getInternalKeyForAttribute($a) : '',
                            \is_array($attributes) ? $attributes : []
                        );
                        $hashToId[$this->getFulltextTableName($collection, $internal)] = $this->filter((string) $indexId);
                    }
                }
            }
        } catch (\Throwable) {
        }

        $entries = [];
        foreach ($tables as $ftsTable) {
            $info = $this->getPDO()->prepare("PRAGMA table_info(`{$ftsTable}`)");
            $info->execute();
            $cols = $info->fetchAll(PDO::FETCH_ASSOC);
            $info->closeCursor();

            $columns = [];
            foreach ($cols as $col) {
                if (! \is_array($col)) {
                    continue;
                }
                $name = \is_scalar($col['name'] ?? null) ? (string) $col['name'] : '';
                if ($name === '') {
                    continue;
                }
                $columns[] = $name;
            }

            $id = $hashToId[$ftsTable] ?? $ftsTable;

            $entries[] = [
                '$id' => $id,
                'indexName' => $id,
                'indexType' => 'FULLTEXT',
                'nonUnique' => 1,
                'columns' => $columns,
                'lengths' => \array_fill(0, \count($columns), null),
            ];
        }

        return $entries;
    }

    /**
     * Parse a SQLite type declaration like `VARCHAR(36)` into the column-info
     * shape exposed by getSchemaAttributes. Mirrors what MariaDB returns from
     * INFORMATION_SCHEMA.COLUMNS so callers don't have to special-case the
     * adapter — TEXT family types report their MariaDB byte ceilings,
     * VARCHAR/CHAR thread the parenthesised size into characterMaximumLength,
     * DATETIME's parenthesised value routes to datetimePrecision, and
     * integer types fall back to MariaDB's default precision values.
     *
     * @return array{
     *     dataType: string,
     *     characterMaximumLength: ?string,
     *     numericPrecision: ?string,
     *     numericScale: ?string,
     *     datetimePrecision: ?string,
     * }
     */
    private function parseSqliteColumnType(string $declaration): array
    {
        $declaration = \trim(\preg_replace('/\s+/', ' ', $declaration) ?? '');

        $base = $declaration;
        $argument = null;
        $secondArgument = null;
        if (\preg_match('/^([A-Za-z]+)\s*\((\d+)(?:\s*,\s*(\d+))?\s*\)/', $declaration, $matches) === 1) {
            $base = $matches[1];
            $argument = (int) $matches[2];
            if (isset($matches[3])) {
                $secondArgument = (int) $matches[3];
            }
        }

        $dataType = \strtolower($base);
        // SQLite spells INT and INTEGER interchangeably for declared types.
        // Under emulation, canonicalise to MariaDB's reported `int` so
        // getSchemaAttributes matches that adapter's contract; otherwise
        // keep the verbatim form the user declared.
        if ($this->emulateMySQL && $dataType === 'integer') {
            $dataType = 'int';
        }

        $result = [
            'dataType' => $dataType,
            'characterMaximumLength' => null,
            'numericPrecision' => null,
            'numericScale' => null,
            'datetimePrecision' => null,
        ];

        // VARCHAR / CHAR / DATETIME(n) / DECIMAL(p,s) length+precision
        // come straight from the declaration — that's true for vanilla
        // SQLite too. The MariaDB byte ceilings (TEXT/MEDIUMTEXT/etc.)
        // and the integer/float precision defaults are MariaDB-specific
        // INFORMATION_SCHEMA conventions, so report them only under
        // emulation.
        switch ($dataType) {
            case 'varchar':
            case 'char':
                if ($argument !== null) {
                    $result['characterMaximumLength'] = (string) $argument;
                }
                break;

            case 'datetime':
            case 'timestamp':
            case 'time':
                if ($argument !== null) {
                    $result['datetimePrecision'] = (string) $argument;
                }
                break;

            case 'decimal':
            case 'numeric':
                if ($argument !== null) {
                    $result['numericPrecision'] = (string) $argument;
                }
                if ($secondArgument !== null) {
                    $result['numericScale'] = (string) $secondArgument;
                } elseif ($this->emulateMySQL && $argument !== null) {
                    $result['numericScale'] = '0';
                }
                break;
        }

        if ($this->emulateMySQL) {
            switch ($dataType) {
                case 'text':
                    $result['characterMaximumLength'] = self::MARIADB_TEXT_BYTES;
                    break;

                case 'mediumtext':
                    $result['characterMaximumLength'] = self::MARIADB_MEDIUMTEXT_BYTES;
                    break;

                case 'longtext':
                case 'json':
                    $result['characterMaximumLength'] = self::MARIADB_LONGTEXT_BYTES;
                    break;

                case 'tinyint':
                    $result['numericPrecision'] = '3';
                    break;

                case 'smallint':
                    $result['numericPrecision'] = '5';
                    break;

                case 'mediumint':
                    $result['numericPrecision'] = '7';
                    break;

                case 'int':
                case 'integer':
                    $result['numericPrecision'] = '10';
                    break;

                case 'bigint':
                    $result['numericPrecision'] = '19';
                    break;

                case 'decimal':
                case 'numeric':
                    if ($result['numericPrecision'] === null) {
                        $result['numericPrecision'] = '10';
                    }
                    break;

                case 'float':
                    $result['numericPrecision'] = '12';
                    break;

                case 'double':
                    $result['numericPrecision'] = '22';
                    break;
            }
        }

        return $result;
    }

    /**
     * SQLite has no MATCH ... AGAINST. Route SEARCH/NOT_SEARCH through the
     * collection's FTS5 virtual table; for LIKE-using comparisons append
     * an explicit ESCAPE clause because SQLite — unlike MariaDB — does
     * not honour `\` as a default escape and the inherited
     * escapeWildcards() emits backslash escapes on every wildcard.
     * Everything else falls through to the MariaDB implementation.
     */
    protected function getSQLCondition(Query $query, array &$binds, ?string $forCollection = null): string
    {
        $method = $query->getMethod();

        $likeMethods = [
            Method::StartsWith,
            Method::NotStartsWith,
            Method::EndsWith,
            Method::NotEndsWith,
            Method::Contains,
            Method::ContainsAny,
            Method::NotContains,
        ];

        if (\in_array($method, $likeMethods, true)) {
            // Array CONTAINS via json_each — exact element match without
            // LIKE substring false positives (`%2%` matching `[12, 200]`).
            $arrayContainsMethods = [
                Method::Contains,
                Method::ContainsAny,
                Method::NotContains,
            ];
            if ($query->onArray() && \in_array($method, $arrayContainsMethods, true)) {
                return $this->buildArrayContainsCondition($query, $binds);
            }

            return $this->getLikeCondition($query, $binds);
        }

        if ($method !== Method::Search && $method !== Method::NotSearch) {
            return parent::getSQLCondition($query, $binds, $forCollection);
        }

        $query->setAttribute($this->getInternalKeyForAttribute($query->getAttribute()));
        $attribute = $this->filter($query->getAttribute());
        $alias = $this->quote(Query::DEFAULT_ALIAS);
        $placeholder = ID::unique();

        $queryValue = $query->getValue();
        $rawValue = \is_scalar($queryValue) ? (string) $queryValue : '';
        $ftsValue = $this->getFTS5Value($rawValue);

        if ($ftsValue === '') {
            // Empty term — FTS5 syntax-errors on the empty string.
            return $method === Method::Search ? '1 = 0' : '1 = 1';
        }

        $ftsTable = $forCollection === null
            ? null
            : $this->findFulltextTableForAttribute($forCollection, $attribute);

        if ($ftsTable === null) {
            // LIKE on the raw value — the FTS5-formatted form embeds
            // `OR`/`*` that LIKE would treat as literal.
            return $this->buildSearchLikeFallback($attribute, $rawValue, $alias, $placeholder, $method, $binds);
        }

        $binds[":{$placeholder}_0"] = $ftsValue;

        $subquery = "{$alias}.`_id` IN (SELECT rowid FROM `{$ftsTable}` WHERE `{$ftsTable}` MATCH :{$placeholder}_0)";

        return $method === Method::Search ? $subquery : "NOT ({$subquery})";
    }

    /**
     * SEARCH fallback to LIKE when no FTS5 table covers the attribute.
     *
     * @param array<string,mixed> $binds
     */
    private function buildSearchLikeFallback(
        string $attribute,
        string $value,
        string $alias,
        string $placeholder,
        Method $method,
        array &$binds,
    ): string {
        $binds[":{$placeholder}_0"] = '%' . $this->escapeWildcards($value) . '%';
        $sql = "{$alias}.{$this->quote($attribute)} LIKE :{$placeholder}_0 ESCAPE '\\'";

        return $method === Method::Search ? $sql : "NOT ({$sql})";
    }

    /**
     * Array CONTAINS / CONTAINS_ANY / NOT_CONTAINS via json_each. Exact
     * element match — avoids the LIKE substring false positives where
     * `%2%` matches `[12, 200]` and `%"apple"%` matches `["pineapple"]`.
     *
     * @param array<string,mixed> $binds
     */
    private function buildArrayContainsCondition(Query $query, array &$binds): string
    {
        $method = $query->getMethod();
        $query->setAttribute($this->getInternalKeyForAttribute($query->getAttribute()));

        $attribute = $this->quote($this->filter($query->getAttribute()));
        $alias = $this->quote(Query::DEFAULT_ALIAS);
        $placeholder = ID::unique();

        $values = $query->getValues();
        if (empty($values)) {
            return '';
        }

        $params = [];
        foreach ($values as $key => $value) {
            $param = ":{$placeholder}_{$key}";
            $binds[$param] = $value;
            $params[] = $param;
        }

        $expression = "EXISTS (SELECT 1 FROM json_each({$alias}.{$attribute}) WHERE value IN ("
            . \implode(', ', $params)
            . '))';

        return $method === Method::NotContains ? "NOT {$expression}" : $expression;
    }

    /**
     * FTS5 vtable on `$collection` that covers `$attribute`. Multi-column
     * indexes can't be addressed from a single attribute alone — the
     * lookup is via the cached attribute → table map.
     */
    protected function findFulltextTableForAttribute(string $collection, string $attribute): ?string
    {
        if (!\array_key_exists($collection, $this->ftsTableCache)) {
            $this->ftsTableCache[$collection] = $this->buildFulltextAttributeMap($collection);
        }

        return $this->ftsTableCache[$collection][$attribute] ?? null;
    }

    /**
     * @return array<string, string>
     */
    private function buildFulltextAttributeMap(string $collection): array
    {
        $map = [];
        foreach ($this->findFulltextTables($collection) as $table) {
            $info = $this->getPDO()->prepare("PRAGMA table_info(`{$table}`)");
            $info->execute();
            $cols = $info->fetchAll(PDO::FETCH_ASSOC);
            $info->closeCursor();
            foreach ($cols as $col) {
                if (! \is_array($col)) {
                    continue;
                }
                $name = $col['name'] ?? null;
                if (\is_string($name) && $name !== '') {
                    $map[$name] = $table;
                }
            }
        }

        return $map;
    }

    /**
     * Compile STARTS_WITH / ENDS_WITH / CONTAINS (and NOT variants) into
     * LIKE with an explicit ESCAPE clause — SQLite needs it to honour
     * the backslash escapes escapeWildcards() inserts.
     *
     * @param array<string,mixed> $binds
     */
    protected function getLikeCondition(Query $query, array &$binds): string
    {
        $method = $query->getMethod();
        $query->setAttribute($this->getInternalKeyForAttribute($query->getAttribute()));

        $attribute = $this->quote($this->filter($query->getAttribute()));
        $alias = $this->quote(Query::DEFAULT_ALIAS);
        $placeholder = ID::unique();

        $isNotQuery = \in_array($method, [
            Method::NotStartsWith,
            Method::NotEndsWith,
            Method::NotContains,
        ], true);

        $conditions = [];
        foreach ($query->getValues() as $key => $value) {
            $strValue = \is_string($value) ? $value : '';
            $bound = match ($method) {
                Method::StartsWith, Method::NotStartsWith => $this->escapeWildcards($strValue) . '%',
                Method::EndsWith, Method::NotEndsWith => '%' . $this->escapeWildcards($strValue),
                Method::Contains, Method::ContainsAny, Method::NotContains => '%' . $this->escapeWildcards($strValue) . '%',
                default => $value,
            };

            $binds[":{$placeholder}_{$key}"] = $bound;
            $operator = $isNotQuery ? 'NOT LIKE' : 'LIKE';
            $conditions[] = "{$alias}.{$attribute} {$operator} :{$placeholder}_{$key} ESCAPE '\\'";
        }

        $separator = $isNotQuery ? ' AND ' : ' OR ';

        return empty($conditions) ? '' : '(' . \implode($separator, $conditions) . ')';
    }

    /**
     * Format a SEARCH term as MariaDB BOOLEAN MODE: OR-joined tokens with
     * the trailing token prefix-matched. Empty when no token survives.
     */
    protected function getFTS5Value(string $value): string
    {
        // Balanced wrapping `"..."` triggers exact-phrase mode.
        $exact = \strlen($value) >= 2
            && \str_starts_with($value, '"')
            && \str_ends_with($value, '"')
            && \substr_count($value, '"') === 2;

        // Strip FTS5 syntax characters so multi-word terms split.
        $sanitized = \preg_replace('/[^\p{L}\p{N}_\s]+/u', ' ', $value) ?? '';
        $sanitized = \trim((string) \preg_replace('/\s+/', ' ', $sanitized));

        if ($sanitized === '') {
            return '';
        }

        if ($exact) {
            return '"' . $sanitized . '"';
        }

        $tokens = \explode(' ', $sanitized);
        // Quote AND/OR/NOT/NEAR — bare, FTS5 treats them as operators.
        $tokens = \array_map(static function (string $token): string {
            if (\preg_match('/^(AND|OR|NOT|NEAR)$/i', $token) === 1) {
                return '"' . $token . '"';
            }
            return $token;
        }, $tokens);
        $last = \array_pop($tokens);
        if (! \str_starts_with($last, '"')) {
            $last .= '*';
        }
        $tokens[] = $last;

        return \implode(' OR ', $tokens);
    }
}
