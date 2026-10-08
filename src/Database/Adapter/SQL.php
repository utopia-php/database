<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Swoole\Database\PDOProxy;
use Swoole\Database\PDOStatementProxy;
use Throwable;
use Utopia\Console;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\SQL\BoundedPage;
use Utopia\Database\Adapter\SQL\Expression;
use Utopia\Database\Adapter\SQL\Hook\Join;
use Utopia\Database\Adapter\SQL\Hook\Permission;
use Utopia\Database\Adapter\SQL\Hook\Tenant;
use Utopia\Database\Adapter\SQL\Hook\WriteContext;
use Utopia\Database\Adapter\SQL\JoinAlias;
use Utopia\Database\Attribute;
use Utopia\Database\Builder\Filtering;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Hook\Tenancy;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\PDO as DatabasePDO;
use Utopia\Database\PDOStatement as DatabasePDOStatement;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Storage;
use Utopia\Database\Validator\BigInt;
use Utopia\Database\Validator\Query\Join as JoinValidator;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\Feature\FullOuterJoins as FullOuterJoinsFeature;
use Utopia\Query\Builder\Feature\InsertOrIgnore as InsertOrIgnoreFeature;
use Utopia\Query\Builder\Feature\MariaDB\Returning as MariaDBReturning;
use Utopia\Query\Builder\Feature\Upsert as UpsertFeature;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Builder\Statement;
use Utopia\Query\CursorDirection;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook\Attribute\Map as AttributeMap;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema;
use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\MySQL as MySQLSchema;
use Utopia\Query\Schema\PostgreSQL as PostgresSchema;
use Utopia\Query\Schema\Table;
use Utopia\Query\Schema\Table\PostgreSQL as PostgresTable;

abstract class SQL extends Adapter implements Feature\Connection, Feature\RawQuery, Feature\QueryBuilder, Feature\Relationships, Feature\Upserts
{
    /**
     * remapRow() drops every column with this prefix from every row it reads. filter() strips `$` from
     * every column name and a join alias cannot hold one, so only the columns an emulated full outer
     * join is ordered by carry it.
     */
    private const string FOJ_ORDER_ALIAS_PREFIX = '$foj_ord_';

    private const string FOJ_ROWS_ALIAS = 'foj_rows';

    protected const string MIN_DATETIME = '1000-01-01 00:00:00';

    /**
     * The internal attributes `alias.*` returns next to the joined `$id`: those a direct read of the joined
     * collection returns, but `$tenant`, which is the read's own tenant on every joined row.
     */
    private const array JOINED_ROW_INTERNALS = [
        Document::SEQUENCE,
        Document::CREATED_AT,
        Document::UPDATED_AT,
        Document::PERMISSIONS,
    ];

    /**
     * MariaDB, MySQL and SQLite accept OFFSET only after a LIMIT; this one bounds nothing on any engine.
     */
    private const int UNBOUNDED_LIMIT = PHP_INT_MAX;

    /**
     * No aggregate alias can start with `$`, so a projected column never shares a result name with an
     * aggregate.
     */
    private const string FOJ_COLUMN_PREFIX = '$foj_col_';

    /**
     * Where the rows of an emulated full outer join come from: a main-side row it paired with no
     * joined row, a main-side row paired with a joined row, a joined row it paired with no main-side
     * row. Every right join after the full outer join adds a bit of its own for its unmatched rows.
     */
    private const int UNPAIRED_MAIN_ROWS = 1;

    private const int PAIRED_ROWS = 2;

    private const int UNPAIRED_JOINED_ROWS = 4;

    protected object $pdo;

    /**
     * Controls how many fractional digits are used when binding float parameters.
     */
    protected int $floatPrecision = 17;

    /**
     * Lazily constructed AttributeMap shared by every newBuilder() call.
     * AttributeMap is a readonly stateless config object, so it can safely
     * be reused across queries on the same adapter.
     */
    private ?AttributeMap $attributeMap = null;

    /**
     * @var \WeakMap<object, Event>|null
     */
    private ?\WeakMap $statementEvents = null;

    /**
     * The metadata the comments ahead of every statement were last written for, when every value is
     * scalar or null, so the same metadata yields the same comments.
     *
     * @var array<string, mixed>|null
     */
    private ?array $commentedMetadata = null;

    private string $comments = '';

    /**
     * @var \WeakMap<object, array<mixed>>|null
     */
    private ?\WeakMap $statementBindings = null;

    /**
     * @var \WeakMap<object, string>|null
     */
    private ?\WeakMap $statementCollections = null;

    /**
     * Accepts Utopia\Database\PDO, a PDO-compatible proxy, or a native PDO.
     */
    public function __construct(object $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @return array<Capability>
     */
    #[\Override]
    public function capabilities(): array
    {
        return array_merge(parent::capabilities(), [
            Capability::Schemas,
            Capability::Caching,
            Capability::IndexFulltext,
            Capability::IndexFulltextMultiple,
            Capability::UpdateLock,
            Capability::TransactionRetries,
            Capability::TransactionNested,
            Capability::Operators,
            Capability::OrderRandom,
            Capability::IndexIdentical,
            Capability::AttributeResizing,
            Capability::DefinedAttributes,
            Capability::Joins,
            Capability::Aggregations,
        ]);
    }

    #[\Override]
    public function getDriver(): DatabasePDO|PDOProxy|PDO
    {
        if ($this->pdo instanceof DatabasePDO || $this->pdo instanceof PDOProxy || $this->pdo instanceof PDO) {
            return $this->pdo;
        }

        throw new DatabaseException('SQL adapter requires Utopia\\Database\\PDO, Swoole\\Database\\PDOProxy, or PDO');
    }

    /**
     * Helper to format a float value according to configured precision for binding/logging.
     */
    protected function getFloatPrecision(float $value): string
    {
        return sprintf('%.'.$this->floatPrecision.'F', $value);
    }

    #[\Override]
    public function hostname(): string
    {
        try {
            if ($this->pdo instanceof DatabasePDO) {
                return $this->pdo->getHostname();
            }

            return $this->hostname;
        } catch (Throwable) {
            return '';
        }
    }

    protected function getLockType(): string
    {
        if ($this->supports(Capability::AlterLock) && $this->locks) {
            return ',LOCK=SHARED';
        }

        return '';
    }

    /**
     * @throws Exception
     * @throws PDOException
     */
    #[\Override]
    public function ping(): bool
    {
        $result = $this->createBuilder()->fromNone()->selectRaw('1')->build();

        return $this->prepareStatement($result->query)->execute();
    }

    #[\Override]
    public function reconnect(): void
    {
        $pdo = $this->getDriver();
        if ($pdo instanceof DatabasePDO) {
            $pdo->reconnect();
        }
        $this->inTransaction = 0;
    }

    #[\Override]
    public function startTransaction(): bool
    {
        try {
            if ($this->inTransaction === 0) {
                try {
                    if ($this->getDriver()->inTransaction()) {
                        $this->getDriver()->rollBack();
                    } else {
                        // If no active transaction, this has no effect.
                        $this->prepareStatement('ROLLBACK')->execute();
                    }
                } catch (PDOException) {
                    // A pooled connection can report a transaction it no longer
                    // holds after a reconnect (e.g. Swoole PDOProxy keeps its own
                    // counter), making this cleanup rollback throw. It is best
                    // effort; swallow it and begin a fresh transaction below.
                }

                $result = $this->getDriver()->beginTransaction();

            } else {
                $this->getDriver()->exec('SAVEPOINT transaction'.$this->inTransaction);
                $result = true;
            }
        } catch (PDOException $e) {
            throw new TransactionException('Failed to start transaction: '.$e->getMessage(), $e->getCode(), $e);
        }

        if ($result !== true) {
            throw new TransactionException('Failed to start transaction');
        }

        $this->inTransaction++;

        return true;
    }

    #[\Override]
    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        if (! $this->getDriver()->inTransaction()) {
            $this->inTransaction = 0;

            throw new TransactionException('Failed to commit transaction: the connection no longer holds the transaction');
        }

        if ($this->inTransaction > 1) {
            $this->inTransaction--;

            return true;
        }

        try {
            $result = $this->getDriver()->commit();
            $this->inTransaction = 0;
        } catch (PDOException $e) {
            throw new TransactionException('Failed to commit transaction: '.$e->getMessage(), $e->getCode(), $e);
        }

        if (! $result) {
            throw new TransactionException('Failed to commit transaction');
        }

        return $result;
    }

    #[\Override]
    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        try {
            if ($this->inTransaction > 1) {
                $this->getDriver()->exec('ROLLBACK TO transaction'.($this->inTransaction - 1));
                $this->inTransaction--;
                $result = true;
            } else {
                $result = $this->getDriver()->rollBack();
                $this->inTransaction = 0;
            }
        } catch (PDOException $e) {
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to rollback transaction: '.$e->getMessage(), $e->getCode(), $e);
        }

        if ($result !== true) {
            throw new TransactionException('Failed to rollback transaction');
        }

        return true;
    }

    #[\Override]
    protected function abandonTransaction(): void
    {
        $pdo = $this->getDriver();
        if (! $pdo->inTransaction()) {
            return;
        }

        try {
            $pdo->rollBack();
        } catch (PDOException) {
            // A connection that only reports a transaction it no longer holds has nothing left to end.
        }
    }

    /**
     * @throws DatabaseException
     */
    #[\Override]
    public function exists(string $database): bool
    {
        $result = $this->createBuilder()
            ->from('INFORMATION_SCHEMA.SCHEMATA')
            ->selectRaw('SCHEMA_NAME')
            ->filter([BaseQuery::equal('SCHEMA_NAME', [$this->filter($database)])])
            ->build();

        return $this->returnsRows($this->executeResult($result, Event::DatabaseList));
    }

    /**
     * @throws DatabaseException
     */
    #[\Override]
    public function collectionExists(string $database, string $collection): bool
    {
        $result = $this->createBuilder()
            ->from('INFORMATION_SCHEMA.TABLES')
            ->selectRaw('TABLE_NAME')
            ->filter([
                BaseQuery::equal('TABLE_SCHEMA', [$this->filter($database)]),
                BaseQuery::equal('TABLE_NAME', ["{$this->getNamespace()}_{$this->filter($collection)}"]),
            ])
            ->build();

        return $this->returnsRows($this->executeResult($result, Event::CollectionRead));
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
            $error = $this->processException($error);

            if ($error instanceof NotFoundException) {
                return false;
            }

            throw $error;
        }

        return ! empty($rows);
    }

    /**
     * @return array<Document>
     */
    #[\Override]
    public function list(): array
    {
        return [];
    }

    /**
     * @throws Exception
     * @throws PDOException
     */
    #[\Override]
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        return $this->createAttributeWithEvent($collection, $attribute, Event::AttributeCreate);
    }

    protected function createAttributeWithEvent(string $collection, Attribute $attribute, Event $event): bool
    {
        $schema = $this->schema();
        $table = $schema->table($this->getTableRaw($collection));
        $this->addAttributeColumn($table, $attribute);
        $result = $table->alter();

        $sql = $result->query;
        $lockType = $this->getLockType();
        if (! empty($lockType)) {
            $sql = rtrim($sql, ';').' '.$lockType;
        }

        try {
            return $this->executeStatement($sql, $event);
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
        $schema = $this->schema();
        $table = $schema->table($this->getTableRaw($collection));
        foreach ($attributes as $attribute) {
            $this->addAttributeColumn($table, $attribute);
        }
        $result = $table->alter();

        $sql = $result->query;
        $lockType = $this->getLockType();
        if (! empty($lockType)) {
            $sql = rtrim($sql, ';').' '.$lockType;
        }

        try {
            return $this->executeStatement($sql, Event::AttributesCreate);
        } catch (PDOException $error) {
            throw $this->processException($error);
        }
    }

    /**
     * @throws Exception
     * @throws PDOException
     */
    #[\Override]
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
            throw $this->processException($e);
        }
    }

    /**
     * @throws Exception
     * @throws PDOException
     */
    #[\Override]
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
     * Whether an earlier rename already moved the column: under shared tables every tenant
     * of a collection id renames the one physical column, so only the first rename runs.
     *
     * @throws DatabaseException
     */
    protected function isRenamed(string $collection, string $old, string $new): bool
    {
        $old = $this->filter($old);
        $new = $this->filter($new);

        if ($old === $new) {
            return false;
        }

        $columns = $this->getColumnNames($collection);

        return ! \in_array($old, $columns, true) && \in_array($new, $columns, true);
    }

    /**
     * The physical column names of a collection's table, empty when the table does not exist.
     *
     * @return array<string>
     *
     * @throws DatabaseException
     */
    abstract protected function getColumnNames(string $collection): array;

    /**
     * @param  Query[]  $queries
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $collectionDoc = $collection;
        $collection = $collection->getId();

        $name = $this->filter($collection);
        $selections = $this->getAttributeSelections($queries);
        $alias = Query::DEFAULT_ALIAS;

        // Fast path: single-row lookup by primary key with no projection and
        // no joins. This is by far the most common shape (metadata fetch,
        // primary cache miss, the locked read of every update); skip the
        // builder pipeline and go directly to a parameterised SELECT, filtered
        // by tenant and locked the way the builder's Tenant\Filter and lock
        // clause do it.
        if (
            empty($selections)
            && ! $this->queriesHaveJoins($queries)
        ) {
            $tableExpr = $this->getTable($name);
            $aliasQuoted = $this->quote($alias);
            $uidQuoted = $this->quote(Storage::UID);
            $sql = "SELECT * FROM {$tableExpr} AS {$aliasQuoted} WHERE {$this->collateDocumentId($uidQuoted)} = " . ':'.Storage::UID;
            $bindings = [':'.Storage::UID => $id];
            if ($this->sharedTables) {
                $tenantColumn = $aliasQuoted.'.'.Storage::TENANT;
                $sql .= $name === Database::METADATA || $name === Storage::permissionsTable(Database::METADATA)
                    ? " AND ({$tenantColumn} IN (:".Storage::TENANT.") OR {$tenantColumn} IS NULL)"
                    : " AND {$tenantColumn} IN (:".Storage::TENANT.')';
                $bindings[':'.Storage::TENANT] = $this->currentTenant();
            }
            if ($forUpdate && $this->supports(Capability::UpdateLock)) {
                $sql .= ' FOR UPDATE';
            }
            $statement = null;
            $row = false;
            $exception = null;

            try {
                $statement = $this->prepareStatement($sql, Event::DocumentRead);
                foreach ($bindings as $parameter => $value) {
                    $statement->bindValue($parameter, $value, $this->getPdoType($value));
                }
                $this->describeStatement($statement, $bindings, $name);
                $this->execute($statement);
                /** @var array<string, mixed>|false $row */
                $row = $statement->fetch(PDO::FETCH_ASSOC);
            } catch (PDOException $e) {
                $exception = $e;
            } finally {
                if ($statement !== null) {
                    try {
                        $statement->closeCursor();
                    } catch (PDOException $e) {
                        $exception ??= $e;
                    }
                }
            }

            if ($exception !== null) {
                throw $this->processException($exception);
            }

            if (! is_array($row) || empty($row)) {
                return new Document([]);
            }

            $this->remapRow($row);

            return Document::fromRow($row);
        }

        if ($this->queriesHaveJoins($queries)) {
            if ($forUpdate) {
                throw new QueryException('Cannot lock a document for update when join queries are present');
            }

            $roles = $this->authorization->getRoles();
            $queries = \array_map(static fn ($query) => clone $query, $queries);
            $joinTablePrefixes = $this->remapJoinQueries($queries);
            $queries = $this->rewriteFullOuterJoins($queries, Method::LeftJoin);

            $builder = $this->newBuilder($name, $alias, $this->keepsUnmatchedRows($queries), unindexed: $this->unindexedJoins($collectionDoc, $queries, $joinTablePrefixes));
            $this->configureFindBuilder(
                $builder,
                $collectionDoc,
                $queries,
                $joinTablePrefixes,
                false,
                false,
                [],
                $name,
                $alias,
                $roles,
                PermissionType::Read,
            );
            $builder->filter([BaseQuery::equal($alias.'.'.Storage::UID, [$id])]);

            $joinAliases = \array_column($joinTablePrefixes, 'alias');
            foreach ($joinAliases as $joinAlias) {
                $builder->sortAsc($this->qualifyOrderAttribute($joinAlias.'.'.Document::SEQUENCE, $joinAliases));
            }
            $builder->limit(1);
        } else {
            $builder = $this->newBuilder($name, $alias);

            if (! \in_array('*', $selections)) {
                $builder->select($this->mapSelectionsToColumns($selections, joinAliases: []));
            }

            $builder->filter([BaseQuery::equal(Storage::UID, [$id])]);

            if ($forUpdate && $this->supports(Capability::UpdateLock)) {
                $builder->forUpdate();
            }
        }

        $rows = $this->executeSelect($builder, Event::DocumentRead, $name);

        if (empty($rows)) {
            return new Document([]);
        }

        /** @var array<string, mixed> $document */
        $document = $rows[0];

        $this->remapRow($document);

        return Document::fromRow($document);
    }

    /**
     * Under ignoreDuplicates() only the documents written are returned and handed to the write
     * hooks, so a skipped document writes no permission rows for a stored one.
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DuplicateException
     * @throws Throwable
     */
    #[\Override]
    public function createDocuments(Document $collection, array $documents): array
    {
        if (empty($documents)) {
            return $documents;
        }

        $this->syncWriteHooks();

        $spatialAttributes = $this->getSpatialAttributes($collection);
        $collection = $collection->getId();
        try {
            $name = $this->filter($collection);
            $hasSequence = $this->batchHasSequence($documents);

            if ($this->isIgnoringDuplicates()) {
                $documents = $this->firstCopies($documents);
                $documents = $this->supportsInsertReturning()
                    ? $this->insertReturning($name, $documents, $spatialAttributes, $hasSequence)
                    : $this->insertThenReadBack($name, $documents, $spatialAttributes, $hasSequence);
            } else {
                $insert = $this->buildDocumentsInsert($name, $documents, $spatialAttributes, $hasSequence)->insert();
                $this->execute($this->executeResult($insert, Event::DocumentsCreate));
            }

            if (! empty($documents)) {
                $context = $this->writeContext();
                $this->runWriteHooks(fn ($hook) => $hook->afterDocumentCreate($name, $documents, $context));
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $documents;
    }

    protected function supportsInsertReturning(): bool
    {
        return true;
    }

    /**
     * MariaDB, MySQL and SQLite cannot name the index to ignore, so they skip any unique collision.
     *
     * @throws DatabaseException
     */
    protected function insertOrIgnore(SQLBuilder $builder): Statement
    {
        if (! $builder instanceof InsertOrIgnoreFeature) {
            throw new DatabaseException('Insert-or-ignore is not supported on this dialect');
        }

        return $builder->insertOrIgnore();
    }

    /**
     * @return list<string>
     */
    protected function documentKeyColumns(): array
    {
        return $this->sharedTables ? [Storage::UID, Storage::TENANT] : [Storage::UID];
    }

    /**
     * @param  array<Document>  $documents
     *
     * @throws DatabaseException
     */
    private function batchHasSequence(array $documents): bool
    {
        $hasSequence = null;
        foreach ($documents as $document) {
            if ($hasSequence === null) {
                $hasSequence = ! empty($document->getSequence());
            } elseif ($hasSequence == empty($document->getSequence())) {
                throw new DatabaseException('All documents must have an sequence if one is set');
            }
        }

        return $hasSequence ?? false;
    }

    /**
     * A single statement writes at most one copy of an id, and a later copy may be written when
     * the first is skipped for another unique value; keeping only the first copy leaves no row
     * whose grants could be taken from another copy.
     *
     * @param  array<Document>  $documents
     * @return list<Document>
     */
    private function firstCopies(array $documents): array
    {
        $seen = [];
        $firstCopies = [];
        foreach ($documents as $document) {
            [$tenant, $id] = $this->documentKey($document);
            if (isset($seen[$tenant][$id])) {
                continue;
            }
            $seen[$tenant][$id] = true;
            $firstCopies[] = $document;
        }

        return $firstCopies;
    }

    /**
     * @param  array<Document>  $documents
     * @param  list<string>  $spatialAttributes
     *
     * @throws DatabaseException
     */
    private function buildDocumentsInsert(string $name, array $documents, array $spatialAttributes, bool $hasSequence): SQLBuilder
    {
        $attributeKeySet = [];
        foreach (Database::INTERNAL_ATTRIBUTE_KEYS as $key) {
            $attributeKeySet[$key] = true;
        }

        foreach ($documents as $document) {
            foreach ($document->getAttributes() as $key => $value) {
                $attributeKeySet[$key] = true;
            }
        }

        $attributeKeys = \array_keys($attributeKeySet);

        if ($hasSequence) {
            $attributeKeys[] = Storage::SEQUENCE;
        }

        $builder = $this->createBuilder()->into($this->getTableRaw($name));

        $spatialMap = \array_fill_keys($spatialAttributes, true);

        foreach ($spatialAttributes as $spatialColumn) {
            $builder->insertColumnExpression($spatialColumn, $this->getSpatialGeometryFromText('?'));
        }

        $intBools = $this->supports(Capability::IntegerBooleans);

        foreach ($documents as $document) {
            $row = $this->buildDocumentRow($document, $attributeKeys, $spatialMap, $intBools);
            $row = $this->decorateRow($row, $document);
            $builder->set($row);
        }

        return $builder;
    }

    /**
     * @param  list<Document>  $documents
     * @param  list<string>  $spatialAttributes
     * @return list<Document>
     *
     * @throws DatabaseException
     */
    private function insertReturning(string $name, array $documents, array $spatialAttributes, bool $hasSequence): array
    {
        $builder = $this->buildDocumentsInsert($name, $documents, $spatialAttributes, $hasSequence);
        $columns = $this->documentKeyColumns();

        if ($builder instanceof MariaDBReturning) {
            $insert = $this->insertOrIgnore($builder->returning($columns));
        } else {
            $insert = $this->insertOrIgnore($builder);
            $quoted = \array_map($this->quote(...), $columns);
            $insert = new Statement($insert->query.' RETURNING '.\implode(', ', $quoted), $insert->bindings);
        }

        $statement = $this->executeResult($insert, Event::DocumentsCreate);
        $this->execute($statement);
        /** @var list<list<mixed>> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_NUM);
        $statement->closeCursor();

        $written = $this->rowKeys($rows);

        return \array_values(\array_filter(
            $documents,
            fn (Document $document): bool => $this->hasKey($written, $document),
        ));
    }

    /**
     * Without RETURNING the ids are read before the insert, which keeps a stored id out of it,
     * and read back when the insert wrote fewer rows than it was sent. A row read back is taken
     * as written only when it carries the document's own permissions: a row another writer
     * stored meanwhile under the same id then gains no grant it does not already state.
     *
     * @param  list<Document>  $documents
     * @param  list<string>  $spatialAttributes
     * @return list<Document>
     *
     * @throws DatabaseException
     */
    private function insertThenReadBack(string $name, array $documents, array $spatialAttributes, bool $hasSequence): array
    {
        $stored = $this->rowKeys($this->readRows($name, $documents, $this->documentKeyColumns()));
        $candidates = \array_values(\array_filter(
            $documents,
            fn (Document $document): bool => ! $this->hasKey($stored, $document),
        ));

        if (empty($candidates)) {
            return [];
        }

        $statement = $this->executeResult(
            $this->insertOrIgnore($this->buildDocumentsInsert($name, $candidates, $spatialAttributes, $hasSequence)),
            Event::DocumentsCreate,
        );
        $this->execute($statement);
        $written = $statement->rowCount();
        $statement->closeCursor();

        if ($written === \count($candidates)) {
            return $candidates;
        }

        $permissions = [];
        foreach ($this->readRows($name, $candidates, [...$this->documentKeyColumns(), Storage::PERMISSIONS]) as $row) {
            $rowPermissions = \end($row);
            $permissions[$this->rowTenant($row)][$this->rowId($row)] = \is_string($rowPermissions) ? \json_decode($rowPermissions, true) : null;
        }

        return \array_values(\array_filter(
            $candidates,
            function (Document $document) use ($permissions): bool {
                [$tenant, $id] = $this->documentKey($document);

                return \array_key_exists($id, $permissions[$tenant] ?? [])
                    && $permissions[$tenant][$id] === $document->getPermissions();
            },
        ));
    }

    /**
     * @param  list<Document>  $documents
     * @param  list<string>  $columns
     * @return list<list<mixed>>
     *
     * @throws DatabaseException
     */
    private function readRows(string $name, array $documents, array $columns): array
    {
        $ids = [];
        $tenants = [];
        foreach ($documents as $document) {
            $ids[] = $document->getId();
            $tenant = $this->documentTenant($document);
            if ($this->sharedTables && $this->tenantPerDocument && ! \in_array($tenant, $tenants, true)) {
                $tenants[] = $tenant;
            }
        }

        $builder = $this->newBuilder($name, tenants: $tenants);
        $builder->select($columns);
        $builder->filter([BaseQuery::equal(Storage::UID, \array_values(\array_unique($ids)))]);

        $statement = $this->executeResult($builder->build(), Event::DocumentRead);
        $this->execute($statement);
        /** @var list<list<mixed>> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_NUM);
        $statement->closeCursor();

        return $rows;
    }

    /**
     * @param  list<list<mixed>>  $rows  each starting with `_uid`, then `_tenant` under shared tables
     * @return array<string, array<string, true>>
     */
    private function rowKeys(array $rows): array
    {
        $keys = [];
        foreach ($rows as $row) {
            $keys[$this->rowTenant($row)][$this->rowId($row)] = true;
        }

        return $keys;
    }

    /**
     * @param  list<mixed>  $row
     */
    private function rowId(array $row): string
    {
        $id = $row[0] ?? null;

        return \is_scalar($id) ? (string) $id : '';
    }

    /**
     * @param  list<mixed>  $row
     */
    private function rowTenant(array $row): string
    {
        $tenant = $this->sharedTables ? ($row[1] ?? null) : null;

        return \is_scalar($tenant) ? (string) $tenant : '';
    }

    /**
     * @param  array<string, array<string, true>>  $keys
     */
    private function hasKey(array $keys, Document $document): bool
    {
        [$tenant, $id] = $this->documentKey($document);

        return isset($keys[$tenant][$id]);
    }

    /**
     * @return array{string, string}
     */
    private function documentKey(Document $document): array
    {
        $tenant = $this->documentTenant($document);

        return [$tenant === null ? '' : (string) $tenant, $document->getId()];
    }

    private function documentTenant(Document $document): int|string|null
    {
        return $this->sharedTables ? ($document->getTenant() ?? $this->currentTenant()) : null;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<string, true>  $skipPermissions
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function updateDocuments(Document $collection, Document $updates, array $documents, array $skipPermissions = []): int
    {
        if (empty($documents)) {
            return 0;
        }

        $this->syncWriteHooks();

        $spatialAttributes = $this->getSpatialAttributes($collection);
        $collection = $collection->getId();

        $attributes = $updates->getAttributes();

        if (! empty($updates->getUpdatedAt())) {
            $attributes[Storage::UPDATED_AT] = $updates->getUpdatedAt();
        }

        if (! empty($updates->getCreatedAt())) {
            $attributes[Storage::CREATED_AT] = $updates->getCreatedAt();
        }

        if ($updates->offsetExists(Document::PERMISSIONS)) {
            $attributes[Storage::PERMISSIONS] = json_encode($updates->getPermissions());
        }

        if (empty($attributes)) {
            return 0;
        }

        $name = $this->filter($collection);

        $builder = $this->newBuilder($name);

        // Single pass over update attributes, bucketing into regular / spatial /
        // operator and applying JSON / boolean conversions inline. Hoisted
        // guards keep the hot path branch-light.
        $spatialMap = \array_fill_keys($spatialAttributes, true);
        $intBools = $this->supports(Capability::IntegerBooleans);

        $regularRow = [];
        $spatialRows = [];
        $operators = [];

        foreach ($attributes as $attribute => $value) {
            if (Operator::isOperator($value)) {
                $operators[$attribute] = $value;

                continue;
            }

            if (isset($spatialMap[$attribute])) {
                $spatialRows[$this->filter($attribute)] = $this->encodeSpatialWriteValue($value);

                continue;
            }

            $column = $this->filter($attribute);

            if (\is_array($value)) {
                $value = \json_encode($value);
            }
            if ($intBools && \is_bool($value)) {
                $value = (int) $value;
            }

            $regularRow[$column] = $value;
        }

        if (! empty($regularRow)) {
            $builder->set($regularRow);
        }

        foreach ($spatialRows as $column => $value) {
            $builder->setRaw($column, $this->getSpatialGeometryFromText('?'), [$value]);
        }

        foreach ($operators as $attribute => $operator) {
            $column = $this->filter($attribute);
            /** @var Operator $operator */
            $expression = $this->getOperatorBuilderExpression($column, $operator);
            $builder->setRaw($column, $expression->sql, $expression->bindings);
        }

        $sequences = \array_map(fn ($document) => $document->getSequence(), $documents);
        $builder->filter([BaseQuery::equal(Storage::SEQUENCE, \array_values($sequences))]);

        $result = $builder->update();
        $statement = $this->executeResult($result, Event::DocumentsUpdate);

        try {
            $this->execute($statement);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $affected = $statement->rowCount();

        $context = $this->writeContext($skipPermissions);
        $this->runWriteHooks(fn ($hook) => $hook->afterDocumentBatchUpdate($name, $updates, $documents, $context));

        return $affected;
    }

    /**
     * @throws DatabaseException
     */
    #[\Override]
    public function upsertDocument(Document $collection, Change $change): Document
    {
        return $this->upsertDocuments($collection, [$change])[0];
    }

    /**
     * @param  array<Change>  $changes
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function upsertDocuments(Document $collection, array $changes, ?string $increase = null): array
    {
        if ($changes === []) {
            return [];
        }

        $this->syncWriteHooks();

        try {
            $spatialAttributes = $this->getSpatialAttributes($collection);

            /** @var array<string, mixed> $attributeDefaults */
            $attributeDefaults = [];
            foreach (self::collectionAttributes($collection) as $declared) {
                $attributeDefaults[$declared->key] = $declared->default;
            }

            $collection = $collection->getId();
            $name = $this->filter($collection);

            $hasOperators = false;
            $firstChange = $changes[0];
            $firstDoc = $firstChange->new;
            $firstExtracted = Operator::extractOperators($firstDoc->getAttributes());

            if (! empty($firstExtracted['operators'])) {
                $hasOperators = true;
            } else {
                foreach ($changes as $change) {
                    $doc = $change->new;
                    $extracted = Operator::extractOperators($doc->getAttributes());
                    if (! empty($extracted['operators'])) {
                        $hasOperators = true;
                        break;
                    }
                }
            }

            if (! $hasOperators) {
                $this->executeUpsertBatch($name, $changes, $spatialAttributes, $increase ?? '', [], $attributeDefaults, false);
            } else {
                $groups = [];

                foreach ($changes as $change) {
                    $document = $change->new;
                    $extracted = Operator::extractOperators($document->getAttributes());
                    $operators = $extracted['operators'];

                    if (empty($operators)) {
                        $signature = 'no_ops';
                    } else {
                        $parts = [];
                        foreach ($operators as $attribute => $operation) {
                            $parts[] = $attribute.':'.$operation->getMethod()->value.':'.json_encode($operation->getValues());
                        }
                        sort($parts);
                        $signature = implode('|', $parts);
                    }

                    if (! isset($groups[$signature])) {
                        $groups[$signature] = [
                            'documents' => [],
                            'operators' => $operators,
                        ];
                    }

                    $groups[$signature]['documents'][] = $change;
                }

                foreach ($groups as $group) {
                    $this->executeUpsertBatch($name, $group['documents'], $spatialAttributes, '', $group['operators'], $attributeDefaults, true);
                }
            }

            $context = $this->writeContext();
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentUpsert($name, $changes, $context));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return \array_map(static fn (Change $change): Document => $change->new, $changes);
    }

    /**
     * @param  array<string>  $sequences
     * @param  array<string>  $permissionIds
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function deleteDocuments(Document $collection, array $sequences, array $permissionIds): int
    {
        if (empty($sequences)) {
            return 0;
        }

        $this->syncWriteHooks();

        try {
            $name = $this->filter($collection->getId());

            $builder = $this->newBuilder($name);
            $builder->filter([BaseQuery::equal(Storage::SEQUENCE, \array_values($sequences))]);
            $result = $builder->delete();
            $statement = $this->executeResult($result, Event::DocumentsDelete);

            if (! $this->execute($statement)) {
                throw new DatabaseException('Failed to delete documents');
            }

            $context = $this->writeContext();
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentDelete($name, \array_values($permissionIds), $context));
        } catch (Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        return $statement->rowCount();
    }

    /**
     * Assign internal IDs for the given documents
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function getSequences(Document $collection, array $documents): array
    {
        $documentIds = [];
        $tenants = [];
        $keyedByTenant = $this->sharedTables && $this->tenantPerDocument;

        foreach ($documents as $document) {
            if (empty($document->getSequence())) {
                $documentIds[] = $document->getId();

                if ($keyedByTenant) {
                    $tenant = $document->getTenant();
                    if (! \in_array($tenant, $tenants, true)) {
                        $tenants[] = $tenant;
                    }
                }
            }
        }

        if (empty($documentIds)) {
            return $documents;
        }

        $builder = $this->newBuilder($collection->getId(), tenants: $tenants);
        $builder->select($keyedByTenant
            ? [Storage::UID, Storage::SEQUENCE, Storage::TENANT]
            : [Storage::UID, Storage::SEQUENCE]);
        $builder->filter([BaseQuery::equal(Storage::UID, $documentIds)]);

        $result = $builder->build();
        $statement = $this->executeResult($result, Event::DocumentRead);
        $this->execute($statement);

        $sequenceKey = static fn (mixed $tenant, mixed $id): string => (\is_scalar($tenant) ? (string) $tenant : '')."\0".(\is_scalar($id) ? (string) $id : '');

        if ($keyedByTenant) {
            $sequences = [];
            /** @var array<string, mixed> $row */
            foreach ($statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
                $sequences[$sequenceKey($row[Storage::TENANT] ?? null, $row[Storage::UID] ?? null)] = $row[Storage::SEQUENCE] ?? null;
            }
        } else {
            /** @var array<string, mixed> $sequences */
            $sequences = $statement->fetchAll(PDO::FETCH_KEY_PAIR);
        }
        $statement->closeCursor();

        foreach ($documents as $document) {
            $key = $keyedByTenant ? $sequenceKey($document->getTenant(), $document->getId()) : $document->getId();
            if (isset($sequences[$key])) {
                $document[Document::SEQUENCE] = $sequences[$key];
            }
        }

        return $documents;
    }

    #[\Override]
    public function increaseDocumentAttribute(
        Document $collection,
        string $id,
        string $attribute,
        int|float|string $value,
        string $updatedAt,
        int|float|string|null $min = null,
        int|float|string|null $max = null
    ): bool {
        $name = $this->filter($collection->getId());
        $attribute = $this->filter($attribute);

        $builder = $this->newBuilder($name);
        $builder->setRaw($attribute, 'COALESCE('.$this->quote($attribute).', 0) + ?', [$value]);
        $builder->set([Storage::UPDATED_AT => $updatedAt]);

        $filters = [BaseQuery::equal(Storage::UID, [$id])];
        if ($max !== null) {
            $withinMaximum = BaseQuery::lessThanEqual($attribute, $max);
            $filters[] = (float) $max >= 0 ? BaseQuery::or([$withinMaximum, BaseQuery::isNull($attribute)]) : $withinMaximum;
        }
        if ($min !== null) {
            $withinMinimum = BaseQuery::greaterThanEqual($attribute, $min);
            $filters[] = (float) $min <= 0 ? BaseQuery::or([$withinMinimum, BaseQuery::isNull($attribute)]) : $withinMinimum;
        }
        $builder->filter($filters);

        $result = $builder->update();
        $event = $value < 0 ? Event::DocumentDecrease : Event::DocumentIncrease;
        $statement = $this->executeResult($result, $event);

        try {
            $this->execute($statement);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return true;
    }

    #[\Override]
    public function deleteDocument(Document $collection, string $id): bool
    {
        try {
            $this->syncWriteHooks();

            $name = $this->filter($collection->getId());

            $builder = $this->newBuilder($name);
            $filters = [BaseQuery::equal(Storage::UID, [$id])];
            $builder->filter($filters);
            $result = $builder->delete();
            $statement = $this->executeResult($result, Event::DocumentDelete);

            if (! $this->execute($statement)) {
                throw new DatabaseException('Failed to delete document');
            }

            $deleted = $statement->rowCount();

            $context = $this->writeContext();
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentDelete($name, [$id], $context));
        } catch (\Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        return $deleted > 0;
    }

    /**
     * @param  array<Query>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<Document>
     *
     * @throws DatabaseException
     * @throws TimeoutException
     * @throws Exception
     */
    #[\Override]
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
    {
        $collectionDoc = $collection;
        $collection = $collection->getId();
        $name = $this->filter($collection);
        $roles = $this->authorization->getRoles();
        $alias = Query::DEFAULT_ALIAS;

        // Fast path: trivial SELECT * with default ORDER BY _id and LIMIT/OFFSET.
        // Triggered when there are no filters/joins/aggregations/cursor queries,
        // a single default order attribute, ascending, no shared tenant, and no
        // active permission filter. This is the common "list documents" case
        // and bypasses Builder allocation entirely.
        if (
            empty($queries)
            && empty($cursor)
            && ! $this->authorization->getStatus()
            && ! $this->sharedTables
            && (count($orderAttributes) === 1)
            && ($orderAttributes[0] === Document::SEQUENCE)
            && (empty($orderTypes) || ($orderTypes[0] ?? OrderDirection::Asc) === OrderDirection::Asc)
            && $cursorDirection === CursorDirection::After
        ) {
            $internalOrder = $this->quote($this->getInternalKeyForAttribute(Document::SEQUENCE));
            $tableExpr = $this->getTable($name);
            $aliasQuoted = $this->quote($alias);
            $pageLimit = $limit ?? ($offset !== null && $offset > 0 ? self::UNBOUNDED_LIMIT : null);
            $limitClause = $pageLimit !== null ? " LIMIT {$pageLimit}" : '';
            $offsetClause = $offset !== null && $offset > 0 ? " OFFSET {$offset}" : ($pageLimit !== null ? ' OFFSET 0' : '');

            $sql = "SELECT * FROM {$tableExpr} AS {$aliasQuoted} ORDER BY {$internalOrder} ASC{$limitClause}{$offsetClause}";
            $statement = null;
            $rows = [];
            $exception = null;

            try {
                $statement = $this->prepareStatement($sql, Event::DocumentFind);
                $this->describeStatement($statement, [], $name);
                $this->execute($statement);
                /** @var array<int, array<string, mixed>> $rows */
                $rows = $statement->fetchAll();
            } catch (PDOException $e) {
                $exception = $e;
            } finally {
                if ($statement !== null) {
                    try {
                        $statement->closeCursor();
                    } catch (PDOException $e) {
                        $exception ??= $e;
                    }
                }
            }

            if ($exception !== null) {
                throw $this->processException($exception);
            }

            $documents = [];
            foreach ($rows as $row) {
                $this->remapRow($row);
                $documents[] = Document::fromRow($row);
            }

            return $documents;
        }

        // Single pass partitioning: pull vector queries out for ORDER BY and
        // detect aggregation/join shape in the same walk. Each Method::value
        // is checked once per query rather than three times.
        // Defer the defensive `clone` until we know the query path will mutate
        // the Query objects (joins or aggregations-with-joins). The vast
        // majority of finds take neither path and don't need a per-query
        // clone allocation.
        $vectorQueries = [];
        $otherQueries = [];
        $adapterFilterQueries = [];
        $hasAggregation = false;
        $hasJoins = false;
        $hasDistinct = false;

        foreach ($queries as $query) {
            $method = $query->getMethod();

            if ($method->isVector()) {
                $vectorQueries[] = $query;

                continue;
            }

            if ($this->isAdapterFilterQuery($query)) {
                $adapterFilterQueries[] = $query;

                continue;
            }

            $otherQueries[] = $query;

            if ($method->isAggregate() || $method === Method::GroupBy) {
                $hasAggregation = true;
            }
            if ($method->isJoin()) {
                $hasJoins = true;
            }
            if ($method === Method::Distinct) {
                $hasDistinct = true;
            }
        }

        $queries = $otherQueries;

        if ($hasJoins) {
            $queries = \array_map(static fn ($query) => clone $query, $queries);
        }

        $joinTablePrefixes = [];
        if ($hasJoins) {
            $joinTablePrefixes = $this->remapJoinQueries($queries);
        }
        $unindexed = $this->unindexedJoins($collectionDoc, $queries, $joinTablePrefixes);

        $hasPreservingOuterJoin = false;
        if ($hasJoins) {
            foreach ($queries as $query) {
                $method = $query->getMethod();
                if ($method === Method::RightJoin || $method === Method::FullOuterJoin) {
                    $hasPreservingOuterJoin = true;
                    break;
                }
            }
        }

        if ($joinTablePrefixes !== []) {
            [$orderAttributes, $cursor] = $this->qualifyJoinedOrders($orderAttributes, $cursor, $collectionDoc, $joinTablePrefixes);
        }

        $joinAliases = \array_column($joinTablePrefixes, 'alias');
        $internalKeyCache = [];
        $resolveInternalKey = function (string $attribute) use (&$internalKeyCache, $joinAliases): string {
            return $internalKeyCache[$attribute]
                ??= $this->qualifyOrderAttribute($attribute, $joinAliases);
        };

        $emulatesFullOuterJoin = $this->needsFullOuterJoinEmulation($this->createBuilder(), $queries);

        if ($emulatesFullOuterJoin && $hasAggregation) {
            $results = $this->findFullOuterJoinAggregate(
                $collectionDoc,
                $queries,
                $joinTablePrefixes,
                $hasDistinct,
                $adapterFilterQueries,
                $name,
                $alias,
                $roles,
                $forPermission,
                $orderAttributes,
                $orderTypes,
                $limit,
                $offset,
                $cursor,
                $cursorDirection,
                $resolveInternalKey,
            );
        } elseif ($emulatesFullOuterJoin) {
            if ($hasDistinct) {
                $this->assertDistinctOrderIsSelected($queries, $orderAttributes, $orderTypes, $joinAliases);
            }

            [$leftQueries, $rightQueries] = $this->emulateFullOuterJoin($queries, $alias);
            $leftPreserving = $this->keepsUnmatchedRows($leftQueries);

            $left = $this->newBuilder($name, $alias, $leftPreserving, unindexed: $unindexed);
            $leftProjected = $this->configureFindBuilder(
                $left,
                $collectionDoc,
                $leftQueries,
                $joinTablePrefixes,
                $hasAggregation,
                $hasDistinct,
                $adapterFilterQueries,
                $name,
                $alias,
                $roles,
                $forPermission,
                orderAttributes: $orderAttributes,
            );
            $this->applyFullOuterJoinOrderProjection(
                $left,
                $collectionDoc,
                $alias,
                $orderAttributes,
                $orderTypes,
                $leftProjected,
                $joinTablePrefixes,
            );
            $this->applyFindCursor(
                $left,
                $orderAttributes,
                $orderTypes,
                $cursor,
                $cursorDirection,
                $resolveInternalKey,
                nullable: true,
            );

            $right = $this->newBuilder($name, $alias, true, unindexed: $unindexed);
            $rightProjected = $this->configureFindBuilder(
                $right,
                $collectionDoc,
                $rightQueries,
                $joinTablePrefixes,
                $hasAggregation,
                $hasDistinct,
                $adapterFilterQueries,
                $name,
                $alias,
                $roles,
                $forPermission,
                orderAttributes: $orderAttributes,
            );
            $this->applyFullOuterJoinOrderProjection(
                $right,
                $collectionDoc,
                $alias,
                $orderAttributes,
                $orderTypes,
                $rightProjected,
                $joinTablePrefixes,
            );
            $this->applyFindCursor(
                $right,
                $orderAttributes,
                $orderTypes,
                $cursor,
                $cursorDirection,
                $resolveInternalKey,
                nullable: true,
            );

            if ($hasDistinct) {
                $left->union($right);
            } else {
                $left->unionAll($right);
            }
            $this->applyFindPage($left, $orderAttributes, $orderTypes, $limit, $offset, $cursorDirection, afterUnion: true);
            $results = $this->executeSelect($left, Event::DocumentFind, $name);
        } else {
            $bound = $hasJoins && ! $hasAggregation && ! $hasDistinct && $vectorQueries === [] && $this->boundsJoinedSort()
                ? $this->boundedPage($collectionDoc, $queries, $adapterFilterQueries, $joinTablePrefixes, $orderAttributes, $orderTypes, $limit, $offset, $cursor)
                : null;

            $builder = $this->newBuilder($name, $alias, $hasPreservingOuterJoin, unindexed: $unindexed);
            $hasSelectionProjection = $this->configureFindBuilder(
                $builder,
                $collectionDoc,
                $bound?->withoutSearches($queries) ?? $queries,
                $joinTablePrefixes,
                $hasAggregation,
                $hasDistinct,
                $bound?->withoutSearches($adapterFilterQueries) ?? $adapterFilterQueries,
                $name,
                $alias,
                $roles,
                $forPermission,
                orderAttributes: $orderAttributes,
            );

            $vectorDistance = null;
            $vectorQuery = $vectorQueries[0] ?? null;
            if ($vectorQuery !== null) {
                $vectorDistance = $this->getVectorOrderRaw($vectorQuery, $alias);
            }

            if ($vectorDistance !== null && $vectorQuery !== null) {
                $vectorAttribute = $this->quote($this->filter($vectorQuery->getAttribute()));
                $builder->whereRaw($this->quote($alias).".{$vectorAttribute} IS NOT NULL");
            }

            if (! empty($cursor) && $vectorDistance !== null && ! $hasDistinct) {
                $distance = $cursor[Document::DISTANCE] ?? null;
                if (! \is_numeric($distance)) {
                    throw new QueryException('Vector cursor is missing its distance');
                }
                if (empty($orderAttributes)) {
                    throw new QueryException('Vector cursor requires a unique order attribute');
                }

                $vectorCursor = $this->getVectorCursorCondition(
                    $vectorDistance,
                    (float) $distance,
                    \array_values($orderAttributes),
                    \array_values($orderTypes),
                    $cursor,
                    $cursorDirection,
                    $alias,
                    $resolveInternalKey,
                    nullable: $hasJoins,
                );
                $builder->whereRaw($vectorCursor->sql, $vectorCursor->bindings);
            }

            if ($vectorDistance === null || $hasDistinct) {
                $this->applyFindCursor(
                    $builder,
                    $orderAttributes,
                    $orderTypes,
                    $cursor,
                    $cursorDirection,
                    $resolveInternalKey,
                    nullable: $hasJoins,
                );
            }

            // Vector ordering (comes first for similarity search)
            if ($vectorDistance !== null && ! $hasAggregation && ! $hasDistinct) {
                $vectorOrder = $vectorDistance->sql;
                if (! empty($cursor) && $cursorDirection === CursorDirection::Before) {
                    $vectorOrder .= ' DESC';
                }
                $builder->orderByRaw($vectorOrder, $vectorDistance->bindings);

                if (! $hasSelectionProjection) {
                    $builder->select(['*']);
                }
                $builder->selectRaw(
                    $this->getSqlReadableDistance($vectorDistance->sql).' AS '.$this->quote(Storage::DISTANCE),
                    $vectorDistance->bindings
                );
            }

            if ($bound !== null) {
                $this->joinFromBoundedPage(
                    $builder,
                    $collectionDoc,
                    $bound,
                    $cursor,
                    $cursorDirection,
                    $resolveInternalKey,
                    $name,
                    $alias,
                    $roles,
                    $forPermission,
                );
            }

            $this->applyFindPage($builder, $orderAttributes, $orderTypes, $limit, $offset, $cursorDirection, joinAliases: $joinAliases);
            $results = $this->executeSelect($builder, Event::DocumentFind, $name);
        }

        $documents = [];

        if ($hasAggregation) {
            $inputs = $this->bitwiseInputs($queries);
            foreach ($results as $row) {
                /** @var array<string, mixed> $row */
                $documents[] = Document::fromRow($this->bitwiseResults($row, $inputs));
            }

            return $documents;
        }

        foreach ($results as $row) {
            /** @var array<string, mixed> $row */
            $this->remapRow($row);
            $documents[] = Document::fromRow($row);
        }

        if ($cursorDirection === CursorDirection::Before) {
            $documents = \array_reverse($documents);
        }

        return $documents;
    }

    /**
     * @param array<mixed> $bindings
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function rawQuery(string $query, array $bindings = []): array
    {
        try {
            $statement = $this->prepareStatement($query);
            foreach ($bindings as $i => $value) {
                $statement->bindValue($i + 1, $value, $this->getPdoType($value));
            }
            $this->execute($statement);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $results = $statement->fetchAll();
        $statement->closeCursor();

        $documents = [];
        foreach ($results as $row) {
            /** @var array<string, mixed> $row */
            $documents[] = Document::fromRow($row);
        }

        return $documents;
    }

    /**
     * @param  array<Query>  $queries
     *
     * @throws Exception
     * @throws PDOException
     */
    #[\Override]
    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        $collectionDoc = $collection;
        $collection = $collection->getId();
        $name = $this->filter($collection);
        $roles = $this->authorization->getRoles();
        $alias = Query::DEFAULT_ALIAS;

        $otherQueries = [];
        $hasJoins = false;
        foreach ($queries as $query) {
            if ($query->getMethod()->isVector()) {
                continue;
            }
            $otherQueries[] = $query;
            if ($query->getMethod()->isJoin()) {
                $hasJoins = true;
            }
        }

        if ($hasJoins) {
            $innerBuilder = $this->configureCountBuilder(
                $collectionDoc,
                $otherQueries,
                $name,
                $alias,
                $roles,
                $max,
            );

            return $this->executeWrappedCount($innerBuilder, $name);
        }

        if ($otherQueries === []) {
            return $this->countOf($this->fetchAggregate($collectionDoc, $name, $roles, 'COUNT(1)', '1', $max, Event::DocumentCount));
        }

        $otherQueries = $this->mainRowQueries($otherQueries, $collectionDoc);
        $builder = $this->newBuilder($name, $alias);

        $filters = $this->compileRowFilters($builder, $otherQueries);
        if ($filters !== null) {
            return $this->countOf($this->fetchAggregate($collectionDoc, $name, $roles, 'COUNT(1)', '1', $max, Event::DocumentCount, $filters));
        }

        $this->applyFilters($builder, $otherQueries, $name, $alias);

        if ($this->authorization->getStatus() && $this->filtersPerDocument($collectionDoc)) {
            $builder->addHook($this->newPermissionHook($name, $roles));
        }

        if ($max === null && $this->onlyNarrowsRows($otherQueries)) {
            $builder->count('1', 'sum');

            return $this->countOf($this->fetchAggregateRow($builder, Event::DocumentCount, $name));
        }

        $builder->selectRaw('1');
        if (! \is_null($max)) {
            $builder->limit($max);
        }

        return $this->executeWrappedCount($builder, $name);
    }

    /**
     * @param  array<Query>  $queries
     *
     * @throws Exception
     * @throws PDOException
     */
    #[\Override]
    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): int|float
    {
        $collectionDoc = $collection;
        $collection = $collection->getId();
        $name = $this->filter($collection);
        $roles = $this->authorization->getRoles();
        $alias = Query::DEFAULT_ALIAS;

        $otherQueries = [];
        $hasJoins = false;
        foreach ($queries as $query) {
            if ($query->getMethod()->isVector()) {
                continue;
            }
            $otherQueries[] = $query;
            if ($query->getMethod()->isJoin()) {
                $hasJoins = true;
            }
        }

        if ($hasJoins) {
            $innerBuilder = $this->configureCountBuilder(
                $collectionDoc,
                $otherQueries,
                $name,
                $alias,
                $roles,
                $max,
                $attribute,
            );

            return $this->executeWrappedSum($innerBuilder, 'sum_attr', $name);
        }

        $attribute = $this->filter($attribute);

        $column = $this->quote($attribute);

        if ($otherQueries === []) {
            return $this->sumOf($this->fetchAggregate($collectionDoc, $name, $roles, "SUM({$column})", $column, $max, Event::DocumentSum));
        }

        $otherQueries = $this->mainRowQueries($otherQueries, $collectionDoc);
        $builder = $this->newBuilder($name, $alias);

        $filters = $this->compileRowFilters($builder, $otherQueries);
        if ($filters !== null) {
            return $this->sumOf($this->fetchAggregate($collectionDoc, $name, $roles, "SUM({$column})", $column, $max, Event::DocumentSum, $filters));
        }

        $this->applyFilters($builder, $otherQueries, $name, $alias);

        if ($this->authorization->getStatus() && $this->filtersPerDocument($collectionDoc)) {
            $builder->addHook($this->newPermissionHook($name, $roles));
        }

        if ($max === null && $this->onlyNarrowsRows($otherQueries)) {
            $builder->sum($attribute, 'sum');

            return $this->sumOf($this->fetchAggregateRow($builder, Event::DocumentSum, $name));
        }

        $builder->select([$attribute]);
        if (! \is_null($max)) {
            $builder->limit($max);
        }

        return $this->executeWrappedSum($builder, $attribute, $name);
    }

    /**
     * @param  array<Query>  $queries
     * @param  array<string>  $roles
     */
    private function configureCountBuilder(
        Document $collection,
        array $queries,
        string $name,
        string $alias,
        array $roles,
        ?int $max,
        ?string $sumAttribute = null,
    ): SQLBuilder {
        $queries = \array_map(static fn ($query) => clone $query, $queries);

        $adapterFilterQueries = [];
        $filterQueries = [];
        foreach ($queries as $query) {
            if ($this->isAdapterFilterQuery($query)) {
                $adapterFilterQueries[] = $query;

                continue;
            }
            $filterQueries[] = $query;
        }
        $queries = $filterQueries;

        $joinTablePrefixes = $this->remapJoinQueries($queries);
        $unindexed = $this->unindexedJoins($collection, $queries, $joinTablePrefixes);
        $selectRaw = $sumAttribute === null
            ? '1'
            : $this->qualifySumSelect($sumAttribute, $joinTablePrefixes, $collection).' AS '.$this->quote('sum_attr');

        $hasPreservingOuterJoin = false;
        foreach ($queries as $query) {
            $method = $query->getMethod();
            if ($method === Method::RightJoin || $method === Method::FullOuterJoin) {
                $hasPreservingOuterJoin = true;
                break;
            }
        }

        if ($this->needsFullOuterJoinEmulation($this->createBuilder(), $queries)) {
            [$leftQueries, $rightQueries] = $this->emulateFullOuterJoin($queries, $alias);
            $leftPreserving = $this->keepsUnmatchedRows($leftQueries);

            $left = $this->newBuilder($name, $alias, $leftPreserving, unindexed: $unindexed);
            $left->selectRaw($selectRaw);
            $this->applyFindFilters(
                $left,
                $collection,
                $leftQueries,
                $joinTablePrefixes,
                $adapterFilterQueries,
                $name,
                $alias,
                $roles,
                PermissionType::Read,
            );

            $right = $this->newBuilder($name, $alias, true, unindexed: $unindexed);
            $right->selectRaw($selectRaw);
            $this->applyFindFilters(
                $right,
                $collection,
                $rightQueries,
                $joinTablePrefixes,
                $adapterFilterQueries,
                $name,
                $alias,
                $roles,
                PermissionType::Read,
            );

            $left->unionAll($right);
            if (! \is_null($max)) {
                $this->applyFindPage($left, [], [], $max, null, afterUnion: true);
            }

            return $left;
        }

        $builder = $this->newBuilder($name, $alias, $hasPreservingOuterJoin, unindexed: $unindexed);
        $builder->selectRaw($selectRaw);
        $this->applyFindFilters(
            $builder,
            $collection,
            $queries,
            $joinTablePrefixes,
            $adapterFilterQueries,
            $name,
            $alias,
            $roles,
            PermissionType::Read,
        );

        if (! \is_null($max)) {
            $builder->limit($max);
        }

        return $builder;
    }

    /**
     * @param  list<JoinAlias>  $joinTablePrefixes
     */
    private function qualifySumSelect(string $attribute, array $joinTablePrefixes, Document $collection): string
    {
        $aliasSet = \array_fill_keys(\array_column($joinTablePrefixes, 'alias'), true);
        $aliasSet[Query::DEFAULT_ALIAS] = true;
        $mainAttributes = [];
        foreach (self::collectionAttributes($collection) as $declared) {
            $mainAttributes[$declared->key] = true;
        }

        $qualified = $this->qualifyDottedAttribute($attribute, $aliasSet, $mainAttributes);
        if (! \str_contains($qualified, '.')) {
            $qualified = Query::DEFAULT_ALIAS.'.'.$qualified;
        }

        $dot = \strpos($qualified, '.');
        $prefix = \substr($qualified, 0, (int) $dot);
        $name = \substr($qualified, (int) $dot + 1);

        return $this->quote($this->filter($prefix)).'.'.$this->quote($this->filter($name));
    }

    /**
     * The row of a count() or sum(), written out instead of built: the same statement the builder
     * makes, with the filters as the builder compiles them, the tenant condition newBuilder() adds
     * and the permission condition of the permission hook, each from the hook itself, in the order
     * the builder writes them.
     *
     * @param  array<string>  $roles
     * @return array<string, mixed>
     */
    private function fetchAggregate(Document $collection, string $name, array $roles, string $aggregate, string $column, ?int $max, Event $event, ?Condition $filters = null): array
    {
        $alias = Query::DEFAULT_ALIAS;
        $conditions = [];
        $bindings = [];

        if ($filters !== null) {
            $conditions[] = $filters->expression;
            \array_push($bindings, ...$filters->bindings);
        }

        if ($this->sharedTables) {
            $tenant = (new Tenant\Filter($this->currentTenant(), Database::METADATA, $name, quoteCharacter: $this->getIdentifierQuote()))->filter($alias);
            $conditions[] = $tenant->expression;
            \array_push($bindings, ...$tenant->bindings);
        }

        if ($this->authorization->getStatus() && $this->filtersPerDocument($collection)) {
            $permission = $this->newPermissionHook($name, $roles)->filter($alias);
            $conditions[] = $permission->expression;
            \array_push($bindings, ...$permission->bindings);
        }

        $rows = $this->getTable($name).' AS '.$this->quote($alias);
        if ($conditions !== []) {
            $rows .= ' WHERE '.\implode(' AND ', $conditions);
        }

        $sum = $this->quote('sum');
        if ($max === null) {
            $sql = "SELECT {$aggregate} AS {$sum} FROM {$rows}";
        } else {
            $sql = "SELECT {$aggregate} AS {$sum} FROM (SELECT {$column} FROM {$rows} LIMIT ?) AS {$this->quote('table_count')}";
            $bindings[] = $max;
        }

        return $this->runSelect(new Statement($sql, $bindings), $event, $name)[0] ?? [];
    }

    private function executeWrappedCount(SQLBuilder $innerBuilder, string $collection): int
    {
        $outerBuilder = $this->createBuilder();
        $outerBuilder->fromSub($innerBuilder, 'table_count');
        $outerBuilder->count('1', 'sum');

        return $this->countOf($this->fetchAggregateRow($outerBuilder, Event::DocumentCount, $collection));
    }

    private function executeWrappedSum(SQLBuilder $innerBuilder, string $attribute, string $collection): int|float
    {
        $outerBuilder = $this->createBuilder();
        $outerBuilder->fromSub($innerBuilder, 'table_count');
        $outerBuilder->sum($attribute, 'sum');

        return $this->sumOf($this->fetchAggregateRow($outerBuilder, Event::DocumentSum, $collection));
    }

    /**
     * @param  array<string, mixed>  $row
     */
    private function countOf(array $row): int
    {
        $count = $row['sum'] ?? 0;

        return \is_numeric($count) ? (int) $count : 0;
    }

    /**
     * @param  array<string, mixed>  $row
     */
    private function sumOf(array $row): int|float
    {
        $sum = $row['sum'] ?? 0;

        if (\is_numeric($sum)) {
            return \str_contains((string) $sum, '.') ? (float) $sum : (int) $sum;
        }

        return 0;
    }

    /**
     * Copies of the queries of a count() or sum() without joins that name each dotted attribute key by
     * its column, as find() does: the builder reads any other dotted name as a table and a column.
     *
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    private function mainRowQueries(array $queries, Document $collection): array
    {
        $queries = \array_map(static fn (Query $query): Query => clone $query, $queries);
        $this->remapDottedQueryAttributes($queries, [], $collection);

        return $queries;
    }

    /**
     * The queries of a count() or sum() as the builder compiles them into its WHERE clause, when
     * every one only narrows the rows and the builder compiles filters on their own; null when the
     * statement has to be built.
     *
     * @param  array<Query>  $queries
     *
     * @throws QueryException
     */
    private function compileRowFilters(SQLBuilder $builder, array $queries): ?Condition
    {
        if (! $builder instanceof Filtering || ! $this->onlyNarrowsRows($queries)) {
            return null;
        }

        foreach ($queries as $query) {
            if ($this->isAdapterFilterQuery($query)) {
                return null;
            }
        }

        try {
            return $builder->compileFilters(\array_values($queries));
        } catch (ValidationException|UnsupportedException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Whether every query only narrows the rows an aggregate reads, so the aggregate can read the
     * table itself: anything that shapes, orders, groups or bounds the rows needs a derived table.
     *
     * @param  array<Query>  $queries
     */
    private function onlyNarrowsRows(array $queries): bool
    {
        foreach ($queries as $query) {
            $method = $query->getMethod();
            if (
                ! $method->isFilter()
                && ! $method->isSpatial()
                && ! $method->isJson()
                && ! \in_array($method, self::ROW_CONDITION_GROUPS, true)
                && ! $this->isAdapterFilterQuery($query)
            ) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return array<string, mixed>
     */
    private function fetchAggregateRow(SQLBuilder $builder, Event $event, string $collection): array
    {
        return $this->executeSelect($builder, $event, $collection)[0] ?? [];
    }

    private const array ROW_CONDITION_GROUPS = [Method::And, Method::Or, Method::ContainsAll, Method::ElemMatch];

    private const array BITWISE_AGGREGATES = [Method::BitAnd, Method::BitOr, Method::BitXor];

    private const array COLUMN_LIST_METHODS = [Method::GroupBy, Method::Exists, Method::NotExists];

    private const string BITWISE_INPUTS = '$inputs:';

    /**
     * Answer NULL for each bitwise aggregate that had no input values, and
     * drop the input counts populationStatistics() added.
     *
     * @param  array<string, mixed>  $row
     * @param  array<string, BaseQuery>  $inputs  Each bitwise aggregate, keyed by its input count, as bitwiseInputs() gives them
     * @return array<string, mixed>
     */
    private function bitwiseResults(array $row, array $inputs): array
    {
        foreach ($inputs as $count => $aggregate) {
            if (! \array_key_exists($count, $row)) {
                continue;
            }

            $value = $row[$count];
            unset($row[$count]);

            $name = $this->bitwiseResultName($row, $aggregate);
            if ($name !== null && \is_numeric($value) && (int) $value === 0) {
                $row[$name] = null;
            }
        }

        return $row;
    }

    /**
     * The column a bitwise aggregate is returned in: its alias, or for an unaliased one the name
     * MariaDB and MySQL give it, the aggregate's own text (`BIT_AND(`flags`)`, qualified under a
     * join). PostgreSQL names every unaliased BIT_AND `bit_and`, which does not tell two apart; it
     * answers an empty set with NULL itself, so such a column is left as it is.
     *
     * @param  array<string, mixed>  $row
     */
    private function bitwiseResultName(array $row, BaseQuery $aggregate): ?string
    {
        $alias = $aggregate->getAlias();
        if ($alias !== '') {
            return \array_key_exists($alias, $row) ? $alias : null;
        }

        $function = ($aggregate->getMethod()->sqlFunction() ?? '').'(';
        $quote = $this->getIdentifierQuote();
        $expressions = [];
        foreach (\array_keys($row) as $name) {
            if (\str_starts_with(\strtoupper($name), $function) && \str_ends_with($name, ')')) {
                $expressions[\str_replace($quote, '', \substr($name, \strlen($function), -1))] = $name;
            }
        }

        $attribute = $aggregate->getAttribute();
        $dot = \strrpos($attribute, '.');
        $column = $this->filter($this->getInternalKeyForAttribute($dot === false ? $attribute : \substr($attribute, $dot + 1)));
        $exact = $dot === false
            ? [$column, Query::DEFAULT_ALIAS.'.'.$column]
            : [$this->filter(\substr($attribute, 0, $dot)).'.'.$column];
        foreach ($exact as $expression) {
            if (isset($expressions[$expression])) {
                return $expressions[$expression];
            }
        }

        foreach ($expressions as $expression => $name) {
            if (\str_ends_with((string) $expression, '.'.$column)) {
                return $name;
            }
        }

        return null;
    }

    /**
     * InnoDB caps a table at 1017 columns, 64 indexes and a 65535-byte row; the varchar cap is the floor of
     * Postgres 16383, MySQL 16381 and MariaDB 16382; a shared table spends one byte of each index key on `_tenant`.
     */
    #[\Override]
    public function limits(): Limits
    {
        return $this->limits ??= new Limits(
            string: 4294967295,
            varchar: 16381,
            integer: 4294967295,
            bigInteger: Database::MAX_BIG_INT,
            attributes: 1017,
            indexes: 64,
            defaultAttributes: \count(Database::internalAttributesFor(true)),
            defaultIndexes: \count(Database::INTERNAL_INDEXES),
            indexLength: $this->sharedTables ? 767 : 768,
            uidLength: 36,
            documentSize: 65535,
            minDateTime: new \DateTime(static::MIN_DATETIME),
            maxDateTime: new \DateTime(self::MAX_DATETIME),
            idType: ColumnType::Integer,
            keywords: $this->getKeywords(),
            internalIndexKeys: [Storage::INDEX_PRIMARY, Storage::INDEX_CREATED_AT, Storage::INDEX_UPDATED_AT, Storage::INDEX_TENANT_ID],
        );
    }

    #[\Override]
    public function getCountOfAttributes(Document $collection): int
    {
        return \count(self::collectionAttributes($collection)) + $this->limits()->defaultAttributes;
    }

    #[\Override]
    public function getCountOfIndexes(Document $collection): int
    {
        return \count(self::collectionIndexes($collection)) + $this->limits()->defaultIndexes;
    }

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function getAttributeWidth(Document $collection): int
    {
        /**
         * @link https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
         *
         * `_id` bigint => 8 bytes
         * `_uid` varchar(255) => 1021 (4 * 255 + 1) bytes
         * `_tenant` int => 4 bytes
         * `_createdAt` datetime(3) => 7 bytes
         * `_updatedAt` datetime(3) => 7 bytes
         * `_permissions` mediumtext => 20
         */
        $total = 1067;

        foreach (self::collectionAttributes($collection) as $attribute) {
            $attributeSize = $attribute->size ?? 0;

            /**
             * Json / Longtext
             * only the pointer contributes 20 bytes
             * data is stored externally
             */
            if ($attribute->array) {
                $total += 20;

                continue;
            }

            switch ($attribute->type) {
                case ColumnType::Id:
                    $total += 8; //  BIGINT 8 bytes
                    break;

                case ColumnType::String:
                    /**
                     * Text / Mediumtext / Longtext
                     * only the pointer contributes 20 bytes to the row size
                     * data is stored externally
                     */
                    $total += match (true) {
                        $attributeSize > $this->limits()->varchar => 20,
                        $attributeSize > 255 => $attributeSize * 4 + 2,
                        default => $attributeSize * 4 + 1,
                    };

                    break;

                case ColumnType::Varchar:
                    $total += match (true) {
                        $attributeSize > 255 => $attributeSize * 4 + 2,
                        default => $attributeSize * 4 + 1,
                    };
                    break;

                case ColumnType::Text:
                case ColumnType::MediumText:
                case ColumnType::LongText:
                    $total += 20; // Pointer storage for TEXT types
                    break;

                case ColumnType::Integer:
                    if ($attributeSize >= 8) {
                        $total += 8; //  BIGINT 8 bytes
                    } else {
                        $total += 4; // INT 4 bytes
                    }
                    break;

                case ColumnType::BigInteger:
                    $total += 8;
                    break;

                case ColumnType::Float:
                case ColumnType::Double:
                    $total += 8; // DOUBLE 8 bytes
                    break;

                case ColumnType::Boolean:
                    $total += 1; // TINYINT(1) 1 bytes
                    break;

                case ColumnType::Relationship:
                    $total += Database::LENGTH_KEY * 4 + 1; // VARCHAR(<=255)
                    break;

                case ColumnType::Datetime:
                    /**
                     * 1 byte year + month
                     * 1 byte for the day
                     * 3 bytes for the hour, minute, and second
                     * 2 bytes miliseconds DATETIME(3)
                     */
                    $total += 7;
                    break;

                case ColumnType::Object:
                    /**
                     * JSONB/JSON type
                     * Only the pointer contributes 20 bytes to the row size
                     * Data is stored externally
                     */
                    $total += 20;
                    break;

                case ColumnType::Point:
                    $total += $this->getMaxPointSize();
                    break;
                case ColumnType::Linestring:
                case ColumnType::Polygon:
                    $total += 20;
                    break;

                case ColumnType::Vector:
                    // Each dimension is typically 4 bytes (float32)
                    $total += $attributeSize * 4;
                    break;

                default:
                    throw new DatabaseException('Unknown type: '.$attribute->type->value);
            }
        }

        return $total;
    }

    abstract protected function getMaxPointSize(): int;

    /**
     * The reserved words of https://mariadb.com/kb/en/reserved-words/
     *
     * @return list<string>
     */
    protected function getKeywords(): array
    {
        return [
            'ACCESSIBLE',
            'ADD',
            'ALL',
            'ALTER',
            'ANALYZE',
            'AND',
            'AS',
            'ASC',
            'ASENSITIVE',
            'BEFORE',
            'BETWEEN',
            'BIGINT',
            'BINARY',
            'BLOB',
            'BOTH',
            'BY',
            'CALL',
            'CASCADE',
            'CASE',
            'CHANGE',
            'CHAR',
            'CHARACTER',
            'CHECK',
            'COLLATE',
            'COLUMN',
            'CONDITION',
            'CONSTRAINT',
            'CONTINUE',
            'CONVERT',
            'CREATE',
            'CROSS',
            'CURRENT_DATE',
            'CURRENT_ROLE',
            'CURRENT_TIME',
            'CURRENT_TIMESTAMP',
            'CURRENT_USER',
            'CURSOR',
            'DATABASE',
            'DATABASES',
            'DAY_HOUR',
            'DAY_MICROSECOND',
            'DAY_MINUTE',
            'DAY_SECOND',
            'DEC',
            'DECIMAL',
            'DECLARE',
            'DEFAULT',
            'DELAYED',
            'DELETE',
            'DELETE_DOMAIN_ID',
            'DESC',
            'DESCRIBE',
            'DETERMINISTIC',
            'DISTINCT',
            'DISTINCTROW',
            'DIV',
            'DO_DOMAIN_IDS',
            'DOUBLE',
            'DROP',
            'DUAL',
            'EACH',
            'ELSE',
            'ELSEIF',
            'ENCLOSED',
            'ESCAPED',
            'EXCEPT',
            'EXISTS',
            'EXIT',
            'EXPLAIN',
            'FALSE',
            'FETCH',
            'FLOAT',
            'FLOAT4',
            'FLOAT8',
            'FOR',
            'FORCE',
            'FOREIGN',
            'FROM',
            'FULLTEXT',
            'GENERAL',
            'GRANT',
            'GROUP',
            'HAVING',
            'HIGH_PRIORITY',
            'HOUR_MICROSECOND',
            'HOUR_MINUTE',
            'HOUR_SECOND',
            'IF',
            'IGNORE',
            'IGNORE_DOMAIN_IDS',
            'IGNORE_SERVER_IDS',
            'IN',
            'INDEX',
            'INFILE',
            'INNER',
            'INOUT',
            'INSENSITIVE',
            'INSERT',
            'INT',
            'INT1',
            'INT2',
            'INT3',
            'INT4',
            'INT8',
            'INTEGER',
            'INTERSECT',
            'INTERVAL',
            'INTO',
            'IS',
            'ITERATE',
            'JOIN',
            'KEY',
            'KEYS',
            'KILL',
            'LEADING',
            'LEAVE',
            'LEFT',
            'LIKE',
            'LIMIT',
            'LINEAR',
            'LINES',
            'LOAD',
            'LOCALTIME',
            'LOCALTIMESTAMP',
            'LOCK',
            'LONG',
            'LONGBLOB',
            'LONGTEXT',
            'LOOP',
            'LOW_PRIORITY',
            'MASTER_HEARTBEAT_PERIOD',
            'MASTER_SSL_VERIFY_SERVER_CERT',
            'MATCH',
            'MAXVALUE',
            'MEDIUMBLOB',
            'MEDIUMINT',
            'MEDIUMTEXT',
            'MIDDLEINT',
            'MINUTE_MICROSECOND',
            'MINUTE_SECOND',
            'MOD',
            'MODIFIES',
            'NATURAL',
            'NOT',
            'NO_WRITE_TO_BINLOG',
            'NULL',
            'NUMERIC',
            'OFFSET',
            'ON',
            'OPTIMIZE',
            'OPTION',
            'OPTIONALLY',
            'OR',
            'ORDER',
            'OUT',
            'OUTER',
            'OUTFILE',
            'OVER',
            'PAGE_CHECKSUM',
            'PARSE_VCOL_EXPR',
            'PARTITION',
            'POSITION',
            'PRECISION',
            'PRIMARY',
            'PROCEDURE',
            'PURGE',
            'RANGE',
            'READ',
            'READS',
            'READ_WRITE',
            'REAL',
            'RECURSIVE',
            'REF_SYSTEM_ID',
            'REFERENCES',
            'REGEXP',
            'RELEASE',
            'RENAME',
            'REPEAT',
            'REPLACE',
            'REQUIRE',
            'RESIGNAL',
            'RESTRICT',
            'RETURN',
            'RETURNING',
            'REVOKE',
            'RIGHT',
            'RLIKE',
            'ROWS',
            'SCHEMA',
            'SCHEMAS',
            'SECOND_MICROSECOND',
            'SELECT',
            'SENSITIVE',
            'SEPARATOR',
            'SET',
            'SHOW',
            'SIGNAL',
            'SLOW',
            'SMALLINT',
            'SPATIAL',
            'SPECIFIC',
            'SQL',
            'SQLEXCEPTION',
            'SQLSTATE',
            'SQLWARNING',
            'SQL_BIG_RESULT',
            'SQL_CALC_FOUND_ROWS',
            'SQL_SMALL_RESULT',
            'SSL',
            'STARTING',
            'STATS_AUTO_RECALC',
            'STATS_PERSISTENT',
            'STATS_SAMPLE_PAGES',
            'STRAIGHT_JOIN',
            'TABLE',
            'TERMINATED',
            'THEN',
            'TINYBLOB',
            'TINYINT',
            'TINYTEXT',
            'TO',
            'TRAILING',
            'TRIGGER',
            'TRUE',
            'UNDO',
            'UNION',
            'UNIQUE',
            'UNLOCK',
            'UNSIGNED',
            'UPDATE',
            'USAGE',
            'USE',
            'USING',
            'UTC_DATE',
            'UTC_TIME',
            'UTC_TIMESTAMP',
            'VALUES',
            'VARBINARY',
            'VARCHAR',
            'VARCHARACTER',
            'VARYING',
            'WHEN',
            'WHERE',
            'WHILE',
            'WINDOW',
            'WITH',
            'WRITE',
            'XOR',
            'YEAR_MONTH',
            'ZEROFILL',
            'ACTION',
            'BIT',
            'DATE',
            'ENUM',
            'NO',
            'TEXT',
            'TIME',
            'TIMESTAMP',
            'BODY',
            'ELSIF',
            'GOTO',
            'HISTORY',
            'MINUS',
            'OTHERS',
            'PACKAGE',
            'PERIOD',
            'RAISE',
            'ROWNUM',
            'ROWTYPE',
            'SYSDATE',
            'SYSTEM',
            'SYSTEM_TIME',
            'VERSIONING',
            'WITHOUT',
        ];
    }

    /**
     * @throws DatabaseException
     */
    #[\Override]
    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

    /**
     * @throws Exception
     * @throws PDOException
     */
    #[\Override]
    public function delete(string $name): bool
    {
        $name = $this->filter($name);

        $result = $this->schema()->dropDatabase($name);
        $sql = $result->query;

        return $this->executeStatement($sql, Event::DatabaseDelete);
    }

    /**
     * Delete a collection and its permissions table.
     *
     * @throws DatabaseException
     */
    #[\Override]
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
            if ($error instanceof NotFoundException && $this->inTransaction === 0) {
                $this->executeStatement($permissions->query, Event::CollectionDelete);
            }

            throw $error;
        }
    }

    /**
     * Drop the tables a failed createCollection() created. A drop that fails too is logged, so the caller still
     * receives the error that failed the create.
     */
    protected function discardCreatedCollection(string $id): void
    {
        try {
            $this->dropCreatedCollection($id);
        } catch (Throwable $error) {
            Console::error("Failed to rollback collection '{$id}': ".$error->getMessage());
        }
    }

    protected function dropCreatedCollection(string $id): void
    {
        $schema = $this->schema();
        $main = $schema->table($this->getTableRaw($id))->dropIfExists();
        $permissions = $schema->table($this->getTableRaw(Storage::permissionsTable($id)))->dropIfExists();

        $this->executeStatement($main->query.'; '.$permissions->query, Event::CollectionCreate);
    }

    /**
     * Create a relationship between collections by adding foreign key columns.
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function createRelationship(string $collection, Relationship $relationship): bool
    {
        $name = $this->filter($collection);
        $relatedName = $this->filter($relationship->relatedCollection);
        $key = $this->filter($relationship->key ?? '');
        $twoWayKey = $this->filter($relationship->twoWayKey ?? '');

        $schema = $this->schema();
        $addColumn = function (string $tableName, string $columnId) use ($schema): string {
            $table = $schema->table($this->getTableRaw($tableName));
            $table->string($columnId, 255)->nullable()->default(null);
            $result = $table->alter();

            return $result->query;
        };

        $sql = match ($relationship->type) {
            RelationshipType::OneToOne => $addColumn($name, $key) . ';' . ($relationship->twoWay ? $addColumn($relatedName, $twoWayKey) . ';' : ''),
            RelationshipType::OneToMany => $addColumn($relatedName, $twoWayKey) . ';',
            RelationshipType::ManyToOne => $addColumn($name, $key) . ';',
            RelationshipType::ManyToMany => null,
        };

        if ($sql === null) {
            return true;
        }

        return $this->executeStatement($sql, Event::AttributeCreate);
    }

    /**
     * Rename the foreign key columns of a relationship.
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update): bool
    {
        $name = $this->filter($collection);
        $relatedName = $this->filter($relationship->relatedCollection);
        $key = $this->filter($relationship->key ?? '');
        $twoWayKey = $this->filter($relationship->twoWayKey ?? '');
        $twoWay = $update->twoWay ?? $relationship->twoWay;
        $newKey = $update->key === null ? null : $this->filter($update->key);
        $newTwoWayKey = $update->twoWayKey === null ? null : $this->filter($update->twoWayKey);

        $schema = $this->schema();
        $renameColumn = function (string $tableName, string $from, string $to) use ($schema): string {
            $table = $schema->table($this->getTableRaw($tableName));
            $table->renameColumn($from, $to);
            $result = $table->alter();

            return $result->query;
        };

        $sql = '';

        switch ($relationship->type) {
            case RelationshipType::OneToOne:
                if (($twoWay || $side === RelationshipSide::Parent) && $newKey !== null && $key !== $newKey) {
                    $sql = $renameColumn($name, $key, $newKey) . ';';
                }
                if (($twoWay || $side === RelationshipSide::Child) && $newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                    $sql .= $renameColumn($relatedName, $twoWayKey, $newTwoWayKey) . ';';
                }
                break;
            case RelationshipType::OneToMany:
                if ($side === RelationshipSide::Parent) {
                    if ($newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                        $sql = $renameColumn($relatedName, $twoWayKey, $newTwoWayKey) . ';';
                    }
                } elseif ($newKey !== null && $key !== $newKey) {
                    $sql = $renameColumn($name, $key, $newKey) . ';';
                }
                break;
            case RelationshipType::ManyToOne:
                if ($side === RelationshipSide::Child) {
                    if ($newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                        $sql = $renameColumn($relatedName, $twoWayKey, $newTwoWayKey) . ';';
                    }
                } elseif ($newKey !== null && $key !== $newKey) {
                    $sql = $renameColumn($name, $key, $newKey) . ';';
                }
                break;
            case RelationshipType::ManyToMany:
                $junctionName = $this->getJunctionName($collection, $relationship->relatedCollection, $side);

                if ($newKey !== null && $key !== $newKey) {
                    $sql = $renameColumn($junctionName, $key, $newKey) . ';';
                }
                if ($newTwoWayKey !== null && $twoWayKey !== $newTwoWayKey) {
                    $sql .= $renameColumn($junctionName, $twoWayKey, $newTwoWayKey) . ';';
                }
                break;
        }

        if ($sql === '') {
            return true;
        }

        return $this->executeStatement($sql, Event::AttributeUpdate);
    }

    /**
     * Drop the foreign key columns of a relationship, or its junction tables.
     *
     * @throws DatabaseException
     */
    #[\Override]
    public function deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side): bool
    {
        $name = $this->filter($collection);
        $relatedName = $this->filter($relationship->relatedCollection);
        $key = $this->filter($relationship->key ?? '');
        $twoWayKey = $this->filter($relationship->twoWayKey ?? '');
        $twoWay = $relationship->twoWay;

        $schema = $this->schema();
        $dropColumn = function (string $tableName, string $columnId) use ($schema): string {
            $table = $schema->table($this->getTableRaw($tableName));
            $table->dropColumn($columnId);
            $result = $table->alter();

            return $result->query;
        };

        $sql = '';

        switch ($relationship->type) {
            case RelationshipType::OneToOne:
                if ($side === RelationshipSide::Parent) {
                    $sql = $dropColumn($name, $key) . ';';
                    if ($twoWay) {
                        $sql .= $dropColumn($relatedName, $twoWayKey) . ';';
                    }
                } else {
                    $sql = $dropColumn($relatedName, $twoWayKey) . ';';
                    if ($twoWay) {
                        $sql .= $dropColumn($name, $key) . ';';
                    }
                }
                break;
            case RelationshipType::OneToMany:
                $sql = $side === RelationshipSide::Parent
                    ? $dropColumn($relatedName, $twoWayKey) . ';'
                    : $dropColumn($name, $key) . ';';
                break;
            case RelationshipType::ManyToOne:
                $sql = $side === RelationshipSide::Parent
                    ? $dropColumn($name, $key) . ';'
                    : $dropColumn($relatedName, $twoWayKey) . ';';
                break;
            case RelationshipType::ManyToMany:
                $junctionName = $this->getJunctionName($collection, $relationship->relatedCollection, $side);

                $junctionResult = $schema->table($this->getTableRaw($junctionName))->drop();
                $permissionsResult = $schema->table($this->getTableRaw(Storage::permissionsTable($junctionName)))->drop();

                $sql = $junctionResult->query . '; ' . $permissionsResult->query;
                break;
        }

        return $this->executeStatement($sql, Event::AttributeDelete);
    }

    /**
     * The junction collection of a many-to-many relationship, named after the parent's sequence first.
     */
    protected function getJunctionName(string $collection, string $relatedCollection, RelationshipSide $side): string
    {
        $metadataCollection = new Document([Document::ID => Database::METADATA]);
        $collectionDocument = $this->getDocument($metadataCollection, $collection);
        $relatedCollectionDocument = $this->getDocument($metadataCollection, $relatedCollection);

        return $side === RelationshipSide::Parent
            ? '_' . $collectionDocument->getSequence() . '_' . $relatedCollectionDocument->getSequence()
            : '_' . $relatedCollectionDocument->getSequence() . '_' . $collectionDocument->getSequence();
    }

    /**
     * @var array<string, string>
     */
    private const array COLUMN_TYPE_SPELLINGS = [
        '/\s+/' => ' ',
        '/ (NOT )?NULL$/' => '',
        '/^(POINT|LINESTRING|POLYGON)\b.*$/' => '$1',
        '/\b(TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIGINT)\(\d+\)/' => '$1',
    ];

    #[\Override]
    public function getColumnType(Attribute $attribute): ?string
    {
        $type = $this->getAttributeSqlType($attribute);

        return $type === '' ? null : $this->canonicalColumnType($type);
    }

    /**
     * One spelling for a native type, whether the adapter wrote it or the engine's catalog reports it: engines report
     * integer display widths (int(11)), spatial types without their SRID or nullability, and MariaDB's JSON as
     * LONGTEXT.
     */
    protected function canonicalColumnType(string $type): string
    {
        $canonical = \preg_replace(
            \array_keys(self::COLUMN_TYPE_SPELLINGS),
            \array_values(self::COLUMN_TYPE_SPELLINGS),
            \strtoupper(\trim($type)),
        ) ?? $type;

        return $canonical === 'JSON' ? 'LONGTEXT' : $canonical;
    }

    protected function getSqlType(ColumnType $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        if (in_array($type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
            return $this->getSpatialSqlType($type->value, $required);
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
            if ($size > $this->limits()->varchar) {
                return 'TEXT';
            }

            return "VARCHAR({$size})";
        }

        if ($type === ColumnType::Varchar) {
            $this->assertVarcharSize($size);

            return "VARCHAR({$size})";
        }

        if (\in_array($type, [ColumnType::Integer, ColumnType::BigInteger], true)) {
            $suffix = $signed ? '' : ' UNSIGNED';

            return ($type === ColumnType::Integer && $size < 8 ? 'INT' : 'BIGINT') . $suffix;
        }

        if ($type === ColumnType::Float || $type === ColumnType::Double) {
            return 'DOUBLE' . ($signed ? '' : ' UNSIGNED');
        }

        return match ($type) {
            ColumnType::Id => 'BIGINT UNSIGNED',
            ColumnType::Text => 'TEXT',
            ColumnType::MediumText => 'MEDIUMTEXT',
            ColumnType::LongText => 'LONGTEXT',
            ColumnType::Boolean => 'TINYINT(1)',
            ColumnType::Relationship => 'VARCHAR(255)',
            ColumnType::Datetime => 'DATETIME(3)',
            default => throw new DatabaseException('Unknown type: ' . $type->value . '. Must be one of ' . ColumnType::String->value . ', ' . ColumnType::Varchar->value . ', ' . ColumnType::Text->value . ', ' . ColumnType::MediumText->value . ', ' . ColumnType::LongText->value . ', ' . ColumnType::Integer->value . ', ' . ColumnType::Double->value . ', ' . ColumnType::Boolean->value . ', ' . ColumnType::Datetime->value . ', ' . ColumnType::Relationship->value . ', ' . ColumnType::Point->value . ', ' . ColumnType::Linestring->value . ', ' . ColumnType::Polygon->value),
        };
    }

    protected function getSpatialSqlType(string $type, bool $required): string
    {
        $srid = $this->getSpatialColumnSrid();
        $modifier = $srid === null ? '' : "({$srid})";
        $nullability = '';

        if (! $this->supports(Capability::IndexSpatialNull)) {
            if ($required) {
                $nullability = ' NOT NULL';
            } else {
                $nullability = ' NULL';
            }
        }

        return match ($type) {
            ColumnType::Point->value => "POINT{$modifier}{$nullability}",
            ColumnType::Linestring->value => "LINESTRING{$modifier}{$nullability}",
            ColumnType::Polygon->value => "POLYGON{$modifier}{$nullability}",
            default => '',
        };
    }

    protected function getSpatialGeometryFromText(string $wktPlaceholder, ?int $srid = null): string
    {
        $srid = $srid ?? Database::DEFAULT_SRID;
        $geomFromText = "ST_GeomFromText({$wktPlaceholder}, {$srid}";

        if ($this->supports(Capability::SpatialAxisOrder)) {
            $geomFromText .= ', '.$this->getSpatialAxisOrder();
        }

        $geomFromText .= ')';

        return $geomFromText;
    }

    protected function getSpatialAxisOrder(): string
    {
        return "'axis-order=long-lat'";
    }

    /**
     * @param  array<mixed>  $geometry
     *
     * @throws DatabaseException
     */
    protected function convertArrayToWkt(array $geometry): string
    {
        if ($geometry === [] || ! \array_is_list($geometry)) {
            throw new DatabaseException('Unrecognized geometry array format');
        }

        // point [x, y]
        if (count($geometry) === 2 && is_numeric($geometry[0]) && is_numeric($geometry[1])) {
            return "POINT({$geometry[0]} {$geometry[1]})";
        }

        // linestring [[x1, y1], [x2, y2], ...]
        if (is_array($geometry[0]) && count($geometry[0]) === 2 && is_numeric($geometry[0][0])) {
            $points = [];
            foreach ($geometry as $point) {
                if (! is_array($point) || count($point) !== 2 || ! is_numeric($point[0]) || ! is_numeric($point[1])) {
                    throw new DatabaseException('Invalid point format in geometry array');
                }
                $points[] = "{$point[0]} {$point[1]}";
            }

            return 'LINESTRING('.implode(', ', $points).')';
        }

        // polygon [[[x1, y1], [x2, y2], ...], ...]
        if (is_array($geometry[0]) && is_array($geometry[0][0]) && count($geometry[0][0]) === 2) {
            $rings = [];
            foreach ($geometry as $ring) {
                if (! is_array($ring)) {
                    throw new DatabaseException('Invalid ring format in polygon geometry');
                }
                $points = [];
                foreach ($ring as $point) {
                    if (! is_array($point) || count($point) !== 2 || ! is_numeric($point[0]) || ! is_numeric($point[1])) {
                        throw new DatabaseException('Invalid point format in polygon ring');
                    }
                    $points[] = "{$point[0]} {$point[1]}";
                }
                $rings[] = '('.implode(', ', $points).')';
            }

            return 'POLYGON('.implode(', ', $rings).')';
        }

        throw new DatabaseException('Unrecognized geometry array format');
    }

    /**
     * @throws DatabaseException
     */
    protected function getTable(string $name): string
    {
        return "{$this->quote($this->getDatabase())}.{$this->quote($this->getNamespace().'_'.$this->filter($name))}";
    }

    /**
     * Get an unquoted qualified table name (the builder handles quoting).
     *
     * @throws DatabaseException
     */
    protected function getTableRaw(string $name): string
    {
        return $this->getDatabase().'.'.$this->getNamespace().'_'.$this->filter($name);
    }

    abstract protected function createBuilder(): SQLBuilder;

    #[\Override]
    public function schema(): MySQLSchema|PostgresSchema
    {
        return new MySQLSchema();
    }

    /**
     * Applies tenant filtering whenever shared tables are enabled, so that a query made
     * with no tenant selected matches no tenant's rows rather than every tenant's.
     *
     * @param  list<int|string|null>  $tenants  Tenants this query spans, for the reads that cross
     *                                          tenants deliberately; defaults to the selected tenant
     * @param  list<string>  $unindexed  The read's join aliases unindexedJoins() names
     *
     * @throws DatabaseException
     */
    protected function newBuilder(string $table, string $alias = '', bool $allowNullTenant = false, array $tenants = [], array $unindexed = []): SQLBuilder
    {
        $builder = $this->createBuilder()->from($this->getTableRaw($table), $alias);

        // AttributeMap is a readonly stateless config object — share one
        // instance across builders to avoid allocating it on every read.
        $this->attributeMap ??= new AttributeMap(Storage::attributeMap());
        $builder->addHook($this->attributeMap);
        if ($this->sharedTables) {
            $source = $alias !== '' ? $alias : $table;
            $allowNullColumn = '';
            if ($allowNullTenant) {
                $allowNullColumn = $source.'.'.Storage::UID;
            }
            $tenantFilter = new Tenant\Filter(
                $tenants === [] ? $this->currentTenant() : $tenants,
                Database::METADATA,
                $table,
                $allowNullColumn,
                $this->getIdentifierQuote(),
                $unindexed,
            );
            $builder->addHook($tenantFilter);
            $builder->addHook(new Tenant\OuterJoin($tenantFilter, $source));
        }

        return $builder;
    }

    #[\Override]
    public function rawMutation(string $query, array $bindings = []): int
    {
        try {
            $statement = $this->prepareStatement($query);
            foreach ($bindings as $i => $value) {
                $statement->bindValue($i + 1, $value, $this->getPdoType($value));
            }
            $this->execute($statement);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $count = $statement->rowCount();
        $statement->closeCursor();

        return $count;
    }

    /**
     * A builder over the collection's table for Database::from(): it maps document attributes to
     * columns and applies no permissions.
     *
     * Under shared tables it keeps every statement to the selected tenant: the main table and every
     * table joined through the builder's join methods (Tenant\Raw). It does not use
     * newBuilder()'s tenant hooks, which need a read's joins up front; the caller adds these later.
     * Not kept to the tenant: SQL the caller writes, builders that did not come from Database::from()
     * (subqueries, unions, lateral joins) and a dialect's multi-table updates and deletes.
     */
    #[\Override]
    public function builder(string $collection): SQLBuilder
    {
        $name = $this->filter($collection);
        if (! $this->sharedTables) {
            return $this->newBuilder($name);
        }

        $table = $this->getTableRaw($name);
        $tenants = new Tenant\Raw(
            $this->currentTenant(),
            $table,
            $name === Database::METADATA || $name === Storage::permissionsTable(Database::METADATA),
            $this->getIdentifierQuote(),
        );
        $this->attributeMap ??= new AttributeMap(Storage::attributeMap());

        return $this->createBuilder()
            ->from($table)
            ->addHook($this->attributeMap)
            ->addHook($tenants)
            ->addHook(new Tenant\RawOuterJoin($tenants))
            ->beforeBuild($tenants->reset(...));
    }

    protected function getIdentifierQuote(): string
    {
        return '`';
    }

    /**
     * The expression a raw lookup compares a document id column through, so it
     * can use the engine's unique index on that column.
     */
    protected function collateDocumentId(string $column): string
    {
        return $column;
    }

    /**
     * @param  array<string>  $roles
     */
    protected function newPermissionHook(string $collection, array $roles, string $type = PermissionType::Read->value, string $documentColumn = Storage::UID): Permission\Filter
    {
        return new Permission\Filter(
            roles: \array_values($roles),
            permissionsTable: fn (string $table) => $this->getTableRaw(Storage::permissionsTable($collection)),
            type: $type,
            documentColumn: $documentColumn,
            permissionDocumentColumn: Storage::PERMISSIONS_DOCUMENT,
            permissionRoleColumn: Storage::PERMISSIONS_PERMISSION,
            permissionTypeColumn: Storage::PERMISSIONS_TYPE,
            subqueryFilter: $this->sharedTables
                ? new Tenant\Filter(
                    $this->currentTenant(),
                    Database::METADATA,
                    Storage::permissionsTable($collection),
                    quoteCharacter: $this->getIdentifierQuote(),
                )
                : null,
            quoteCharacter: $this->getIdentifierQuote(),
        );
    }

    /**
     * @param  array<string>  $roles
     */
    protected function newJoinPermissionHook(string $collection, array $roles, string $type, string $documentColumn, int $joins, JoinType $joinType): Permission\Filter
    {
        return $this->newPermissionHook($collection, $roles, $type, $documentColumn);
    }

    /**
     * Keeps the write hook this adapter owns registered: Tenancy, while shared tables are active. It stores each
     * row's tenant from the document being written, or the adapter's when the document names none, so it is needed
     * in per-document mode too, where there is no adapter tenant at all.
     *
     * Permissions is deliberately not here, and this does not restore it. It is
     * registered once by whoever builds the Database, so a handle constructed
     * without it never writes a `_perms` row -- the row itself looks correct,
     * its `_permissions` JSON intact, and only the side table the permission
     * filter joins is empty. Do not read this method as a safety net for that.
     */
    protected function syncWriteHooks(): void
    {
        $registered = $this->getTenantHook() !== null;
        if ($registered === $this->sharedTables) {
            return;
        }

        if ($this->sharedTables) {
            $this->addWriteHook(new Tenancy());
        } else {
            $this->removeWriteHook(Tenancy::class);
        }
    }

    /**
     * The context this adapter's write hooks write their own rows through.
     *
     * @param  array<string, true>  $skipPermissions  Ids of the documents whose permissions the write keeps
     */
    protected function writeContext(array $skipPermissions = []): WriteContext
    {
        return new WriteContext(
            builder: fn (string $table): SQLBuilder => $this->newBuilder($table),
            rawBuilder: $this->createBuilder(...),
            rawTable: $this->getTableRaw(...),
            prepare: fn (Statement $statement, Event $event): PDOStatement|DatabasePDOStatement|PDOStatementProxy => $this->executeResult($statement, $event),
            execute: fn (PDOStatement|DatabasePDOStatement|PDOStatementProxy $statement): bool => $this->execute($statement),
            decorateRow: $this->decorateRow(...),
            ignoreDuplicates: $this->isIgnoringDuplicates(),
            skipPermissions: $skipPermissions,
        );
    }

    /**
     * Execute a Statement through the transformation system with positional bindings.
     *
     * Prepares the SQL statement and binds positional parameters from the Statement.
     * Does NOT call execute() - the caller is responsible for that.
     *
     * @param  string  $collection  The collection the statement reads or writes, for the profiler
     */
    protected function executeResult(Statement $result, ?Event $event = null, string $collection = ''): PDOStatement|DatabasePDOStatement|PDOStatementProxy
    {
        $prepared = $this->prepareStatement($result->query, $event);
        $this->describeStatement($prepared, $result->bindings, $collection);
        foreach ($result->bindings as $i => $value) {
            if (\is_bool($value) && $this->supports(Capability::IntegerBooleans)) {
                $value = (int) $value;
            }
            if (\is_float($value)) {
                $prepared->bindValue($i + 1, $this->getFloatPrecision($value), PDO::PARAM_STR);
            } else {
                $prepared->bindValue($i + 1, $value, $this->getPdoType($value));
            }
        }

        return $prepared;
    }

    /**
     * @param  PDOStatement|DatabasePDOStatement|PDOStatementProxy  $statement
     */
    protected function execute(mixed $statement, ?Event $event = null): bool
    {
        return $this->executeAndProfile($statement);
    }

    /**
     * Run a prepared statement and hand it to the profiler when one is attached.
     *
     * Subclasses that wrap execute() with engine-specific timeout handling call
     * this instead of $statement->execute(), so the statement is still counted.
     *
     * @param  PDOStatement|DatabasePDOStatement|PDOStatementProxy  $statement
     */
    protected function executeAndProfile(mixed $statement): bool
    {
        if ($this->profiler === null || ! $this->profiler->isEnabled()) {
            return $statement->execute();
        }

        $start = \microtime(true);
        $result = $statement->execute();
        $this->profiler->log(
            $statement instanceof DatabasePDOStatement ? $statement->getQueryString() : ($statement->queryString ?? ''),
            $this->statementBindings[$statement] ?? [],
            (\microtime(true) - $start) * 1000,
            $this->statementCollections[$statement] ?? '',
            $this->getStatementEvent($statement)->value ?? '',
        );

        return $result;
    }

    /**
     * Keep the values bound to a statement and the collection it runs on for the profiler, while
     * one is recording.
     *
     * @param  array<mixed>  $bindings
     */
    protected function describeStatement(PDOStatement|DatabasePDOStatement|PDOStatementProxy $statement, array $bindings, string $collection): void
    {
        if ($this->profiler === null || ! $this->profiler->isEnabled()) {
            return;
        }

        $this->statementBindings ??= new \WeakMap();
        $this->statementBindings[$statement] = $bindings;
        $this->statementCollections ??= new \WeakMap();
        $this->statementCollections[$statement] = $collection;
    }

    protected function getStatementEvent(PDOStatement|DatabasePDOStatement|PDOStatementProxy $statement): ?Event
    {
        if ($this->statementEvents === null) {
            return null;
        }

        return $this->statementEvents[$statement] ?? null;
    }

    protected function prepareStatement(string $sql, ?Event $event = null): DatabasePDOStatement|PDOStatementProxy|PDOStatement
    {
        $sql = $this->comments().$sql;

        if ($event !== null) {
            $sql = $this->transformQuery($event, $sql);
        }

        $statement = $this->getDriver()->prepare($sql);
        if (! $statement instanceof DatabasePDOStatement && ! $statement instanceof PDOStatementProxy && ! $statement instanceof PDOStatement) {
            throw new DatabaseException('Failed to prepare SQL statement');
        }

        if ($event !== null) {
            $this->statementEvents ??= new \WeakMap();
            $this->statementEvents[$statement] = $event;
        }

        return $statement;
    }

    private function comments(): string
    {
        if ($this->commentedMetadata === $this->metadata) {
            return $this->comments;
        }

        $comments = '';
        $scalar = true;
        foreach ($this->metadata as $key => $value) {
            $comments .= '/* '.$this->commentText($key).': '.$this->commentText($value).' */'."\n";
            $scalar = $scalar && ($value === null || \is_scalar($value));
        }

        if ($scalar) {
            $this->commentedMetadata = $this->metadata;
            $this->comments = $comments;
        }

        return $comments;
    }

    private function commentText(mixed $value): string
    {
        $text = match (true) {
            \is_scalar($value), $value instanceof \Stringable => (string) $value,
            default => \json_encode(
                $value,
                JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE | JSON_PARTIAL_OUTPUT_ON_ERROR,
            ) ?: \get_debug_type($value),
        };

        if (\preg_match('/[^\x20-\x7E]/', $text) !== 0) {
            $text = \preg_replace('/[\p{Cc}\p{Zl}\p{Zp}]/u', ' ', \mb_scrub($text, 'UTF-8')) ?? '';
        }

        return \str_replace(['/*', '*/'], ['/ *', '* /'], $text);
    }

    protected function executeStatement(string $sql, Event $event): bool
    {
        return $this->execute($this->prepareStatement($sql, $event));
    }

    private function transformQuery(Event $event, string $sql): string
    {
        foreach ($this->transforms as $transform) {
            $sql = $transform->transform($event, $sql);
        }

        return $sql;
    }

    /**
     * Builds an INSERT ... ON CONFLICT/DUPLICATE KEY UPDATE statement via the
     * query builder, handling spatial columns, shared-table tenant guards,
     * increment attributes, and operator expressions.
     *
     * @param  string  $name  The filtered collection name
     * @param  array<Change>  $changes  The changes to upsert
     * @param  list<string>  $spatialAttributes  Spatial column names
     * @param  string  $attribute  Increment attribute name (empty if none)
     * @param  array<string, Operator>  $operators  Operator map keyed by attribute name
     * @param  array<string, mixed>  $attributeDefaults  Attribute default values
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
        $builder = $this->createBuilder()->into($this->getTableRaw($name));

        foreach ($spatialAttributes as $spatialColumn) {
            $builder->insertColumnExpression($spatialColumn, $this->getSpatialGeometryFromText('?'));
        }

        if ($this->insertRequiresAlias()) {
            $builder->insertAs('target');
        }

        $allColumnNames = [];
        $documentsData = [];

        foreach ($changes as $change) {
            $document = $change->new;

            if ($hasOperators) {
                $extracted = Operator::extractOperators($document->getAttributes());
                $currentRegularAttributes = $extracted['updates'];
                $extractedOperators = $extracted['operators'];

                if ($change->old->isEmpty() && ! empty($extractedOperators)) {
                    foreach ($extractedOperators as $operatorKey => $operator) {
                        $default = $attributeDefaults[$operatorKey] ?? null;
                        $currentRegularAttributes[$operatorKey] = $this->applyOperatorToValue($operator, $default);
                    }
                }

                $currentRegularAttributes[Storage::UID] = $document->getId();
                $currentRegularAttributes[Storage::CREATED_AT] = $document->getCreatedAt() ? $document->getCreatedAt() : null;
                $currentRegularAttributes[Storage::UPDATED_AT] = $document->getUpdatedAt() ? $document->getUpdatedAt() : null;
            } else {
                $currentRegularAttributes = $document->getAttributes();
                $currentRegularAttributes[Storage::UID] = $document->getId();
                $currentRegularAttributes[Storage::CREATED_AT] = $document->getCreatedAt() ? DateTime::setTimezone($document->getCreatedAt()) : null;
                $currentRegularAttributes[Storage::UPDATED_AT] = $document->getUpdatedAt() ? DateTime::setTimezone($document->getUpdatedAt()) : null;
            }

            $currentRegularAttributes[Storage::PERMISSIONS] = \json_encode($document->getPermissions());

            if (! empty($document->getSequence())) {
                $currentRegularAttributes[Storage::SEQUENCE] = $document->getSequence();
            }

            $currentRegularAttributes = $this->decorateRow($currentRegularAttributes, $document);

            foreach (\array_keys($currentRegularAttributes) as $column) {
                $allColumnNames[$column] = true;
            }

            $documentsData[] = $currentRegularAttributes;
        }

        foreach (\array_keys($operators) as $column) {
            $allColumnNames[$column] = true;
        }

        $allColumnNames = \array_keys($allColumnNames);
        \sort($allColumnNames);

        $spatialMap = \array_fill_keys($spatialAttributes, true);
        $integerBooleans = $this->supports(Capability::IntegerBooleans);

        foreach ($documentsData as $values) {
            $row = [];
            foreach ($allColumnNames as $key) {
                $value = $values[$key] ?? null;
                if (isset($spatialMap[$key])) {
                    $value = $this->encodeSpatialWriteValue($value);
                } elseif (\is_array($value)) {
                    $value = \json_encode($value);
                }
                if ($integerBooleans && ! isset($spatialMap[$key])) {
                    $value = (\is_bool($value)) ? (int) $value : $value;
                }
                $row[$key] = $value;
            }
            $builder->set($row);
        }

        $conflictKeys = $this->sharedTables ? [Storage::UID, Storage::TENANT] : [Storage::UID];

        $skipColumns = [Storage::UID, Storage::SEQUENCE, Storage::CREATED_AT, Storage::TENANT];

        if (! empty($attribute)) {
            $updateColumns = [$this->filter($attribute), Storage::UPDATED_AT];
        } else {
            $updateColumns = \array_values(\array_filter(
                $allColumnNames,
                static fn (int|string $column): bool => ! \in_array($column, $skipColumns)
            ));
        }

        $builder->onConflict($conflictKeys, $updateColumns);

        // conflictSetRaw() takes the column names given to onConflict(); the expression methods quote their own.
        if (! empty($attribute)) {
            $incrementColumn = $this->filter($attribute);
            if ($this->sharedTables) {
                $builder->conflictSetRaw($incrementColumn, $this->getConflictTenantIncrementExpression($incrementColumn));
                $builder->conflictSetRaw(Storage::UPDATED_AT, $this->getConflictTenantExpression(Storage::UPDATED_AT));
            } else {
                $builder->conflictSetRaw($incrementColumn, $this->getConflictIncrementExpression($incrementColumn));
            }
        } elseif (! empty($operators)) {
            foreach ($allColumnNames as $column) {
                if (\in_array($column, $skipColumns)) {
                    continue;
                }
                if (isset($operators[$column])) {
                    $filteredColumn = $this->filter($column);
                    $expression = $this->getOperatorUpsertExpression($filteredColumn, $operators[$column]);
                    $builder->conflictSetRaw($column, $expression->sql, $expression->bindings);
                } elseif ($this->sharedTables) {
                    $builder->conflictSetRaw($column, $this->getConflictTenantExpression($column));
                }
            }
        } elseif ($this->sharedTables) {
            foreach ($updateColumns as $column) {
                $builder->conflictSetRaw($column, $this->getConflictTenantExpression($column));
            }
        }

        if (! $builder instanceof UpsertFeature) {
            throw new DatabaseException('Upserts are not supported on this dialect');
        }

        $result = $builder->upsert();
        $statement = $this->executeResult($result, Event::DocumentsUpsert);
        $this->execute($statement);
        $statement->closeCursor();
    }

    /**
     * Converts user-facing attribute names (like $id, $sequence) to internal
     * database column names (like _uid, _id) and ensures internal columns
     * are always included.
     *
     * An `alias.*` selection stands for the joined columns $joinSelections lists under that alias.
     *
     * @param  array<string>  $selections
     * @param  array<string>  $joinAliases
     * @param  array<string, list<string>>  $joinSelections  The selections `alias.*` makes under each join alias
     */
    private function applySelectionProjection(
        SQLBuilder $builder,
        array $selections,
        bool $includeInternal = true,
        array $joinAliases = [],
        array $joinSelections = [],
    ): void {
        $expanded = [];
        foreach ($selections as $selection) {
            if (\str_ends_with($selection, '.*') && isset($joinSelections[\substr($selection, 0, -2)])) {
                \array_push($expanded, ...$joinSelections[\substr($selection, 0, -2)]);
            } else {
                $expanded[] = $selection;
            }
        }

        $mapped = $this->mapSelectionsToColumns(\array_values(\array_unique($expanded)), $includeInternal, $joinAliases);
        $simple = [];
        foreach ($mapped as $column) {
            if (\str_contains($column, ' AS ')) {
                $builder->selectRaw($column);
            } else {
                $simple[] = $column;
            }
        }
        if ($simple !== []) {
            $builder->select($simple);
        }
    }

    /**
     * @param  array<BaseQuery>  $queries
     * @param  list<JoinAlias>  $joinTablePrefixes
     */
    private function remapDottedQueryAttributes(array $queries, array $joinTablePrefixes, Document $collection): void
    {
        $aliasSet = \array_fill_keys(\array_column($joinTablePrefixes, 'alias'), true);
        $aliasSet[Query::DEFAULT_ALIAS] = true;
        $mainAttributes = [];
        foreach (self::collectionAttributes($collection) as $attribute) {
            $mainAttributes[$attribute->key] = true;
        }

        foreach ($queries as $query) {
            $this->remapDottedQuery($query, $aliasSet, $mainAttributes);
        }
    }

    /**
     * @param  array<string, true>  $aliasSet
     * @param  array<string, true>  $mainAttributes
     */
    private function remapDottedQuery(BaseQuery $query, array $aliasSet, array $mainAttributes): void
    {
        $method = $query->getMethod();
        if ($method === Method::Select) {
            return;
        }

        if ($method->isJoin()) {
            foreach ($query->getJoinOnQueries() as $onQuery) {
                if ($onQuery->getMethod() !== Method::On) {
                    $this->remapDottedQuery($onQuery, $aliasSet, $mainAttributes);
                }
            }

            return;
        }

        if ($query->isNested()) {
            foreach ($query->getValues() as $child) {
                if ($child instanceof BaseQuery) {
                    $this->remapDottedQuery($child, $aliasSet, $mainAttributes);
                }
            }

            return;
        }

        if (\in_array($method, self::COLUMN_LIST_METHODS, true)) {
            $values = $query->getValues();
            $changed = false;
            foreach ($values as $i => $column) {
                if (! \is_string($column) || ! \str_contains($column, '.')) {
                    continue;
                }
                $values[$i] = $this->qualifyDottedAttribute($column, $aliasSet, $mainAttributes);
                $changed = true;
            }
            if ($changed) {
                $query->setValues($values);
            }

            return;
        }

        $attribute = $query->getAttribute();
        if ($attribute === '' || $attribute === '*' || ! \str_contains($attribute, '.')) {
            return;
        }

        $query->setAttribute($this->qualifyDottedAttribute($attribute, $aliasSet, $mainAttributes));
    }

    /**
     * @param  array<string, true>  $aliasSet
     * @param  array<string, true>  $mainAttributes
     */
    private function qualifyDottedAttribute(string $attribute, array $aliasSet, array $mainAttributes): string
    {
        if (isset($mainAttributes[$attribute])) {
            return $this->filter($this->getInternalKeyForAttribute($attribute));
        }

        $dot = \strpos($attribute, '.');
        if ($dot === false) {
            return $this->filter($this->getInternalKeyForAttribute($attribute));
        }

        $prefix = \substr($attribute, 0, $dot);
        if (isset($aliasSet[$prefix])) {
            $name = \substr($attribute, $dot + 1);

            return $this->filter($prefix).'.'.$this->filter($this->getInternalKeyForAttribute($name));
        }

        return $attribute;
    }

    /**
     * @param  array<string>  $selections
     * @param  array<string>  $joinAliases
     * @return array<string>
     */
    protected function mapSelectionsToColumns(array $selections, bool $includeInternal = true, array $joinAliases = []): array
    {
        $internalKeys = [
            Document::ID,
            Document::SEQUENCE,
            Document::PERMISSIONS,
            Document::CREATED_AT,
            Document::UPDATED_AT,
        ];

        $explicitInternals = [];
        foreach ($selections as $selection) {
            if (\in_array($selection, $internalKeys, true)) {
                $explicitInternals[] = $selection;
            }
        }

        $selections = \array_values(\array_diff($selections, [...$internalKeys, Document::COLLECTION]));

        if ($includeInternal) {
            foreach ($internalKeys as $internalKey) {
                $selections[] = $this->getInternalKeyForAttribute($internalKey);
            }
        } else {
            foreach (\array_values(\array_unique($explicitInternals)) as $internalKey) {
                $selections[] = $this->getInternalKeyForAttribute($internalKey);
            }
        }

        $aliasSet = \array_fill_keys($joinAliases, true);
        $quote = $this->getIdentifierQuote();
        $columns = [];
        foreach ($selections as $selection) {
            $dot = \strpos($selection, '.');
            if ($dot !== false) {
                $prefix = \substr($selection, 0, $dot);
                if (isset($aliasSet[$prefix])) {
                    $name = \substr($selection, $dot + 1);
                    $internal = $this->filter($this->getInternalKeyForAttribute($name));
                    $qualified = $quote.$this->filter($prefix).$quote.'.'.$quote.$internal.$quote;
                    $output = $prefix.'.'.$internal;
                    $columns[] = $qualified.' AS '.$quote.$output.$quote;

                    continue;
                }
            }
            $columns[] = $this->filter($selection);
        }

        return $columns;
    }

    /**
     * The projection of a join without a select or with `*`: every column of the main table, and under each
     * join alias the joined collection's `$id` and the attributes the Database layer handed over for it. A
     * joined table's internal columns are returned only when a select names them or when the read orders by
     * them, so that every row it returns can be passed back as its cursor.
     *
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @param  array<string>  $additions  Selections next to `*` and order attributes; those under a join alias are projected too
     */
    private function applyJoinProjection(SQLBuilder $builder, Document $collection, array $joinTablePrefixes, string $alias, array $additions = []): void
    {
        $builder->select([$this->filter($alias).'.*']);

        $joinAliases = \array_column($joinTablePrefixes, 'alias');
        $aliasSet = \array_fill_keys($joinAliases, true);
        $selections = \array_merge(...\array_values($this->joinSelections($collection, $joinTablePrefixes)));
        foreach ($additions as $addition) {
            $dot = \strpos($addition, '.');
            if ($dot !== false && isset($aliasSet[\substr($addition, 0, $dot)])) {
                $selections[] = $addition;
            }
        }

        $this->applySelectionProjection(
            $builder,
            $selections,
            includeInternal: false,
            joinAliases: $joinAliases,
            joinSelections: $this->joinWildcardSelections($collection, $joinTablePrefixes),
        );
    }

    /**
     * What a read without a select returns under each join alias: the joined collection's `$id` and
     * the attributes the Database layer handed over for it.
     *
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @return array<string, list<string>>
     */
    private function joinSelections(Document $collection, array $joinTablePrefixes): array
    {
        $joinAttributes = $collection->getAttribute(Database::JOIN_ATTRIBUTES, []);
        $selections = [];
        foreach ($joinTablePrefixes as $join) {
            $selections[$join->alias] ??= [];
            $selections[$join->alias][] = $join->alias.'.'.Document::ID;

            $attributes = \is_array($joinAttributes) ? ($joinAttributes[$join->table] ?? []) : [];
            foreach (\is_array($attributes) ? $attributes : [] as $attribute) {
                if (\is_string($attribute) && $attribute !== '') {
                    $selections[$join->alias][] = $join->alias.'.'.$attribute;
                }
            }
        }

        return $selections;
    }

    /**
     * What `alias.*` selects under each join alias: what a read without a select returns there, and the joined
     * collection's internal attributes a direct read of it returns.
     *
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @return array<string, list<string>>
     */
    private function joinWildcardSelections(Document $collection, array $joinTablePrefixes): array
    {
        $selections = $this->joinSelections($collection, $joinTablePrefixes);
        foreach ($selections as $joinAlias => $columns) {
            foreach (self::JOINED_ROW_INTERNALS as $internal) {
                $columns[] = $joinAlias.'.'.$internal;
            }
            $selections[$joinAlias] = $columns;
        }

        return $selections;
    }

    /**
     * @throws DatabaseException
     */
    protected function addAttributeColumn(Table $table, Attribute $attribute): Column
    {
        return $this->addTableColumn($table, $attribute->key, $attribute->type, $attribute->size ?? 0, $attribute->signed, $attribute->array, $attribute->required);
    }

    protected function getAttributeSqlType(Attribute $attribute): string
    {
        return $this->getSqlType($attribute->type, $attribute->size ?? 0, $attribute->signed, $attribute->array, $attribute->required);
    }

    /**
     * A relationship stores a column on the side that holds the foreign key: never for many-to-many, which uses a
     * junction table.
     */
    protected static function storesColumn(Attribute $attribute): bool
    {
        $relationship = $attribute->relationship;
        if ($relationship === null) {
            return true;
        }

        $parent = $attribute->side === RelationshipSide::Parent;

        return match ($relationship->type) {
            RelationshipType::OneToOne => $parent || $relationship->twoWay,
            RelationshipType::OneToMany => ! $parent,
            RelationshipType::ManyToOne => $parent,
            RelationshipType::ManyToMany => false,
        };
    }

    /**
     * Map Database type constants to Schema Table column definitions.
     *
     * @throws DatabaseException
     */
    protected function addTableColumn(
        Table $table,
        string $id,
        ColumnType $type,
        int $size,
        bool $signed = true,
        bool $array = false,
        bool $required = false
    ): Column {
        $filteredId = $this->filter($id);

        if (\in_array($type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
            $column = $this->addSpatialColumn($table, $filteredId, $type);
            if (! $required || $this->supports(Capability::IndexSpatialNull)) {
                $column->nullable();
            }

            return $column;
        }

        if ($array) {
            return $table->json($filteredId)->nullable();
        }

        if ($type === ColumnType::Varchar) {
            $this->assertVarcharSize($size);
        }

        $column = match ($type) {
            ColumnType::String => match (true) {
                $size > 16777215 => $table->longText($filteredId),
                $size > 65535 => $table->mediumText($filteredId),
                $size > $this->limits()->varchar => $table->text($filteredId),
                $size <= 0 => $table->text($filteredId),
                default => $table->string($filteredId, $size),
            },
            ColumnType::Integer => $size >= 8
                ? $table->bigInteger($filteredId)
                : $table->integer($filteredId),
            ColumnType::BigInteger => $table->bigInteger($filteredId),
            ColumnType::Float, ColumnType::Double => $table->float($filteredId),
            ColumnType::Boolean => $table->boolean($filteredId),
            ColumnType::Datetime => $table->datetime($filteredId, 3),
            ColumnType::Relationship => $table->string($filteredId, 255),
            ColumnType::Id => $table->bigInteger($filteredId),
            ColumnType::Varchar => $table->string($filteredId, $size),
            ColumnType::Text => $table->text($filteredId),
            ColumnType::MediumText => $table->mediumText($filteredId),
            ColumnType::LongText => $table->longText($filteredId),
            ColumnType::Object => $table->json($filteredId),
            ColumnType::Vector => $this->addVectorColumn($table, $filteredId, $size),
            default => throw new DatabaseException('Unknown type: '.$type->value),
        };

        if (! $signed && \in_array($type, [ColumnType::Integer, ColumnType::BigInteger, ColumnType::Float, ColumnType::Double], true)) {
            $column->unsigned();
        }

        if ($type === ColumnType::Id) {
            $column->unsigned();
        }

        // Non-spatial columns are nullable by default to match existing behavior
        $column->nullable();

        return $column;
    }

    /**
     * @throws DatabaseException
     */
    protected function assertVarcharSize(int $size): void
    {
        if ($size <= 0) {
            throw new DatabaseException('VARCHAR size ' . $size . ' is invalid; must be > 0. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.');
        }
        if ($size > $this->limits()->varchar) {
            throw new DatabaseException('VARCHAR size ' . $size . ' exceeds maximum varchar length ' . $this->limits()->varchar . '. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.');
        }
    }

    /**
     * @throws DatabaseException
     */
    private function addSpatialColumn(Table $table, string $name, ColumnType $type): Column
    {
        $srid = $this->getSpatialColumnSrid();
        if ($srid === null) {
            return $table->addColumn($name, $type);
        }

        return match ($type) {
            ColumnType::Point => $table->point($name, $srid),
            ColumnType::Linestring => $table->linestring($name, $srid),
            ColumnType::Polygon => $table->polygon($name, $srid),
            default => throw new DatabaseException('Unknown spatial type: '.$type->value),
        };
    }

    /**
     * SRID written into spatial column definitions, or null for a dialect that cannot declare one on a column.
     */
    protected function getSpatialColumnSrid(): ?int
    {
        return Database::DEFAULT_SRID;
    }

    private function addVectorColumn(Table $table, string $name, int $size): Column
    {
        if (! $table instanceof PostgresTable) {
            throw new DatabaseException('Vector columns are only supported on PostgreSQL');
        }

        return $table->vector($name, $size);
    }

    /**
     * @param  array<BaseQuery>  $queries
     */
    private function queriesHaveJoins(array $queries): bool
    {
        foreach ($queries as $query) {
            if ($query->getMethod()->isJoin()) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param  array<BaseQuery>  $queries
     * @return list<JoinAlias>
     *
     * @throws QueryException
     */
    private function remapJoinQueries(array &$queries): array
    {
        $joinTablePrefixes = [];
        $this->assertJoinAliases($queries);

        foreach ($queries as $query) {
            if (! $query->getMethod()->isJoin()) {
                continue;
            }

            $joinTable = $query->getAttribute();
            $query->setAttribute($this->getTableRaw($this->filter($joinTable)));

            $joinAlias = $query->getAlias();
            if ($query->isNestedJoin()) {
                $query->setValues($this->remapJoinOnQueries($query, Query::DEFAULT_ALIAS, $joinAlias));
            }

            $joinTablePrefixes[] = new JoinAlias($joinTable, $joinAlias);
        }

        return $joinTablePrefixes;
    }

    /**
     * @param  array<BaseQuery>  $queries
     *
     * @throws QueryException  when a join alias is invalid or declared more than once
     */
    private function assertJoinAliases(array $queries): void
    {
        $declared = [];
        foreach ($queries as $query) {
            if (! $query->getMethod()->isJoin()) {
                continue;
            }

            $alias = $query->getAlias();
            $invalid = JoinValidator::describeInvalidAlias($alias);
            if ($invalid !== null) {
                throw new QueryException($invalid);
            }

            $key = \strtolower($alias);
            if (isset($declared[$key])) {
                throw new QueryException("Join alias \"{$alias}\" is declared more than once");
            }
            $declared[$key] = true;
        }
    }

    /**
     * @return list<BaseQuery>
     */
    private function remapJoinOnQueries(BaseQuery $query, string $mainAlias, string $joinAlias): array
    {
        $values = [];
        foreach ($query->getJoinOnQueries() as $onQuery) {
            $values[] = $this->remapJoinOnQuery($onQuery, $mainAlias, $joinAlias);
        }

        return $values;
    }

    private function remapJoinOnQuery(BaseQuery $onQuery, string $mainAlias, string $joinAlias): BaseQuery
    {
        if ($onQuery->getMethod() !== Method::On) {
            return $onQuery;
        }

        $values = $onQuery->getValues();
        $left = $values[0] ?? null;
        $right = $values[2] ?? null;
        if (! \is_string($left) || $left === '' || ! \is_string($right) || $right === '') {
            throw new QueryException('Join ON requires left and right columns');
        }

        $values[0] = $this->qualifyJoinColumn($left, $mainAlias);
        $values[2] = $this->qualifyJoinColumn($right, $joinAlias);
        $onQuery->setValues($values);

        return $onQuery;
    }

    /**
     * @param  array<BaseQuery>  $queries
     */
    private function needsFullOuterJoinEmulation(SQLBuilder $builder, array $queries): bool
    {
        if ($builder instanceof FullOuterJoinsFeature) {
            return false;
        }

        foreach ($queries as $query) {
            if ($query->getMethod() === Method::FullOuterJoin) {
                return true;
            }
        }

        return false;
    }

    /**
     * Split a query set with a full outer join the engine cannot run into the two halves of a flat
     * UNION ALL, every table staying at the top level of FROM where later ON and WHERE conditions
     * reach it. The first half runs the full outer join as a left join and keeps every row holding a
     * main-side row; the second runs it as a right join and keeps only the joined table's unmatched rows.
     *
     * A later right join runs in both halves, so its unmatched rows are kept by one half only: the one
     * whose rows alone decide what the right join matches — the first when its ON reaches a table joined
     * before the full outer join, the second when it reaches the full outer joined table. A chain neither
     * half can decide alone, or with a second full outer join, is rejected.
     *
     * @param  array<BaseQuery>  $queries  With the join columns remapJoinQueries() qualified
     * @return array{0: array<BaseQuery>, 1: array<BaseQuery>}
     *
     * @throws QueryException
     */
    private function emulateFullOuterJoin(array $queries, string $alias): array
    {
        $fullJoinAlias = null;
        $joinedAliases = [$alias];
        $mainSideAliases = [];
        $reach = [];
        $firstHalfRows = self::UNPAIRED_MAIN_ROWS | self::PAIRED_ROWS;
        $secondHalfRows = self::PAIRED_ROWS | self::UNPAIRED_JOINED_ROWS;
        $presentRows = $firstHalfRows | $secondHalfRows;
        $nextRows = self::UNPAIRED_JOINED_ROWS << 1;
        $firstHalfExclusions = [];
        $secondHalfInclusions = [];

        foreach ($queries as $query) {
            $method = $query->getMethod();
            if (! $method->isJoin()) {
                continue;
            }

            $joinAlias = $query->getAlias();

            if ($method === Method::FullOuterJoin) {
                if ($fullJoinAlias !== null) {
                    throw new QueryException('A query can hold only one full outer join on this database');
                }

                $fullJoinAlias = $joinAlias;
                $mainSideAliases = $joinedAliases;
                foreach ($mainSideAliases as $mainSideAlias) {
                    $reach[$mainSideAlias] = self::UNPAIRED_MAIN_ROWS | self::PAIRED_ROWS;
                }
                $reach[$joinAlias] = self::PAIRED_ROWS | self::UNPAIRED_JOINED_ROWS;
                $joinedAliases[] = $joinAlias;

                continue;
            }

            if ($fullJoinAlias === null) {
                $joinedAliases[] = $joinAlias;

                continue;
            }

            $rows = $presentRows;
            foreach ($this->joinConditionAliases($query) as $conditionAlias) {
                $rows &= $reach[$conditionAlias] ?? $presentRows;
            }

            if ($method === Method::RightJoin) {
                $unmatchedRows = $nextRows;
                $nextRows <<= 1;

                if (($rows & ~$firstHalfRows) === 0) {
                    $firstHalfRows |= $unmatchedRows;
                } elseif (($rows & ~$secondHalfRows) === 0) {
                    $secondHalfRows |= $unmatchedRows;
                    $firstHalfExclusions[] = $this->anyOf([
                        ...\array_map(static fn (string $joined): BaseQuery => BaseQuery::isNotNull($joined.'.'.Storage::UID), $joinedAliases),
                        BaseQuery::isNull($joinAlias.'.'.Storage::UID),
                    ]);
                    $between = \array_slice($joinedAliases, \count($mainSideAliases) + 1);
                    $secondHalfInclusions[] = $this->allOf([
                        ...\array_map(static fn (string $joined): BaseQuery => BaseQuery::isNull($joined.'.'.Storage::UID), $between),
                        BaseQuery::isNotNull($joinAlias.'.'.Storage::UID),
                    ]);
                } else {
                    throw new QueryException('A right join after a full outer join has to join on a table joined before it, or on the full outer joined table');
                }

                $rows |= $unmatchedRows;
                $presentRows |= $unmatchedRows;
            } elseif ($method === Method::CrossJoin || $method === Method::NaturalJoin) {
                $rows = $presentRows;
            }

            $reach[$joinAlias] = $rows;
            $joinedAliases[] = $joinAlias;
        }

        if ($fullJoinAlias === null) {
            throw new DatabaseException('The query holds no full outer join to emulate');
        }

        $firstHalf = $this->rewriteFullOuterJoins($queries, Method::LeftJoin);
        \array_push($firstHalf, ...$firstHalfExclusions);

        $secondHalf = $this->rewriteFullOuterJoins($queries, Method::RightJoin);
        foreach ($mainSideAliases as $mainSideAlias) {
            $secondHalf[] = BaseQuery::isNull($mainSideAlias.'.'.Storage::UID);
        }
        $secondHalf[] = $this->anyOf([
            BaseQuery::isNotNull($fullJoinAlias.'.'.Storage::UID),
            ...$secondHalfInclusions,
        ]);

        return [$firstHalf, $secondHalf];
    }

    /**
     * The aliases whose columns a join's ON compares, other than the join's own.
     *
     * @return list<string>
     */
    private function joinConditionAliases(BaseQuery $join): array
    {
        $method = $join->getMethod();
        if ($method === Method::CrossJoin || $method === Method::NaturalJoin) {
            return [];
        }

        $columns = [];
        foreach ($join->getJoinOnQueries() as $condition) {
            if ($condition->getMethod() === Method::On) {
                $values = $condition->getValues();
                $columns[] = $values[0] ?? null;
                $columns[] = $values[2] ?? null;
            }
        }

        $joinAlias = $join->getAlias();
        $aliases = [];
        foreach ($columns as $column) {
            if (! \is_string($column)) {
                continue;
            }

            $dot = \strpos($column, '.');
            if ($dot === false) {
                continue;
            }

            $conditionAlias = \substr($column, 0, $dot);
            if ($conditionAlias !== $joinAlias) {
                $aliases[] = $conditionAlias;
            }
        }

        return $aliases;
    }

    /**
     * @param  non-empty-list<BaseQuery>  $conditions
     */
    private function anyOf(array $conditions): BaseQuery
    {
        return \count($conditions) === 1 ? $conditions[0] : BaseQuery::or($conditions);
    }

    /**
     * @param  non-empty-list<BaseQuery>  $conditions
     */
    private function allOf(array $conditions): BaseQuery
    {
        return \count($conditions) === 1 ? $conditions[0] : BaseQuery::and($conditions);
    }

    /**
     * @param  array<BaseQuery>  $queries
     */
    private function keepsUnmatchedRows(array $queries): bool
    {
        foreach ($queries as $query) {
            $method = $query->getMethod();
            if ($method === Method::RightJoin || $method === Method::FullOuterJoin) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param  array<BaseQuery>  $queries
     * @return array<BaseQuery>
     */
    private function rewriteFullOuterJoins(array $queries, Method $replacement): array
    {
        $rewritten = [];
        foreach ($queries as $query) {
            $clone = clone $query;
            if ($clone->getMethod() === Method::FullOuterJoin) {
                $clone->setMethod($replacement);
            }
            $rewritten[] = $clone;
        }

        return $rewritten;
    }

    /**
     * @param  array<BaseQuery>  $queries
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @param  array<Query>  $adapterFilterQueries
     * @param  array<string>  $roles
     * @param  array<string>  $orderAttributes  The attributes the read orders by
     */
    private function configureFindBuilder(
        SQLBuilder $builder,
        Document $collection,
        array $queries,
        array $joinTablePrefixes,
        bool $hasAggregation,
        bool $hasDistinct,
        array $adapterFilterQueries,
        string $name,
        string $alias,
        array $roles,
        PermissionType $forPermission,
        bool $qualifyCollidingGroups = true,
        array $orderAttributes = [],
    ): bool {
        $hasSelectionProjection = false;
        if (! $hasAggregation) {
            $selections = [];
            foreach ($queries as $query) {
                if ($query->getMethod() === Method::Select) {
                    foreach ($query->getValues() as $value) {
                        /** @var string $value */
                        $selections[] = $value;
                    }
                }
            }
            if (! empty($selections) && ! \in_array('*', $selections)) {
                $this->applySelectionProjection(
                    $builder,
                    $selections,
                    includeInternal: ! $hasDistinct,
                    joinAliases: \array_column($joinTablePrefixes, 'alias'),
                    joinSelections: $this->joinWildcardSelections($collection, $joinTablePrefixes),
                );
                // The projection replaces the select; forwarded as well, the builder would compile the caller's
                // raw attribute names whenever the projection holds only aliased joined columns.
                $queries = \array_values(\array_filter($queries, static fn (BaseQuery $query): bool => $query->getMethod() !== Method::Select));
                $hasSelectionProjection = true;
            } elseif (! empty($joinTablePrefixes)) {
                $this->applyJoinProjection(
                    $builder,
                    $collection,
                    $joinTablePrefixes,
                    $alias,
                    $hasDistinct ? $selections : [...$selections, ...$orderAttributes],
                );
                $hasSelectionProjection = true;
            }
        }

        if ($hasAggregation && ! empty($joinTablePrefixes)) {
            $mainAttributes = [];
            foreach ([...Database::internalAttributesFor(true), ...self::collectionAttributes($collection)] as $attribute) {
                $mainAttributes[$attribute->key] = true;
            }

            $joinAttributes = $collection->getAttribute(Database::JOIN_ATTRIBUTES, []);
            $declared = [];
            foreach ($joinTablePrefixes as $join) {
                $keys = \is_array($joinAttributes) ? ($joinAttributes[$join->table] ?? null) : null;
                $declared[$join->alias] = \is_array($keys) ? \array_flip(\array_filter($keys, \is_string(...))) : null;
            }

            $qualify = function (string $attribute) use ($mainAttributes, $declared): string {
                if (
                    $attribute === '*'
                    || $attribute === ''
                    || \is_numeric($attribute)
                    || \str_contains($attribute, '.')
                    || isset($mainAttributes[$attribute])
                ) {
                    return $attribute;
                }

                $aliases = [];
                foreach ($declared as $alias => $attributes) {
                    if ($attributes === null || isset($attributes[$attribute])) {
                        $aliases[] = $alias;
                    }
                }

                if (\count($aliases) > 1) {
                    throw new QueryException('Attribute "'.$attribute.'" is ambiguous across joins; qualify it with a join alias');
                }

                if ($aliases === []) {
                    throw new QueryException('Attribute not found in schema: '.$attribute);
                }

                return $aliases[0].'.'.$this->getInternalKeyForAttribute($attribute);
            };

            // The builder leaves a name that is also an aggregate alias unqualified, so an aggregate reads a main
            // attribute qualified: a joined column of the same name would otherwise make it ambiguous.
            foreach ($queries as $query) {
                if ($query->getMethod()->isAggregate()) {
                    $attribute = $query->getAttribute();
                    $query->setAttribute(isset($mainAttributes[$attribute]) ? $alias.'.'.$attribute : $qualify($attribute));
                } elseif ($query->getMethod() === Method::GroupBy) {
                    $query->setValues(\array_map(
                        static fn (mixed $column): mixed => \is_string($column) ? $qualify($column) : $column,
                        $query->getValues(),
                    ));
                }
            }
        }

        if ($hasAggregation) {
            // An aggregation returns only its groups and aggregates: a select the validators accept names a group,
            // which is selected below, or a wildcard, so no select reaches the statement.
            $queries = \array_values(\array_filter($queries, static fn (BaseQuery $query): bool => $query->getMethod() !== Method::Select));

            foreach ($queries as $query) {
                if ($query->getMethod() === Method::GroupBy) {
                    // Each group is selected as the GROUP BY clause names it once applyFindFilters() maps it.
                    $columns = clone $query;
                    $this->remapDottedQueryAttributes([$columns], $joinTablePrefixes, $collection);
                    /** @var array<string> $groupCols */
                    $groupCols = $columns->getValues();
                    /** @var array<string> $groups */
                    $groups = $query->getValues();
                    $qualified = $qualifyCollidingGroups ? $this->qualifiedGroupNames($groups) : [];
                    $plain = [];
                    foreach ($groupCols as $index => $col) {
                        if (! isset($qualified[$index])) {
                            $plain[] = \str_contains($col, '.') ? $col : $this->filter($this->getInternalKeyForAttribute($col));
                        }
                    }
                    if ($plain !== []) {
                        $builder->select($plain);
                    }
                    foreach ($qualified as $index => $group) {
                        [$table, $column] = \explode('.', $groupCols[$index], 2);
                        $builder->select($this->quote($this->filter($table)).'.'.$this->quote($this->filter($column)).' AS '.$this->quote($group));
                    }
                }
            }
        }

        $this->applyFindFilters(
            $builder,
            $collection,
            $queries,
            $joinTablePrefixes,
            $adapterFilterQueries,
            $name,
            $alias,
            $roles,
            $forPermission,
        );

        return $hasSelectionProjection;
    }

    /**
     * @param  array<Query>  $queries
     */
    private function applyFilters(SQLBuilder $builder, array $queries, string $name, string $alias): void
    {
        $builderQueries = [];
        $adapterFilters = [];
        foreach ($queries as $query) {
            if ($this->isAdapterFilterQuery($query)) {
                $adapterFilters[] = $this->compileAdapterFilter($query, $name, $alias);

                continue;
            }
            $builderQueries[] = $query;
        }

        $builder->filter($builderQueries);

        foreach ($adapterFilters as $filter) {
            if ($filter !== null) {
                $builder->whereRaw($filter->sql, $filter->bindings);
            }
        }
    }

    /**
     * @param  array<BaseQuery>  $queries
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @param  array<Query>  $adapterFilterQueries
     * @param  array<string>  $roles
     */
    private function applyFindFilters(
        SQLBuilder $builder,
        Document $collection,
        array $queries,
        array $joinTablePrefixes,
        array $adapterFilterQueries,
        string $name,
        string $alias,
        array $roles,
        PermissionType $forPermission,
    ): void {
        $queries = $this->populationStatistics($queries);
        $adapterFilterQueries = \array_map(static fn (Query $query): Query => clone $query, $adapterFilterQueries);
        $this->remapDottedQueryAttributes([...$queries, ...$adapterFilterQueries], $joinTablePrefixes, $collection);
        $builder->filter($queries);

        foreach ($adapterFilterQueries as $query) {
            $compiled = $this->compileAdapterFilter($query, $name, $alias, $joinTablePrefixes);
            if ($compiled !== null) {
                $builder->whereRaw($compiled->sql, $compiled->bindings);
            }
        }

        $chain = Join\Chain::fromQueries($queries);
        $preserving = $chain->hasPreservingOuterJoin();

        if ($this->sharedTables && $preserving) {
            $tenantFilter = new Tenant\Filter($this->currentTenant(), quoteCharacter: $this->getIdentifierQuote());
            $tenantConditions = [];
            foreach ($joinTablePrefixes as $join) {
                $tenantConditions[$join->alias] = $tenantFilter->joined($join->alias);
            }
            $builder->addHook(new Join\OuterChain($chain, $tenantConditions, $this->getIdentifierQuote()));
        }

        if ($this->authorization->getStatus()) {
            $hasJoins = ! empty($joinTablePrefixes);
            $granted = $hasJoins && $collection->getAttribute(Database::COLLECTION_GRANTED, false) === true;
            $permissionConditions = [];
            if (! $granted && $this->filtersPerDocument($collection)) {
                $docCol = $hasJoins ? $alias.'.'.Storage::UID : Storage::UID;
                $permissionHook = $this->newPermissionHook($name, $roles, $forPermission->value, $docCol);
                if ($preserving) {
                    $permissionConditions[$alias] = $permissionHook->filter($alias);
                    $permissionHook = new Permission\AllowNullUid(
                        $permissionHook,
                        $docCol,
                        $this->getIdentifierQuote(),
                    );
                }
                $builder->addHook($permissionHook);
            }

            $joinDocumentSecurity = $collection->getAttribute(Database::JOIN_DOCUMENT_SECURITY, []);
            /** @var array<string, mixed> $joinDocumentSecurity */
            $joinDocumentSecurity = \is_array($joinDocumentSecurity) ? $joinDocumentSecurity : [];

            foreach ($joinTablePrefixes as $join) {
                if ($this->joinDocumentSecurityEnabled($joinDocumentSecurity, $join->table) === false) {
                    continue;
                }

                $permissionHook = $this->newJoinPermissionHook(
                    $this->filter($join->table),
                    $roles,
                    $forPermission->value,
                    $join->alias.'.'.Storage::UID,
                    \count($joinTablePrefixes),
                    $chain->type($join->alias),
                );
                if ($preserving) {
                    $permissionConditions[$join->alias] = $permissionHook->filter($join->alias);
                }
                $builder->addHook(new Permission\Join(
                    $permissionHook,
                    $join->alias,
                    $this->getIdentifierQuote(),
                    $preserving,
                ));
            }

            if ($permissionConditions !== []) {
                $builder->addHook(new Permission\OuterJoin($alias, $permissionConditions, $this->getIdentifierQuote()));
                $builder->addHook(new Join\OuterChain($chain, $permissionConditions, $this->getIdentifierQuote()));
            }
        }
    }

    /**
     * Rewrite the two ambiguous statistical aggregates to their explicit
     * population forms, and count the inputs of every bitwise aggregate.
     *
     * Bare `STDDEV` and `VARIANCE` are not portable: MySQL and MariaDB read
     * both as the population statistic, PostgreSQL reads both as the sample
     * one, so the same query answered 67.0238 on one engine and 77.3985 on
     * the other. `STDDEV_POP` and `VAR_POP` mean the population statistic on
     * every engine this adapter targets, so emitting them explicitly fixes
     * the contract at population - which is what MySQL and MariaDB already
     * returned, and what the ClickHouse builder already chose. Callers who
     * want the sample statistic ask for it by name with stddevSamp() or
     * varSamp(), which were always unambiguous.
     *
     * Over no input values MySQL and MariaDB answer `BIT_AND` with every bit
     * set and `BIT_OR` / `BIT_XOR` with zero, where PostgreSQL answers NULL
     * as every engine does for each aggregate but count. The input count
     * lets bitwiseResults() answer NULL on every engine.
     *
     * @param  array<BaseQuery>  $queries
     * @return array<BaseQuery>
     */
    private function populationStatistics(array $queries): array
    {
        foreach ($queries as $index => $query) {
            $method = match ($query->getMethod()) {
                Method::Stddev => Method::StddevPop,
                Method::Variance => Method::VarPop,
                default => null,
            };

            if ($method !== null) {
                $queries[$index] = (clone $query)->setMethod($method);
            }
        }

        foreach ($this->bitwiseInputs($queries) as $count => $aggregate) {
            $queries[] = Query::count($aggregate->getAttribute(), $count);
        }

        return $queries;
    }

    /**
     * Each bitwise aggregate, keyed by the alias of the input count populationStatistics()
     * adds for it: `$inputs:<n>` for the n-th of them. The name stays short because PostgreSQL
     * truncates an identifier to 63 bytes, and a truncated count named another aggregate's alias.
     *
     * @param  array<BaseQuery>  $queries
     * @return array<string, BaseQuery>
     */
    private function bitwiseInputs(array $queries): array
    {
        $inputs = [];
        foreach ($queries as $query) {
            if (\in_array($query->getMethod(), self::BITWISE_AGGREGATES, true)) {
                $inputs[self::BITWISE_INPUTS.\count($inputs)] = $query;
            }
        }

        return $inputs;
    }

    private function filtersPerDocument(Document $collection): bool
    {
        return (bool) $collection->getAttribute('documentSecurity', false)
            || $collection->getId() === Database::METADATA;
    }

    /**
     * @param  array<string, mixed>  $joinDocumentSecurity
     */
    private function joinDocumentSecurityEnabled(array $joinDocumentSecurity, string $table): bool
    {
        foreach ($this->joinDocumentSecurityLookupKeys($table) as $key) {
            if (\array_key_exists($key, $joinDocumentSecurity)) {
                return (bool) $joinDocumentSecurity[$key];
            }
        }

        if ($joinDocumentSecurity === []) {
            return true;
        }

        $candidates = $this->joinDocumentSecurityLookupKeys($table);
        foreach ($joinDocumentSecurity as $key => $enabled) {
            $key = (string) $key;
            if ($key === '') {
                continue;
            }

            if (\array_intersect($candidates, $this->joinDocumentSecurityLookupKeys($key)) !== []) {
                return (bool) $enabled;
            }
        }

        return true;
    }

    /**
     * @return list<string>
     */
    private function joinDocumentSecurityLookupKeys(string $table): array
    {
        $filtered = $this->filter($table);
        $keys = [$table, $filtered];
        $qualified = $this->getTableRaw($filtered);
        $keys[] = $qualified;

        $dot = \strrpos($qualified, '.');
        if ($dot !== false) {
            $keys[] = \substr($qualified, $dot + 1);
        }

        return \array_values(\array_unique($keys));
    }

    /**
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  list<JoinAlias>  $joinTablePrefixes
     */
    private function applyFullOuterJoinOrderProjection(
        SQLBuilder $builder,
        Document $collection,
        string $alias,
        array $orderAttributes,
        array $orderTypes,
        bool $hasSelectionProjection,
        array $joinTablePrefixes = [],
    ): void {
        $hasOrderColumns = false;
        foreach (\array_keys($orderAttributes) as $i) {
            $orderType = $orderTypes[$i] ?? OrderDirection::Asc;
            if ($orderType !== OrderDirection::Random) {
                $hasOrderColumns = true;
                break;
            }
        }

        if (! $hasOrderColumns) {
            return;
        }

        if (! $hasSelectionProjection) {
            if (empty($joinTablePrefixes)) {
                $builder->select(['*']);
            } else {
                $this->applyJoinProjection($builder, $collection, $joinTablePrefixes, $alias);
            }
        }

        $joinAliases = \array_column($joinTablePrefixes, 'alias');
        foreach ($orderAttributes as $i => $attribute) {
            $orderType = $orderTypes[$i] ?? OrderDirection::Asc;
            if ($orderType === OrderDirection::Random) {
                continue;
            }

            $expression = $this->quoteOrderColumn($this->qualifyOrderAttribute($attribute, $joinAliases), $alias);
            $builder->selectRaw($expression.' AS '.$this->quote(self::FOJ_ORDER_ALIAS_PREFIX.$i));
        }
    }

    /**
     * Quote an order key from qualifyOrderAttribute() as a table-qualified column: a join-qualified key
     * keeps its join alias, any other key belongs to the main table.
     */
    private function quoteOrderColumn(string $key, string $alias): string
    {
        $dot = \strpos($key, '.');
        if ($dot === false) {
            return $this->quote($alias).'.'.$this->quote($key);
        }

        return $this->quote(\substr($key, 0, $dot)).'.'.$this->quote(\substr($key, $dot + 1));
    }

    /**
     * Aggregate an emulated full outer join once, over the rows of both halves. Each half keeps its own
     * joins, filters, tenant and permission conditions and projects the columns the aggregation reads;
     * their UNION ALL is read as one derived table, and the aggregates, groups, having, distinct(), order
     * and page run over it through the projection and fetch a native full outer join goes through.
     *
     * @param  array<BaseQuery>  $queries  With the join columns remapJoinQueries() qualified
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @param  array<Query>  $adapterFilterQueries
     * @param  array<string>  $roles
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @param  callable(string): string  $resolveInternalKey
     * @return array<int, array<string, mixed>>
     *
     * @throws DatabaseException
     */
    private function findFullOuterJoinAggregate(
        Document $collection,
        array $queries,
        array $joinTablePrefixes,
        bool $hasDistinct,
        array $adapterFilterQueries,
        string $name,
        string $alias,
        array $roles,
        PermissionType $forPermission,
        array $orderAttributes,
        array $orderTypes,
        ?int $limit,
        ?int $offset,
        array $cursor,
        CursorDirection $cursorDirection,
        callable $resolveInternalKey,
    ): array {
        $aggregationQueries = [];
        $rowQueries = [];
        $aggregateAliases = [];
        foreach ($queries as $query) {
            $method = $query->getMethod();
            if (! $this->shapesAggregatedRows($method)) {
                $rowQueries[] = $query;

                continue;
            }

            $aggregationQueries[] = $query;
            if ($method->isAggregate() && $query->getAlias() !== '') {
                $aggregateAliases[$query->getAlias()] = true;
            }
        }

        $joinAliases = \array_column($joinTablePrefixes, 'alias');
        $aggregation = $this->createBuilder();
        // The halves carry every tenant and permission condition of the read. The aggregation reads only
        // their rows, so it takes none of the permission filters configureFindBuilder() gives a builder
        // that reads the tables.
        $this->authorization->skip(fn (): bool => $this->configureFindBuilder(
            $aggregation,
            $collection,
            $aggregationQueries,
            $joinTablePrefixes,
            true,
            $hasDistinct,
            [],
            $name,
            $alias,
            $roles,
            $forPermission,
            qualifyCollidingGroups: false,
        ));
        $this->applyFindPage($aggregation, $orderAttributes, $orderTypes, $limit, $offset, $cursorDirection, joinAliases: $joinAliases);
        $columns = $this->fullOuterJoinColumns($aggregationQueries, $orderAttributes, $orderTypes, $joinAliases, $aggregateAliases, $alias);

        [$leftQueries, $rightQueries] = $this->emulateFullOuterJoin($rowQueries, $alias);
        $leftPreserving = $this->keepsUnmatchedRows($leftQueries);
        $halves = [];
        $unindexed = $this->unindexedJoins($collection, $rowQueries, $joinTablePrefixes);
        foreach ([[$leftQueries, $leftPreserving], [$rightQueries, true]] as [$halfQueries, $preservingOuter]) {
            $half = $this->newBuilder($name, $alias, $preservingOuter, unindexed: $unindexed);
            if ($columns === []) {
                $half->selectRaw('1');
            }
            foreach ($columns as $source => $column) {
                $half->selectRaw($this->quoteOrderColumn($source, $alias).' AS '.$this->quote($column));
            }
            $this->applyFindFilters($half, $collection, $halfQueries, $joinTablePrefixes, $adapterFilterQueries, $name, $alias, $roles, $forPermission);
            $this->applyFindCursor($half, $orderAttributes, $orderTypes, $cursor, $cursorDirection, $resolveInternalKey);
            $halves[] = $half;
        }

        [$left, $right] = $halves;
        $left->unionAll($right);
        $aggregation->fromSub($left, self::FOJ_ROWS_ALIAS);
        $aggregation->addHook(new AttributeMap($this->fullOuterJoinColumnSpellings($columns, $aggregateAliases, $alias)));

        $qualifiedGroups = [];
        foreach ($aggregationQueries as $query) {
            if ($query->getMethod() === Method::GroupBy) {
                /** @var array<string> $groups */
                $groups = $query->getValues();
                foreach ($this->qualifiedGroupNames($groups) as $group) {
                    $dot = (int) \strpos($group, '.');
                    $qualifiedGroups[\substr($group, 0, $dot).'.'.$this->getInternalKeyForAttribute(\substr($group, $dot + 1))] = $group;
                }
            }
        }

        return $this->fullOuterJoinResultNames($this->executeSelect($aggregation, Event::DocumentFind, $name), $columns, $qualifiedGroups);
    }

    private function shapesAggregatedRows(Method $method): bool
    {
        return $method->isAggregate() || match ($method) {
            Method::GroupBy, Method::Having, Method::Select, Method::Distinct => true,
            default => false,
        };
    }

    /**
     * The columns an aggregation over an emulated full outer join reads — aggregated attributes, groups,
     * having conditions and order attributes — keyed by table-qualified column, each with the column both
     * halves project it as. A select reads none: configureFindBuilder() leaves it out of an aggregation.
     *
     * @param  array<BaseQuery>  $queries  The aggregation's queries, as configureFindBuilder() left them
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string>  $joinAliases
     * @param  array<string, true>  $aggregateAliases
     * @return array<string, string>
     */
    private function fullOuterJoinColumns(array $queries, array $orderAttributes, array $orderTypes, array $joinAliases, array $aggregateAliases, string $alias): array
    {
        $references = [];
        while ($queries !== []) {
            $query = \array_shift($queries);
            $method = $query->getMethod();

            if ($method->isNested()) {
                foreach ($query->getValues() as $condition) {
                    if ($condition instanceof BaseQuery) {
                        $queries[] = $condition;
                    }
                }
            } elseif ($method === Method::GroupBy) {
                foreach ($query->getValues() as $column) {
                    if (\is_string($column)) {
                        $references[] = $column;
                    }
                }
            } elseif ($method !== Method::Select) {
                $references[] = $query->getAttribute();
            }
        }

        foreach ($orderAttributes as $i => $attribute) {
            if (($orderTypes[$i] ?? OrderDirection::Asc) !== OrderDirection::Random) {
                $references[] = $this->qualifyOrderAttribute($attribute, $joinAliases);
            }
        }

        $columns = [];
        foreach ($references as $reference) {
            if ($reference === '' || $reference === '*' || \is_numeric($reference) || isset($aggregateAliases[$reference])) {
                continue;
            }

            $dot = \strpos($reference, '.');
            $source = $dot === false
                ? $alias.'.'.$this->getInternalKeyForAttribute($reference)
                : \substr($reference, 0, $dot).'.'.$this->getInternalKeyForAttribute(\substr($reference, $dot + 1));
            $columns[$source] ??= self::FOJ_COLUMN_PREFIX.\count($columns);
        }

        return $columns;
    }

    /**
     * Every spelling the aggregation's queries can give a projected column — table-qualified or, on the
     * main table, bare; by internal or public name — resolved to the derived column that holds it. An
     * aggregate alias keeps naming its aggregate.
     *
     * @param  array<string, string>  $columns
     * @param  array<string, true>  $aggregateAliases
     * @return array<string, string>
     */
    private function fullOuterJoinColumnSpellings(array $columns, array $aggregateAliases, string $alias): array
    {
        $spellings = [];
        foreach ($columns as $source => $column) {
            [$table, $name] = \explode('.', $source, 2);
            $candidates = [$source, $table.'.'.Storage::attribute($name)];
            if ($table === $alias) {
                $candidates[] = $name;
                $candidates[] = Storage::attribute($name);
            }

            foreach ($candidates as $spelling) {
                if (! isset($aggregateAliases[$spelling])) {
                    $spellings[$spelling] = self::FOJ_ROWS_ALIAS.'.'.$column;
                }
            }
        }

        return $spellings;
    }

    /**
     * Name each result column the way the single statement names it: a derived column after the column
     * it holds, an expression over derived columns after the same expression over the columns they hold.
     *
     * A joined group that qualifiedGroupNames() names by its alias comes last under that name, where the
     * single statement selects it.
     *
     * @param  array<int, array<string, mixed>>  $rows
     * @param  array<string, string>  $columns
     * @param  array<string, string>  $qualifiedGroups  The name of each joined group qualified by its alias, by the column it holds
     * @return array<int, array<string, mixed>>
     */
    private function fullOuterJoinResultNames(array $rows, array $columns, array $qualifiedGroups = []): array
    {
        $names = [];
        $qualified = [];
        $expressions = [];
        foreach ($columns as $source => $column) {
            [$table, $name] = \explode('.', $source, 2);
            $names[$column] = $name;
            if (isset($qualifiedGroups[$source])) {
                $qualified[$column] = $qualifiedGroups[$source];
            }
            $expressions[$this->quote(self::FOJ_ROWS_ALIAS).'.'.$this->quote($column)] = $this->quote($table).'.'.$this->quote($name);
        }

        foreach ($rows as $index => $row) {
            $named = [];
            $trailing = [];
            foreach ($row as $key => $value) {
                $key = (string) $key;
                if (isset($qualified[$key])) {
                    $trailing[$qualified[$key]] = $value;

                    continue;
                }
                $named[$names[$key] ?? \strtr($key, $expressions)] = $value;
            }
            $rows[$index] = [...$named, ...$trailing];
        }

        return $rows;
    }

    /**
     * The groups returned under their qualified name (`alias.attribute`), by position: a joined group
     * whose column name another group of the query is also returned under. Every other group keeps
     * the column name the engine gives it, so a joined group alone under its name stays reachable
     * by that bare name, and the main collection's group keeps it when both are grouped.
     *
     * @param  array<string>  $groups
     * @return array<string>
     */
    private function qualifiedGroupNames(array $groups): array
    {
        $names = [];
        foreach ($groups as $index => $group) {
            $dot = \strrpos($group, '.');
            $names[$index] = $this->filter($this->getInternalKeyForAttribute($dot === false ? $group : \substr($group, $dot + 1)));
        }

        $counts = \array_count_values($names);
        $qualified = [];
        foreach ($groups as $index => $group) {
            if ($counts[$names[$index]] > 1 && \str_contains($group, '.')) {
                $qualified[$index] = $group;
            }
        }

        return $qualified;
    }

    /**
     * distinct() over an emulated full outer join removes a row both halves return with UNION, which
     * compares every projected column, the order columns among them. A single statement compares the
     * selected columns only, so ordering by an attribute the selection leaves out has no emulation.
     * Without a select every row carries each table's `$id`, so no order column can tell two rows apart
     * that the selection would not.
     *
     * @param  array<BaseQuery>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string>  $joinAliases
     *
     * @throws QueryException
     */
    private function assertDistinctOrderIsSelected(array $queries, array $orderAttributes, array $orderTypes, array $joinAliases): void
    {
        $selected = [];
        foreach ($queries as $query) {
            if ($query->getMethod() !== Method::Select) {
                continue;
            }

            foreach ($query->getValues() as $value) {
                if ($value === '*') {
                    return;
                }
                if (\is_string($value)) {
                    $selected[$this->qualifyOrderAttribute($value, $joinAliases)] = true;
                }
            }
        }

        if ($selected === []) {
            return;
        }

        foreach ($orderAttributes as $i => $attribute) {
            if (($orderTypes[$i] ?? OrderDirection::Asc) === OrderDirection::Random) {
                continue;
            }

            if (! isset($selected[$this->qualifyOrderAttribute($attribute, $joinAliases)])) {
                throw new QueryException("A distinct() query over a full outer join can only be ordered by a selected attribute on this database, and {$attribute} is not selected");
            }
        }
    }

    /**
     * With $nullable, a cursor value may be null and each comparison keeps the engine's own null placement: an
     * equal prefix on null is IS NULL, and nulls come after every value in a direction that sorts them last.
     *
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @param  callable(string): string  $resolveInternalKey
     */
    private function applyFindCursor(
        SQLBuilder $builder,
        array $orderAttributes,
        array $orderTypes,
        array $cursor,
        CursorDirection $cursorDirection,
        callable $resolveInternalKey,
        bool $nullable = false,
    ): void {
        if ($cursor === []) {
            return;
        }

        $cursorConditions = $this->cursorConditions($orderAttributes, $orderTypes, $cursor, $cursorDirection, $resolveInternalKey, $nullable);

        if ($cursorConditions === []) {
            return;
        }

        $builder->filter([$this->anyOf($cursorConditions)]);
    }

    /**
     * One condition per order position: the rows equal to the cursor before it and after the cursor in it. A row
     * follows the cursor when it meets any of them.
     *
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @param  callable(string): string  $resolveInternalKey
     * @return list<BaseQuery>
     */
    private function cursorConditions(
        array $orderAttributes,
        array $orderTypes,
        array $cursor,
        CursorDirection $cursorDirection,
        callable $resolveInternalKey,
        bool $nullable,
    ): array {
        $cursorConditions = [];

        foreach ($orderAttributes as $i => $originalAttribute) {
            $orderType = $orderTypes[$i] ?? OrderDirection::Asc;
            if ($orderType === OrderDirection::Random) {
                continue;
            }

            $direction = $orderType;

            if ($cursorDirection === CursorDirection::Before) {
                $direction = ($direction === OrderDirection::Asc)
                    ? OrderDirection::Desc
                    : OrderDirection::Asc;
            }

            $internalAttr = $resolveInternalKey($originalAttribute);

            if (! $nullable && count($orderAttributes) === 1 && $i === 0 && $originalAttribute === Document::SEQUENCE) {
                /** @var bool|float|int|string $cursorVal */
                $cursorVal = $cursor[$originalAttribute];
                if ($direction === OrderDirection::Desc) {
                    $cursorConditions[] = BaseQuery::lessThan($internalAttr, $cursorVal);
                } else {
                    $cursorConditions[] = BaseQuery::greaterThan($internalAttr, $cursorVal);
                }
                break;
            }

            $andConditions = [];

            for ($j = 0; $j < $i; $j++) {
                $andConditions[] = $this->cursorEquality($orderAttributes[$j], $cursor, $resolveInternalKey, $nullable);
            }

            if ($nullable) {
                /** @var bool|float|int|string|null $nullableValue */
                $nullableValue = $cursor[$originalAttribute];
                $comparison = $this->nullableCursorComparison($internalAttr, $nullableValue, $direction);
                if ($comparison === null) {
                    continue;
                }
                $andConditions[] = $comparison;
            } else {
                /** @var bool|float|int|string $cursorAttrVal */
                $cursorAttrVal = $cursor[$originalAttribute];
                if ($direction === OrderDirection::Desc) {
                    $andConditions[] = BaseQuery::lessThan($internalAttr, $cursorAttrVal);
                } else {
                    $andConditions[] = BaseQuery::greaterThan($internalAttr, $cursorAttrVal);
                }
            }

            $cursorConditions[] = $this->allOf($andConditions);
        }

        return $cursorConditions;
    }

    /**
     * @param  array<string, mixed>  $cursor
     * @param  callable(string): string  $resolveInternalKey
     */
    private function cursorEquality(string $attribute, array $cursor, callable $resolveInternalKey, bool $nullable): BaseQuery
    {
        $column = $resolveInternalKey($attribute);
        if ($nullable && $cursor[$attribute] === null) {
            return BaseQuery::isNull($column);
        }

        /** @var array<array<mixed>|bool|float|int|string|null> $values */
        $values = [$cursor[$attribute]];

        return BaseQuery::equal($column, $values);
    }

    /**
     * Whether a read whose rows are its left-joined rows picks the main rows its page can reach before it joins
     * them (boundedPage()). An engine that cannot read an order over two tables from an index sorts the whole join
     * before the limit otherwise.
     */
    protected function boundsJoinedSort(): bool
    {
        return false;
    }

    /**
     * Whether the engine looks a joined table's rows up by an equality on the leading `_tenant` of an index
     * alone, once per row the join pairs, when no index serves the join.
     */
    protected function looksUpByTenantAlone(): bool
    {
        return false;
    }

    /**
     * The aliases of the joins no index of their collection serves: no ON equality of the read reaches a column
     * one of its key or unique indexes leads with, nor an internal column every collection indexes. Under shared
     * tables every other index of such a table leads with `_tenant`. A join whose collection the Database layer
     * described no indexes of is taken as served.
     *
     * @param  array<BaseQuery>  $queries  With the join columns remapJoinQueries() qualified
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @return list<string>
     */
    private function unindexedJoins(Document $collection, array $queries, array $joinTablePrefixes): array
    {
        if (! $this->sharedTables || $joinTablePrefixes === [] || ! $this->looksUpByTenantAlone()) {
            return [];
        }

        $joinIndexed = $collection->getAttribute(Database::JOIN_INDEXED, []);
        if (! \is_array($joinIndexed)) {
            return [];
        }

        $bound = $this->joinEqualityColumns($queries);
        $internal = \array_map(\strtolower(...), [Storage::UID, Storage::SEQUENCE, Storage::CREATED_AT, Storage::UPDATED_AT]);

        $unindexed = [];
        foreach ($joinTablePrefixes as $join) {
            $leads = $joinIndexed[$join->table] ?? null;
            if (! \is_array($leads)) {
                continue;
            }

            $indexed = $internal;
            foreach ($leads as $lead) {
                if (\is_string($lead)) {
                    $indexed[] = \strtolower($this->getInternalKeyForAttribute($lead));
                }
            }

            if (\array_intersect($bound[$join->alias] ?? [], $indexed) === []) {
                $unindexed[] = $join->alias;
            }
        }

        return $unindexed;
    }

    /**
     * The columns each alias compares for equality with another alias's column in a join's ON.
     *
     * @param  array<BaseQuery>  $queries  With the join columns remapJoinQueries() qualified
     * @return array<string, list<string>>
     */
    private function joinEqualityColumns(array $queries): array
    {
        $columns = [];
        foreach ($queries as $query) {
            if (! $query->getMethod()->isJoin()) {
                continue;
            }

            foreach ($query->getJoinOnQueries() as $on) {
                $values = $on->getValues();
                if ($on->getMethod() !== Method::On || ($values[1] ?? null) !== '=' || ! \is_string($values[0] ?? null) || ! \is_string($values[2] ?? null)) {
                    continue;
                }

                $left = \explode('.', $values[0], 2);
                $right = \explode('.', $values[2], 2);
                if (\count($left) !== 2 || \count($right) !== 2 || $left[0] === $right[0]) {
                    continue;
                }

                $columns[$left[0]][] = \strtolower($left[1]);
                $columns[$right[0]][] = \strtolower($right[1]);
            }
        }

        return $columns;
    }

    /**
     * A read ordered by main attributes up to a unique one, then by joined ones, returns every joined row of one main
     * document together. Without inner joins and without conditions on joined attributes, every main document it
     * matches gives at least one row, so its page of `limit` rows after `offset` rows (and after the cursor) comes
     * from the first `offset + limit` main documents in that order after the cursor's own, plus the cursor's own.
     * A search on main attributes only keeps or drops main documents, so it joins the main conditions.
     *
     * @param  array<BaseQuery>  $queries
     * @param  array<Query>  $adapterFilterQueries
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     */
    private function boundedPage(
        Document $collection,
        array $queries,
        array $adapterFilterQueries,
        array $joinTablePrefixes,
        array $orderAttributes,
        array $orderTypes,
        ?int $limit,
        ?int $offset,
        array $cursor,
    ): ?BoundedPage {
        if ($limit === null) {
            return null;
        }

        $reach = (int) $offset + ($cursor === [] ? 0 : 1);
        if ($limit > PHP_INT_MAX - $reach) {
            return null;
        }

        $joinAliases = \array_column($joinTablePrefixes, 'alias');
        $mainOrder = $this->mainOrderPrefix($orderAttributes, $orderTypes, $joinAliases);
        if ($mainOrder === null) {
            return null;
        }

        $conditions = [];
        $searches = [];
        foreach ($queries as $query) {
            $method = $query->getMethod();
            if ($method === Method::Select) {
                continue;
            }

            if ($method->isJoin()) {
                if ($method !== Method::LeftJoin) {
                    return null;
                }

                continue;
            }

            if ($this->isMainSearch($query, $joinAliases)) {
                $searches[] = $query;
            } elseif (! $this->isMainRowCondition($query, $joinAliases)) {
                return null;
            }

            $conditions[] = clone $query;
        }

        $adapterConditions = [];
        foreach ($adapterFilterQueries as $query) {
            if (! $this->isMainSearch($query, $joinAliases)) {
                return null;
            }

            $adapterConditions[] = clone $query;
            $searches[] = $query;
        }

        $this->remapDottedQueryAttributes($conditions, $joinTablePrefixes, $collection);

        [$mainAttributes, $mainTypes] = $mainOrder;

        return new BoundedPage(
            orderAttributes: $mainAttributes,
            orderTypes: $mainTypes,
            rows: $limit + $reach,
            conditions: $conditions,
            adapterConditions: $adapterConditions,
            searches: $searches,
        );
    }

    /**
     * The read joins from a derived table of the main rows its page can come from (an index can serve its order up
     * to its limit), so the join and its sort only see their rows. The derived table holds every main row the page
     * needs, so the rows, their order and the page are those of the read without it. The searches run there only: a
     * derived table has no fulltext index.
     *
     * @param  array<string, mixed>  $cursor
     * @param  callable(string): string  $resolveInternalKey
     * @param  array<string>  $roles
     */
    private function joinFromBoundedPage(
        SQLBuilder $builder,
        Document $collection,
        BoundedPage $bound,
        array $cursor,
        CursorDirection $cursorDirection,
        callable $resolveInternalKey,
        string $name,
        string $alias,
        array $roles,
        PermissionType $forPermission,
    ): void {
        $page = $this->newBuilder($name, $alias);
        $page->select([$alias.'.*']);
        $page->filter($bound->conditions);
        $this->applyFilters($page, $bound->adapterConditions, $name, $alias);

        if (
            $this->authorization->getStatus()
            && $collection->getAttribute(Database::COLLECTION_GRANTED, false) !== true
            && $this->filtersPerDocument($collection)
        ) {
            $page->addHook($this->newPermissionHook($name, $roles, $forPermission->value, $alias.'.'.Storage::UID));
        }

        if ($cursor !== []) {
            $conditions = $this->cursorConditions($bound->orderAttributes, $bound->orderTypes, $cursor, $cursorDirection, $resolveInternalKey, nullable: true);
            $equalities = [];
            foreach ($bound->orderAttributes as $attribute) {
                $equalities[] = $this->cursorEquality($attribute, $cursor, $resolveInternalKey, nullable: true);
            }
            $conditions[] = $this->allOf($equalities);
            $page->filter([$this->anyOf($conditions)]);
        }

        $this->applyFindPage($page, $bound->orderAttributes, $bound->orderTypes, $bound->rows, null, $cursorDirection);

        $builder->fromSub($page, $alias);
    }

    /**
     * @param  array<string>  $joinAliases
     */
    private function isMainSearch(BaseQuery $query, array $joinAliases): bool
    {
        $method = $query->getMethod();

        return ($method === Method::Search || $method === Method::NotSearch)
            && $this->joinAliasOf($query->getAttribute(), $joinAliases) === null;
    }

    /**
     * The leading main attributes of an order, when they hold a unique one and joined attributes follow them: the
     * order a read returns the joined rows of one main document together in.
     *
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string>  $joinAliases
     * @return array{non-empty-list<string>, list<OrderDirection>}|null
     */
    private function mainOrderPrefix(array $orderAttributes, array $orderTypes, array $joinAliases): ?array
    {
        $attributes = [];
        $types = [];
        $unique = false;
        $joined = false;
        foreach (\array_values($orderAttributes) as $position => $attribute) {
            $type = $orderTypes[$position] ?? OrderDirection::Asc;
            if ($type === OrderDirection::Random) {
                return null;
            }
            if ($this->joinAliasOf($attribute, $joinAliases) !== null) {
                $joined = true;
                break;
            }
            $attributes[] = $attribute;
            $types[] = $type;
            $unique = $unique || $attribute === Document::SEQUENCE || $attribute === Document::ID;
        }

        if (! $unique || ! $joined || $attributes === []) {
            return null;
        }

        return [$attributes, $types];
    }

    /**
     * Whether a condition reads only main attributes, so it keeps or drops a main document with all its joined rows.
     *
     * @param  array<string>  $joinAliases
     */
    private function isMainRowCondition(BaseQuery $query, array $joinAliases): bool
    {
        $method = $query->getMethod();
        if (
            $method === Method::Search
            || $method === Method::NotSearch
            || (! $method->isFilter()
            && ! $method->isSpatial()
            && ! $method->isJson()
            && ! \in_array($method, self::ROW_CONDITION_GROUPS, true))
        ) {
            return false;
        }

        if ($this->joinAliasOf($query->getAttribute(), $joinAliases) !== null) {
            return false;
        }

        foreach ($query->getValues() as $value) {
            if ($value instanceof BaseQuery && ! $this->isMainRowCondition($value, $joinAliases)) {
                return false;
            }
            if (
                \is_string($value)
                && \in_array($method, [Method::Exists, Method::NotExists], true)
                && $this->joinAliasOf($value, $joinAliases) !== null
            ) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param  array<string>  $joinAliases
     */
    private function joinAliasOf(string $attribute, array $joinAliases): ?string
    {
        $dot = \strpos($attribute, '.');
        if ($dot === false) {
            return null;
        }

        $prefix = \substr($attribute, 0, $dot);

        return \in_array($prefix, $joinAliases, true) ? $prefix : null;
    }

    /**
     * The rows after a cursor value in one order position, or null when no row can follow it there: a null the
     * direction sorts last is followed only by rows tied on it, which a later position decides.
     *
     * @param  bool|float|int|string|null  $value
     */
    private function nullableCursorComparison(string $attribute, mixed $value, OrderDirection $direction): ?BaseQuery
    {
        $nullsFirst = $direction === $this->getNullOrder();

        if ($value === null) {
            return $nullsFirst ? BaseQuery::isNotNull($attribute) : null;
        }

        $comparison = $direction === OrderDirection::Desc
            ? BaseQuery::lessThan($attribute, $value)
            : BaseQuery::greaterThan($attribute, $value);

        return $nullsFirst ? $comparison : BaseQuery::or([$comparison, BaseQuery::isNull($attribute)]);
    }

    /**
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string>  $joinAliases
     */
    private function applyFindPage(
        SQLBuilder $builder,
        array $orderAttributes,
        array $orderTypes,
        ?int $limit,
        ?int $offset,
        CursorDirection $cursorDirection = CursorDirection::After,
        bool $afterUnion = false,
        array $joinAliases = [],
    ): void {
        if ($limit === null && $offset !== null) {
            $limit = self::UNBOUNDED_LIMIT;
        }

        if ($afterUnion) {
            $quote = $this->getIdentifierQuote();
            $builder->afterBuild(function (Statement $result) use (
                $orderAttributes,
                $orderTypes,
                $limit,
                $offset,
                $cursorDirection,
                $quote,
            ): Statement {
                $sql = $result->query;
                $bindings = $result->bindings;

                $orderParts = [];
                foreach (\array_keys($orderAttributes) as $i) {
                    $orderType = $orderTypes[$i] ?? OrderDirection::Asc;
                    if ($orderType === OrderDirection::Random) {
                        $orderParts[] = $this->createBuilder()->compileOrder(BaseQuery::orderRandom());
                        $sql = 'SELECT * FROM ('.$result->query.') AS '.$quote.self::FOJ_ROWS_ALIAS.$quote;

                        continue;
                    }

                    $direction = $orderType;
                    if ($cursorDirection === CursorDirection::Before) {
                        $direction = ($direction === OrderDirection::Asc)
                            ? OrderDirection::Desc
                            : OrderDirection::Asc;
                    }

                    $orderParts[] = $quote.self::FOJ_ORDER_ALIAS_PREFIX.$i.$quote.($direction === OrderDirection::Desc ? ' DESC' : ' ASC');
                }

                if ($orderParts !== []) {
                    $sql .= ' ORDER BY '.\implode(', ', $orderParts);
                }
                if (! \is_null($limit)) {
                    $sql .= ' LIMIT ?';
                    $bindings[] = $limit;
                }
                if (! \is_null($offset)) {
                    $sql .= ' OFFSET ?';
                    $bindings[] = $offset;
                }

                return new Statement($sql, $bindings, $result->readOnly);
            });

            return;
        }

        foreach ($orderAttributes as $i => $originalAttribute) {
            $orderType = $orderTypes[$i] ?? OrderDirection::Asc;

            if ($orderType === OrderDirection::Random) {
                $builder->sortRandom();

                continue;
            }

            $internalAttr = $this->qualifyOrderAttribute($originalAttribute, $joinAliases);
            $direction = $orderType;

            if ($cursorDirection === CursorDirection::Before) {
                $direction = ($direction === OrderDirection::Asc)
                    ? OrderDirection::Desc
                    : OrderDirection::Asc;
            }

            if ($direction === OrderDirection::Desc) {
                $builder->sortDesc($internalAttr);
            } else {
                $builder->sortAsc($internalAttr);
            }
        }

        if (! \is_null($limit)) {
            $builder->limit($limit);
        }
        if (! \is_null($offset)) {
            $builder->offset($offset);
        }
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function executeSelect(SQLBuilder $builder, Event $event, string $collection = ''): array
    {
        try {
            $result = $builder->build();
        } catch (ValidationException|UnsupportedException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }

        return $this->runSelect($result, $event, $collection);
    }

    /**
     * @return array<int, array<string, mixed>>
     *
     * @throws Exception
     */
    private function runSelect(Statement $result, Event $event, string $collection): array
    {
        $statement = null;
        $results = [];
        $exception = null;
        try {
            $statement = $this->executeResult($result, $event, $collection);
            $this->execute($statement);
            /** @var array<int, array<string, mixed>> $results */
            $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            $exception = $e;
        } finally {
            if ($statement !== null) {
                try {
                    $statement->closeCursor();
                } catch (PDOException $e) {
                    $exception ??= $e;
                }
            }
        }

        if ($exception !== null) {
            throw $this->processSelectException($exception, $result);
        }

        return $results;
    }

    private function qualifyJoinColumn(string $column, string $defaultAlias): string
    {
        $dot = \strpos($column, '.');
        if ($dot === false) {
            return $defaultAlias.'.'.$this->getInternalKeyForAttribute($column);
        }

        $prefix = \substr($column, 0, $dot);
        $name = \substr($column, $dot + 1);

        return $prefix.'.'.$this->getInternalKeyForAttribute($name);
    }

    /**
     * An order names a bare attribute the main collection does not declare by the one join whose
     * collection declares it, as an aggregate or a group does, and the cursor value under that name
     * follows it. A name several joins declare is refused rather than read from one of them.
     *
     * @param  array<string>  $orderAttributes
     * @param  array<string, mixed>  $cursor
     * @param  list<JoinAlias>  $joinTablePrefixes
     * @return array{array<string>, array<string, mixed>}
     *
     * @throws QueryException
     */
    private function qualifyJoinedOrders(array $orderAttributes, array $cursor, Document $collection, array $joinTablePrefixes): array
    {
        $main = [];
        foreach ([...Database::internalAttributesFor(true), ...self::collectionAttributes($collection)] as $attribute) {
            $main[$attribute->key] = true;
        }

        $joinAttributes = $collection->getAttribute(Database::JOIN_ATTRIBUTES, []);
        $declared = [];
        foreach ($joinTablePrefixes as $join) {
            $keys = \is_array($joinAttributes) ? ($joinAttributes[$join->table] ?? []) : [];
            foreach (\is_array($keys) ? $keys : [] as $key) {
                if (\is_string($key)) {
                    $declared[$key][] = $join->alias;
                }
            }
        }

        foreach ($orderAttributes as $index => $attribute) {
            if (\str_contains($attribute, '.') || isset($main[$attribute]) || ! isset($declared[$attribute])) {
                continue;
            }

            $aliases = \array_values(\array_unique($declared[$attribute]));
            if (\count($aliases) > 1) {
                throw new QueryException('Attribute "'.$attribute.'" is ambiguous across joins; qualify it with a join alias');
            }

            $qualified = $aliases[0].'.'.$attribute;
            $orderAttributes[$index] = $qualified;
            if (\array_key_exists($attribute, $cursor) && ! \array_key_exists($qualified, $cursor)) {
                $cursor[$qualified] = $cursor[$attribute];
            }
        }

        return [$orderAttributes, $cursor];
    }

    /**
     * @param  array<string>  $joinAliases
     */
    private function qualifyOrderAttribute(string $attribute, array $joinAliases = []): string
    {
        $dot = \strpos($attribute, '.');
        if ($dot !== false) {
            $prefix = \substr($attribute, 0, $dot);
            if (\in_array($prefix, $joinAliases, true)) {
                $name = \substr($attribute, $dot + 1);

                return $this->filter($prefix).'.'.$this->filter($this->getInternalKeyForAttribute($name));
            }
        }

        return $this->filter($this->getInternalKeyForAttribute($attribute));
    }

    /**
     * @param array<int|string, mixed> $row
     */
    private function remapRow(array &$row): void
    {
        foreach (\array_keys($row) as $key) {
            if (\is_int($key)) {
                unset($row[$key]);
                continue;
            }
            if (\str_starts_with($key, self::FOJ_ORDER_ALIAS_PREFIX)) {
                unset($row[$key]);

                continue;
            }
            if (! \str_contains($key, '.')) {
                continue;
            }
            $separator = \strrpos($key, '.');
            if (! \is_int($separator)) {
                continue;
            }
            $prefix = \substr($key, 0, $separator);
            $bare = \trim(\substr($key, $separator + 1), '`"');
            $public = Storage::attribute($bare);
            $dotted = $prefix.'.'.$public;

            if ($prefix === Query::DEFAULT_ALIAS && $bare !== '' && ! \array_key_exists($bare, $row)) {
                $row[$bare] = $row[$key];
            }

            $value = $row[$key];
            if ($value !== null && ($bare === Storage::PERMISSIONS || $public === Document::PERMISSIONS)) {
                $value = \json_decode(\is_string($value) ? $value : '[]', true);
            }
            if (! \array_key_exists($dotted, $row) || $key === $dotted) {
                $row[$dotted] = $value;
            }
            if ($key !== $dotted) {
                unset($row[$key]);
            }
        }

        foreach (Storage::columnMap() as $internal => $public) {
            if ($internal === Storage::PERMISSIONS || $internal === Storage::DISTANCE) {
                continue;
            }
            if (\array_key_exists($internal, $row)) {
                $row[$public] = $row[$internal];
                unset($row[$internal]);
            }
        }
        if (\array_key_exists(Storage::PERMISSIONS, $row)) {
            $row[Document::PERMISSIONS] = \json_decode(\is_string($row[Storage::PERMISSIONS]) ? $row[Storage::PERMISSIONS] : '[]', true);
            unset($row[Storage::PERMISSIONS]);
        }
        if (\array_key_exists(Storage::DISTANCE, $row)) {
            $distance = $row[Storage::DISTANCE];
            $row[Document::DISTANCE] = \is_numeric($distance) ? (float) $distance : null;
            unset($row[Storage::DISTANCE]);
        }
    }

    /**
     * Converts internal attributes ($id, $createdAt, etc.) to their column names
     * and encodes arrays as JSON. Spatial attributes are included with their raw
     * value (the caller must handle ST_GeomFromText wrapping separately).
     *
     * @param  list<string>  $attributeKeys
     * @param  array<string, true>  $spatialMap  Pre-built lookup map; the caller
     *         hoists this out of the per-document loop so we don't allocate it
     *         per row in batch inserts.
     * @return array<string, mixed>
     */
    protected function buildDocumentRow(Document $document, array $attributeKeys, array $spatialMap = [], ?bool $intBools = null): array
    {
        $attributes = $document->getAttributes();
        $row = [
            Storage::UID => $document->getId(),
            Storage::CREATED_AT => $document->getCreatedAt(),
            Storage::UPDATED_AT => $document->getUpdatedAt(),
            Storage::PERMISSIONS => \json_encode($document->getPermissions()),
        ];

        if (! empty($document->getSequence())) {
            $row[Storage::SEQUENCE] = $document->getSequence();
        }

        $intBools ??= $this->supports(Capability::IntegerBooleans);

        foreach ($attributeKeys as $key) {
            if (isset($row[$key])) {
                continue;
            }
            $value = $attributes[$key] ?? null;
            if (isset($spatialMap[$key])) {
                $value = $this->encodeSpatialWriteValue($value);
            } elseif (\is_array($value)) {
                $value = \json_encode($value);
            }
            if ($intBools && ! isset($spatialMap[$key])) {
                $value = (\is_bool($value)) ? (int) $value : $value;
            }
            $row[$key] = $value;
        }

        return $row;
    }

    /**
     * @return list<string>
     */
    protected function getSpatialAttributes(Document $collection): array
    {
        $spatialAttributes = [];
        foreach (self::collectionAttributes($collection) as $attribute) {
            if ($attribute->isSpatial()) {
                $spatialAttributes[] = $attribute->key;
            }
        }

        return $spatialAttributes;
    }

    protected function encodeSpatialWriteValue(mixed $value): mixed
    {
        if (\is_array($value)) {
            return $this->convertArrayToWkt($value);
        }

        return $value;
    }

    /**
     * @return string|null Returns null if operator can't be expressed in SQL
     */
    abstract protected function getOperatorSql(string $column, Operator $operator, int &$bindIndex): ?string;

    protected function bindOperatorParameters(PDOStatement|DatabasePDOStatement|PDOStatementProxy $statement, Operator $operator, int &$bindIndex): void
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            case OperatorType::Increment:
            case OperatorType::Decrement:
            case OperatorType::Multiply:
            case OperatorType::Divide:
                $value = $values[0] ?? 1;
                $bindKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$bindKey, $value, $this->getPdoType($value));
                $bindIndex++;

                if (isset($values[1])) {
                    $limitKey = "op_{$bindIndex}";
                    $limit = self::exactLimit($values[1]);
                    $statement->bindValue(':'.$limitKey, $limit, $this->getPdoType($limit));
                    $bindIndex++;
                }
                break;

            case OperatorType::Modulo:
                $value = $values[0] ?? 1;
                $bindKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$bindKey, $value, $this->getPdoType($value));
                $bindIndex++;
                break;

            case OperatorType::Power:
                $value = $values[0] ?? 1;
                $bindKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$bindKey, $value, $this->getPdoType($value));
                $bindIndex++;

                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $limit = self::exactLimit($values[1]);
                    $statement->bindValue(':'.$maxKey, $limit, $this->getPdoType($limit));
                    $bindIndex++;
                }
                break;

            case OperatorType::StringConcat:
                $value = $values[0] ?? '';
                $bindKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$bindKey, $value, PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::StringReplace:
                $search = $values[0] ?? '';
                $replace = $values[1] ?? '';
                $searchKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$searchKey, $search, PDO::PARAM_STR);
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$replaceKey, $replace, PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::DateAddDays:
            case OperatorType::DateSubDays:
                $days = $values[0] ?? 0;
                $bindKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$bindKey, $days, PDO::PARAM_INT);
                $bindIndex++;
                break;

            case OperatorType::ArrayAppend:
            case OperatorType::ArrayPrepend:
                if (\count($values) > Operator::MAX_ARRAY_OPERATOR_SIZE) {
                    throw new DatabaseException('Array size '.\count($values).' exceeds maximum allowed size of '.Operator::MAX_ARRAY_OPERATOR_SIZE.' for array operations');
                }

                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$bindKey, $arrayValue, PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayRemove:
                $value = $values[0] ?? null;
                $bindKey = "op_{$bindIndex}";
                if (is_array($value)) {
                    $value = json_encode($value);
                }
                $statement->bindValue(':'.$bindKey, $value, $this->getPdoType($value));
                $bindIndex++;
                break;

            case OperatorType::ArrayInsert:
                $index = $values[0] ?? 0;
                $value = $values[1] ?? null;
                $indexKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$indexKey, $index, PDO::PARAM_INT);
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$valueKey, json_encode($value), PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayIntersect:
            case OperatorType::ArrayDiff:
                if (\count($values) > Operator::MAX_ARRAY_OPERATOR_SIZE) {
                    throw new DatabaseException('Array size '.\count($values).' exceeds maximum allowed size of '.Operator::MAX_ARRAY_OPERATOR_SIZE.' for array operations');
                }

                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $statement->bindValue(':'.$bindKey, $arrayValue, PDO::PARAM_STR);
                $bindIndex++;
                break;
        }
    }

    /**
     * Get the operator expression and positional bindings for use with the query builder's setRaw().
     *
     * Calls getOperatorSql() to get the expression with named bindings, strips the
     * column assignment prefix, and converts named :op_N bindings to positional ? placeholders.
     *
     * @param  string  $column  The unquoted column name
     *
     * @throws DatabaseException
     */
    protected function getOperatorBuilderExpression(string $column, Operator $operator): Expression
    {
        $bindIndex = 0;
        $fullExpression = $this->getOperatorSql($column, $operator, $bindIndex);

        if ($fullExpression === null) {
            throw new DatabaseException('Operator cannot be expressed in SQL: '.$operator->getMethod()->value);
        }

        $quotedColumn = $this->quote($column);
        $prefix = $quotedColumn.' = ';
        $expression = $fullExpression;
        if (str_starts_with($expression, $prefix)) {
            $expression = substr($expression, strlen($prefix));
        }

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
                $namedBindings["op_{$idx}"] = is_array($value) ? json_encode($value) : $value;
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

        // Process longest keys first to avoid partial replacement (e.g., :op_10 vs :op_1)
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

        // Replace from right to left to preserve positions
        $result = $expression;
        for ($i = count($replacements) - 1; $i >= 0; $i--) {
            $r = $replacements[$i];
            $result = substr_replace($result, '?', $r['pos'], $r['len']);
        }

        foreach ($replacements as $r) {
            $positionalBindings[] = $namedBindings[$r['key']];
        }

        return new Expression($result, $positionalBindings);
    }

    /**
     * By default this delegates to getOperatorBuilderExpression(). Adapters
     * that need to reference the existing row differently in upsert context
     * (e.g. Postgres using target.col) should override this method.
     *
     * @param  string  $column  The unquoted, filtered column name
     */
    protected function getOperatorUpsertExpression(string $column, Operator $operator): Expression
    {
        return $this->getOperatorBuilderExpression($column, $operator);
    }

    /**
     * Apply an operator to a value (used for new documents with only operators).
     * This method applies the operator logic in PHP to compute what the SQL would compute.
     *
     * @param  mixed  $value  The current value (typically the attribute default)
     */
    protected function applyOperatorToValue(Operator $operator, mixed $value): mixed
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();
        $exact = BigInt::calculateOutsideNative($method, $value ?? 0, $values[0] ?? 1);
        if ($exact !== null) {
            $bound = self::exactLimit($values[1] ?? null);
            if (BigInt::isIntegerValue($bound)) {
                $upper = \in_array($method, [OperatorType::Increment, OperatorType::Multiply, OperatorType::Power], true);
                if (($upper && BigInt::compare($exact, $bound) > 0)
                    || (! $upper && $method !== OperatorType::Modulo && BigInt::compare($exact, $bound) < 0)) {
                    return BigInt::toNative($value ?? 0);
                }
            }

            return $exact;
        }

        $numVal = is_numeric($value) ? $value + 0 : 0;
        $firstValue = count($values) > 0 ? $values[0] : null;
        $numOp = is_numeric($firstValue) ? $firstValue + 0 : 1;
        /** @var array<mixed> $arrVal */
        $arrVal = is_array($value) ? $value : [];

        $result = match ($method) {
            OperatorType::Increment => $numVal + $numOp,
            OperatorType::Decrement => $numVal - $numOp,
            OperatorType::Multiply => $numVal * $numOp,
            OperatorType::Divide => $numOp != 0 ? $numVal / $numOp : $numVal,
            OperatorType::Modulo => $numOp != 0 ? (int) $numVal % (int) $numOp : (int) $numVal,
            OperatorType::Power => pow($numVal, $numOp),
            OperatorType::ArrayAppend => array_merge($arrVal, $values),
            OperatorType::ArrayPrepend => array_merge($values, $arrVal),
            OperatorType::ArrayInsert => (function () use ($arrVal, $values) {
                $arr = $arrVal;
                $insertIdxRaw = count($values) > 0 ? $values[0] : 0;
                $insertIdx = \is_numeric($insertIdxRaw) ? (int) $insertIdxRaw : 0;
                array_splice($arr, $insertIdx, 0, [count($values) > 1 ? $values[1] : null]);

                return $arr;
            })(),
            OperatorType::ArrayRemove => (function () use ($arrVal, $values) {
                $arr = self::stringifyList($arrVal);
                $toRemove = $values[0] ?? null;
                $remove = \is_array($toRemove) ? self::stringifyList($toRemove) : [self::stringify($toRemove)];

                return array_values(array_diff($arr, $remove));
            })(),
            OperatorType::ArrayUnique => array_values(array_unique(self::stringifyList($arrVal))),
            OperatorType::ArrayIntersect => array_values(array_intersect(self::stringifyList($arrVal), self::stringifyList($values))),
            OperatorType::ArrayDiff => array_values(array_diff(self::stringifyList($arrVal), self::stringifyList($values))),
            OperatorType::ArrayFilter => self::filterArray($arrVal, $values[0] ?? null, $values[1] ?? null),
            OperatorType::StringConcat => (\is_scalar($value) ? (string) $value : '') . (count($values) > 0 && \is_scalar($values[0]) ? (string) $values[0] : ''),
            OperatorType::StringReplace => str_replace(count($values) > 0 && \is_scalar($values[0]) ? (string) $values[0] : '', count($values) > 1 && \is_scalar($values[1]) ? (string) $values[1] : '', \is_scalar($value) ? (string) $value : ''),
            OperatorType::Toggle => ! ($value ?? false),
            OperatorType::DateAddDays => self::shiftDays($value, \is_numeric($firstValue) ? (int) $firstValue : 0),
            OperatorType::DateSubDays => self::shiftDays($value, \is_numeric($firstValue) ? -(int) $firstValue : 0),
            OperatorType::DateSetNow => DateTime::now(),
        };

        return self::keepWithinBound($method, $numVal, $result, $values[1] ?? null);
    }

    protected static function exactLimit(mixed $limit): mixed
    {
        if (! \is_float($limit) || ! \is_finite($limit)) {
            return $limit;
        }

        return BigInt::integralValue($limit) ?? $limit;
    }

    private static function keepWithinBound(OperatorType $method, int|float $current, mixed $result, mixed $bound): mixed
    {
        if (! \is_numeric($bound) || (! \is_int($result) && ! \is_float($result))) {
            return $result;
        }

        $limit = \is_float($bound) && \is_finite($bound) ? (BigInt::integralValue($bound) ?? $bound) : $bound;
        $comparison = \is_int($result) && BigInt::isIntegerValue($limit)
            ? BigInt::compare($result, $limit)
            : $result <=> (\is_string($limit) ? (float) $limit : $limit);

        $crossed = match ($method) {
            OperatorType::Increment, OperatorType::Multiply, OperatorType::Power => \is_nan((float) $result) || $comparison > 0,
            OperatorType::Decrement, OperatorType::Divide => $comparison < 0,
            default => false,
        };

        return $crossed ? $current : $result;
    }

    /**
     * @param  array<mixed>  $items
     * @return list<mixed>
     */
    private static function filterArray(array $items, mixed $condition, mixed $compare): array
    {
        return \array_values(\array_filter($items, static fn (mixed $item): bool => match ($condition) {
            Method::Equal->value => $item == $compare,
            Method::NotEqual->value => $item != $compare,
            Method::GreaterThan->value => \is_numeric($compare) && \is_numeric($item) && $item + 0 > $compare + 0,
            Method::GreaterThanEqual->value => \is_numeric($compare) && \is_numeric($item) && $item + 0 >= $compare + 0,
            Method::LessThan->value => \is_numeric($compare) && \is_numeric($item) && $item + 0 < $compare + 0,
            Method::LessThanEqual->value => \is_numeric($compare) && \is_numeric($item) && $item + 0 <= $compare + 0,
            Method::IsNull->value => $item === null,
            Method::IsNotNull->value => $item !== null,
            default => true,
        }));
    }

    private static function shiftDays(mixed $value, int $days): mixed
    {
        if (! \is_string($value) || $value === '') {
            return $value;
        }

        try {
            $date = new \DateTime($value);
        } catch (Throwable) {
            return $value;
        }

        $date->setTimezone(new \DateTimeZone(\date_default_timezone_get()));
        $date->modify(\sprintf('%+d days', $days));

        return DateTime::format($date);
    }

    /**
     * @param  array<mixed>  $values
     * @return list<string>
     */
    private static function stringifyList(array $values): array
    {
        $out = [];
        foreach ($values as $value) {
            $out[] = self::stringify($value);
        }

        return $out;
    }

    private static function stringify(mixed $value): string
    {
        if (\is_string($value)) {
            return $value;
        }
        if (\is_scalar($value) || $value === null) {
            return (string) $value;
        }

        return \get_debug_type($value);
    }

    protected function quote(string $string): string
    {
        return '`'.\str_replace('`', '``', $string).'`';
    }

    /**
     * Whether the adapter requires an alias on INSERT for conflict resolution.
     *
     * PostgreSQL needs INSERT INTO table AS target so that the ON CONFLICT
     * clause can reference the existing row via target.column. MariaDB does
     * not need this because it uses VALUES(column) syntax.
     */
    protected function insertRequiresAlias(): bool
    {
        return false;
    }

    /**
     * Get the conflict-resolution expression for a regular column in shared-tables mode.
     *
     * The returned expression is used as the RHS of "col = <expression>" in the
     * ON CONFLICT / ON DUPLICATE KEY UPDATE clause. It must conditionally update
     * the column only when the tenant matches.
     *
     * @param  string  $column  The unquoted column name
     * @return string The raw SQL expression (with positional ? placeholders if needed)
     */
    abstract protected function getConflictTenantExpression(string $column): string;

    /**
     * Get the conflict-resolution expression for an increment column.
     *
     * Returns the RHS expression that adds the incoming value to the existing
     * column value (e.g. col + VALUES(col) for MariaDB, target.col + EXCLUDED.col
     * for Postgres).
     *
     * @param  string  $column  The unquoted column name
     * @return string The raw SQL expression
     */
    abstract protected function getConflictIncrementExpression(string $column): string;

    /**
     * Get the conflict-resolution expression for an increment column in shared-tables mode.
     *
     * Like getConflictTenantExpression but the "new value" is the existing column
     * value plus the incoming value.
     *
     * @param  string  $column  The unquoted column name
     * @return string The raw SQL expression
     */
    abstract protected function getConflictTenantIncrementExpression(string $column): string;

    /**
     * @throws Exception
     */
    protected function getPdoType(mixed $value): int
    {
        return match (gettype($value)) {
            'string', 'double' => \PDO::PARAM_STR,
            'integer', 'boolean' => \PDO::PARAM_INT,
            'NULL' => \PDO::PARAM_NULL,
            default => throw new DatabaseException('Unknown PDO Type for ' . \gettype($value)),
        };
    }

    /**
     * The direction in which this engine sorts null before every other value.
     */
    protected function getNullOrder(): OrderDirection
    {
        return OrderDirection::Asc;
    }

    /**
     * Get vector distance ORDER BY expression with positional bindings.
     *
     * Returns null when vectors are unsupported. Subclasses that support vectors
     * should override this to return the expression string with `?` placeholders
     * and the matching binding values.
     *
     */
    protected function getVectorOrderRaw(Query $query, string $alias): ?Expression
    {
        return null;
    }

    /**
     * @param list<string> $orderAttributes
     * @param list<OrderDirection> $orderTypes
     * @param array<string, mixed> $cursor
     * @param callable(string): string $resolveInternalKey
     */
    private function getVectorCursorCondition(
        Expression $vector,
        float $distance,
        array $orderAttributes,
        array $orderTypes,
        array $cursor,
        CursorDirection $cursorDirection,
        string $alias,
        callable $resolveInternalKey,
        bool $nullable = false,
    ): Expression {
        $distance = \json_encode($distance, JSON_THROW_ON_ERROR);
        $distanceOperator = $cursorDirection === CursorDirection::Before ? '<' : '>';
        $clauses = ["({$vector->sql}) {$distanceOperator} ?"];
        $bindings = [];
        \array_push($bindings, ...$vector->bindings);
        $bindings[] = $distance;

        foreach ($orderAttributes as $index => $attribute) {
            if (! \array_key_exists($attribute, $cursor)) {
                throw new QueryException("Vector cursor is missing order attribute '{$attribute}'");
            }

            $parts = ["({$vector->sql}) = ?"];
            $clauseBindings = [];
            \array_push($clauseBindings, ...$vector->bindings);
            $clauseBindings[] = $distance;

            for ($previous = 0; $previous < $index; $previous++) {
                $previousAttribute = $orderAttributes[$previous];
                if (! \array_key_exists($previousAttribute, $cursor)) {
                    throw new QueryException("Vector cursor is missing order attribute '{$previousAttribute}'");
                }

                $previousColumn = $this->quoteOrderColumn($resolveInternalKey($previousAttribute), $alias);
                if ($nullable && $cursor[$previousAttribute] === null) {
                    $parts[] = "{$previousColumn} IS NULL";

                    continue;
                }
                $parts[] = "{$previousColumn} = ?";
                $clauseBindings[] = $cursor[$previousAttribute];
            }

            $direction = $orderTypes[$index] ?? OrderDirection::Asc;
            if ($cursorDirection === CursorDirection::Before) {
                $direction = $direction === OrderDirection::Asc
                    ? OrderDirection::Desc
                    : OrderDirection::Asc;
            }
            $operator = $direction === OrderDirection::Desc ? '<' : '>';
            $column = $this->quoteOrderColumn($resolveInternalKey($attribute), $alias);
            if ($nullable && $cursor[$attribute] === null) {
                if ($direction !== $this->getNullOrder()) {
                    continue;
                }
                $parts[] = "{$column} IS NOT NULL";
            } elseif ($nullable && $direction !== $this->getNullOrder()) {
                $parts[] = "COALESCE({$column} {$operator} ?, TRUE)";
                $clauseBindings[] = $cursor[$attribute];
            } else {
                $parts[] = "{$column} {$operator} ?";
                $clauseBindings[] = $cursor[$attribute];
            }
            $clauses[] = '('.\implode(' AND ', $parts).')';
            \array_push($bindings, ...$clauseBindings);
        }

        return new Expression(
            '('.\implode(' OR ', $clauses).')',
            $bindings,
        );
    }

    /**
     * Render a vector distance expression in a form safe to hydrate as a PHP float.
     */
    protected function getSqlReadableDistance(string $distance): string
    {
        return $distance;
    }

    #[\Override]
    protected function escapeWildcards(string $value): string
    {
        $wildcards = ['\\', '%', '_', '[', ']', '^', '-', '.', '*', '+', '?', '(', ')', '{', '}', '|'];

        foreach ($wildcards as $wildcard) {
            $value = \str_replace($wildcard, "\\$wildcard", $value);
        }

        return $value;
    }

    protected function processException(PDOException $e): Exception
    {
        return $e;
    }

    /**
     * A driver error the adapter maps to a lock conflict is transient too, even when it reached the transaction
     * without being mapped.
     */
    #[\Override]
    protected function isTransient(Throwable $error): bool
    {
        return parent::isTransient($error)
            || ($error instanceof PDOException && $this->processException($error) instanceof ContentionException);
    }

    protected function processSelectException(PDOException $e, Statement $statement): Exception
    {
        return $this->processException($e);
    }

    /**
     * Quote a search attribute, keeping join-qualified paths on the join alias.
     *
     * @return array{0: string, 1: string}
     */
    protected function quoteSearchAttribute(string $attribute, string $alias): array
    {
        $dot = \strpos($attribute, '.');
        if ($dot !== false) {
            $prefix = \substr($attribute, 0, $dot);
            $name = \substr($attribute, $dot + 1);

            return [
                $this->quote($this->filter($prefix)),
                $this->quote($this->filter($this->getInternalKeyForAttribute($name))),
            ];
        }

        return [
            $this->quote($alias),
            $this->quote($this->filter($this->getInternalKeyForAttribute($attribute))),
        ];
    }

    /**
     * Whether `$query` should bypass the upstream Builder pipeline and be
     * compiled by the adapter directly via {@see compileAdapterFilter()}.
     *
     * Used by adapters whose query semantics aren't expressible through the
     * Builder's typed methods — e.g. SQLite's FTS5 search needs an
     * `IN (SELECT rowid FROM <fts_table> ...)` subquery that requires the
     * collection name and metadata.
     */
    protected function isAdapterFilterQuery(Query $query): bool
    {
        return false;
    }

    /**
     * Compile an adapter-specific filter to a raw WHERE expression with
     * positional bindings. Called for queries flagged by
     * {@see isAdapterFilterQuery()}. Returning null skips emission.
     *
     * @param  list<JoinAlias>  $joins
     */
    protected function compileAdapterFilter(Query $query, string $collection, string $alias, array $joins = []): ?Expression
    {
        return null;
    }
}
