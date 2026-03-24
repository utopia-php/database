<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Swoole\Database\PDOStatementProxy;
use Throwable;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Hook\PermissionFilter;
use Utopia\Database\Hook\PermissionWrite;
use Utopia\Database\Hook\TenantFilter;
use Utopia\Database\Hook\TenantWrite;
use Utopia\Database\Hook\WriteContext;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\PDO as DatabasePDO;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\CursorDirection;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook\Attribute\Map as AttributeMap;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * Abstract base adapter for SQL-based database engines (MariaDB, MySQL, PostgreSQL, SQLite).
 */
abstract class SQL extends Adapter implements Feature\ConnectionId, Feature\Relationships, Feature\SchemaAttributes, Feature\Spatial, Feature\Upserts
{
    protected DatabasePDO $pdo;

    /**
     * Maximum array size for array operations to prevent memory exhaustion.
     * Large arrays in JSON_TABLE operations can cause significant memory usage.
     */
    protected const MAX_ARRAY_OPERATOR_SIZE = 10000;

    private const COLUMN_RENAME_MAP = [
        '_uid' => '$id',
        '_id' => '$sequence',
        '_tenant' => '$tenant',
        '_createdAt' => '$createdAt',
        '_updatedAt' => '$updatedAt',
        '_version' => '$version',
    ];

    /**
     * Controls how many fractional digits are used when binding float parameters.
     */
    protected int $floatPrecision = 17;

    /**
     * Constructor.
     *
     * Set connection and settings
     */
    public function __construct(DatabasePDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * Get the list of capabilities supported by SQL adapters.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        return array_merge(parent::capabilities(), [
            Capability::Schemas,
            Capability::BoundaryInclusive,
            Capability::Fulltext,
            Capability::MultipleFulltextIndexes,
            Capability::Regex,
            Capability::Casting,
            Capability::UpdateLock,
            Capability::BatchOperations,
            Capability::BatchCreateAttributes,
            Capability::TransactionRetries,
            Capability::NestedTransactions,
            Capability::QueryContains,
            Capability::Operators,
            Capability::OrderRandom,
            Capability::IdenticalIndexes,
            Capability::Reconnection,
            Capability::CacheSkipOnFailure,
            Capability::Hostname,
            Capability::AttributeResizing,
            Capability::DefinedAttributes,
            Capability::SchemaAttributes,
            Capability::Spatial,
            Capability::Relationships,
            Capability::Upserts,
            Capability::ConnectionId,
            Capability::Joins,
            Capability::Aggregations,
        ]);
    }

    /**
     * Returns the current PDO object
     */
    protected function getPDO(): DatabasePDO
    {
        return $this->pdo;
    }

    /**
     * Returns default PDO configuration
     *
     * @return array<int, mixed>
     */
    public static function getPDOAttributes(): array
    {
        return [
            PDO::ATTR_TIMEOUT => 3, // Specifies the timeout duration in seconds. Takes a value of type int.
            PDO::ATTR_PERSISTENT => true, // Create a persistent connection
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC, // Fetch a result row as an associative array.
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // PDO will throw a PDOException on errors
            PDO::ATTR_EMULATE_PREPARES => true, // Emulate prepared statements
            PDO::ATTR_STRINGIFY_FETCHES => true, // Returns all fetched data as Strings
        ];
    }

    /**
     * Configure float precision for parameter binding/logging.
     */
    public function setFloatPrecision(int $precision): void
    {
        $this->floatPrecision = $precision;
    }

    /**
     * Helper to format a float value according to configured precision for binding/logging.
     */
    protected function getFloatPrecision(float $value): string
    {
        return sprintf('%.'.$this->floatPrecision.'F', $value);
    }

    /**
     * Get the hostname of the database connection.
     *
     * @return string
     */
    public function getHostname(): string
    {
        try {
            return $this->pdo->getHostname();
        } catch (Throwable) {
            return '';
        }
    }

    /**
     * Get the internal ID attribute type used by SQL adapters.
     *
     * @return string
     */
    public function getIdAttributeType(): string
    {
        return ColumnType::Integer->value;
    }

    /**
     * Set whether the adapter supports attribute definitions. Always true for SQL.
     *
     * @param bool $support Whether to enable attribute support
     * @return bool
     */
    public function setSupportForAttributes(bool $support): bool
    {
        return true;
    }

    /**
     * Get the ALTER TABLE lock type clause for concurrent DDL operations.
     *
     * @return string
     */
    public function getLockType(): string
    {
        if ($this->supports(Capability::AlterLock) && $this->alterLocks) {
            return ',LOCK=SHARED';
        }

        return '';
    }

    /**
     * Ping Database
     *
     * @throws Exception
     * @throws PDOException
     */
    public function ping(): bool
    {
        $result = $this->createBuilder()->fromNone()->selectRaw('1')->build();

        return $this->getPDO()
            ->prepare($result->query)
            ->execute();
    }

    /**
     * Reconnect to the database and reset the transaction counter.
     *
     * @return void
     */
    public function reconnect(): void
    {
        $this->getPDO()->reconnect();
        $this->inTransaction = 0;
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

                $this->getPDO()->beginTransaction();

            } else {
                $this->getPDO()->exec('SAVEPOINT transaction'.$this->inTransaction);
            }
        } catch (PDOException $e) {
            throw new TransactionException('Failed to start transaction: '.$e->getMessage(), $e->getCode(), $e);
        }

        $this->inTransaction++;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        if (! $this->getPDO()->inTransaction()) {
            $this->inTransaction = 0;

            return false;
        }

        if ($this->inTransaction > 1) {
            $this->inTransaction--;

            return true;
        }

        try {
            $result = $this->getPDO()->commit();
            $this->inTransaction = 0;
        } catch (PDOException $e) {
            throw new TransactionException('Failed to commit transaction: '.$e->getMessage(), $e->getCode(), $e);
        }

        if (! $result) {
            throw new TransactionException('Failed to commit transaction');
        }

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
            } else {
                $this->getPDO()->rollBack();
                $this->inTransaction = 0;
            }
        } catch (PDOException $e) {
            $this->inTransaction = 0;
            throw new DatabaseException('Failed to rollback transaction: '.$e->getMessage(), $e->getCode(), $e);
        }

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

        if (! \is_null($collection)) {
            $collection = $this->filter($collection);
            $builder = $this->createBuilder();
            $result = $builder
                ->from('INFORMATION_SCHEMA.TABLES')
                ->selectRaw('TABLE_NAME')
                ->filter([
                    BaseQuery::equal('TABLE_SCHEMA', [$database]),
                    BaseQuery::equal('TABLE_NAME', ["{$this->getNamespace()}_{$collection}"]),
                ])
                ->build();
            $stmt = $this->getPDO()->prepare($result->query);
            foreach ($result->bindings as $i => $v) {
                $stmt->bindValue($i + 1, $v);
            }
        } else {
            $builder = $this->createBuilder();
            $result = $builder
                ->from('INFORMATION_SCHEMA.SCHEMATA')
                ->selectRaw('SCHEMA_NAME')
                ->filter([BaseQuery::equal('SCHEMA_NAME', [$database])])
                ->build();
            $stmt = $this->getPDO()->prepare($result->query);
            foreach ($result->bindings as $i => $v) {
                $stmt->bindValue($i + 1, $v);
            }
        }

        try {
            $stmt->execute();
            $document = $stmt->fetchAll();
            $stmt->closeCursor();
        } catch (PDOException $e) {
            $e = $this->processException($e);

            if ($e instanceof NotFoundException) {
                return false;
            }

            throw $e;
        }

        if (empty($document)) {
            return false;
        }

        return true;
    }

    /**
     * List Databases
     *
     * @return array<Document>
     */
    public function list(): array
    {
        return [];
    }

    /**
     * Create Attribute
     *
     * @throws Exception
     * @throws PDOException
     */
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        $schema = $this->createSchemaBuilder();
        $result = $schema->alter($this->getSQLTableRaw($collection), function (Blueprint $table) use ($attribute) {
            $this->addBlueprintColumn($table, $attribute->key, $attribute->type, $attribute->size, $attribute->signed, $attribute->array, $attribute->required);
        });

        $sql = $result->query;
        $lockType = $this->getLockType();
        if (! empty($lockType)) {
            $sql = rtrim($sql, ';').' '.$lockType;
        }

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Create Attributes
     *
     * @param  array<Attribute>  $attributes
     *
     * @throws DatabaseException
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        $schema = $this->createSchemaBuilder();
        $result = $schema->alter($this->getSQLTableRaw($collection), function (Blueprint $table) use ($attributes) {
            foreach ($attributes as $attribute) {
                $this->addBlueprintColumn(
                    $table,
                    $attribute->key,
                    $attribute->type,
                    $attribute->size,
                    $attribute->signed,
                    $attribute->array,
                    $attribute->required,
                );
            }
        });

        $sql = $result->query;
        $lockType = $this->getLockType();
        if (! empty($lockType)) {
            $sql = rtrim($sql, ';').' '.$lockType;
        }

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Delete Attribute
     *
     * @throws Exception
     * @throws PDOException
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $schema = $this->createSchemaBuilder();
        $result = $schema->alter($this->getSQLTableRaw($collection), function (Blueprint $table) use ($id) {
            $table->dropColumn($this->filter($id));
        });

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
     * Rename Attribute
     *
     * @throws Exception
     * @throws PDOException
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $schema = $this->createSchemaBuilder();
        $result = $schema->alter($this->getSQLTableRaw($collection), function (Blueprint $table) use ($old, $new) {
            $table->renameColumn($this->filter($old), $this->filter($new));
        });

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
     * Get Document
     *
     * @param  Query[]  $queries
     *
     * @throws DatabaseException
     */
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $collection = $collection->getId();

        $name = $this->filter($collection);
        $selections = $this->getAttributeSelections($queries);
        $alias = Query::DEFAULT_ALIAS;

        $builder = $this->newBuilder($name, $alias);

        if (! empty($selections) && ! \in_array('*', $selections)) {
            $builder->select($this->mapSelectionsToColumns($selections));
        }

        $builder->filter([BaseQuery::equal('_uid', [$id])]);

        if ($forUpdate && $this->supports(Capability::UpdateLock)) {
            $builder->forUpdate();
        }

        $result = $builder->build();

        try {
            $stmt = $this->executeResult($result);
            $this->execute($stmt);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        /** @var array<int, array<string, mixed>> $rows */
        $rows = $stmt->fetchAll();
        $stmt->closeCursor();

        if (empty($rows)) {
            return new Document([]);
        }

        /** @var array<string, mixed> $document */
        $document = $rows[0];

        $this->remapRow($document);

        return new Document($document);
    }

    /**
     * Create Documents in batches
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DuplicateException
     * @throws Throwable
     */
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

            $attributeKeySet = [];
            foreach (Database::INTERNAL_ATTRIBUTE_KEYS as $k) {
                $attributeKeySet[$k] = true;
            }

            $hasSequence = null;
            foreach ($documents as $document) {
                foreach ($document->getAttributes() as $key => $value) {
                    $attributeKeySet[$key] = true;
                }

                if ($hasSequence === null) {
                    $hasSequence = ! empty($document->getSequence());
                } elseif ($hasSequence == empty($document->getSequence())) {
                    throw new DatabaseException('All documents must have an sequence if one is set');
                }
            }

            $attributeKeys = \array_keys($attributeKeySet);

            if ($hasSequence) {
                $attributeKeys[] = '_id';
            }

            $builder = $this->createBuilder()->into($this->getSQLTableRaw($name));

            // Register spatial column expressions for ST_GeomFromText wrapping
            foreach ($spatialAttributes as $spatialCol) {
                $builder->insertColumnExpression($spatialCol, $this->getSpatialGeomFromText('?'));
            }

            foreach ($documents as $document) {
                $row = $this->buildDocumentRow($document, $attributeKeys, $spatialAttributes);
                $row = $this->decorateRow($row, $this->documentMetadata($document));
                $builder->set($row);
            }

            $result = $builder->insert();
            $stmt = $this->executeResult($result, Event::DocumentCreate);
            $this->execute($stmt);

            $ctx = $this->buildWriteContext($name);
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentCreate($name, $documents, $ctx));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $documents;
    }

    /**
     * Update documents
     *
     * Updates all documents which match the given query.
     *
     * @param  array<Document>  $documents
     *
     * @throws DatabaseException
     */
    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        if (empty($documents)) {
            return 0;
        }

        $this->syncWriteHooks();

        $spatialAttributes = $this->getSpatialAttributes($collection);
        $collection = $collection->getId();

        $attributes = $updates->getAttributes();

        if (! empty($updates->getUpdatedAt())) {
            $attributes['_updatedAt'] = $updates->getUpdatedAt();
        }

        if (! empty($updates->getCreatedAt())) {
            $attributes['_createdAt'] = $updates->getCreatedAt();
        }

        if ($updates->offsetExists('$permissions')) {
            $attributes['_permissions'] = json_encode($updates->getPermissions());
        }

        if (empty($attributes)) {
            return 0;
        }

        $name = $this->filter($collection);

        // Separate regular attributes from operators
        $operators = [];
        foreach ($attributes as $attribute => $value) {
            if (Operator::isOperator($value)) {
                $operators[$attribute] = $value;
            }
        }

        // Build the UPDATE using the query builder
        $builder = $this->newBuilder($name);

        // Regular (non-operator, non-spatial) attributes go into set()
        $regularRow = [];
        foreach ($attributes as $attribute => $value) {
            if (isset($operators[$attribute])) {
                continue; // Handled via setRaw below
            }
            if (\in_array($attribute, $spatialAttributes)) {
                continue; // Handled via setRaw below
            }

            $column = $this->filter($attribute);

            if (\is_array($value)) {
                $value = \json_encode($value);
            }
            if ($this->supports(Capability::IntegerBooleans)) {
                $value = (\is_bool($value)) ? (int) $value : $value;
            }

            $regularRow[$column] = $value;
        }

        if (! empty($regularRow)) {
            $builder->set($regularRow);
        }

        // Spatial attributes use setRaw with ST_GeomFromText(?)
        foreach ($attributes as $attribute => $value) {
            if (! \in_array($attribute, $spatialAttributes)) {
                continue;
            }
            $column = $this->filter($attribute);

            if (\is_array($value)) {
                $value = $this->convertArrayToWKT($value);
            }

            $builder->setRaw($column, $this->getSpatialGeomFromText('?'), [$value]);
        }

        // Operator attributes use setRaw with converted expressions
        foreach ($operators as $attribute => $operator) {
            $column = $this->filter($attribute);
            /** @var Operator $operator */
            $opResult = $this->getOperatorBuilderExpression($column, $operator);
            $builder->setRaw($column, $opResult['expression'], $opResult['bindings']);
        }

        $builder->setRaw('_version', $this->quote('_version') . ' + 1', []);

        // WHERE _id IN (sequence values)
        $sequences = \array_map(fn ($document) => $document->getSequence(), $documents);
        $builder->filter([BaseQuery::equal('_id', \array_values($sequences))]);

        $result = $builder->update();
        $stmt = $this->executeResult($result, Event::DocumentsUpdate);

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $affected = $stmt->rowCount();

        $ctx = $this->buildWriteContext($name);
        $this->runWriteHooks(fn ($hook) => $hook->afterDocumentBatchUpdate($name, $updates, $documents, $ctx));

        return $affected;
    }

    /**
     * @param  array<Change>  $changes
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    public function upsertDocuments(
        Document $collection,
        string $attribute,
        array $changes
    ): array {
        if (empty($changes)) {
            return $changes;
        }
        try {
            $spatialAttributes = $this->getSpatialAttributes($collection);

            /** @var array<string, mixed> $attributeDefaults */
            $attributeDefaults = [];
            /** @var array<mixed> $collAttrs */
            $collAttrs = $collection->getAttribute('attributes', []);
            foreach ($collAttrs as $attr) {
                /** @var array<string, mixed> $attr */
                $attrIdRaw = $attr['$id'] ?? '';
                $attrId = \is_scalar($attrIdRaw) ? (string) $attrIdRaw : '';
                $attributeDefaults[$attrId] = $attr['default'] ?? null;
            }

            $collection = $collection->getId();
            $name = $this->filter($collection);

            $hasOperators = false;
            $firstChange = $changes[0];
            $firstDoc = $firstChange->getNew();
            $firstExtracted = Operator::extractOperators($firstDoc->getAttributes());

            if (! empty($firstExtracted['operators'])) {
                $hasOperators = true;
            } else {
                foreach ($changes as $change) {
                    $doc = $change->getNew();
                    $extracted = Operator::extractOperators($doc->getAttributes());
                    if (! empty($extracted['operators'])) {
                        $hasOperators = true;
                        break;
                    }
                }
            }

            if (! $hasOperators) {
                $this->executeUpsertBatch($name, $changes, $spatialAttributes, $attribute, [], $attributeDefaults, false);
            } else {
                $groups = [];

                foreach ($changes as $change) {
                    $document = $change->getNew();
                    $extracted = Operator::extractOperators($document->getAttributes());
                    $operators = $extracted['operators'];

                    if (empty($operators)) {
                        $signature = 'no_ops';
                    } else {
                        $parts = [];
                        foreach ($operators as $attr => $op) {
                            $parts[] = $attr.':'.$op->getMethod()->value.':'.json_encode($op->getValues());
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

            $ctx = $this->buildWriteContext($name);
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentUpsert($name, $changes, $ctx));
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return \array_map(fn ($change) => $change->getNew(), $changes);
    }

    /**
     * Delete Documents
     *
     * @param  array<string>  $sequences
     * @param  array<string>  $permissionIds
     *
     * @throws DatabaseException
     */
    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        if (empty($sequences)) {
            return 0;
        }

        $this->syncWriteHooks();

        try {
            $name = $this->filter($collection);

            // Delete documents
            $builder = $this->newBuilder($name);
            $builder->filter([BaseQuery::equal('_id', \array_values($sequences))]);
            $result = $builder->delete();
            $stmt = $this->executeResult($result, Event::DocumentsDelete);

            if (! $stmt->execute()) {
                throw new DatabaseException('Failed to delete documents');
            }

            $ctx = $this->buildWriteContext($name);
            $this->runWriteHooks(fn ($hook) => $hook->afterDocumentDelete($name, \array_values($permissionIds), $ctx));
        } catch (Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        return $stmt->rowCount();
    }

    /**
     * Assign internal IDs for the given documents
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    public function getSequences(string $collection, array $documents): array
    {
        $documentIds = [];

        foreach ($documents as $document) {
            if (empty($document->getSequence())) {
                $documentIds[] = $document->getId();
            }
        }

        if (empty($documentIds)) {
            return $documents;
        }

        $builder = $this->newBuilder($collection);
        $builder->select(['_uid', '_id']);
        $builder->filter([BaseQuery::equal('_uid', $documentIds)]);

        $result = $builder->build();
        $stmt = $this->executeResult($result);
        $stmt->execute();
        /** @var array<string, mixed> $sequences */
        $sequences = $stmt->fetchAll(PDO::FETCH_KEY_PAIR); // Fetch as [documentId => sequence]
        $stmt->closeCursor();

        foreach ($documents as $document) {
            if (isset($sequences[$document->getId()])) {
                $document['$sequence'] = $sequences[$document->getId()];
            }
        }

        return $documents;
    }

    /**
     * Find Documents
     *
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
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
    {
        $collectionDoc = $collection;
        $collection = $collection->getId();
        $name = $this->filter($collection);
        $roles = $this->authorization->getRoles();
        $alias = Query::DEFAULT_ALIAS;

        $queries = array_map(fn ($query) => clone $query, $queries);

        // Extract vector queries for ORDER BY
        $vectorQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod()->isVector()) {
                $vectorQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $queries = $otherQueries;

        $hasAggregation = false;
        $hasJoins = false;
        foreach ($queries as $query) {
            if ($query->getMethod()->isAggregate() || $query->getMethod() === Method::GroupBy) {
                $hasAggregation = true;
            }
            if ($query->getMethod()->isJoin()) {
                $hasJoins = true;
            }
        }

        $builder = $this->newBuilder($name, $alias);

        if (! $hasAggregation) {
            $selections = $this->getAttributeSelections($queries);
            if (! empty($selections) && ! \in_array('*', $selections)) {
                $builder->select($this->mapSelectionsToColumns($selections));
            }
        }

        $joinTablePrefixes = [];
        $joinIndex = 0;

        if ($hasJoins) {
            foreach ($queries as $query) {
                if ($query->getMethod()->isJoin()) {
                    $joinTable = $query->getAttribute();
                    $resolvedTable = $this->getSQLTableRaw($this->filter($joinTable));
                    $joinAlias = 'j' . $joinIndex++;
                    $query->setAttribute($resolvedTable);

                    $values = $query->getValues();
                    if (count($values) >= 3) {
                        /** @var string $leftCol */
                        $leftCol = $values[0];
                        /** @var string $rightCol */
                        $rightCol = $values[2];

                        $leftInternal = $this->getInternalKeyForAttribute($leftCol);
                        $rightInternal = $this->getInternalKeyForAttribute($rightCol);

                        $values[0] = $alias . '.' . $leftInternal;
                        $values[2] = $joinAlias . '.' . $rightInternal;
                        $values[3] = $joinAlias;
                        $query->setValues($values);

                        $joinTablePrefixes[$joinTable] = $joinAlias;
                    }
                }
            }
        }

        if ($hasAggregation && ! empty($joinTablePrefixes)) {
            /** @var array<Document> $collectionAttrs */
            $collectionAttrs = $collectionDoc->getAttribute('attributes', []);
            $mainAttributeIds = \array_map(
                fn (Document $attr) => $attr->getId(),
                $collectionAttrs
            );
            $defaultJoinPrefix = \array_values($joinTablePrefixes)[0];

            foreach ($queries as $query) {
                if ($query->getMethod()->isAggregate()) {
                    $attr = $query->getAttribute();
                    if ($attr !== '*' && $attr !== '' && ! \str_contains($attr, '.') && ! \in_array($attr, $mainAttributeIds)) {
                        $internalAttr = $this->getInternalKeyForAttribute($attr);
                        $query->setAttribute($defaultJoinPrefix . '.' . $internalAttr);
                    }
                } elseif ($query->getMethod() === Method::GroupBy) {
                    $values = $query->getValues();
                    $qualified = false;
                    foreach ($values as $i => $col) {
                        if (\is_string($col) && ! \str_contains($col, '.') && ! \in_array($col, $mainAttributeIds)) {
                            $internalCol = $this->getInternalKeyForAttribute($col);
                            $values[$i] = $defaultJoinPrefix . '.' . $internalCol;
                            $qualified = true;
                        }
                    }
                    if ($qualified) {
                        $query->setValues($values);
                    }
                }
            }
        }

        if ($hasAggregation) {
            foreach ($queries as $query) {
                if ($query->getMethod() === Method::GroupBy) {
                    /** @var array<string> $groupCols */
                    $groupCols = $query->getValues();
                    $builder->select(\array_map(
                        fn (string $col) => \str_contains($col, '.') ? $col : $this->filter($this->getInternalKeyForAttribute($col)),
                        $groupCols
                    ));
                }
            }
        }

        // Pass all queries (filters, aggregations, joins, groupBy, having) to the builder
        $builder->filter($queries);

        // Permission subquery (qualify document column with table alias when joins are present to avoid ambiguity)
        if ($this->authorization->getStatus()) {
            $docCol = $hasJoins ? $alias . '._uid' : '_uid';
            $builder->addHook($this->newPermissionHook($name, $roles, $forPermission->value, $docCol));
        }

        // Cursor pagination - build nested Query objects for complex multi-attribute cursor conditions
        if (! empty($cursor)) {
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

                $internalAttr = $this->filter($this->getInternalKeyForAttribute($originalAttribute));

                // Special case: single attribute on unique primary key
                if (count($orderAttributes) === 1 && $i === 0 && $originalAttribute === '$sequence') {
                    /** @var bool|float|int|string $cursorVal */
                    $cursorVal = $cursor[$originalAttribute];
                    if ($direction === OrderDirection::Desc) {
                        $cursorConditions[] = BaseQuery::lessThan($internalAttr, $cursorVal);
                    } else {
                        $cursorConditions[] = BaseQuery::greaterThan($internalAttr, $cursorVal);
                    }
                    break;
                }

                // Multi-attribute cursor: (prev_attrs equal) AND (current_attr > or < cursor)
                $andConditions = [];

                for ($j = 0; $j < $i; $j++) {
                    $prevOriginal = $orderAttributes[$j];
                    $prevAttr = $this->filter($this->getInternalKeyForAttribute($prevOriginal));
                    /** @var array<array<mixed>|bool|float|int|string|null> $prevCursorVals */
                    $prevCursorVals = [$cursor[$prevOriginal]];
                    $andConditions[] = BaseQuery::equal($prevAttr, $prevCursorVals);
                }

                /** @var bool|float|int|string $cursorAttrVal */
                $cursorAttrVal = $cursor[$originalAttribute];
                if ($direction === OrderDirection::Desc) {
                    $andConditions[] = BaseQuery::lessThan($internalAttr, $cursorAttrVal);
                } else {
                    $andConditions[] = BaseQuery::greaterThan($internalAttr, $cursorAttrVal);
                }

                if (count($andConditions) === 1) {
                    $cursorConditions[] = $andConditions[0];
                } else {
                    $cursorConditions[] = BaseQuery::and($andConditions);
                }
            }

            if (! empty($cursorConditions)) {
                if (count($cursorConditions) === 1) {
                    $builder->filter($cursorConditions);
                } else {
                    $builder->filter([BaseQuery::or($cursorConditions)]);
                }
            }
        }

        // Vector ordering (comes first for similarity search)
        foreach ($vectorQueries as $query) {
            $vectorRaw = $this->getVectorOrderRaw($query, $alias);
            if ($vectorRaw !== null) {
                $builder->orderByRaw($vectorRaw['expression'], $vectorRaw['bindings']);
            }
        }

        // Full-text search relevance scoring
        $searchQueries = $this->extractSearchQueries($queries);
        if (! empty($searchQueries)) {
            $builder->select(['*']);
            foreach ($searchQueries as $searchQuery) {
                $relevanceRaw = $this->getSearchRelevanceRaw($searchQuery, $alias);
                if ($relevanceRaw !== null) {
                    $builder->selectRaw($relevanceRaw['expression'], $relevanceRaw['bindings']);
                    $builder->orderByRaw($relevanceRaw['order']);
                }
            }
        }

        // Regular ordering
        foreach ($orderAttributes as $i => $originalAttribute) {
            $orderType = $orderTypes[$i] ?? OrderDirection::Asc;

            if ($orderType === OrderDirection::Random) {
                $builder->sortRandom();

                continue;
            }

            $internalAttr = $this->filter($this->getInternalKeyForAttribute($originalAttribute));
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

        // Limit/offset
        if (! \is_null($limit)) {
            $builder->limit($limit);
        }
        if (! \is_null($offset)) {
            $builder->offset($offset);
        }

        try {
            $result = $builder->build();
        } catch (ValidationException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }

        $sql = $result->query;

        try {
            $stmt = $this->getPDO()->prepare($sql);
            foreach ($result->bindings as $i => $value) {
                if (\is_bool($value) && $this->supports(Capability::IntegerBooleans)) {
                    $value = (int) $value;
                }
                if (\is_array($value)) {
                    $value = \json_encode($value);
                }
                if (\is_float($value)) {
                    $stmt->bindValue($i + 1, $this->getFloatPrecision($value), PDO::PARAM_STR);
                } else {
                    $stmt->bindValue($i + 1, $value, $this->getPDOType($value));
                }
            }
            $this->execute($stmt);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $results = $stmt->fetchAll();
        $stmt->closeCursor();

        $documents = [];

        if ($hasAggregation) {
            foreach ($results as $row) {
                /** @var array<string, mixed> $row */
                $documents[] = new Document($row);
            }

            return $documents;
        }

        foreach ($results as $row) {
            /** @var array<string, mixed> $row */
            $this->remapRow($row);
            $documents[] = new Document($row);
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
    public function rawQuery(string $query, array $bindings = []): array
    {
        try {
            $stmt = $this->getPDO()->prepare($query);
            foreach ($bindings as $i => $value) {
                $stmt->bindValue($i + 1, $value, $this->getPDOType($value));
            }
            $this->execute($stmt);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $results = $stmt->fetchAll();
        $stmt->closeCursor();

        $documents = [];
        foreach ($results as $row) {
            /** @var array<string, mixed> $row */
            $documents[] = new Document($row);
        }

        return $documents;
    }

    /**
     * Count Documents
     *
     * @param  array<Query>  $queries
     *
     * @throws Exception
     * @throws PDOException
     */
    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        $collection = $collection->getId();
        $name = $this->filter($collection);
        $roles = $this->authorization->getRoles();
        $alias = Query::DEFAULT_ALIAS;

        $queries = array_map(fn ($query) => clone $query, $queries);

        $otherQueries = [];
        foreach ($queries as $query) {
            if (! $query->getMethod()->isVector()) {
                $otherQueries[] = $query;
            }
        }

        // Build inner query: SELECT 1 FROM table WHERE ... LIMIT
        $innerBuilder = $this->newBuilder($name, $alias);
        $innerBuilder->selectRaw('1');
        $innerBuilder->filter($otherQueries);

        // Permission subquery
        if ($this->authorization->getStatus()) {
            $innerBuilder->addHook($this->newPermissionHook($name, $roles));
        }

        if (! \is_null($max)) {
            $innerBuilder->limit($max);
        }

        // Wrap in outer count: SELECT COUNT(1) as sum FROM (...) table_count
        $outerBuilder = $this->createBuilder();
        $outerBuilder->fromSub($innerBuilder, 'table_count');
        $outerBuilder->count('1', 'sum');

        $result = $outerBuilder->build();
        $sql = $result->query;
        $stmt = $this->getPDO()->prepare($sql);

        foreach ($result->bindings as $i => $value) {
            if (\is_bool($value) && $this->supports(Capability::IntegerBooleans)) {
                $value = (int) $value;
            }
            if (\is_float($value)) {
                $stmt->bindValue($i + 1, $this->getFloatPrecision($value), PDO::PARAM_STR);
            } else {
                $stmt->bindValue($i + 1, $value, $this->getPDOType($value));
            }
        }

        try {
            $this->execute($stmt);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $result = $stmt->fetchAll();
        $stmt->closeCursor();
        if (! empty($result)) {
            $result = $result[0];
        }

        if (\is_array($result)) {
            $sumInt = $result['sum'] ?? 0;

            return \is_numeric($sumInt) ? (int) $sumInt : 0;
        }

        return 0;
    }

    /**
     * Sum an Attribute
     *
     * @param  array<Query>  $queries
     *
     * @throws Exception
     * @throws PDOException
     */
    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): int|float
    {
        $collection = $collection->getId();
        $name = $this->filter($collection);
        $attribute = $this->filter($attribute);
        $roles = $this->authorization->getRoles();
        $alias = Query::DEFAULT_ALIAS;

        $queries = array_map(fn ($query) => clone $query, $queries);

        $otherQueries = [];
        foreach ($queries as $query) {
            if (! $query->getMethod()->isVector()) {
                $otherQueries[] = $query;
            }
        }

        // Build inner query: SELECT attribute FROM table WHERE ... LIMIT
        $innerBuilder = $this->newBuilder($name, $alias);
        $innerBuilder->select([$attribute]);
        $innerBuilder->filter($otherQueries);

        // Permission subquery
        if ($this->authorization->getStatus()) {
            $innerBuilder->addHook($this->newPermissionHook($name, $roles));
        }

        if (! \is_null($max)) {
            $innerBuilder->limit($max);
        }

        // Wrap in outer sum: SELECT SUM(attribute) as sum FROM (...) table_count
        $outerBuilder = $this->createBuilder();
        $outerBuilder->fromSub($innerBuilder, 'table_count');
        $outerBuilder->sum($attribute, 'sum');

        $result = $outerBuilder->build();
        $sql = $result->query;
        $stmt = $this->getPDO()->prepare($sql);

        foreach ($result->bindings as $i => $value) {
            if (\is_bool($value) && $this->supports(Capability::IntegerBooleans)) {
                $value = (int) $value;
            }
            if (\is_float($value)) {
                $stmt->bindValue($i + 1, $this->getFloatPrecision($value), PDO::PARAM_STR);
            } else {
                $stmt->bindValue($i + 1, $value, $this->getPDOType($value));
            }
        }

        try {
            $this->execute($stmt);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $result = $stmt->fetchAll();
        $stmt->closeCursor();
        if (! empty($result)) {
            $result = $result[0];
        }

        if (\is_array($result)) {
            $sumVal = $result['sum'] ?? 0;

            if (\is_numeric($sumVal)) {
                return \str_contains((string) $sumVal, '.') ? (float) $sumVal : (int) $sumVal;
            }

            return 0;
        }

        return 0;
    }

    /**
     * Get max STRING limit
     */
    public function getLimitForString(): int
    {
        return 4294967295;
    }

    /**
     * Get max INT limit
     */
    public function getLimitForInt(): int
    {
        return 4294967295;
    }

    /**
     * Get maximum column limit.
     * https://mariadb.com/kb/en/innodb-limitations/#limitations-on-schema
     * Can be inherited by MySQL since we utilize the InnoDB engine
     */
    public function getLimitForAttributes(): int
    {
        return 1017;
    }

    /**
     * Get maximum index limit.
     * https://mariadb.com/kb/en/innodb-limitations/#limitations-on-schema
     */
    public function getLimitForIndexes(): int
    {
        return 64;
    }

    /**
     * Get current attribute count from collection document
     */
    public function getCountOfAttributes(Document $collection): int
    {
        /** @var array<mixed> $attrs */
        $attrs = $collection->getAttribute('attributes') ?? [];
        $attributes = \count($attrs);

        return $attributes + $this->getCountOfDefaultAttributes();
    }

    /**
     * Get current index count from collection document
     */
    public function getCountOfIndexes(Document $collection): int
    {
        /** @var array<mixed> $idxs */
        $idxs = $collection->getAttribute('indexes') ?? [];
        $indexes = \count($idxs);

        return $indexes + $this->getCountOfDefaultIndexes();
    }

    /**
     * Returns number of attributes used by default.
     */
    public function getCountOfDefaultAttributes(): int
    {
        return \count(Database::internalAttributes());
    }

    /**
     * Returns number of indexes used by default.
     */
    public function getCountOfDefaultIndexes(): int
    {
        return \count(Database::INTERNAL_INDEXES);
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     */
    public function getDocumentSizeLimit(): int
    {
        return 65535;
    }

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     *
     * @throws DatabaseException
     */
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

        /** @var array<int, array<string, mixed>> $attributes */
        $attributes = $collection->getAttributes()['attributes'] ?? [];

        foreach ($attributes as $attribute) {
            /**
             * Json / Longtext
             * only the pointer contributes 20 bytes
             * data is stored externally
             */
            if ($attribute['array'] ?? false) {
                $total += 20;

                continue;
            }

            $attrSize = (int) (is_scalar($attribute['size'] ?? 0) ? ($attribute['size'] ?? 0) : 0);
            $attrType = (string) (is_scalar($attribute['type'] ?? '') ? ($attribute['type'] ?? '') : '');

            switch ($attrType) {
                case ColumnType::Id->value:
                    $total += 8; //  BIGINT 8 bytes
                    break;

                case ColumnType::String->value:
                    /**
                     * Text / Mediumtext / Longtext
                     * only the pointer contributes 20 bytes to the row size
                     * data is stored externally
                     */
                    $total += match (true) {
                        $attrSize > $this->getMaxVarcharLength() => 20,
                        $attrSize > 255 => $attrSize * 4 + 2, //  VARCHAR(>255) + 2 length
                        default => $attrSize * 4 + 1, //  VARCHAR(<=255) + 1 length
                    };

                    break;

                case ColumnType::Varchar->value:
                    $total += match (true) {
                        $attrSize > 255 => $attrSize * 4 + 2, //  VARCHAR(>255) + 2 length
                        default => $attrSize * 4 + 1, //  VARCHAR(<=255) + 1 length
                    };
                    break;

                case ColumnType::Text->value:
                case ColumnType::MediumText->value:
                case ColumnType::LongText->value:
                    $total += 20; // Pointer storage for TEXT types
                    break;

                case ColumnType::Integer->value:
                    if ($attrSize >= 8) {
                        $total += 8; //  BIGINT 8 bytes
                    } else {
                        $total += 4; // INT 4 bytes
                    }
                    break;

                case ColumnType::Double->value:
                    $total += 8; // DOUBLE 8 bytes
                    break;

                case ColumnType::Boolean->value:
                    $total += 1; // TINYINT(1) 1 bytes
                    break;

                case ColumnType::Relationship->value:
                    $total += Database::LENGTH_KEY * 4 + 1; // VARCHAR(<=255)
                    break;

                case ColumnType::Datetime->value:
                    /**
                     * 1 byte year + month
                     * 1 byte for the day
                     * 3 bytes for the hour, minute, and second
                     * 2 bytes miliseconds DATETIME(3)
                     */
                    $total += 7;
                    break;

                case ColumnType::Object->value:
                    /**
                     * JSONB/JSON type
                     * Only the pointer contributes 20 bytes to the row size
                     * Data is stored externally
                     */
                    $total += 20;
                    break;

                case ColumnType::Point->value:
                    $total += $this->getMaxPointSize();
                    break;
                case ColumnType::Linestring->value:
                case ColumnType::Polygon->value:
                    $total += 20;
                    break;

                case ColumnType::Vector->value:
                    // Each dimension is typically 4 bytes (float32)
                    $total += $attrSize * 4;
                    break;

                default:
                    throw new DatabaseException('Unknown type: ' . $attrType);
            }
        }

        return $total;
    }

    /**
     * Get the maximum VARCHAR column length supported across SQL engines.
     *
     * @return int
     */
    public function getMaxVarcharLength(): int
    {
        return 16381; // Floor value for Postgres:16383 | MySQL:16381 | MariaDB:16382
    }

    /**
     * Size of POINT spatial type
     */
    abstract protected function getMaxPointSize(): int;

    /**
     * Get the maximum combined index key length in bytes.
     *
     * @return int
     */
    public function getMaxIndexLength(): int
    {
        /**
         * $tenant int = 1
         */
        return $this->sharedTables ? 767 : 768;
    }

    /**
     * Get the maximum length for unique document IDs.
     *
     * @return int
     */
    public function getMaxUIDLength(): int
    {
        return 255;
    }

    /**
     * Get list of keywords that cannot be used
     *  Refference: https://mariadb.com/kb/en/reserved-words/
     *
     * @return array<string>
     */
    public function getKeywords(): array
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
     * Get the keys of internally managed indexes.
     *
     * @return array<string>
     */
    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    /**
     * Convert a type string and size to the corresponding SQL column type definition.
     *
     * @param string $type The column type value
     * @param int $size The column size
     * @param bool $signed Whether the column is signed
     * @param bool $array Whether the column stores an array
     * @param bool $required Whether the column is required
     * @return string
     *
     * @throws DatabaseException For unknown type values.
     */
    public function getColumnType(string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        $columnType = ColumnType::tryFrom($type);
        if ($columnType === null) {
            throw new DatabaseException('Unknown column type: '.$type);
        }

        return $this->getSQLType($columnType, $size, $signed, $array, $required);
    }

    abstract protected function getSQLType(
        ColumnType $type,
        int $size,
        bool $signed = true,
        bool $array = false,
        bool $required = false
    ): string;

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
            IndexType::Fulltext => 'FULLTEXT INDEX',
            default => throw new DatabaseException('Unknown index type: '.$type->value.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value),
        };
    }

    /**
     * Extract the spatial geometry type name from a WKT string.
     *
     * @param string $wkt The Well-Known Text representation
     * @return string The lowercase type name (e.g. "point", "polygon")
     *
     * @throws DatabaseException If the WKT is invalid.
     */
    public function getSpatialTypeFromWKT(string $wkt): string
    {
        $wkt = trim($wkt);
        $pos = strpos($wkt, '(');
        if ($pos === false) {
            throw new DatabaseException('Invalid spatial type');
        }

        return strtolower(trim(substr($wkt, 0, $pos)));
    }

    /**
     * Generate ST_GeomFromText call with proper SRID and axis order support
     */
    protected function getSpatialGeomFromText(string $wktPlaceholder, ?int $srid = null): string
    {
        $srid = $srid ?? Database::DEFAULT_SRID;
        $geomFromText = "ST_GeomFromText({$wktPlaceholder}, {$srid}";

        if ($this->supports(Capability::SpatialAxisOrder)) {
            $geomFromText .= ', '.$this->getSpatialAxisOrderSpec();
        }

        $geomFromText .= ')';

        return $geomFromText;
    }

    /**
     * Get the spatial axis order specification string
     */
    protected function getSpatialAxisOrderSpec(): string
    {
        return "'axis-order=long-lat'";
    }

    /**
     * Build geometry WKT string from array input for spatial queries
     *
     * @param  array<mixed>  $geometry
     *
     * @throws DatabaseException
     */
    protected function convertArrayToWKT(array $geometry): string
    {
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
     * Decode a WKB or WKT POINT into a coordinate array [x, y].
     *
     * @param string $wkb The WKB binary or WKT string
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
    public function decodeLinestring(string $wkb): array
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

    /**
     * Get SQL table
     *
     * @throws DatabaseException
     */
    protected function getSQLTable(string $name): string
    {
        return "{$this->quote($this->getDatabase())}.{$this->quote($this->getNamespace().'_'.$this->filter($name))}";
    }

    /**
     * Get an unquoted qualified table name (the builder handles quoting).
     *
     * @throws DatabaseException
     */
    protected function getSQLTableRaw(string $name): string
    {
        return $this->getDatabase().'.'.$this->getNamespace().'_'.$this->filter($name);
    }

    /**
     * Create a new query builder instance for this adapter's SQL dialect.
     */
    abstract protected function createBuilder(): SQLBuilder;

    /**
     * Create a new schema builder instance for this adapter's SQL dialect.
     */
    abstract protected function createSchemaBuilder(): Schema;

    /**
     * Create and configure a new query builder for a given table.
     *
     * Automatically applies tenant filtering when shared tables are enabled.
     *
     * @throws DatabaseException
     */
    protected function newBuilder(string $table, string $alias = ''): SQLBuilder
    {
        $builder = $this->createBuilder()->from($this->getSQLTableRaw($table), $alias);
        $builder->addHook(new AttributeMap([
            '$id' => '_uid',
            '$sequence' => '_id',
            '$collection' => '_collection',
            '$tenant' => '_tenant',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            '$permissions' => '_permissions',
        ]));
        if ($this->sharedTables && $this->tenant !== null) {
            $builder->addHook(new TenantFilter($this->tenant, Database::METADATA));
        }

        return $builder;
    }

    public function rawMutation(string $query, array $bindings = []): int
    {
        try {
            $stmt = $this->getPDO()->prepare($query);
            foreach ($bindings as $i => $value) {
                $stmt->bindValue($i + 1, $value, $this->getPDOType($value));
            }
            $this->execute($stmt);
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $count = $stmt->rowCount();
        $stmt->closeCursor();

        return $count;
    }

    public function getBuilder(string $collection): SQLBuilder
    {
        return $this->newBuilder($this->filter($collection));
    }

    protected function getIdentifierQuoteChar(): string
    {
        return '`';
    }

    /**
     * @param  array<string>  $roles
     */
    protected function newPermissionHook(string $collection, array $roles, string $type = PermissionType::Read->value, string $documentColumn = '_uid'): PermissionFilter
    {
        return new PermissionFilter(
            roles: \array_values($roles),
            permissionsTable: fn (string $table) => $this->getSQLTableRaw($collection.'_perms'),
            type: $type,
            documentColumn: $documentColumn,
            permDocumentColumn: '_document',
            permRoleColumn: '_permission',
            permTypeColumn: '_type',
            subqueryFilter: ($this->sharedTables && $this->tenant !== null) ? new TenantFilter($this->tenant) : null,
            quoteChar: $this->getIdentifierQuoteChar(),
        );
    }

    /**
     * Synchronize write hooks with current adapter configuration.
     *
     * Ensures PermissionWrite is always registered and TenantWrite is registered
     * when shared tables with a tenant are active.
     */
    protected function syncWriteHooks(): void
    {
        if (empty(array_filter($this->writeHooks, fn ($h) => $h instanceof PermissionWrite))) {
            $this->addWriteHook(new PermissionWrite());
        }

        $this->removeWriteHook(TenantWrite::class);
        if ($this->sharedTables && ($this->tenant !== null || $this->tenantPerDocument)) {
            $this->addWriteHook(new TenantWrite($this->tenant ?? 0));
        }
    }

    /**
     * Build a WriteContext that delegates to this adapter's query infrastructure.
     *
     * @param  string  $collection  The filtered collection name
     */
    protected function buildWriteContext(string $collection): WriteContext
    {
        $name = $this->filter($collection);

        return new WriteContext(
            newBuilder: fn (string $table, string $alias = '') => $this->newBuilder($table, $alias),
            executeResult: fn (BuildResult $result, ?Event $event = null) => $this->executeResult($result, $event),
            execute: fn (mixed $stmt) => $this->execute($stmt),
            decorateRow: fn (array $row, array $metadata) => $this->decorateRow($row, $metadata),
            createBuilder: fn () => $this->createBuilder(),
            getTableRaw: fn (string $table) => $this->getSQLTableRaw($table),
        );
    }

    /**
     * Execute a BuildResult through the transformation system with positional bindings.
     *
     * Prepares the SQL statement and binds positional parameters from the BuildResult.
     * Does NOT call execute() - the caller is responsible for that.
     *
     * @param  Event|null  $event  Optional event to run through transformation system
     * @return PDOStatement|PDOStatementProxy
     */
    protected function executeResult(BuildResult $result, ?Event $event = null): PDOStatement|PDOStatementProxy
    {
        $sql = $result->query;
        if ($event !== null) {
            foreach ($this->queryTransforms as $transform) {
                $sql = $transform->transform($event, $sql);
            }
        }
        $stmt = $this->getPDO()->prepare($sql);
        foreach ($result->bindings as $i => $value) {
            if (\is_bool($value) && $this->supports(Capability::IntegerBooleans)) {
                $value = (int) $value;
            }
            if (\is_float($value)) {
                $stmt->bindValue($i + 1, $this->getFloatPrecision($value), PDO::PARAM_STR);
            } else {
                $stmt->bindValue($i + 1, $value, $this->getPDOType($value));
            }
        }

        return $stmt;
    }

    protected function execute(mixed $stmt): bool
    {
        /** @var PDOStatement|PDOStatementProxy $stmt */
        if ($this->profiler !== null && $this->profiler->isEnabled()) {
            $start = \microtime(true);
            $result = $stmt->execute();
            $durationMs = (\microtime(true) - $start) * 1000;
            $this->profiler->log(
                $stmt->queryString ?? '',
                [],
                $durationMs,
            );

            return $result;
        }

        return $stmt->execute();
    }

    /**
     * Execute a single upsert batch using the query builder.
     *
     * Builds an INSERT ... ON CONFLICT/DUPLICATE KEY UPDATE statement via the
     * query builder, handling spatial columns, shared-table tenant guards,
     * increment attributes, and operator expressions.
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
        $builder = $this->createBuilder()->into($this->getSQLTableRaw($name));

        // Register spatial column expressions for ST_GeomFromText wrapping
        foreach ($spatialAttributes as $spatialCol) {
            $builder->insertColumnExpression($spatialCol, $this->getSpatialGeomFromText('?'));
        }

        // Postgres requires an alias on the INSERT target for conflict resolution
        if ($this->insertRequiresAlias()) {
            $builder->insertAs('target');
        }

        // Collect all column names and build rows
        $allColumnNames = [];
        $documentsData = [];

        foreach ($changes as $change) {
            $document = $change->getNew();

            if ($hasOperators) {
                $extracted = Operator::extractOperators($document->getAttributes());
                $currentRegularAttributes = $extracted['updates'];
                $extractedOperators = $extracted['operators'];

                // For new documents, apply operators to attribute defaults
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

            $documentsData[] = $currentRegularAttributes;
        }

        // Include operator column names in the column set
        foreach (\array_keys($operators) as $colName) {
            $allColumnNames[$colName] = true;
        }

        $allColumnNames = \array_keys($allColumnNames);
        \sort($allColumnNames);

        // Build rows for the builder, applying JSON/boolean/spatial conversions
        foreach ($documentsData as $docAttrs) {
            $row = [];
            foreach ($allColumnNames as $key) {
                $value = $docAttrs[$key] ?? null;
                if (\is_array($value)) {
                    $value = \json_encode($value);
                }
                if (! \in_array($key, $spatialAttributes) && $this->supports(Capability::IntegerBooleans)) {
                    $value = (\is_bool($value)) ? (int) $value : $value;
                }
                $row[$key] = $value;
            }
            $builder->set($row);
        }

        // Determine conflict keys
        $conflictKeys = $this->sharedTables ? ['_uid', '_tenant'] : ['_uid'];

        // Determine which columns to update on conflict
        $skipColumns = ['_uid', '_id', '_createdAt', '_tenant'];

        if (! empty($attribute)) {
            // Increment mode: only update the increment column and _updatedAt
            $updateColumns = [$this->filter($attribute), '_updatedAt'];
        } else {
            // Normal mode: update all columns except the skip set
            $updateColumns = \array_values(\array_filter(
                $allColumnNames,
                fn ($c) => ! \in_array($c, $skipColumns)
            ));
        }

        $builder->onConflict($conflictKeys, $updateColumns);

        // Apply conflict-resolution expressions
        // Column names passed to conflictSetRaw() must match the names in onConflict().
        // The expression-generating methods handle their own quoting/filtering internally.
        if (! empty($attribute)) {
            // Increment attribute
            $filteredAttr = $this->filter($attribute);
            if ($this->sharedTables) {
                $builder->conflictSetRaw($filteredAttr, $this->getConflictTenantIncrementExpression($filteredAttr));
                $builder->conflictSetRaw('_updatedAt', $this->getConflictTenantExpression('_updatedAt'));
            } else {
                $builder->conflictSetRaw($filteredAttr, $this->getConflictIncrementExpression($filteredAttr));
            }
        } elseif (! empty($operators)) {
            // Operator columns
            foreach ($allColumnNames as $colName) {
                if (\in_array($colName, $skipColumns)) {
                    continue;
                }
                if (isset($operators[$colName])) {
                    $filteredCol = $this->filter($colName);
                    $opResult = $this->getOperatorUpsertExpression($filteredCol, $operators[$colName]);
                    $builder->conflictSetRaw($colName, $opResult['expression'], $opResult['bindings']);
                } elseif ($this->sharedTables) {
                    $builder->conflictSetRaw($colName, $this->getConflictTenantExpression($colName));
                }
            }
        } elseif ($this->sharedTables) {
            // Shared tables without operators or increment: tenant-guard all update columns
            foreach ($updateColumns as $col) {
                $builder->conflictSetRaw($col, $this->getConflictTenantExpression($col));
            }
        }

        $result = $builder->upsert();
        $stmt = $this->executeResult($result, Event::DocumentCreate);
        $stmt->execute();
        $stmt->closeCursor();
    }

    /**
     * Map attribute selections to database column names.
     *
     * Converts user-facing attribute names (like $id, $sequence) to internal
     * database column names (like _uid, _id) and ensures internal columns
     * are always included.
     *
     * @param  array<string>  $selections
     * @return array<string>
     */
    protected function mapSelectionsToColumns(array $selections): array
    {
        $internalKeys = [
            '$id',
            '$sequence',
            '$permissions',
            '$createdAt',
            '$updatedAt',
        ];

        $selections = \array_diff($selections, [...$internalKeys, '$collection']);

        foreach ($internalKeys as $internalKey) {
            $selections[] = $this->getInternalKeyForAttribute($internalKey);
        }

        $columns = [];
        foreach ($selections as $selection) {
            $columns[] = $this->filter($selection);
        }

        return $columns;
    }

    /**
     * Map Database type constants to Schema Blueprint column definitions.
     *
     * @throws DatabaseException
     */
    protected function addBlueprintColumn(
        Blueprint $table,
        string $id,
        ColumnType $type,
        int $size,
        bool $signed = true,
        bool $array = false,
        bool $required = false
    ): Column {
        $filteredId = $this->filter($id);

        if (\in_array($type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon])) {
            $col = match ($type) {
                ColumnType::Point => $table->point($filteredId, Database::DEFAULT_SRID),
                ColumnType::Linestring => $table->linestring($filteredId, Database::DEFAULT_SRID),
                ColumnType::Polygon => $table->polygon($filteredId, Database::DEFAULT_SRID),
            };
            if (! $required) {
                $col->nullable();
            }

            return $col;
        }

        if ($array) {
            // Arrays use JSON type and are nullable by default
            return $table->json($filteredId)->nullable();
        }

        $col = match ($type) {
            ColumnType::String => match (true) {
                $size > 16777215 => $table->longText($filteredId),
                $size > 65535 => $table->mediumText($filteredId),
                $size > $this->getMaxVarcharLength() => $table->text($filteredId),
                $size <= 0 => $table->text($filteredId),
                default => $table->string($filteredId, $size),
            },
            ColumnType::Integer => $size >= 8
                ? $table->bigInteger($filteredId)
                : $table->integer($filteredId),
            ColumnType::Double => $table->float($filteredId),
            ColumnType::Boolean => $table->boolean($filteredId),
            ColumnType::Datetime => $table->datetime($filteredId, 3),
            ColumnType::Relationship => $table->string($filteredId, 255),
            ColumnType::Id => $table->bigInteger($filteredId),
            ColumnType::Varchar => $table->string($filteredId, $size),
            ColumnType::Text => $table->text($filteredId),
            ColumnType::MediumText => $table->mediumText($filteredId),
            ColumnType::LongText => $table->longText($filteredId),
            ColumnType::Object => $table->json($filteredId),
            ColumnType::Vector => $table->vector($filteredId, $size),
            default => throw new DatabaseException('Unknown type: '.$type->value),
        };

        // Apply unsigned for types that support it
        if (! $signed && \in_array($type, [ColumnType::Integer, ColumnType::Double])) {
            $col->unsigned();
        }

        // Id type is always unsigned
        if ($type === ColumnType::Id) {
            $col->unsigned();
        }

        // Non-spatial columns are nullable by default to match existing behavior
        $col->nullable();

        return $col;
    }

    /**
     * Build a key-value row array from a Document for batch INSERT.
     *
     * Converts internal attributes ($id, $createdAt, etc.) to their column names
     * and encodes arrays as JSON. Spatial attributes are included with their raw
     * value (the caller must handle ST_GeomFromText wrapping separately).
     *
     * @param  array<string>  $attributeKeys
     * @param  array<string>  $spatialAttributes
     * @return array<string, mixed>
     */
    /**
     * @param array<string, mixed> $row
     */
    private function remapRow(array &$row): void
    {
        foreach (self::COLUMN_RENAME_MAP as $internal => $public) {
            if (\array_key_exists($internal, $row)) {
                $row[$public] = $row[$internal];
                unset($row[$internal]);
            }
        }
        if (\array_key_exists('_permissions', $row)) {
            $row['$permissions'] = \json_decode(\is_string($row['_permissions']) ? $row['_permissions'] : '[]', true);
            unset($row['_permissions']);
        }
    }

    protected function buildDocumentRow(Document $document, array $attributeKeys, array $spatialAttributes = []): array
    {
        $attributes = $document->getAttributes();
        $row = [
            '_uid' => $document->getId(),
            '_createdAt' => $document->getCreatedAt(),
            '_updatedAt' => $document->getUpdatedAt(),
            '_permissions' => \json_encode($document->getPermissions()),
        ];

        $version = $document->getVersion();
        if ($version !== null) {
            $row['_version'] = $version;
        }

        if (! empty($document->getSequence())) {
            $row['_id'] = $document->getSequence();
        }

        foreach ($attributeKeys as $key) {
            if (isset($row[$key])) {
                continue;
            }
            $value = $attributes[$key] ?? null;
            if (\is_array($value)) {
                $value = \json_encode($value);
            }
            if (! \in_array($key, $spatialAttributes) && $this->supports(Capability::IntegerBooleans)) {
                $value = (\is_bool($value)) ? (int) $value : $value;
            }
            $row[$key] = $value;
        }

        return $row;
    }

    /**
     * Helper method to extract spatial type attributes from collection attributes
     *
     * @return array<int,string>
     */
    protected function getSpatialAttributes(Document $collection): array
    {
        /** @var array<mixed> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);
        $spatialAttributes = [];
        foreach ($collectionAttributes as $attr) {
            if ($attr instanceof Document) {
                $attributeType = $attr->getAttribute('type');
                if (in_array($attributeType, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value])) {
                    $spatialAttributes[] = $attr->getId();
                }
            }
        }

        return $spatialAttributes;
    }

    /**
     * Generate SQL expression for operator
     * Each adapter must implement operators specific to their SQL dialect
     *
     * @return string|null Returns null if operator can't be expressed in SQL
     */
    abstract protected function getOperatorSQL(string $column, Operator $operator, int &$bindIndex): ?string;

    /**
     * Bind operator parameters to prepared statement
     */
    protected function bindOperatorParams(PDOStatement|PDOStatementProxy $stmt, Operator $operator, int &$bindIndex): void
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            // Numeric operators with optional limits
            case OperatorType::Increment:
            case OperatorType::Decrement:
            case OperatorType::Multiply:
            case OperatorType::Divide:
                $value = $values[0] ?? 1;
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $value, $this->getPDOType($value));
                $bindIndex++;

                // Bind limit if provided
                if (isset($values[1])) {
                    $limitKey = "op_{$bindIndex}";
                    $stmt->bindValue(':'.$limitKey, $values[1], $this->getPDOType($values[1]));
                    $bindIndex++;
                }
                break;

            case OperatorType::Modulo:
                $value = $values[0] ?? 1;
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $value, $this->getPDOType($value));
                $bindIndex++;
                break;

            case OperatorType::Power:
                $value = $values[0] ?? 1;
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $value, $this->getPDOType($value));
                $bindIndex++;

                // Bind max limit if provided
                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $stmt->bindValue(':'.$maxKey, $values[1], $this->getPDOType($values[1]));
                    $bindIndex++;
                }
                break;

                // String operators
            case OperatorType::StringConcat:
                $value = $values[0] ?? '';
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $value, PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::StringReplace:
                $search = $values[0] ?? '';
                $replace = $values[1] ?? '';
                $searchKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$searchKey, $search, PDO::PARAM_STR);
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$replaceKey, $replace, PDO::PARAM_STR);
                $bindIndex++;
                break;

                // Boolean operators
            case OperatorType::Toggle:
                // No parameters to bind
                break;

                // Date operators
            case OperatorType::DateAddDays:
            case OperatorType::DateSubDays:
                $days = $values[0] ?? 0;
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $days, PDO::PARAM_INT);
                $bindIndex++;
                break;

            case OperatorType::DateSetNow:
                // No parameters to bind
                break;

                // Array operators
            case OperatorType::ArrayAppend:
            case OperatorType::ArrayPrepend:
                // PERFORMANCE: Validate array size to prevent memory exhaustion
                if (\count($values) > self::MAX_ARRAY_OPERATOR_SIZE) {
                    throw new DatabaseException('Array size '.\count($values).' exceeds maximum allowed size of '.self::MAX_ARRAY_OPERATOR_SIZE.' for array operations');
                }

                // Bind JSON array
                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $arrayValue, PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayRemove:
                $value = $values[0] ?? null;
                $bindKey = "op_{$bindIndex}";
                if (is_array($value)) {
                    $value = json_encode($value);
                }
                $stmt->bindValue(':'.$bindKey, $value, PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayUnique:
                // No parameters to bind
                break;

                // Complex array operators
            case OperatorType::ArrayInsert:
                $index = $values[0] ?? 0;
                $value = $values[1] ?? null;
                $indexKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$indexKey, $index, PDO::PARAM_INT);
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$valueKey, json_encode($value), PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayIntersect:
            case OperatorType::ArrayDiff:
                // PERFORMANCE: Validate array size to prevent memory exhaustion
                if (\count($values) > self::MAX_ARRAY_OPERATOR_SIZE) {
                    throw new DatabaseException('Array size '.\count($values).' exceeds maximum allowed size of '.self::MAX_ARRAY_OPERATOR_SIZE.' for array operations');
                }

                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $arrayValue, PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayFilter:
                $condition = \is_string($values[0] ?? null) ? $values[0] : 'equal';
                $value = $values[1] ?? null;

                $validConditions = [
                    'equal', 'notEqual',  // Comparison
                    'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual',  // Numeric
                    'isNull', 'isNotNull',  // Null checks
                ];
                if (! in_array($condition, $validConditions, true)) {
                    throw new DatabaseException("Invalid filter condition: {$condition}. Must be one of: ".implode(', ', $validConditions));
                }

                $conditionKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$conditionKey, $condition, PDO::PARAM_STR);
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                if ($value !== null) {
                    $stmt->bindValue(':'.$valueKey, json_encode($value), PDO::PARAM_STR);
                } else {
                    $stmt->bindValue(':'.$valueKey, null, PDO::PARAM_NULL);
                }
                $bindIndex++;
                break;
        }
    }

    /**
     * Get the operator expression and positional bindings for use with the query builder's setRaw().
     *
     * Calls getOperatorSQL() to get the expression with named bindings, strips the
     * column assignment prefix, and converts named :op_N bindings to positional ? placeholders.
     *
     * @param  string  $column  The unquoted column name
     * @param  Operator  $operator  The operator to convert
     * @return array{expression: string, bindings: list<mixed>} The expression and binding values
     *
     * @throws DatabaseException
     */
    protected function getOperatorBuilderExpression(string $column, Operator $operator): array
    {
        $bindIndex = 0;
        $fullExpression = $this->getOperatorSQL($column, $operator, $bindIndex);

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

        // Replace each named binding occurrence with ? and collect positional bindings
        // Process longest keys first to avoid partial replacement (e.g., :op_10 vs :op_1)
        $positionalBindings = [];
        $keys = array_keys($namedBindings);
        usort($keys, fn ($a, $b) => strlen($b) - strlen($a));

        // Find all occurrences of all named bindings and sort by position
        $replacements = [];
        foreach ($keys as $key) {
            $search = ':'.$key;
            $offset = 0;
            while (($pos = strpos($expression, $search, $offset)) !== false) {
                $replacements[] = ['pos' => $pos, 'len' => strlen($search), 'key' => $key];
                $offset = $pos + strlen($search);
            }
        }

        // Sort by position (ascending) to replace in order
        usort($replacements, fn ($a, $b) => $a['pos'] - $b['pos']);

        // Replace from right to left to preserve positions
        $result = $expression;
        for ($i = count($replacements) - 1; $i >= 0; $i--) {
            $r = $replacements[$i];
            $result = substr_replace($result, '?', $r['pos'], $r['len']);
        }

        // Collect bindings in positional order (left to right)
        foreach ($replacements as $r) {
            $positionalBindings[] = $namedBindings[$r['key']];
        }

        return ['expression' => $result, 'bindings' => $positionalBindings];
    }

    /**
     * Get a builder-compatible operator expression for use in upsert conflict resolution.
     *
     * By default this delegates to getOperatorBuilderExpression(). Adapters
     * that need to reference the existing row differently in upsert context
     * (e.g. Postgres using target.col) should override this method.
     *
     * @param  string  $column  The unquoted, filtered column name
     * @param  Operator  $operator  The operator to convert
     * @return array{expression: string, bindings: list<mixed>}
     */
    protected function getOperatorUpsertExpression(string $column, Operator $operator): array
    {
        return $this->getOperatorBuilderExpression($column, $operator);
    }

    /**
     * Apply an operator to a value (used for new documents with only operators).
     * This method applies the operator logic in PHP to compute what the SQL would compute.
     *
     * @param  mixed  $value  The current value (typically the attribute default)
     * @return mixed The result after applying the operator
     */
    protected function applyOperatorToValue(Operator $operator, mixed $value): mixed
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        $numVal = is_numeric($value) ? $value + 0 : 0;
        $firstValue = count($values) > 0 ? $values[0] : null;
        $numOp = is_numeric($firstValue) ? $firstValue + 0 : 1;
        /** @var array<mixed> $arrVal */
        $arrVal = is_array($value) ? $value : [];

        return match ($method) {
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
                $arr = $arrVal;
                $toRemove = $values[0] ?? null;

                return is_array($toRemove)
                    ? array_values(array_diff($arr, $toRemove))
                    : array_values(array_diff($arr, [$toRemove]));
            })(),
            OperatorType::ArrayUnique => array_values(array_unique($arrVal)),
            OperatorType::ArrayIntersect => array_values(array_intersect($arrVal, $values)),
            OperatorType::ArrayDiff => array_values(array_diff($arrVal, $values)),
            OperatorType::ArrayFilter => $arrVal,
            OperatorType::StringConcat => (\is_scalar($value) ? (string) $value : '') . (count($values) > 0 && \is_scalar($values[0]) ? (string) $values[0] : ''),
            OperatorType::StringReplace => str_replace(count($values) > 0 && \is_scalar($values[0]) ? (string) $values[0] : '', count($values) > 1 && \is_scalar($values[1]) ? (string) $values[1] : '', \is_scalar($value) ? (string) $value : ''),
            OperatorType::Toggle => ! ($value ?? false),
            OperatorType::DateAddDays,
            OperatorType::DateSubDays => $value,
            OperatorType::DateSetNow => DateTime::now(),
        };
    }

    /**
     * Whether the adapter requires an alias on INSERT for conflict resolution.
     *
     * PostgreSQL needs INSERT INTO table AS target so that the ON CONFLICT
     * clause can reference the existing row via target.column. MariaDB does
     * not need this because it uses VALUES(column) syntax.
     */
    abstract protected function insertRequiresAlias(): bool;

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
     * Get PDO Type
     *
     * @throws Exception
     */
    abstract protected function getPDOType(mixed $value): int;

    /**
     * Get the SQL function for random ordering
     */
    abstract protected function getRandomOrder(): string;

    /**
     * Get SQL Operator
     *
     * @throws Exception
     */
    protected function getSQLOperator(Method $method): string
    {
        return match ($method) {
            Method::Equal => '=',
            Method::NotEqual => '!=',
            Method::LessThan => '<',
            Method::LessThanEqual => '<=',
            Method::GreaterThan => '>',
            Method::GreaterThanEqual => '>=',
            Method::IsNull => 'IS NULL',
            Method::IsNotNull => 'IS NOT NULL',
            Method::StartsWith,
            Method::EndsWith,
            Method::Contains,
            Method::ContainsAny,
            Method::ContainsAll,
            Method::NotStartsWith,
            Method::NotEndsWith,
            Method::NotContains => $this->getLikeOperator(),
            Method::Regex => $this->getRegexOperator(),
            Method::VectorDot,
            Method::VectorCosine,
            Method::VectorEuclidean => throw new DatabaseException('Vector queries are not supported by this database'),
            Method::Exists,
            Method::NotExists => throw new DatabaseException('Exists queries are not supported by this database'),
            default => throw new DatabaseException('Unknown method: '.$method->value),
        };
    }

    /**
     * @param  array<string, mixed>  $binds
     *
     * @throws Exception
     */
    abstract protected function getSQLCondition(Query $query, array &$binds): string;

    /**
     * Build a combined SQL WHERE clause from multiple query objects.
     *
     * @param  array<Query>  $queries
     * @param  array<string, mixed>  $binds
     * @param  string  $separator  The logical operator joining conditions (AND/OR)
     * @return string
     *
     * @throws Exception
     */
    public function getSQLConditions(array $queries, array &$binds, string $separator = 'AND'): string
    {
        $conditions = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select) {
                continue;
            }

            if ($query->isNested()) {
                /** @var array<Query> $nestedQueries */
                $nestedQueries = $query->getValues();
                $conditions[] = $this->getSQLConditions($nestedQueries, $binds, strtoupper($query->getMethod()->value));
            } else {
                $conditions[] = $this->getSQLCondition($query, $binds);
            }
        }

        $tmp = implode(' '.$separator.' ', $conditions);

        return empty($tmp) ? '' : '('.$tmp.')';
    }

    protected function getFulltextValue(string $value): string
    {
        $exact = str_ends_with($value, '"') && str_starts_with($value, '"');

        /** Replace reserved chars with space. */
        $specialChars = '@,+,-,*,),(,<,>,~,"';
        $value = str_replace(explode(',', $specialChars), ' ', $value);
        $value = (string) preg_replace('/\s+/', ' ', $value); // Remove multiple whitespaces
        $value = trim($value);

        if (empty($value)) {
            return '';
        }

        if ($exact) {
            $value = '"'.$value.'"';
        } else {
            /** Prepend wildcard by default on the back. */
            $value .= '*';
        }

        return $value;
    }

    /**
     * Get vector distance calculation for ORDER BY clause (named binds - legacy).
     *
     * @param  array<string, mixed>  $binds
     */
    protected function getVectorDistanceOrder(Query $query, array &$binds, string $alias): ?string
    {
        return null;
    }

    /**
     * Get vector distance ORDER BY expression with positional bindings.
     *
     * Returns null when vectors are unsupported. Subclasses that support vectors
     * should override this to return the expression string with `?` placeholders
     * and the matching binding values.
     *
     * @return array{expression: string, bindings: list<mixed>}|null
     */
    protected function getVectorOrderRaw(Query $query, string $alias): ?array
    {
        return null;
    }

    /**
     * Get the SQL LIKE operator for this adapter.
     *
     * @return string
     */
    public function getLikeOperator(): string
    {
        return 'LIKE';
    }

    /**
     * Get the SQL regex matching operator for this adapter.
     *
     * @return string
     */
    public function getRegexOperator(): string
    {
        return 'REGEXP';
    }

    public function getSchemaIndexes(string $collection): array
    {
        return [];
    }

    public function getSupportForSchemaIndexes(): bool
    {
        return false;
    }

    /**
     * Get the SQL tenant filter clause for shared-table queries.
     *
     * @param string $collection The collection name
     * @param string $alias Optional table alias
     * @param int $tenantCount Number of tenant values for IN clause
     * @param string $condition The logical condition prefix (AND/WHERE)
     * @return string
     *
     * @deprecated Use TenantFilter hook with the query builder instead.
     */
    public function getTenantQuery(
        string $collection,
        string $alias = '',
        int $tenantCount = 0,
        string $condition = 'AND'
    ): string {
        if (! $this->sharedTables) {
            return '';
        }

        $dot = '';
        if ($alias !== '') {
            $dot = '.';
            $alias = $this->quote($alias);
        }

        $bindings = [];
        if ($tenantCount === 0) {
            $bindings[] = ':_tenant';
        } else {
            for ($index = 0; $index < $tenantCount; $index++) {
                $bindings[] = ":_tenant_{$index}";
            }
        }
        $bindings = \implode(',', $bindings);

        $orIsNull = '';
        if ($collection === Database::METADATA) {
            $orIsNull = " OR {$alias}{$dot}_tenant IS NULL";
        }

        return "{$condition} ({$alias}{$dot}_tenant IN ({$bindings}) {$orIsNull})";
    }

    /**
     * Get the SQL projection given the selected attributes
     *
     * @param  array<string>  $selections
     *
     * @throws Exception
     */
    protected function getAttributeProjection(array $selections, string $prefix): mixed
    {
        if (empty($selections) || \in_array('*', $selections)) {
            return "{$this->quote($prefix)}.*";
        }

        // Handle specific selections with spatial conversion where needed
        $internalKeys = [
            '$id',
            '$sequence',
            '$permissions',
            '$createdAt',
            '$updatedAt',
        ];

        $selections = \array_diff($selections, [...$internalKeys, '$collection']);

        foreach ($internalKeys as $internalKey) {
            $selections[] = $this->getInternalKeyForAttribute($internalKey);
        }

        $projections = [];
        foreach ($selections as $selection) {
            $filteredSelection = $this->filter($selection);
            $quotedSelection = $this->quote($filteredSelection);
            $projections[] = "{$this->quote($prefix)}.{$quotedSelection}";
        }

        return \implode(',', $projections);
    }

    protected function getInternalKeyForAttribute(string $attribute): string
    {
        return match ($attribute) {
            '$id' => '_uid',
            '$sequence' => '_id',
            '$collection' => '_collection',
            '$tenant' => '_tenant',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            '$permissions' => '_permissions',
            '$version' => '_version',
            default => $attribute
        };
    }

    protected function escapeWildcards(string $value): string
    {
        $wildcards = ['%', '_', '[', ']', '^', '-', '.', '*', '+', '?', '(', ')', '{', '}', '|'];

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
     * Extract search queries from the query list (non-destructive).
     *
     * @param array<Query> $queries
     * @return array<Query>
     */
    protected function extractSearchQueries(array $queries): array
    {
        $searchQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Search) {
                $searchQueries[] = $query;
            }
        }

        return $searchQueries;
    }

    /**
     * Get the raw SQL expression for full-text search relevance scoring.
     *
     * @return array{expression: string, order: string, bindings: list<mixed>}|null
     */
    protected function getSearchRelevanceRaw(Query $query, string $alias): ?array
    {
        return null;
    }
}
