<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDOException;
use Swoole\Database\PDOStatementProxy;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\CursorDirection;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
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
use Utopia\Database\OrderDirection;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook\Attribute\Map as AttributeMap;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

abstract class SQL extends Adapter implements Feature\ConnectionId, Feature\Relationships, Feature\SchemaAttributes, Feature\Spatial, Feature\Upserts
{
    protected mixed $pdo;

    /**
     * Maximum array size for array operations to prevent memory exhaustion.
     * Large arrays in JSON_TABLE operations can cause significant memory usage.
     */
    protected const MAX_ARRAY_OPERATOR_SIZE = 10000;

    /**
     * Controls how many fractional digits are used when binding float parameters.
     */
    protected int $floatPrecision = 17;

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
     * Constructor.
     *
     * Set connection and settings
     */
    public function __construct(mixed $pdo)
    {
        $this->pdo = $pdo;
    }

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
        ]);
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

    public function reconnect(): void
    {
        $this->getPDO()->reconnect();
        $this->inTransaction = 0;
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
                    \Utopia\Query\Query::equal('TABLE_SCHEMA', [$database]),
                    \Utopia\Query\Query::equal('TABLE_NAME', ["{$this->getNamespace()}_{$collection}"]),
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
                ->filter([\Utopia\Query\Query::equal('SCHEMA_NAME', [$database])])
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
        $result = $schema->alter($this->getSQLTableRaw($collection), function (\Utopia\Query\Schema\Blueprint $table) use ($attribute) {
            $this->addBlueprintColumn($table, $attribute->key, $attribute->type->value, $attribute->size, $attribute->signed, $attribute->array, $attribute->required);
        });

        $sql = $result->query;
        $lockType = $this->getLockType();
        if (! empty($lockType)) {
            $sql = rtrim($sql, ';').' '.$lockType;
        }
        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

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
        $result = $schema->alter($this->getSQLTableRaw($collection), function (\Utopia\Query\Schema\Blueprint $table) use ($attributes) {
            foreach ($attributes as $attribute) {
                $this->addBlueprintColumn(
                    $table,
                    $attribute->key,
                    $attribute->type->value,
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
        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

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
        $result = $schema->alter($this->getSQLTableRaw($collection), function (\Utopia\Query\Schema\Blueprint $table) use ($old, $new) {
            $table->renameColumn($this->filter($old), $this->filter($new));
        });

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $result->query);

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
        $result = $schema->alter($this->getSQLTableRaw($collection), function (\Utopia\Query\Schema\Blueprint $table) use ($id) {
            $table->dropColumn($this->filter($id));
        });

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $result->query);

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

        $builder->filter([\Utopia\Query\Query::equal('_uid', [$id])]);

        if ($forUpdate && $this->supports(Capability::UpdateLock)) {
            $builder->forUpdate();
        }

        $result = $builder->build();
        $stmt = $this->executeResult($result);
        $stmt->execute();
        $document = $stmt->fetchAll();
        $stmt->closeCursor();

        if (empty($document)) {
            return new Document([]);
        }

        $document = $document[0];

        if (\array_key_exists('_id', $document)) {
            $document['$sequence'] = $document['_id'];
            unset($document['_id']);
        }
        if (\array_key_exists('_uid', $document)) {
            $document['$id'] = $document['_uid'];
            unset($document['_uid']);
        }
        if (\array_key_exists('_tenant', $document)) {
            $document['$tenant'] = $document['_tenant'];
            unset($document['_tenant']);
        }
        if (\array_key_exists('_createdAt', $document)) {
            $document['$createdAt'] = $document['_createdAt'];
            unset($document['_createdAt']);
        }
        if (\array_key_exists('_updatedAt', $document)) {
            $document['$updatedAt'] = $document['_updatedAt'];
            unset($document['_updatedAt']);
        }
        if (\array_key_exists('_permissions', $document)) {
            $document['$permissions'] = json_decode($document['_permissions'] ?? '[]', true);
            unset($document['_permissions']);
        }

        return new Document($document);
    }

    /**
     * Helper method to extract spatial type attributes from collection attributes
     *
     * @return array<int,string>
     */
    protected function getSpatialAttributes(Document $collection): array
    {
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
            $opResult = $this->getOperatorBuilderExpression($column, $operator);
            $builder->setRaw($column, $opResult['expression'], $opResult['bindings']);
        }

        // WHERE _id IN (sequence values)
        $sequences = \array_map(fn ($document) => $document->getSequence(), $documents);
        $builder->filter([\Utopia\Query\Query::equal('_id', \array_values($sequences))]);

        $result = $builder->update();
        $stmt = $this->executeResult($result, Database::EVENT_DOCUMENTS_UPDATE);

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        $affected = $stmt->rowCount();

        $ctx = $this->buildWriteContext($name);
        foreach ($this->writeHooks as $hook) {
            $hook->afterDocumentBatchUpdate($name, $updates, $documents, $ctx);
        }

        return $affected;
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
            $builder->filter([\Utopia\Query\Query::equal('_id', \array_values($sequences))]);
            $result = $builder->delete();
            $stmt = $this->executeResult($result, Database::EVENT_DOCUMENTS_DELETE);

            if (! $stmt->execute()) {
                throw new DatabaseException('Failed to delete documents');
            }

            $ctx = $this->buildWriteContext($name);
            foreach ($this->writeHooks as $hook) {
                $hook->afterDocumentDelete($name, $permissionIds, $ctx);
            }
        } catch (\Throwable $e) {
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
        $builder->filter([\Utopia\Query\Query::equal('_uid', $documentIds)]);

        $result = $builder->build();
        $stmt = $this->executeResult($result);
        $stmt->execute();
        $sequences = $stmt->fetchAll(\PDO::FETCH_KEY_PAIR); // Fetch as [documentId => sequence]
        $stmt->closeCursor();

        foreach ($documents as $document) {
            if (isset($sequences[$document->getId()])) {
                $document['$sequence'] = $sequences[$document->getId()];
            }
        }

        return $documents;
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
        $attributes = \count($collection->getAttribute('attributes') ?? []);

        return $attributes + $this->getCountOfDefaultAttributes();
    }

    /**
     * Get current index count from collection document
     */
    public function getCountOfIndexes(Document $collection): int
    {
        $indexes = \count($collection->getAttribute('indexes') ?? []);

        return $indexes + $this->getCountOfDefaultIndexes();
    }

    /**
     * Returns number of attributes used by default.
     */
    public function getCountOfDefaultAttributes(): int
    {
        return \count(Database::INTERNAL_ATTRIBUTES);
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

            switch ($attribute['type']) {
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
                        $attribute['size'] > $this->getMaxVarcharLength() => 20,
                        $attribute['size'] > 255 => $attribute['size'] * 4 + 2, //  VARCHAR(>255) + 2 length
                        default => $attribute['size'] * 4 + 1, //  VARCHAR(<=255) + 1 length
                    };

                    break;

                case ColumnType::Varchar->value:
                    $total += match (true) {
                        $attribute['size'] > 255 => $attribute['size'] * 4 + 2, //  VARCHAR(>255) + 2 length
                        default => $attribute['size'] * 4 + 1, //  VARCHAR(<=255) + 1 length
                    };
                    break;

                case ColumnType::Text->value:
                case ColumnType::MediumText->value:
                case ColumnType::LongText->value:
                    $total += 20; // Pointer storage for TEXT types
                    break;

                case ColumnType::Integer->value:
                    if ($attribute['size'] >= 8) {
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
                    $total += ($attribute['size'] ?? 0) * 4;
                    break;

                default:
                    throw new DatabaseException('Unknown type: '.$attribute['type']);
            }
        }

        return $total;
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

    protected function getFulltextValue(string $value): string
    {
        $exact = str_ends_with($value, '"') && str_starts_with($value, '"');

        /** Replace reserved chars with space. */
        $specialChars = '@,+,-,*,),(,<,>,~,"';
        $value = str_replace(explode(',', $specialChars), ' ', $value);
        $value = preg_replace('/\s+/', ' ', $value); // Remove multiple whitespaces
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
     * Get SQL Operator
     *
     * @throws Exception
     */
    protected function getSQLOperator(\Utopia\Query\Method $method): string
    {
        return match ($method) {
            Query::TYPE_EQUAL => '=',
            Query::TYPE_NOT_EQUAL => '!=',
            Query::TYPE_LESSER => '<',
            Query::TYPE_LESSER_EQUAL => '<=',
            Query::TYPE_GREATER => '>',
            Query::TYPE_GREATER_EQUAL => '>=',
            Query::TYPE_IS_NULL => 'IS NULL',
            Query::TYPE_IS_NOT_NULL => 'IS NOT NULL',
            Query::TYPE_STARTS_WITH,
            Query::TYPE_ENDS_WITH,
            Query::TYPE_CONTAINS,
            Query::TYPE_CONTAINS_ANY,
            Query::TYPE_CONTAINS_ALL,
            Query::TYPE_NOT_STARTS_WITH,
            Query::TYPE_NOT_ENDS_WITH,
            Query::TYPE_NOT_CONTAINS => $this->getLikeOperator(),
            Query::TYPE_REGEX => $this->getRegexOperator(),
            Query::TYPE_VECTOR_DOT,
            Query::TYPE_VECTOR_COSINE,
            Query::TYPE_VECTOR_EUCLIDEAN => throw new DatabaseException('Vector queries are not supported by this database'),
            Query::TYPE_EXISTS,
            Query::TYPE_NOT_EXISTS => throw new DatabaseException('Exists queries are not supported by this database'),
            default => throw new DatabaseException('Unknown method: '.$method->value),
        };
    }

    abstract protected function getSQLType(
        string $type,
        int $size,
        bool $signed = true,
        bool $array = false,
        bool $required = false
    ): string;

    /**
     * Create a new query builder instance for this adapter's SQL dialect.
     */
    abstract protected function createBuilder(): \Utopia\Query\Builder\SQL;

    /**
     * Create a new schema builder instance for this adapter's SQL dialect.
     */
    abstract protected function createSchemaBuilder(): \Utopia\Query\Schema;

    /**
     * @throws DatabaseException For unknown type values.
     */
    public function getColumnType(string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        return $this->getSQLType($type, $size, $signed, $array, $required);
    }

    /**
     * Get SQL Index Type
     *
     * @throws Exception
     */
    protected function getSQLIndexType(string $type): string
    {
        return match ($type) {
            IndexType::Key->value => 'INDEX',
            IndexType::Unique->value => 'UNIQUE INDEX',
            IndexType::Fulltext->value => 'FULLTEXT INDEX',
            default => throw new DatabaseException('Unknown index type: '.$type.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value),
        };
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
     * Create and configure a new query builder for a given table.
     *
     * Automatically applies tenant filtering when shared tables are enabled.
     *
     * @throws DatabaseException
     */
    protected function newBuilder(string $table, string $alias = ''): \Utopia\Query\Builder\SQL
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

    /**
     * Create a configured Permission hook for permission subquery filtering.
     *
     * @param  string  $collection  The collection name (used to derive the permissions table)
     * @param  array<string>  $roles  The roles to check permissions for
     * @param  string  $type  The permission type (read, create, update, delete)
     * @return PermissionFilter
     *
     * @throws DatabaseException
     */
    protected function getIdentifierQuoteChar(): string
    {
        return '`';
    }

    protected function newPermissionHook(string $collection, array $roles, string $type = PermissionType::Read->value): PermissionFilter
    {
        return new PermissionFilter(
            roles: $roles,
            permissionsTable: fn (string $table) => $this->getSQLTableRaw($collection.'_perms'),
            type: $type,
            documentColumn: '_uid',
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
            $this->addWriteHook(new PermissionWrite);
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
            executeResult: fn (\Utopia\Query\Builder\BuildResult $result, ?string $event = null) => $this->executeResult($result, $event),
            execute: fn (mixed $stmt) => $this->execute($stmt),
            decorateRow: fn (array $row, array $metadata) => $this->decorateRow($row, $metadata),
            createBuilder: fn () => $this->createBuilder(),
            getTableRaw: fn (string $table) => $this->getSQLTableRaw($table),
        );
    }

    /**
     * Execute a BuildResult through the trigger system with positional bindings.
     *
     * Prepares the SQL statement and binds positional parameters from the BuildResult.
     * Does NOT call execute() - the caller is responsible for that.
     *
     * @param  string|null  $event  Optional event name to run through trigger system
     */
    protected function executeResult(\Utopia\Query\Builder\BuildResult $result, ?string $event = null): mixed
    {
        $sql = $result->query;
        if ($event !== null) {
            $sql = $this->trigger($event, $sql);
        }
        $stmt = $this->getPDO()->prepare($sql);
        foreach ($result->bindings as $i => $value) {
            if (\is_bool($value) && $this->supports(Capability::IntegerBooleans)) {
                $value = (int) $value;
            }
            if (\is_float($value)) {
                $stmt->bindValue($i + 1, $this->getFloatPrecision($value), \PDO::PARAM_STR);
            } else {
                $stmt->bindValue($i + 1, $value, $this->getPDOType($value));
            }
        }

        return $stmt;
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
        \Utopia\Query\Schema\Blueprint $table,
        string $id,
        string $type,
        int $size,
        bool $signed = true,
        bool $array = false,
        bool $required = false
    ): \Utopia\Query\Schema\Column {
        $filteredId = $this->filter($id);

        if (\in_array($type, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value])) {
            $col = match ($type) {
                ColumnType::Point->value => $table->point($filteredId, Database::DEFAULT_SRID),
                ColumnType::Linestring->value => $table->linestring($filteredId, Database::DEFAULT_SRID),
                ColumnType::Polygon->value => $table->polygon($filteredId, Database::DEFAULT_SRID),
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
            ColumnType::String->value => match (true) {
                $size > 16777215 => $table->longText($filteredId),
                $size > 65535 => $table->mediumText($filteredId),
                $size > $this->getMaxVarcharLength() => $table->text($filteredId),
                $size <= 0 => $table->text($filteredId),
                default => $table->string($filteredId, $size),
            },
            ColumnType::Integer->value => $size >= 8
                ? $table->bigInteger($filteredId)
                : $table->integer($filteredId),
            ColumnType::Double->value => $table->float($filteredId),
            ColumnType::Boolean->value => $table->boolean($filteredId),
            ColumnType::Datetime->value => $table->datetime($filteredId, 3),
            ColumnType::Relationship->value => $table->string($filteredId, 255),
            ColumnType::Id->value => $table->bigInteger($filteredId),
            ColumnType::Varchar->value => $table->string($filteredId, $size),
            ColumnType::Text->value => $table->text($filteredId),
            ColumnType::MediumText->value => $table->mediumText($filteredId),
            ColumnType::LongText->value => $table->longText($filteredId),
            ColumnType::Object->value => $table->json($filteredId),
            ColumnType::Vector->value => $table->vector($filteredId, $size),
            default => throw new DatabaseException('Unknown type: '.$type),
        };

        // Apply unsigned for types that support it
        if (! $signed && \in_array($type, [ColumnType::Integer->value, ColumnType::Double->value])) {
            $col->unsigned();
        }

        // Id type is always unsigned
        if ($type === ColumnType::Id->value) {
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
    protected function buildDocumentRow(Document $document, array $attributeKeys, array $spatialAttributes = []): array
    {
        $attributes = $document->getAttributes();
        $row = [
            '_uid' => $document->getId(),
            '_createdAt' => $document->getCreatedAt(),
            '_updatedAt' => $document->getUpdatedAt(),
            '_permissions' => \json_encode($document->getPermissions()),
        ];

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
     * Generate SQL expression for operator
     * Each adapter must implement operators specific to their SQL dialect
     *
     * @return string|null Returns null if operator can't be expressed in SQL
     */
    abstract protected function getOperatorSQL(string $column, Operator $operator, int &$bindIndex): ?string;

    /**
     * Bind operator parameters to prepared statement
     */
    protected function bindOperatorParams(\PDOStatement|PDOStatementProxy $stmt, Operator $operator, int &$bindIndex): void
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            // Numeric operators with optional limits
            case OperatorType::Increment->value:
            case OperatorType::Decrement->value:
            case OperatorType::Multiply->value:
            case OperatorType::Divide->value:
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

            case OperatorType::Modulo->value:
                $value = $values[0] ?? 1;
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $value, $this->getPDOType($value));
                $bindIndex++;
                break;

            case OperatorType::Power->value:
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
            case OperatorType::StringConcat->value:
                $value = $values[0] ?? '';
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $value, \PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::StringReplace->value:
                $search = $values[0] ?? '';
                $replace = $values[1] ?? '';
                $searchKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$searchKey, $search, \PDO::PARAM_STR);
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$replaceKey, $replace, \PDO::PARAM_STR);
                $bindIndex++;
                break;

                // Boolean operators
            case OperatorType::Toggle->value:
                // No parameters to bind
                break;

                // Date operators
            case OperatorType::DateAddDays->value:
            case OperatorType::DateSubDays->value:
                $days = $values[0] ?? 0;
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $days, \PDO::PARAM_INT);
                $bindIndex++;
                break;

            case OperatorType::DateSetNow->value:
                // No parameters to bind
                break;

                // Array operators
            case OperatorType::ArrayAppend->value:
            case OperatorType::ArrayPrepend->value:
                // PERFORMANCE: Validate array size to prevent memory exhaustion
                if (\count($values) > self::MAX_ARRAY_OPERATOR_SIZE) {
                    throw new DatabaseException('Array size '.\count($values).' exceeds maximum allowed size of '.self::MAX_ARRAY_OPERATOR_SIZE.' for array operations');
                }

                // Bind JSON array
                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $arrayValue, \PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayRemove->value:
                $value = $values[0] ?? null;
                $bindKey = "op_{$bindIndex}";
                if (is_array($value)) {
                    $value = json_encode($value);
                }
                $stmt->bindValue(':'.$bindKey, $value, \PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayUnique->value:
                // No parameters to bind
                break;

                // Complex array operators
            case OperatorType::ArrayInsert->value:
                $index = $values[0] ?? 0;
                $value = $values[1] ?? null;
                $indexKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$indexKey, $index, \PDO::PARAM_INT);
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$valueKey, json_encode($value), \PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayIntersect->value:
            case OperatorType::ArrayDiff->value:
                // PERFORMANCE: Validate array size to prevent memory exhaustion
                if (\count($values) > self::MAX_ARRAY_OPERATOR_SIZE) {
                    throw new DatabaseException('Array size '.\count($values).' exceeds maximum allowed size of '.self::MAX_ARRAY_OPERATOR_SIZE.' for array operations');
                }

                $arrayValue = json_encode($values);
                $bindKey = "op_{$bindIndex}";
                $stmt->bindValue(':'.$bindKey, $arrayValue, \PDO::PARAM_STR);
                $bindIndex++;
                break;

            case OperatorType::ArrayFilter->value:
                $condition = $values[0] ?? 'equal';
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
                $stmt->bindValue(':'.$conditionKey, $condition, \PDO::PARAM_STR);
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                if ($value !== null) {
                    $stmt->bindValue(':'.$valueKey, json_encode($value), \PDO::PARAM_STR);
                } else {
                    $stmt->bindValue(':'.$valueKey, null, \PDO::PARAM_NULL);
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
                $namedBindings["op_{$idx}"] = is_array($value) ? json_encode($value) : $value;
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

        return match ($method) {
            OperatorType::Increment->value => ($value ?? 0) + ($values[0] ?? 1),
            OperatorType::Decrement->value => ($value ?? 0) - ($values[0] ?? 1),
            OperatorType::Multiply->value => ($value ?? 0) * ($values[0] ?? 1),
            OperatorType::Divide->value => (float) ($values[0] ?? 1) !== 0.0 ? ($value ?? 0) / ($values[0] ?? 1) : ($value ?? 0),
            OperatorType::Modulo->value => (float) ($values[0] ?? 1) !== 0.0 ? ($value ?? 0) % ($values[0] ?? 1) : ($value ?? 0),
            OperatorType::Power->value => pow($value ?? 0, $values[0] ?? 1),
            OperatorType::ArrayAppend->value => array_merge($value ?? [], $values),
            OperatorType::ArrayPrepend->value => array_merge($values, $value ?? []),
            OperatorType::ArrayInsert->value => (function () use ($value, $values) {
                $arr = $value ?? [];
                array_splice($arr, $values[0] ?? 0, 0, [$values[1] ?? null]);

                return $arr;
            })(),
            OperatorType::ArrayRemove->value => (function () use ($value, $values) {
                $arr = $value ?? [];
                $toRemove = $values[0] ?? null;

                return is_array($toRemove)
                    ? array_values(array_diff($arr, $toRemove))
                    : array_values(array_diff($arr, [$toRemove]));
            })(),
            OperatorType::ArrayUnique->value => array_values(array_unique($value ?? [])),
            OperatorType::ArrayIntersect->value => array_values(array_intersect($value ?? [], $values)),
            OperatorType::ArrayDiff->value => array_values(array_diff($value ?? [], $values)),
            OperatorType::ArrayFilter->value => $value ?? [],
            OperatorType::StringConcat->value => ($value ?? '').($values[0] ?? ''),
            OperatorType::StringReplace->value => str_replace($values[0] ?? '', $values[1] ?? '', $value ?? ''),
            OperatorType::Toggle->value => ! ($value ?? false),
            OperatorType::DateAddDays->value,
            OperatorType::DateSubDays->value => $value,
            OperatorType::DateSetNow->value => DateTime::now(),
            default => $value,
        };
    }

    /**
     * Returns the current PDO object
     */
    protected function getPDO(): mixed
    {
        return $this->pdo;
    }

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
     * Returns default PDO configuration
     *
     * @return array<int, mixed>
     */
    public static function getPDOAttributes(): array
    {
        return [
            \PDO::ATTR_TIMEOUT => 3, // Specifies the timeout duration in seconds. Takes a value of type int.
            \PDO::ATTR_PERSISTENT => true, // Create a persistent connection
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC, // Fetch a result row as an associative array.
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION, // PDO will throw a PDOException on errors
            \PDO::ATTR_EMULATE_PREPARES => true, // Emulate prepared statements
            \PDO::ATTR_STRINGIFY_FETCHES => true, // Returns all fetched data as Strings
        ];
    }

    public function getHostname(): string
    {
        try {
            return $this->pdo->getHostname();
        } catch (\Throwable) {
            return '';
        }
    }

    public function getMaxVarcharLength(): int
    {
        return 16381; // Floor value for Postgres:16383 | MySQL:16381 | MariaDB:16382
    }

    /**
     * Size of POINT spatial type
     */
    abstract protected function getMaxPointSize(): int;

    public function getIdAttributeType(): string
    {
        return ColumnType::Integer->value;
    }

    public function getMaxIndexLength(): int
    {
        /**
         * $tenant int = 1
         */
        return $this->sharedTables ? 767 : 768;
    }

    public function getMaxUIDLength(): int
    {
        return 36;
    }

    /**
     * @param  array<string, mixed>  $binds
     *
     * @throws Exception
     */
    abstract protected function getSQLCondition(Query $query, array &$binds): string;

    /**
     * @param  array<Query>  $queries
     * @param  array<string, mixed>  $binds
     *
     * @throws Exception
     */
    public function getSQLConditions(array $queries, array &$binds, string $separator = 'AND'): string
    {
        $conditions = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                continue;
            }

            if ($query->isNested()) {
                $conditions[] = $this->getSQLConditions($query->getValues(), $binds, strtoupper($query->getMethod()->value));
            } else {
                $conditions[] = $this->getSQLCondition($query, $binds);
            }
        }

        $tmp = implode(' '.$separator.' ', $conditions);

        return empty($tmp) ? '' : '('.$tmp.')';
    }

    public function getLikeOperator(): string
    {
        return 'LIKE';
    }

    public function getRegexOperator(): string
    {
        return 'REGEXP';
    }

    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    /**
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

    protected function processException(PDOException $e): \Exception
    {
        return $e;
    }

    protected function execute(mixed $stmt): bool
    {
        return $stmt->execute();
    }

    /**
     * Create Documents in batches
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DuplicateException
     * @throws \Throwable
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

            $attributeKeys = Database::INTERNAL_ATTRIBUTE_KEYS;

            $hasSequence = null;
            foreach ($documents as $document) {
                $attributes = $document->getAttributes();
                $attributeKeys = [...$attributeKeys, ...\array_keys($attributes)];

                if ($hasSequence === null) {
                    $hasSequence = ! empty($document->getSequence());
                } elseif ($hasSequence == empty($document->getSequence())) {
                    throw new DatabaseException('All documents must have an sequence if one is set');
                }
            }

            $attributeKeys = array_unique($attributeKeys);

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
            $stmt = $this->executeResult($result, Database::EVENT_DOCUMENT_CREATE);
            $this->execute($stmt);

            $ctx = $this->buildWriteContext($name);
            foreach ($this->writeHooks as $hook) {
                $hook->afterDocumentCreate($name, $documents, $ctx);
            }

        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $documents;
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

            $attributeDefaults = [];
            foreach ($collection->getAttribute('attributes', []) as $attr) {
                $attributeDefaults[$attr['$id']] = $attr['default'] ?? null;
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
                            $parts[] = $attr.':'.$op->getMethod().':'.json_encode($op->getValues());
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
            foreach ($this->writeHooks as $hook) {
                $hook->afterDocumentUpsert($name, $changes, $ctx);
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return \array_map(fn ($change) => $change->getNew(), $changes);
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
        $stmt = $this->executeResult($result, Database::EVENT_DOCUMENT_CREATE);
        $stmt->execute();
        $stmt->closeCursor();
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
     * Find Documents
     *
     * @param  array<Query>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<string>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<Document>
     *
     * @throws DatabaseException
     * @throws TimeoutException
     * @throws Exception
     */
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = CursorDirection::After->value, string $forPermission = PermissionType::Read->value): array
    {
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

        $builder = $this->newBuilder($name, $alias);

        // Selections
        $selections = $this->getAttributeSelections($queries);
        if (! empty($selections) && ! \in_array('*', $selections)) {
            $builder->select($this->mapSelectionsToColumns($selections));
        }

        // Filter conditions from queries
        $builder->filter($queries);

        // Permission subquery
        if ($this->authorization->getStatus()) {
            $builder->addHook($this->newPermissionHook($name, $roles, $forPermission));
        }

        // Cursor pagination - build nested Query objects for complex multi-attribute cursor conditions
        if (! empty($cursor)) {
            $cursorConditions = [];

            foreach ($orderAttributes as $i => $originalAttribute) {
                $orderType = $orderTypes[$i] ?? OrderDirection::ASC->value;
                if ($orderType === OrderDirection::RANDOM->value) {
                    continue;
                }

                $orderType = $this->filter($orderType);
                $direction = $orderType;

                if ($cursorDirection === CursorDirection::Before->value) {
                    $direction = ($direction === OrderDirection::ASC->value)
                        ? OrderDirection::DESC->value
                        : OrderDirection::ASC->value;
                }

                $internalAttr = $this->filter($this->getInternalKeyForAttribute($originalAttribute));

                // Special case: single attribute on unique primary key
                if (count($orderAttributes) === 1 && $i === 0 && $originalAttribute === '$sequence') {
                    if ($direction === OrderDirection::DESC->value) {
                        $cursorConditions[] = \Utopia\Query\Query::lessThan($internalAttr, $cursor[$originalAttribute]);
                    } else {
                        $cursorConditions[] = \Utopia\Query\Query::greaterThan($internalAttr, $cursor[$originalAttribute]);
                    }
                    break;
                }

                // Multi-attribute cursor: (prev_attrs equal) AND (current_attr > or < cursor)
                $andConditions = [];

                for ($j = 0; $j < $i; $j++) {
                    $prevOriginal = $orderAttributes[$j];
                    $prevAttr = $this->filter($this->getInternalKeyForAttribute($prevOriginal));
                    $andConditions[] = \Utopia\Query\Query::equal($prevAttr, [$cursor[$prevOriginal]]);
                }

                if ($direction === OrderDirection::DESC->value) {
                    $andConditions[] = \Utopia\Query\Query::lessThan($internalAttr, $cursor[$originalAttribute]);
                } else {
                    $andConditions[] = \Utopia\Query\Query::greaterThan($internalAttr, $cursor[$originalAttribute]);
                }

                if (count($andConditions) === 1) {
                    $cursorConditions[] = $andConditions[0];
                } else {
                    $cursorConditions[] = \Utopia\Query\Query::and($andConditions);
                }
            }

            if (! empty($cursorConditions)) {
                if (count($cursorConditions) === 1) {
                    $builder->filter($cursorConditions);
                } else {
                    $builder->filter([\Utopia\Query\Query::or($cursorConditions)]);
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

        // Regular ordering
        foreach ($orderAttributes as $i => $originalAttribute) {
            $orderType = $orderTypes[$i] ?? OrderDirection::ASC->value;

            if ($orderType === OrderDirection::RANDOM->value) {
                $builder->sortRandom();

                continue;
            }

            $internalAttr = $this->filter($this->getInternalKeyForAttribute($originalAttribute));
            $orderType = $this->filter($orderType);
            $direction = $orderType;

            if ($cursorDirection === CursorDirection::Before->value) {
                $direction = ($direction === OrderDirection::ASC->value)
                    ? OrderDirection::DESC->value
                    : OrderDirection::ASC->value;
            }

            if ($direction === OrderDirection::DESC->value) {
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

        $sql = $this->trigger(Database::EVENT_DOCUMENT_FIND, $result->query);

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
                    $stmt->bindValue($i + 1, $this->getFloatPrecision($value), \PDO::PARAM_STR);
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

        foreach ($results as $index => $document) {
            if (\array_key_exists('_uid', $document)) {
                $results[$index]['$id'] = $document['_uid'];
                unset($results[$index]['_uid']);
            }
            if (\array_key_exists('_id', $document)) {
                $results[$index]['$sequence'] = $document['_id'];
                unset($results[$index]['_id']);
            }
            if (\array_key_exists('_tenant', $document)) {
                $results[$index]['$tenant'] = $document['_tenant'];
                unset($results[$index]['_tenant']);
            }
            if (\array_key_exists('_createdAt', $document)) {
                $results[$index]['$createdAt'] = $document['_createdAt'];
                unset($results[$index]['_createdAt']);
            }
            if (\array_key_exists('_updatedAt', $document)) {
                $results[$index]['$updatedAt'] = $document['_updatedAt'];
                unset($results[$index]['_updatedAt']);
            }
            if (\array_key_exists('_permissions', $document)) {
                $results[$index]['$permissions'] = \json_decode($document['_permissions'] ?? '[]', true);
                unset($results[$index]['_permissions']);
            }

            $results[$index] = new Document($results[$index]);
        }

        if ($cursorDirection === CursorDirection::Before->value) {
            $results = \array_reverse($results);
        }

        return $results;
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
        $sql = $this->trigger(Database::EVENT_DOCUMENT_COUNT, $result->query);
        $stmt = $this->getPDO()->prepare($sql);

        foreach ($result->bindings as $i => $value) {
            if (\is_bool($value) && $this->supports(Capability::IntegerBooleans)) {
                $value = (int) $value;
            }
            if (\is_float($value)) {
                $stmt->bindValue($i + 1, $this->getFloatPrecision($value), \PDO::PARAM_STR);
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

        return $result['sum'] ?? 0;
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
        $sql = $this->trigger(Database::EVENT_DOCUMENT_SUM, $result->query);
        $stmt = $this->getPDO()->prepare($sql);

        foreach ($result->bindings as $i => $value) {
            if (\is_bool($value) && $this->supports(Capability::IntegerBooleans)) {
                $value = (int) $value;
            }
            if (\is_float($value)) {
                $stmt->bindValue($i + 1, $this->getFloatPrecision($value), \PDO::PARAM_STR);
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

        return $result['sum'] ?? 0;
    }

    public function getSpatialTypeFromWKT(string $wkt): string
    {
        $wkt = trim($wkt);
        $pos = strpos($wkt, '(');
        if ($pos === false) {
            throw new DatabaseException('Invalid spatial type');
        }

        return strtolower(trim(substr($wkt, 0, $pos)));
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

        return [(float) $coords[1], (float) $coords[2]];
    }

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

            $points[] = [(float) $xArr[1], (float) $yArr[1]];
            $offset += 16;
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

        $type = $typeArr[1];
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

                $x = (float) $xArr[1];

                $yArr = unpack('d', substr($wkb, $offset + 8, 8));
                if ($yArr === false) {
                    throw new DatabaseException('Failed to unpack Y coordinate from WKB.');
                }

                $y = (float) $yArr[1];

                $ring[] = [$x, $y];
                $offset += 16;
            }

            $rings[] = $ring;
        }

        return $rings;
    }

    public function setSupportForAttributes(bool $support): bool
    {
        return true;
    }

    public function getLockType(): string
    {
        if ($this->supports(Capability::AlterLock) && $this->alterLocks) {
            return ',LOCK=SHARED';
        }

        return '';
    }
}
