<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Query;

abstract class SQL extends Adapter
{
    protected mixed $pdo;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param mixed $pdo
     */
    public function __construct(mixed $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @inheritDoc
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
                $result = $this->getPDO()->exec('SAVEPOINT transaction' . $this->inTransaction);
            }
        } catch (PDOException $e) {
            throw new TransactionException('Failed to start transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        if (!$result) {
            throw new TransactionException('Failed to start transaction');
        }

        $this->inTransaction++;
        return $result;
    }

    /**
     * @inheritDoc
     */
    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        } elseif ($this->inTransaction > 1) {
            $this->inTransaction--;
            return true;
        }

        if (!$this->getPDO()->inTransaction()) {
            $this->inTransaction = 0;
            return false;
        }

        try {
            $result = $this->getPDO()->commit();
            $this->inTransaction = 0;
        } catch (PDOException $e) {
            throw new TransactionException('Failed to commit transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        if (!$result) {
            throw new TransactionException('Failed to commit transaction');
        }

        return $result;
    }

    /**
     * @inheritDoc
     */
    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        try {
            if ($this->inTransaction > 1) {
                $result = $this->getPDO()->exec('ROLLBACK TO transaction' . ($this->inTransaction - 1));
                $this->inTransaction--;
            } else {
                $result = $this->getPDO()->rollBack();
                $this->inTransaction = 0;
            }
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to rollback transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        if (!$result) {
            throw new TransactionException('Failed to rollback transaction');
        }

        return $result;
    }

    /**
     * Ping Database
     *
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function ping(): bool
    {
        return $this->getPDO()
            ->prepare("SELECT 1;")
            ->execute();
    }

    /**
     * Check if Database exists
     * Optionally check if collection exists in Database
     *
     * @param string $database
     * @param string|null $collection
     * @return bool
     * @throws DatabaseException
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        $database = $this->filter($database);

        if (!\is_null($collection)) {
            $collection = $this->filter($collection);
            $stmt = $this->getPDO()->prepare("
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = :schema 
                  AND TABLE_NAME = :table
            ");
            $stmt->bindValue(':schema', $database, PDO::PARAM_STR);
            $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", PDO::PARAM_STR);
        } else {
            $stmt = $this->getPDO()->prepare("
                SELECT SCHEMA_NAME FROM
                INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME = :schema
            ");
            $stmt->bindValue(':schema', $database, PDO::PARAM_STR);
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
     * Get Document
     *
     * @param string $collection
     * @param string $id
     * @param Query[] $queries
     * @param bool $forUpdate
     * @return Document
     * @throws DatabaseException
     */
    public function getDocument(string $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $name = $this->filter($collection);
        $selections = $this->getAttributeSelections($queries);

        $forUpdate = $forUpdate ? 'FOR UPDATE' : '';

        $sql = "
		    SELECT {$this->getAttributeProjection($selections)}
            FROM {$this->getSQLTable($name)}
            WHERE _uid = :_uid 
            {$this->getTenantQuery($collection)}
		";

        if ($this->getSupportForUpdateLock()) {
            $sql .= " {$forUpdate}";
        }

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_uid', $id);

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->getTenant());
        }

        $stmt->execute();
        $document = $stmt->fetchAll();
        $stmt->closeCursor();

        if (empty($document)) {
            return new Document([]);
        }

        $document = $document[0];

        if (\array_key_exists('_id', $document)) {
            $document['$internalId'] = $document['_id'];
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
     * Get max STRING limit
     *
     * @return int
     */
    public function getLimitForString(): int
    {
        return 4294967295;
    }

    /**
     * Get max INT limit
     *
     * @return int
     */
    public function getLimitForInt(): int
    {
        return 4294967295;
    }

    /**
     * Get maximum column limit.
     * https://mariadb.com/kb/en/innodb-limitations/#limitations-on-schema
     * Can be inherited by MySQL since we utilize the InnoDB engine
     *
     * @return int
     */
    public function getLimitForAttributes(): int
    {
        return 1017;
    }

    /**
     * Get maximum index limit.
     * https://mariadb.com/kb/en/innodb-limitations/#limitations-on-schema
     *
     * @return int
     */
    public function getLimitForIndexes(): int
    {
        return 64;
    }

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    public function getSupportForSchemas(): bool
    {
        return true;
    }

    /**
     * Is index supported?
     *
     * @return bool
     */
    public function getSupportForIndex(): bool
    {
        return true;
    }

    /**
     * Are attributes supported?
     *
     * @return bool
     */
    public function getSupportForAttributes(): bool
    {
        return true;
    }

    /**
     * Is unique index supported?
     *
     * @return bool
     */
    public function getSupportForUniqueIndex(): bool
    {
        return true;
    }

    /**
     * Is fulltext index supported?
     *
     * @return bool
     */
    public function getSupportForFulltextIndex(): bool
    {
        return true;
    }

    /**
     * Are FOR UPDATE locks supported?
     *
     * @return bool
     */
    public function getSupportForUpdateLock(): bool
    {
        return true;
    }

    /**
     * Is Attribute Resizing Supported?
     *
     * @return bool
     */
    public function getSupportForAttributeResizing(): bool
    {
        return true;
    }

    /**
     * Are batch operations supported?
     *
     * @return bool
     */
    public function getSupportForBatchOperations(): bool
    {
        return true;
    }

    /**
     * Is get connection id supported?
     *
     * @return bool
     */
    public function getSupportForGetConnectionId(): bool
    {
        return true;
    }

    /**
     * Are aggregate queries supported?
     *
     * @return bool
     */
    public function getSupportForAggregateQueries(): bool
    {
        return true;
    }

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfAttributes(Document $collection): int
    {
        $attributes = \count($collection->getAttribute('attributes') ?? []);

        return $attributes + static::getCountOfDefaultAttributes();
    }

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfIndexes(Document $collection): int
    {
        $indexes = \count($collection->getAttribute('indexes') ?? []);
        return $indexes + static::getCountOfDefaultIndexes();
    }

    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    public static function getCountOfDefaultAttributes(): int
    {
        return \count(Database::INTERNAL_ATTRIBUTES);
    }

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    public static function getCountOfDefaultIndexes(): int
    {
        return \count(Database::INTERNAL_INDEXES);
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    public static function getDocumentSizeLimit(): int
    {
        return 65535;
    }

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     *
     * @param Document $collection
     * @return int
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

        $attributes = $collection->getAttributes()['attributes'];

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
                case Database::VAR_STRING:
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

                case Database::VAR_INTEGER:
                    if ($attribute['size'] >= 8) {
                        $total += 8; //  BIGINT 8 bytes
                    } else {
                        $total += 4; // INT 4 bytes
                    }
                    break;

                case Database::VAR_FLOAT:
                    $total += 8; // DOUBLE 8 bytes
                    break;

                case Database::VAR_BOOLEAN:
                    $total += 1; // TINYINT(1) 1 bytes
                    break;

                case Database::VAR_RELATIONSHIP:
                    $total += Database::LENGTH_KEY * 4 + 1; // VARCHAR(<=255)
                    break;

                case Database::VAR_DATETIME:
                    /**
                     * 1 byte year + month
                     * 1 byte for the day
                     * 3 bytes for the hour, minute, and second
                     * 2 bytes miliseconds DATETIME(3)
                     */
                    $total += 7;
                    break;
                default:
                    throw new DatabaseException('Unknown type: ' . $attribute['type']);
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
            'WITHOUT'
        ];
    }

    /**
     * Does the adapter handle casting?
     *
     * @return bool
     */
    public function getSupportForCasting(): bool
    {
        return false;
    }

    /**
     * Does the adapter handle Query Array Contains?
     *
     * @return bool
     */
    public function getSupportForQueryContains(): bool
    {
        return true;
    }

    /**
     * Does the adapter handle array Overlaps?
     *
     * @return bool
     */
    abstract public function getSupportForJSONOverlaps(): bool;

    public function getSupportForCastIndexArray(): bool
    {
        return false;
    }

    public function getSupportForRelationships(): bool
    {
        return true;
    }

    /**
     * @param mixed $stmt
     * @param Query $query
     * @return void
     * @throws Exception
     */
    protected function bindConditionValue(mixed $stmt, Query $query): void
    {
        if ($query->getMethod() == Query::TYPE_SELECT || $query->getMethod() == Query::TYPE_SUM) {
            return;
        }

        if ($query->isNested()) {
            foreach ($query->getValues() as $value) {
                $this->bindConditionValue($stmt, $value);
            }
            return;
        }

        if ($this->getSupportForJSONOverlaps() && $query->onArray() && $query->getMethod() == Query::TYPE_CONTAINS) {
            $placeholder = $this->getSQLPlaceholder($query) . '_0';
            $stmt->bindValue($placeholder, json_encode($query->getValues()), PDO::PARAM_STR);
            return;
        }

        foreach ($query->getValues() as $key => $value) {
            $value = match ($query->getMethod()) {
                Query::TYPE_STARTS_WITH => $this->escapeWildcards($value) . '%',
                Query::TYPE_ENDS_WITH => '%' . $this->escapeWildcards($value),
                Query::TYPE_SEARCH => $this->getFulltextValue($value),
                Query::TYPE_CONTAINS => $query->onArray() ? \json_encode($value) : '%' . $this->escapeWildcards($value) . '%',
                default => $value
            };

            $placeholder = $this->getSQLPlaceholder($query) . '_' . $key;

            $stmt->bindValue($placeholder, $value, $this->getPDOType($value));
        }
    }

    /**
     * @param string $value
     * @return string
     */
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
            $value = '"' . $value . '"';
        } else {
            /** Prepend wildcard by default on the back. */
            $value .= '*';
        }

        return $value;
    }

    /**
     * Get SQL Operator
     *
     * @param string $method
     * @return string
     * @throws Exception
     */
    protected function getSQLOperator(string $method): string
    {
        switch ($method) {
            case Query::TYPE_EQUAL:
                return '=';
            case Query::TYPE_NOT_EQUAL:
                return '!=';
            case Query::TYPE_LESSER:
                return '<';
            case Query::TYPE_LESSER_EQUAL:
                return '<=';
            case Query::TYPE_GREATER:
                return '>';
            case Query::TYPE_GREATER_EQUAL:
                return '>=';
            case Query::TYPE_IS_NULL:
                return 'IS NULL';
            case Query::TYPE_IS_NOT_NULL:
                return 'IS NOT NULL';
            case Query::TYPE_STARTS_WITH:
            case Query::TYPE_ENDS_WITH:
            case Query::TYPE_CONTAINS:
                return $this->getLikeOperator();
            default:
                throw new DatabaseException('Unknown method: ' . $method);
        }
    }

    /**
     * @param Query $query
     * @return string
     * @throws Exception
     */
    protected function getSQLPlaceholder(Query $query): string
    {
        $json = \json_encode([$query->getAttribute(), $query->getMethod(), $query->getValues()]);

        if ($json === false) {
            throw new DatabaseException('Failed to encode query');
        }

        return \md5($json);
    }

    public function escapeWildcards(string $value): string
    {
        $wildcards = ['%', '_', '[', ']', '^', '-', '.', '*', '+', '?', '(', ')', '{', '}', '|'];

        foreach ($wildcards as $wildcard) {
            $value = \str_replace($wildcard, "\\$wildcard", $value);
        }

        return $value;
    }

    /**
     * Get SQL Index Type
     *
     * @param string $type
     * @return string
     * @throws Exception
     */
    protected function getSQLIndexType(string $type): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
                return 'INDEX';

            case Database::INDEX_UNIQUE:
                return 'UNIQUE INDEX';

            case Database::INDEX_FULLTEXT:
                return 'FULLTEXT INDEX';

            default:
                throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_FULLTEXT);
        }
    }

    /**
     * Get SQL condition for permissions
     *
     * @param string $collection
     * @param array<string> $roles
     * @return string
     * @throws Exception
     */
    protected function getSQLPermissionsCondition(string $collection, array $roles, string $type = Database::PERMISSION_READ): string
    {
        if (!in_array($type, Database::PERMISSIONS)) {
            throw new DatabaseException('Unknown permission type: ' . $type);
        }

        $roles = array_map(fn (string $role) => $this->getPDO()->quote($role), $roles);

        return "table_main._uid IN (
                    SELECT _document
                    FROM {$this->getSQLTable($collection . '_perms')}
                    WHERE _permission IN (" . implode(', ', $roles) . ")
                      AND _type = '{$type}'
                      {$this->getTenantQuery($collection)}
                )";
    }

    /**
     * Get SQL table
     *
     * @param string $name
     * @return string
     * @throws DatabaseException
     */
    protected function getSQLTable(string $name): string
    {
        return "`{$this->getDatabase()}`.`{$this->getNamespace()}_{$this->filter($name)}`";
    }

    /**
     * Returns the current PDO object
     * @return mixed
     */
    protected function getPDO(): mixed
    {
        return $this->pdo;
    }

    /**
     * Get PDO Type
     *
     * @param mixed $value
     * @return int
     * @throws Exception
     */
    abstract protected function getPDOType(mixed $value): int;

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
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // PDO will throw a PDOException on srrors
            PDO::ATTR_EMULATE_PREPARES => true, // Emulate prepared statements
            PDO::ATTR_STRINGIFY_FETCHES => true // Returns all fetched data as Strings
        ];
    }

    /**
     * @return int
     */
    public function getMaxVarcharLength(): int
    {
        return 16381; // Floor value for Postgres:16383 | MySQL:16381 | MariaDB:16382
    }

    /**
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
     * @param Query $query
     * @return string
     * @throws Exception
     */
    abstract protected function getSQLCondition(Query $query): string;

    /**
     * @param array<Query> $queries
     * @param string $separator
     * @return string
     * @throws Exception
     */
    public function getSQLConditions(array $queries = [], string $separator = 'AND'): string
    {
        $conditions = [];
        foreach ($queries as $query) {

            if ($query->getMethod() === Query::TYPE_SELECT || in_array($query->getMethod(), Query::AGGREGATE_TYPES)) {
                continue;
            }

            if ($query->isNested()) {
                $conditions[] = $this->getSQLConditions($query->getValues(), $query->getMethod());
            } else {
                $conditions[] = $this->getSQLCondition($query);
            }
        }

        $tmp = implode(' '. $separator .' ', $conditions);
        return empty($tmp) ? '' : '(' . $tmp . ')';
    }

    /**
     * @return string
     */
    public function getLikeOperator(): string
    {
        return 'LIKE';
    }

    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    public function getSchemaAttributes(string $collection): array
    {
        return [];
    }

    public function getTenantQuery(string $collection, string $parentAlias = ''): string
    {
        if (!$this->sharedTables) {
            return '';
        }

        if (!empty($parentAlias) || $parentAlias === '0') {
            $parentAlias .= '.';
        }

        $query = "AND ({$parentAlias}_tenant = :_tenant";

        if ($collection === Database::METADATA) {
            $query .= " OR {$parentAlias}_tenant IS NULL";
        }

        $query .= ")";

        return $query;
    }

    protected function processException(PDOException $e): \Exception
    {
        return $e;
    }
}
