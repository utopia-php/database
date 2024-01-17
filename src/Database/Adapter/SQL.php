<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
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

        $stmt->execute();

        $document = $stmt->fetchAll();
        $stmt->closeCursor();

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
     * @return Document
     * @throws Exception
     */
    public function getDocument(string $collection, string $id, array $queries = []): Document
    {
        $name = $this->filter($collection);
        $selections = $this->getAttributeSelections($queries);

        $sql = "
		    SELECT {$this->getAttributeProjection($selections)} 
            FROM {$this->getSQLTable($name)}
            WHERE _uid = :_uid 
		";

        if ($this->shareTables) {
            $sql .= "AND _tenant = :_tenant";
        }

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_uid', $id);

        if ($this->shareTables) {
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
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfAttributes(Document $collection): int
    {
        $attributes = \count($collection->getAttribute('attributes') ?? []);

        // +1 ==> virtual columns count as total, so add as buffer
        return $attributes + static::getCountOfDefaultAttributes() + 1;
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
        // Default collection has:
        // `_id` int(11) => 4 bytes
        // `_uid` char(255) => 1020 (255 bytes * 4 for utf8mb4)
        // but this number seems to vary, so we give a +500 byte buffer
        $total = 1500;

        $attributes = $collection->getAttributes()['attributes'];

        foreach ($attributes as $attribute) {
            switch ($attribute['type']) {
                case Database::VAR_STRING:
                    $total += match (true) {
                        // 8 bytes length + 4 bytes for LONGTEXT
                        $attribute['size'] > 16777215 => 12,
                        // 8 bytes length + 3 bytes for MEDIUMTEXT
                        $attribute['size'] > 65535 => 11,
                        // 8 bytes length + 2 bytes for TEXT
                        $attribute['size'] > $this->getMaxVarcharLength() => 10,
                        // $size = $size * 4; // utf8mb4 up to 4 bytes per char
                        // 8 bytes length + 2 bytes for VARCHAR(>255)
                        $attribute['size'] > 255 => ($attribute['size'] * 4) + 2,
                        // $size = $size * 4; // utf8mb4 up to 4 bytes per char
                        // 8 bytes length + 1 bytes for VARCHAR(<=255)
                        default => ($attribute['size'] * 4) + 1,
                    };
                    break;

                case Database::VAR_INTEGER:
                    if ($attribute['size'] >= 8) {
                        $total += 8; // BIGINT takes 8 bytes
                    } else {
                        $total += 4; // INT takes 4 bytes
                    }
                    break;
                case Database::VAR_FLOAT:
                    // DOUBLE takes 8 bytes
                    $total += 8;
                    break;

                case Database::VAR_BOOLEAN:
                    // TINYINT(1) takes one byte
                    $total += 1;
                    break;

                case Database::VAR_RELATIONSHIP:
                    // INT(11)
                    $total += 4;
                    break;

                case Database::VAR_DATETIME:
                    $total += 19; // 2022-06-26 14:46:24
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
        if ($query->getMethod() == Query::TYPE_SELECT) {
            return;
        }

        if($query->isNested()) {
            foreach ($query->getValues() as $value) {
                $this->bindConditionValue($stmt, $value);
            }
            return;
        }

        if($this->getSupportForJSONOverlaps() && $query->onArray() && $query->getMethod() == Query::TYPE_CONTAINS) {
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
    protected function getSQLPermissionsCondition(string $collection, array $roles): string
    {
        $roles = array_map(fn (string $role) => $this->getPDO()->quote($role), $roles);

        $tenantQuery = '';
        if ($this->shareTables) {
            $tenantQuery = 'AND _tenant = :_tenant';
        }

        return "table_main._uid IN (
                    SELECT _document
                    FROM {$this->getSQLTable($collection . '_perms')}
                    WHERE _permission IN (" . implode(', ', $roles) . ")
                      AND _type = 'read'
                      {$tenantQuery}
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
        return 768;
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

            if ($query->getMethod() === Query::TYPE_SELECT) {
                continue;
            }

            if($query->isNested()) {
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

}
