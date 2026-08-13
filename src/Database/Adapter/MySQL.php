<?php

namespace Utopia\Database\Adapter;

use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Operator;
use Utopia\Database\Query;

class MySQL extends MariaDB
{
    /**
     * The server's data directory. Hardcoded to the default rather than read from
     * `@@datadir`, which would cost a query per connection. A deployment that moved its
     * datadir will fail the readable check in getSizeOfCollectionOnDisk() rather than
     * measure the wrong path, and can be given a setter then.
     */
    protected const DATA_DIRECTORY = '/var/lib/mysql';

    /**
     * Set max execution time
     * @param int $milliseconds
     * @param string $event
     * @return void
     * @throws DatabaseException
     */
    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        if (!$this->getSupportForTimeouts()) {
            return;
        }
        if ($milliseconds <= 0) {
            throw new DatabaseException('Timeout must be greater than 0');
        }

        $this->timeout = $milliseconds;

        $this->before($event, 'timeout', function ($sql) use ($milliseconds) {
            return \preg_replace(
                pattern: '/SELECT/',
                replacement: "SELECT /*+ max_execution_time({$milliseconds}) */",
                subject: $sql,
                limit: 1
            );
        });
    }

    /**
     * Get size of collection on disk
     *
     * REMINDER: do not go back to INFORMATION_SCHEMA.INNODB_TABLESPACES (or FILES) for
     * this. Those views are unindexed and the server materialises a row per tablespace
     * before applying any WHERE, so a lookup by name costs the same as listing every
     * tablespace on the instance. Measured on a shared schema holding ~224k tables and
     * ~1.6M fulltext auxiliary tablespaces, `WHERE NAME = '<one table>'` did not finish
     * inside a 10s statement timeout. The cost grows with the instance, not with the
     * collection, so it cannot be tuned away — that is why this reads from disk instead.
     *
     * A stat of two files costs nothing and needs no privileges (the tablespace views
     * additionally require PROCESS, which an application user does not normally hold).
     *
     * The collection's tablespace and its `_perms` sibling are both stat'ed, and reported
     * in allocated bytes — the same quantity ALLOCATED_SIZE gave.
     *
     * stat(2) rather than a shell command on purpose: `du -B1` is a GNU extension, and on
     * an Alpine image du is BusyBox, which rejects the flag and leaves nothing on stdout —
     * a silent zero. PHP's stat needs no subprocess and behaves the same everywhere.
     *
     * Requires the caller to run on the database host, or with the data directory mounted
     * at the same path — the files live on the server, so a client elsewhere measures
     * nothing. The path is the default datadir; see DATA_DIRECTORY.
     *
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(Document $collection): int
    {
        $fulltext = false;

        foreach ($collection->getAttribute('indexes', []) as $index) {
            if ($index->getAttribute('type') === Database::INDEX_FULLTEXT) {
                $fulltext = true;
                break;
            }
        }

        $collection = $this->filter($collection->getId());
        $collection = $this->getNamespace() . '_' . $collection;

        $directory = self::DATA_DIRECTORY . '/' . $this->getDatabase();

        $files = [
            $directory . '/' . $collection . '.ibd',
            $directory . '/' . $collection . '_perms.ibd',
        ];

        // A fulltext index lives in tablespaces of its own, named for the table's id, so its
        // bytes are in none of the files above. Reading that id costs an ibd2sdi call, so it
        // is only paid when an index says there is something to find.
        if ($fulltext) {
            $files = \array_merge($files, $this->fulltextFiles($directory, $collection));
        }

        if (!\is_readable($directory)) {
            throw new DatabaseException("Failed to get collection size: data directory '{$directory}' is not readable. This adapter reads sizes from disk, so it must run on the database host or with the data directory mounted.");
        }

        $size = 0;

        foreach ($files as $file) {
            var_dump($file);
            $stat = @\stat($file);
            var_dump($stat);
            if ($stat === false) {
                // A collection can legitimately lack a tablespace of its own — it lives in
                // the system tablespace, or was dropped between the two stats.
                continue;
            }

            // `blocks` counts the 512-byte units the filesystem has committed, which is
            // what ALLOCATED_SIZE reported and what a sparse file (page compression)
            // actually occupies. `size` would be the apparent length instead.
            $size += (int) $stat['blocks'] * 512;
        }

        return $size;
    }

    /**
     * The tablespaces holding a collection's fulltext index — five common ones per table
     * plus six per index.
     *
     * The filenames carry the owning table's id, never its name, so attributing them needs
     * that id. It is read with ibd2sdi from the collection's own tablespace, because the
     * alternatives do not hold up: INNODB_TABLES and INNODB_TABLESPACES enumerate every
     * tablespace on the instance (see getSizeOfCollectionOnDisk), and INNODB_TABLESTATS
     * lists only what is currently in the dictionary cache, so a collection nobody touched
     * recently would silently measure zero.
     *
     * Both ids come from the same SDI read, so every file is addressed by name and no
     * directory is scanned — a schema here holds millions of files.
     *
     * @param string $directory schema directory inside the datadir
     * @param string $collection namespaced, filtered table name
     * @return array<int, string> absolute paths, whether or not they exist
     * @throws DatabaseException
     */
    protected function fulltextFiles(string $directory, string $collection): array
    {
        [$tableId, $indexIds] = $this->readFulltextIds($directory . '/' . $collection . '.ibd');

        if ($tableId === null) {
            return [];
        }   

        $names = ['deleted', 'deleted_cache', 'being_deleted', 'being_deleted_cache', 'config'];

        foreach ($indexIds as $indexId) {
            for ($bucket = 1; $bucket <= 6; $bucket++) {
                $names[] = $indexId . '_index_' . $bucket;
            }
        }

        $files = [];

        foreach ($names as $name) {
            $files[] = $directory . '/fts_' . $tableId . '_' . $name . '.ibd';
        }

        return $files;
    }

    /**
     * The table id and its fulltext index ids, as the 16-hex-digit strings the auxiliary
     * filenames use, read from a tablespace's serialized dictionary information.
     *
     * The table's InnoDB id is `se_private_id` on the table object; an index's is `id`
     * inside its `se_private_data`, and a fulltext index is type 4 in the dictionary.
     *
     * @return array{0: string|null, 1: array<int, string>}
     * @throws DatabaseException
     */
    protected function readFulltextIds(string $file): array
    {
        $output = [];
        $status = 1;
        @\exec(\sprintf('ibd2sdi %s 2>/dev/null', \escapeshellarg($file)), $output, $status);

        if ($status !== 0) {
            throw new DatabaseException("Failed to read the fulltext size: ibd2sdi could not read '{$file}'. It ships with the MySQL server package, and the tablespace must be readable from this host.");
        }

        try {
            $sdi = \json_decode(\implode("\n", $output), true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new DatabaseException("Failed to read the fulltext size: ibd2sdi returned unreadable dictionary information for '{$file}': " . $e->getMessage());
        }

        if (!\is_array($sdi)) {
            throw new DatabaseException("Failed to read the fulltext size: ibd2sdi returned no dictionary information for '{$file}', got " . \gettype($sdi));
        }

        foreach ($sdi as $entry) {
            $object = $entry['object']['dd_object'] ?? null;

            if (!\is_array($object) || !isset($object['se_private_id'])) {
                continue;
            }

            $indexIds = [];

            foreach ($object['indexes'] ?? [] as $index) {
                // dd::Index::IT_FULLTEXT
                if (($index['type'] ?? 0) !== 4) {
                    continue;
                }

                if (\preg_match('/\bid=(\d+)/', (string) ($index['se_private_data'] ?? ''), $matches) === 1) {
                    $indexIds[] = \sprintf('%016x', (int) $matches[1]);
                }
            }

            return [\sprintf('%016x', (int) $object['se_private_id']), $indexIds];
        }

        return [null, []];
    }

    /**
     * Handle distance spatial queries
     *
     * @param Query $query
     * @param array<string, mixed> $binds
     * @param string $attribute
     * @param string $type
     * @param string $alias
     * @param string $placeholder
     * @return string
    */
    protected function handleDistanceSpatialQueries(Query $query, array &$binds, string $attribute, string $type, string $alias, string $placeholder): string
    {
        $distanceParams = $query->getValues()[0];
        $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($distanceParams[0]);
        $binds[":{$placeholder}_1"] = $distanceParams[1];

        $useMeters = isset($distanceParams[2]) && $distanceParams[2] === true;

        switch ($query->getMethod()) {
            case Query::TYPE_DISTANCE_EQUAL:
                $operator = '=';
                break;
            case Query::TYPE_DISTANCE_NOT_EQUAL:
                $operator = '!=';
                break;
            case Query::TYPE_DISTANCE_GREATER_THAN:
                $operator = '>';
                break;
            case Query::TYPE_DISTANCE_LESS_THAN:
                $operator = '<';
                break;
            default:
                throw new DatabaseException('Unknown spatial query method: ' . $query->getMethod());
        }

        if ($useMeters) {
            $attr = "ST_SRID({$alias}.{$attribute}, " . Database::DEFAULT_SRID . ")";
            $geom = $this->getSpatialGeomFromText(":{$placeholder}_0", null);
            return "ST_Distance({$attr}, {$geom}, 'metre') {$operator} :{$placeholder}_1";
        }
        // need to use srid 0 because of geometric distance
        $attr = "ST_SRID({$alias}.{$attribute}, " . 0 . ")";
        $geom = $this->getSpatialGeomFromText(":{$placeholder}_0", 0);
        return "ST_Distance({$attr}, {$geom}) {$operator} :{$placeholder}_1";
    }

    public function getSupportForIndexArray(): bool
    {
        /**
         * @link https://bugs.mysql.com/bug.php?id=111037
         */
        return true;
    }

    public function getSupportForCastIndexArray(): bool
    {
        if (!$this->getSupportForIndexArray()) {
            return false;
        }

        return true;
    }

    protected function processException(PDOException $e): \Exception
    {
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1366) {
            return new CharacterException('Invalid character', $e->getCode(), $e);
        }

        // Timeout
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Regex timeout
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3699) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Functional index dependency
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3837) {
            return new DependencyException('Attribute cannot be deleted because it is used in an index', $e->getCode(), $e);
        }

        if ($e->getCode() === '22004' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1138) {
            return new StructureException('Attribute does not allow null values', $e->getCode(), $e);
        }

        return parent::processException($e);
    }
    /**
     * Does the adapter includes boundary during spatial contains?
     *
     * @return bool
     */
    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return false;
    }
    /**
     * Does the adapter support order attribute in spatial indexes?
     *
     * @return bool
    */
    public function getSupportForSpatialIndexOrder(): bool
    {
        return false;
    }

    /**
     * Does the adapter support calculating distance(in meters) between multidimension geometry(line, polygon,etc)?
     *
     * @return bool
     */
    public function getSupportForDistanceBetweenMultiDimensionGeometryInMeters(): bool
    {
        return true;
    }

    /**
     * Spatial type attribute
    */
    public function getSpatialSQLType(string $type, bool $required): string
    {
        switch ($type) {
            case Database::VAR_POINT:
                $type = 'POINT SRID 4326';
                if (!$this->getSupportForSpatialIndexNull()) {
                    if ($required) {
                        $type .= ' NOT NULL';
                    } else {
                        $type .= ' NULL';
                    }
                }
                return $type;

            case Database::VAR_LINESTRING:
                $type = 'LINESTRING SRID 4326';
                if (!$this->getSupportForSpatialIndexNull()) {
                    if ($required) {
                        $type .= ' NOT NULL';
                    } else {
                        $type .= ' NULL';
                    }
                }
                return $type;


            case Database::VAR_POLYGON:
                $type = 'POLYGON SRID 4326';
                if (!$this->getSupportForSpatialIndexNull()) {
                    if ($required) {
                        $type .= ' NOT NULL';
                    } else {
                        $type .= ' NULL';
                    }
                }
                return $type;
        }
        return '';
    }

    /**
     * Does the adapter support spatial axis order specification?
     *
     * @return bool
     */
    public function getSupportForSpatialAxisOrder(): bool
    {
        return true;
    }

    public function getSupportForObjectIndexes(): bool
    {
        return false;
    }

    /**
     * Get the spatial axis order specification string for MySQL
     * MySQL with SRID 4326 expects lat-long by default, but our data is in long-lat format
     *
     * @return string
     */
    protected function getSpatialAxisOrderSpec(): string
    {
        return "'axis-order=long-lat'";
    }

    /**
     * Adapter supports optional spatial attributes with existing rows.
     *
     * @return bool
     */
    public function getSupportForOptionalSpatialAttributeWithExistingRows(): bool
    {
        return false;
    }

    /**
     * Get SQL expression for operator
     * Override for MySQL-specific operator implementations
     *
     * @param string $column
     * @param \Utopia\Database\Operator $operator
     * @param array<string, mixed> $binds
     * @return ?string
     */
    protected function getOperatorSQL(string $column, \Utopia\Database\Operator $operator, array &$binds): ?string
    {
        $quotedColumn = $this->quote($column);
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            case Operator::TYPE_ARRAY_APPEND:
                $bindKey = $this->registerOperatorBind($binds, json_encode($values));
                return "{$quotedColumn} = JSON_MERGE_PRESERVE(IFNULL({$quotedColumn}, JSON_ARRAY()), :$bindKey)";

            case Operator::TYPE_ARRAY_PREPEND:
                $bindKey = $this->registerOperatorBind($binds, json_encode($values));
                return "{$quotedColumn} = JSON_MERGE_PRESERVE(:$bindKey, IFNULL({$quotedColumn}, JSON_ARRAY()))";

            case Operator::TYPE_ARRAY_UNIQUE:
                return "{$quotedColumn} = IFNULL((
                    SELECT JSON_ARRAYAGG(value)
                    FROM (
                        SELECT DISTINCT value
                        FROM JSON_TABLE({$quotedColumn}, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt
                    ) AS distinct_values
                ), JSON_ARRAY())";
        }

        // For all other operators, use parent implementation
        return parent::getOperatorSQL($column, $operator, $binds);
    }

    public function getSupportForTTLIndexes(): bool
    {
        return false;
    }
}
