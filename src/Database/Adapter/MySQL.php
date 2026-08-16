<?php

namespace Utopia\Database\Adapter;

use PDOException;
use Utopia\Console;
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
     * Serialized dictionary information, as it sits inside a tablespace. Values confirmed
     * against MySQL 8.0.43; see readTableDictionary() and parseSdiPage().
     *
     * FIL_PAGE_SDI is easy to get wrong: 17855 is FIL_PAGE_INDEX, and reading a table's own
     * clustered index as dictionary information yields records that parse but mean nothing.
     */
    protected const FIL_PAGE_SDI = 17853;       // FIL_PAGE_TYPE of an SDI page
    protected const SDI_TYPE_TABLE = 1;         // SDI record type of a table object
    protected const SDI_INDEX_FULLTEXT = 4;     // dd::Index::IT_FULLTEXT
    protected const SDI_INFIMUM = 99;           // first record of a compact page
    protected const SDI_RECORD_BLOB = 33;       // record start to its zlib stream
    protected const SDI_PAGE_SCAN = 8;          // pages searched for the SDI (it is page 3)
    protected const SDI_RECORD_SCAN = 64;       // records walked before giving up
    protected const SDI_RECORD_DELETED = 0x20;  // REC_INFO_DELETED_FLAG, in the byte at -5

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

        $id = $collection->getId();
        $table = $this->getNamespace() . '_' . $this->filter($id);

        $directory = self::DATA_DIRECTORY . '/' . $this->getDatabase();

        if (!\is_readable($directory)) {
            throw new DatabaseException("Failed to get collection size: data directory '{$directory}' is not readable. This adapter reads sizes from disk, so it must run on the database host or with the data directory mounted.");
        }

        $files = [
            $directory . '/' . $table . '.ibd',
            $directory . '/' . $table . '_perms.ibd',
        ];

        if ($fulltext) {
            // A dictionary that cannot be read costs accuracy, not the caller's work: the
            // base tablespaces are still measurable, so report the shortfall and return a
            // size that is short by the index rather than no size at all. Logged every
            // time — a total quietly missing a third of a collection is the failure this
            // guards against, and nothing else in the return value says it happened.
            try {
                $files = \array_merge($files, $this->fulltextFiles($id));
            } catch (DatabaseException $e) {
                Console::error("[Database] Fulltext index size for '{$table}' is missing from its total: " . $e->getMessage());
            }
        }

        $size = 0;

        foreach ($files as $file) {
            $stat = @\stat($file);
            if ($stat === false) {
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
     * that id. It is read from the collection's own tablespace, because the alternatives do
     * not hold up: INNODB_TABLES and INNODB_TABLESPACES enumerate every tablespace on the
     * instance (see getSizeOfCollectionOnDisk), and INNODB_TABLESTATS lists only what is
     * currently in the dictionary cache, so a collection nobody touched recently would
     * silently measure zero.
     *
     * Both ids come from the same SDI read, so every file is addressed by name and no
     * directory is scanned — a schema here holds millions of files.
     *
     * The dictionary decides, not the collection's metadata — the metadata only says whether
     * paying for the read is worth it. A dictionary that declares no fulltext index is
     * answering about the disk, so an empty list is a complete answer, and metadata claiming
     * otherwise is drift in the metadata.
     *
     * An exception therefore means the size is unknown, never that it is zero: a partial list
     * is indistinguishable from a table with no fulltext index, and on a production table
     * those files came to a third of the collection. getSizeOfCollectionOnDisk() logs the
     * failure and keeps going; a caller that needs the number to be exact should call this
     * directly and handle the exception.
     *
     * The likely cause is a tablespace rebuilt moments ago — which is what ADD FULLTEXT does.
     * It has no dictionary on disk until InnoDB flushes it, so a caller that needs the number
     * right then must flush first (FLUSH TABLES ... FOR EXPORT).
     *
     * @param string $collection collection id, as the caller knows it
     * @return array<int, string> absolute paths, whether or not they exist
     * @throws DatabaseException
     */
    public function fulltextFiles(string $collection): array
    {
        $directory = self::DATA_DIRECTORY . '/' . $this->getDatabase();
        $table = $this->getNamespace() . '_' . $this->filter($collection);

        [$tableId, $indexIds] = $this->readFulltextIds($directory . '/' . $table . '.ibd');

        // An empty index list is not an error, and the collection's metadata claiming a
        // fulltext index does not make it one — the two drift apart when an index is dropped
        // outside the library, and dropping one is exactly what this looks like: MySQL removes
        // the six per-index tablespaces but leaves the five common ones behind, measured at
        // 5 x 112K still in the schema directory afterwards. Those are the collection's bytes
        // and they are named from the table id alone, so they are listed below either way:
        // counted while they linger, absent from the total once the drop clears them up.
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
     * Read from the file first and only shelled out to ibd2sdi when the parser cannot do it:
     * the parser handles the ordinary case with no subprocess, and the utility covers what it
     * declines — an SDI too large for its page is stored externally, and following that
     * pointer is not implemented here.
     *
     * The parser's own exception is what propagates when both fail, since it names the step
     * that gave up. The utility reads the same bytes, so it cannot rescue a tablespace whose
     * dictionary is not on disk yet, only one the parser cannot make sense of.
     *
     * @return array{0: string, 1: array<int, string>}
     * @throws DatabaseException
     */
    protected function readFulltextIds(string $file): array
    {
        $object = $this->readTableDictionary($file);
        //$object = $this->readTableDictionaryWithUtility($file); // lets leave this comment until we decide what method to use

        if (!isset($object['se_private_id'])) {
            throw new DatabaseException("Cannot read the dictionary: no table id in the dictionary of '{$file}'");
        }

        $indexIds = [];

        foreach ($object['indexes'] ?? [] as $index) {
            if (($index['type'] ?? 0) !== self::SDI_INDEX_FULLTEXT) {
                continue;
            }

            // An index's own id lives in its se_private_data, as `id=<decimal>`.
            if (\preg_match('/\bid=(\d+)/', (string) ($index['se_private_data'] ?? ''), $matches) === 1) {
                $indexIds[] = \sprintf('%016x', (int) $matches[1]);
            }
        }

        return [\sprintf('%016x', (int) $object['se_private_id']), $indexIds];
    }

    /**
     * A tablespace's table dictionary object, parsed out of the file itself.
     *
     * InnoDB keeps the serialized dictionary information in a B-tree inside the tablespace:
     * one page of type FIL_PAGE_SDI (page 3 in practice) whose records hold zlib-compressed
     * JSON. The record layout is fixed, so the table object can be read with two seeks and
     * an inflate — no subprocess, and no privileges beyond reading the file.
     *
     * Offsets confirmed against MySQL 8.0.43. Returns null rather than throwing whenever
     * the file does not match those expectations, so the caller can fall back: a non-leaf
     * root, an externally stored blob, an encrypted or page-compressed tablespace, or a
     * future change to the on-disk format all land here.
     *
     * @return array<string, mixed>|null the `dd_object` of the first table record
     */
    protected function readTableDictionary(string $file): array
    {
        $handle = @\fopen($file, 'rb');

        if ($handle === false) {
            throw new DatabaseException(\is_file($file)
                ? "Cannot read the dictionary: '{$file}' is not readable"
                : "Cannot read the dictionary: '{$file}' does not exist");
        }

        try {
            $header = \fread($handle, 64);

            if (!\is_string($header) || \strlen($header) < 58) {
                throw new DatabaseException("Cannot read the dictionary: '{$file}' is shorter than a page header");
            }

            $flags = \unpack('N', \substr($header, 54, 4))[1];

            // Bit 14 of the flags is supposed to say the tablespace carries an SDI, and it is
            // deliberately not used as a precondition: a tablespace whose page 0 has not been
            // flushed since it was rebuilt reports flags from an earlier state, and refusing
            // on that basis skips an SDI that is present and readable. Whether the page is
            // there is the only question that matters, so ask it directly.

            // PAGE_SSIZE, bits 6-9: zero means the 16K default, otherwise 512 << ssize. Trust
            // it only if it divides the file, since the same staleness applies.
            $ssize = ($flags >> 6) & 0xF;
            $pageSize = $ssize === 0 ? 16384 : (512 << $ssize);
            $length = (int) \filesize($file);

            if ($pageSize <= 0 || $length % $pageSize !== 0) {
                $pageSize = 16384;
            }

            $pages = (int) ($length / $pageSize);
            $seen = [];

            for ($number = 0; $number < \min($pages, self::SDI_PAGE_SCAN); $number++) {
                if (\fseek($handle, $number * $pageSize) !== 0) {
                    throw new DatabaseException("Cannot read the dictionary: seek to page {$number} of '{$file}' failed");
                }

                $page = \fread($handle, $pageSize);

                if (!\is_string($page) || \strlen($page) < $pageSize) {
                    throw new DatabaseException("Cannot read the dictionary: short read on page {$number} of '{$file}'");
                }

                $type = \unpack('n', \substr($page, 24, 2))[1];
                $seen[] = $number . ':' . $type;

                if ($type !== self::FIL_PAGE_SDI) {
                    continue;
                }

                $object = $this->parseSdiPage($page, $pageSize);

                // A tablespace can hold more than one SDI page; keep looking rather than
                // giving up because the first was an internal node or held nothing usable.
                if ($object !== null) {
                    return $object;
                }
            }

            throw new DatabaseException(\sprintf(
                "Cannot read the dictionary: no SDI page in '%s' — flags 0x%x, %d pages of %d, types %s."
                . ' A tablespace rebuilt moments ago still holds the stub written at creation;'
                . ' its header and SDI pages reach the file only when InnoDB flushes them.',
                $file,
                $flags,
                $pages,
                $pageSize,
                \implode(' ', $seen)
            ));
        } finally {
            \fclose($handle);
        }
    }

    /**
     * The first table object on an SDI leaf page.
     *
     * Records are reached through the page's own linked list — each compact record header
     * ends with the offset of the next record, relative to that record — starting at the
     * infimum. Every SDI record then has a fixed shape:
     *
     *   +0   uint32  SDI type, 1 for a table
     *   +4   uint64  SDI id
     *   +12  6 bytes DB_TRX_ID
     *   +18  7 bytes DB_ROLL_PTR
     *   +25  uint32  uncompressed length
     *   +29  uint32  compressed length
     *   +33  zlib stream of the JSON
     *
     * @return array<string, mixed>|null
     */
    protected function parseSdiPage(string $page, int $pageSize): array
    {
        // Only a leaf root can be read straight through; a taller tree would have to be
        // descended, which the caller's fallback covers instead.
        $level = \unpack('n', \substr($page, 64, 2))[1];

        if ($level !== 0) {
            throw new DatabaseException("Cannot read the dictionary: SDI page is level {$level}, not a leaf");
        }

        $records = [];
        $offset = self::SDI_INFIMUM;

        for ($record = 0; $record < self::SDI_RECORD_SCAN; $record++) {
            $next = \unpack('n', \substr($page, $offset - 2, 2))[1];

            if ($next === 0) {
                $records[] = 'end';
                break;
            }

            $offset = ($offset + $next) % $pageSize;

            if (\substr($page, $offset, 8) === 'supremum') {
                $records[] = 'supremum';
                break;
            }

            if ($offset + self::SDI_RECORD_BLOB > $pageSize) {
                $records[] = "record at {$offset} past page end";
                break;
            }

            // Every check below is about this record alone, so a record that cannot be used
            // moves to the next one. Only a table's *current* dictionary is wanted, and a
            // page accumulates a delete-marked copy per DDL until purge clears them.
            $info = \ord($page[$offset - 5]);
            $type = \unpack('N', \substr($page, $offset, 4))[1];
            $uncompressed = \unpack('N', \substr($page, $offset + 25, 4))[1];
            $compressed = \unpack('N', \substr($page, $offset + 29, 4))[1];

            $note = \sprintf('@%d info=0x%02x type=%d u=%d c=%d', $offset, $info, $type, $uncompressed, $compressed);

            if (($info & self::SDI_RECORD_DELETED) !== 0) {
                $records[] = $note . ' deleted';
                continue;
            }

            if ($type !== self::SDI_TYPE_TABLE) {
                $records[] = $note . ' not-a-table';
                continue;
            }

            // A blob too large for its page is stored externally behind a pointer this
            // parser does not follow.
            if ($compressed === 0 || $offset + self::SDI_RECORD_BLOB + $compressed > $pageSize) {
                $records[] = $note . ' external-blob';
                continue;
            }

            $json = @\gzuncompress(\substr($page, $offset + self::SDI_RECORD_BLOB, $compressed));

            // The declared length is the check that the record was read correctly.
            if (!\is_string($json)) {
                $records[] = $note . ' inflate-failed';
                continue;
            }

            if (\strlen($json) !== $uncompressed) {
                $records[] = $note . ' length-mismatch=' . \strlen($json);
                continue;
            }

            try {
                $decoded = \json_decode($json, true, flags: JSON_THROW_ON_ERROR);
            } catch (\JsonException) {
                $records[] = $note . ' not-json';
                continue;
            }

            $object = \is_array($decoded) ? ($decoded['dd_object'] ?? null) : null;

            if (\is_array($object) && isset($object['se_private_id'])) {
                return $object;
            }

            $records[] = $note . ' no-se_private_id';
        }

        throw new DatabaseException('Cannot read the dictionary: no usable table record on the SDI page — records ['
            . \implode(', ', $records) . ']');
    }

    /**
     * The same dictionary object by way of ibd2sdi, for tablespaces readTableDictionary()
     * declines. It ships with the MySQL server package, so it is present wherever the data
     * directory is.
     *
     * @return array<string, mixed>|null
     */
    protected function readTableDictionaryWithUtility(string $file): ?array
    {
        $output = [];
        $status = 1;
        @\exec(\sprintf('ibd2sdi %s 2>/dev/null', \escapeshellarg($file)), $output, $status);

        if ($status !== 0) {
            return null;
        }

        try {
            $sdi = \json_decode(\implode("\n", $output), true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return null;
        }

        if (!\is_array($sdi)) {
            return null;
        }

        foreach ($sdi as $entry) {
            $object = $entry['object']['dd_object'] ?? null;

            if (\is_array($object) && isset($object['se_private_id'])) {
                return $object;
            }
        }

        return null;
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
