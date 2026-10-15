<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDOException;
use Utopia\Database\Adapter\SQL\Hook\Permission;
use Utopia\Database\Builder\MySQL as MySQLBuilder;
use Utopia\Database\Builder\Scoping;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Storage;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Schema\ColumnType;

class MySQL extends MariaDB
{
    /**
     * @return array<Capability>
     */
    #[\Override]
    public function capabilities(): array
    {
        $remove = [
            Capability::IndexSpatialOrder,
        ];

        return array_values(array_filter(
            array_merge(parent::capabilities(), [
                Capability::SpatialAxisOrder,
                Capability::IndexArrayCast,
            ]),
            fn (Capability $c) => ! in_array($c, $remove, true)
        ));
    }

    #[\Override]
    protected function getTimeoutStatement(int $milliseconds): string
    {
        return "SET SESSION MAX_EXECUTION_TIME = {$milliseconds}";
    }

    /**
     * @throws DatabaseException
     */
    #[\Override]
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        $collection = $this->filter($collection);
        $collection = $this->getNamespace().'_'.$collection;
        $database = $this->getDatabase();
        $name = $database.'/'.$collection;
        $permissions = $database.'/'.Storage::permissionsTable($collection);

        $collectionSize = $this->prepareStatement('
             SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
             FROM INFORMATION_SCHEMA.INNODB_TABLESPACES
             WHERE NAME = :name
        ', Event::CollectionRead);

        $permissionsSize = $this->prepareStatement('
             SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
             FROM INFORMATION_SCHEMA.INNODB_TABLESPACES
             WHERE NAME = :permissions
        ', Event::CollectionRead);

        $collectionSize->bindParam(':name', $name);
        $permissionsSize->bindParam(':permissions', $permissions);

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

    #[\Override]
    protected function processException(PDOException $e): Exception
    {
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1366) {
            return new CharacterException('Invalid character', $e->getCode(), $e);
        }

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

    #[\Override]
    protected function dialectBuilder(): SQLBuilder&Scoping
    {
        return new MySQLBuilder();
    }

    #[\Override]
    protected function supportsInsertReturning(): bool
    {
        return false;
    }

    /**
     * MySQL merges each permission check into the join as a semi-join, one more table for its join
     * order search, which grows about tenfold with each table past ten. From this many joins every
     * joined table's check stays a subquery.
     */
    private const int LARGE_JOIN = 5;

    /**
     * Inside an outer join's ON clause MySQL runs a semi-joined check by scanning its materialised
     * rows once per outer row, so an outer-joined table's check always stays a subquery.
     */
    #[\Override]
    protected function newJoinPermissionHook(string $collection, array $roles, string $type, string $documentColumn, int $joins, JoinType $joinType): Permission\Filter
    {
        $hook = parent::newJoinPermissionHook($collection, $roles, $type, $documentColumn, $joins, $joinType);

        return $joins >= self::LARGE_JOIN || self::isOuterJoin($joinType) ? $hook->withoutSemiJoin() : $hook;
    }

    #[\Override]
    protected function looksUpByTenantAlone(): bool
    {
        return true;
    }

    private static function isOuterJoin(JoinType $joinType): bool
    {
        return match ($joinType) {
            JoinType::Left, JoinType::Right, JoinType::FullOuter => true,
            default => false,
        };
    }

    #[\Override]
    protected function getSpatialSqlType(string $type, bool $required): string
    {
        switch ($type) {
            case ColumnType::Point->value:
                $type = 'POINT SRID 4326';
                if (! $this->supports(Capability::IndexSpatialNull)) {
                    if ($required) {
                        $type .= ' NOT NULL';
                    } else {
                        $type .= ' NULL';
                    }
                }

                return $type;

            case ColumnType::Linestring->value:
                $type = 'LINESTRING SRID 4326';
                if (! $this->supports(Capability::IndexSpatialNull)) {
                    if ($required) {
                        $type .= ' NOT NULL';
                    } else {
                        $type .= ' NULL';
                    }
                }

                return $type;

            case ColumnType::Polygon->value:
                $type = 'POLYGON SRID 4326';
                if (! $this->supports(Capability::IndexSpatialNull)) {
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

    #[\Override]
    protected function getSpatialColumnSrid(): ?int
    {
        return Database::DEFAULT_SRID;
    }

    /**
     * MySQL with SRID 4326 expects lat-long by default, but our data is in long-lat format
     */
    #[\Override]
    protected function getSpatialAxisOrder(): string
    {
        return "'axis-order=long-lat'";
    }

    #[\Override]
    protected function getOperatorSql(string $column, Operator $operator, int &$bindIndex): ?string
    {
        $quotedColumn = $this->quote($column);
        $method = $operator->getMethod();
        $values = $operator->getValues();

        switch ($method) {
            case OperatorType::ArrayAppend:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = JSON_MERGE_PRESERVE(IFNULL({$quotedColumn}, JSON_ARRAY()), :$bindKey)";

            case OperatorType::ArrayPrepend:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = JSON_MERGE_PRESERVE(:$bindKey, IFNULL({$quotedColumn}, JSON_ARRAY()))";

            case OperatorType::ArrayUnique:
                return "{$quotedColumn} = IFNULL((
                    SELECT JSON_ARRAYAGG(value)
                    FROM (
                        SELECT DISTINCT value
                        FROM JSON_TABLE({$quotedColumn}, '\$[*]' COLUMNS(value TEXT PATH '\$')) AS jt
                    ) AS distinct_values
                ), JSON_ARRAY())";
        }

        return parent::getOperatorSql($column, $operator, $bindIndex);
    }
}
