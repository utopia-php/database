<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDOException;
use PDOStatement;
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
use Utopia\Database\Query;
use Utopia\Query\Builder\MySQL as MySQLBuilder;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

/**
 * Database adapter for MySQL, extending MariaDB with MySQL-specific behavior and overrides.
 */
class MySQL extends MariaDB
{
    /**
     * Get the list of capabilities supported by the MySQL adapter.
     *
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        $remove = [
            Capability::BoundaryInclusive,
            Capability::SpatialIndexOrder,
            Capability::OptionalSpatial,
        ];

        return array_values(array_filter(
            array_merge(parent::capabilities(), [
                Capability::SpatialAxisOrder,
                Capability::MultiDimensionDistance,
                Capability::CastIndexArray,
            ]),
            fn (Capability $c) => ! in_array($c, $remove, true)
        ));
    }

    /**
     * Set max execution time
     *
     * @throws DatabaseException
     */
    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        if (! $this->supports(Capability::Timeouts)) {
            return;
        }
        if ($milliseconds <= 0) {
            throw new DatabaseException('Timeout must be greater than 0');
        }

        $this->timeout = $milliseconds;
    }

    protected function execute(mixed $stmt): bool
    {
        $this->getPDO()->exec("SET SESSION MAX_EXECUTION_TIME = {$this->timeout}");
        /** @var PDOStatement|\Swoole\Database\PDOStatementProxy $stmt */
        return $stmt->execute();
    }

    /**
     * Get size of collection on disk
     *
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        $collection = $this->filter($collection);
        $collection = $this->getNamespace().'_'.$collection;
        $database = $this->getDatabase();
        $name = $database.'/'.$collection;
        $permissions = $database.'/'.$collection.'_perms';

        $collectionSize = $this->getPDO()->prepare('
             SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
             FROM INFORMATION_SCHEMA.INNODB_TABLESPACES
             WHERE NAME = :name
        ');

        $permissionsSize = $this->getPDO()->prepare('
             SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
             FROM INFORMATION_SCHEMA.INNODB_TABLESPACES
             WHERE NAME = :permissions
        ');

        $collectionSize->bindParam(':name', $name);
        $permissionsSize->bindParam(':permissions', $permissions);

        try {
            $collectionSize->execute();
            $permissionsSize->execute();
            $collVal = $collectionSize->fetchColumn();
            $permVal = $permissionsSize->fetchColumn();
            $size = (int)(\is_numeric($collVal) ? $collVal : 0) + (int)(\is_numeric($permVal) ? $permVal : 0);
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: '.$e->getMessage());
        }

        return $size;
    }

    /**
     * Handle distance spatial queries
     *
     * @param  array<string, mixed>  $binds
     */
    protected function handleDistanceSpatialQueries(Query $query, array &$binds, string $attribute, string $type, string $alias, string $placeholder): string
    {
        /** @var array<mixed> $distanceParams */
        $distanceParams = $query->getValues()[0];
        $geomArray = \is_array($distanceParams[0]) ? $distanceParams[0] : [];
        $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($geomArray);
        $binds[":{$placeholder}_1"] = $distanceParams[1];

        $useMeters = isset($distanceParams[2]) && $distanceParams[2] === true;

        $operator = match ($query->getMethod()) {
            Method::DistanceEqual => '=',
            Method::DistanceNotEqual => '!=',
            Method::DistanceGreaterThan => '>',
            Method::DistanceLessThan => '<',
            default => throw new DatabaseException('Unknown spatial query method: '.$query->getMethod()->value),
        };

        if ($useMeters) {
            $attr = "ST_SRID({$alias}.{$attribute}, ".Database::DEFAULT_SRID.')';
            $geom = $this->getSpatialGeomFromText(":{$placeholder}_0", null);

            return "ST_Distance({$attr}, {$geom}, 'metre') {$operator} :{$placeholder}_1";
        }
        // need to use srid 0 because of geometric distance
        $attr = "ST_SRID({$alias}.{$attribute}, ". 0 .')';
        $geom = $this->getSpatialGeomFromText(":{$placeholder}_0", 0);

        return "ST_Distance({$attr}, {$geom}) {$operator} :{$placeholder}_1";
    }

    protected function processException(PDOException $e): Exception
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

    protected function createBuilder(): SQLBuilder
    {
        return new MySQLBuilder();
    }

    /**
     * Get the MySQL SQL type definition for spatial column types with SRID support.
     *
     * @param string $type The spatial type (point, linestring, polygon)
     * @param bool $required Whether the column is NOT NULL
     * @return string
     */
    public function getSpatialSQLType(string $type, bool $required): string
    {
        switch ($type) {
            case ColumnType::Point->value:
                $type = 'POINT SRID 4326';
                if (! $this->supports(Capability::SpatialIndexNull)) {
                    if ($required) {
                        $type .= ' NOT NULL';
                    } else {
                        $type .= ' NULL';
                    }
                }

                return $type;

            case ColumnType::Linestring->value:
                $type = 'LINESTRING SRID 4326';
                if (! $this->supports(Capability::SpatialIndexNull)) {
                    if ($required) {
                        $type .= ' NOT NULL';
                    } else {
                        $type .= ' NULL';
                    }
                }

                return $type;

            case ColumnType::Polygon->value:
                $type = 'POLYGON SRID 4326';
                if (! $this->supports(Capability::SpatialIndexNull)) {
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
     * Get the spatial axis order specification string for MySQL
     * MySQL with SRID 4326 expects lat-long by default, but our data is in long-lat format
     */
    protected function getSpatialAxisOrderSpec(): string
    {
        return "'axis-order=long-lat'";
    }

    /**
     * Get SQL expression for operator
     * Override for MySQL-specific operator implementations
     */
    protected function getOperatorSQL(string $column, Operator $operator, int &$bindIndex): ?string
    {
        $quotedColumn = $this->quote($column);
        $method = $operator->getMethod();

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

        // For all other operators, use parent implementation
        return parent::getOperatorSQL($column, $operator, $bindIndex);
    }
}
