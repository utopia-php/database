<?php

namespace Utopia\Database\Adapter;

use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Query;

class MySQL extends MariaDB
{
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
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        $collection = $this->filter($collection);
        $collection = $this->getNamespace() . '_' . $collection;
        $database = $this->getDatabase();
        $name = $database . '/' . $collection;
        $permissions = $database . '/' . $collection . '_perms';

        $collectionSize = $this->getPDO()->prepare("
             SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
             FROM INFORMATION_SCHEMA.INNODB_TABLESPACES
             WHERE NAME = :name
        ");

        $permissionsSize = $this->getPDO()->prepare("
             SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
             FROM INFORMATION_SCHEMA.INNODB_TABLESPACES
             WHERE NAME = :permissions
        ");

        $collectionSize->bindParam(':name', $name);
        $permissionsSize->bindParam(':permissions', $permissions);

        try {
            $collectionSize->execute();
            $permissionsSize->execute();
            $size = $collectionSize->fetchColumn() + $permissionsSize->fetchColumn();
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: ' . $e->getMessage());
        }

        return $size;
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
            $attr = "ST_SRID({$alias}.{$attribute}, " . Database::SRID . ")";
            $geom = $this->getSpatialGeomFromText(":{$placeholder}_0");
            return "ST_Distance({$attr}, {$geom}, 'metre') {$operator} :{$placeholder}_1";
        }
        $attr = "ST_GeomFromText(ST_AsText({$alias}.{$attribute}), 0)";
        $geom = $this->getSpatialGeomFromText(":{$placeholder}_0", 0);
        return "ST_Distance({$attr}, {$geom}) {$operator} :{$placeholder}_1";
        // Without meters, use default behavior
        // return "ST_Distance(ST_GeomFromText({$alias}.{$attribute}, 0, 'axis-order=lat-long')), ST_GeomFromText(:{$placeholder}_0, 0, 'axis-order=lat-long')) {$operator} :{$placeholder}_1";
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
        // Timeout
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Functional index dependency
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3837) {
            return new DependencyException('Attribute cannot be deleted because it is used in an index', $e->getCode(), $e);
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
        return false; // Temporarily disable to test
    }
}
