<?php

namespace Utopia\Database\Adapter;

use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Timeout as TimeoutException;

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

    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return false;
    }

    public function getSupportForSpatialIndexOrder(): bool
    {
        return false;
    }
}
