<?php

namespace Utopia\Database\Adapter;

use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
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
     * @param PDOException $e
     * @throws TimeoutException
     * @throws DuplicateException
     */
    protected function processException(PDOException $e): void
    {
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024) {
            throw new TimeoutException($e->getMessage(), $e->getCode(), $e);
        } elseif ($e->getCode() === 3024 && isset($e->errorInfo[0]) && $e->errorInfo[0] === "HY000") {
            throw new TimeoutException($e->getMessage(), $e->getCode(), $e);
        }

        if ($e->getCode() === '42S01' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1050) {
            throw new DuplicateException($e->getMessage(), $e->getCode(), $e);
        } elseif ($e->getCode() === 1050 && isset($e->errorInfo[0]) && $e->errorInfo[0] === '42S01') {
            throw new DuplicateException($e->getMessage(), $e->getCode(), $e);
        }

        throw $e;
    }

    /**
    * Get Collection Size
    * @param string $collection
    * @return int
    * @throws DatabaseException
    */
    public function getSizeOfCollection(string $collection): int
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
     * @return bool
     */
    public function castIndexArray(): bool
    {
        return true;
    }
}
