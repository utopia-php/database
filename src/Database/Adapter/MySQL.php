<?php

namespace Utopia\Database\Adapter;

use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Timeout;

class MySQL extends MariaDB
{
    /**
     * Get SQL Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     *
     * @return string
     * @throws DatabaseException
     */
    protected function getSQLIndex(string $collection, string $id, string $type, array $attributes): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
                $type = 'INDEX';
                break;

            case Database::INDEX_ARRAY:
                $type = 'INDEX';

                foreach ($attributes as $key => $value) {
                    $attributes[$key] = '(CAST(' . $value . ' AS char(255) ARRAY))';
                }
                break;

            case Database::INDEX_UNIQUE:
                $type = 'UNIQUE INDEX';
                break;

            case Database::INDEX_FULLTEXT:
                $type = 'FULLTEXT INDEX';
                break;

            default:
                throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_ARRAY . ', ' . Database::INDEX_FULLTEXT);
        }

        return 'CREATE '.$type.' `'.$id.'` ON `'.$this->getDefaultDatabase().'`.`'.$this->getNamespace().'_'.$collection.'` ( '.implode(', ', $attributes).' );';
    }

    /**
     * Returns Max Execution Time
     * @param string $sql
     * @param int $milliseconds
     * @return string
     */
    protected function setTimeOut(string $sql, int $milliseconds): string
    {
        return preg_replace('/SELECT/', "SELECT /*+ max_execution_time({$milliseconds}) */", $sql, 1);
    }

    /**
     * @param PDOException $e
     * @throws Timeout
     */
    protected function processException(PDOException $e): void
    {
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024) {
            throw new Timeout($e->getMessage());
        }

        // PDOProxy which who switches errorInfo
        if ($e->getCode() === 3024 && isset($e->errorInfo[0]) && $e->errorInfo[0] === "HY000") {
            throw new Timeout($e->getMessage());
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
        $database = $this->getDefaultDatabase();
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
}
