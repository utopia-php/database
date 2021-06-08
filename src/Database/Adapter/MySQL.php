<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Database;

class MySQL extends MariaDB
{
    /**
     * Create Collection
     * 
     * @param string $id
     * @return bool
     */
    public function createCollection(string $id): bool
    {
        $id = $this->filter($id);

        $this->getPDO()
            ->prepare("CREATE TABLE IF NOT EXISTS {$this->getNamespace()}.{$id} (
                `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `_uid` CHAR(255) NOT NULL,
                `_read` JSON NOT NULL,
                `_write` TEXT NOT NULL,
                PRIMARY KEY (`_id`),
                UNIQUE KEY `_index1` (`_uid`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
            ->execute();

        return $this->createIndex($id, '_index2', Database::INDEX_ARRAY, ['_read'], [], []);
    }

    /**
     * Get SQL Index
     * 
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array $attributes
     * 
     * @return string
     */
    protected function getSQLIndex(string $collection, string $id, string $type, array $attributes): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
                $type = 'INDEX';
            break;

            case Database::INDEX_ARRAY:
                $type = 'INDEX';

                foreach ($attributes as $key => &$value) {
                    $value = '(CAST('.$value.' AS char(255) ARRAY))';
                }
            break;
            
            case Database::INDEX_UNIQUE:
                $type = 'UNIQUE INDEX';
            break;
            
            case Database::INDEX_FULLTEXT:
                $type = 'FULLTEXT INDEX';
            break;
            
            default:
                throw new Exception('Unknown Index Type:' . $type);
            break;
        }

        return 'CREATE '.$type.' '.$id.' ON '.$this->getNamespace().'.'.$collection.' ( '.implode(', ', $attributes).' );';
    }

    /**
     * Get SQL Permissions
     * 
     * @param array $roles
     * @param string $operator
     * @param string $placeholder
     * @param mixed $value
     * 
     * @return string
     */
    protected function getSQLPermissions(array $roles): string
    {
        foreach($roles as &$role) {
            $role = 'JSON_CONTAINS(_read, '.$this->getPDO()->quote("\"".$role."\"").', \'$\')';
        }

        return '('.implode(' OR ', $roles).')';
    }
}