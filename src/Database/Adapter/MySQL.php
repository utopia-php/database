<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use Utopia\Database\Database;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

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

    /**
     * Find and Delete Documents
     *
     * Find and delete data sets using chosen queries
     *
     * @param string $collection
     * @param \Utopia\Database\Query[] $queries
     * @param int $limit
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     * @param bool $count
     *
     * @return bool
     */
    public function findAndDelete(string $collection, array $queries = [], int $limit = 25, array $orderAttributes = [], array $orderTypes = []): bool
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = ['1=1'];
        $orders = [];
        
        foreach($orderAttributes as $i => $attribute) {
            $attribute = $this->filter($attribute);
            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);
            $orders[] = $attribute.' '.$orderType;
        }

        $permissions = (Authorization::$status) ? $this->getSQLPermissions($roles) : '1=1'; // Disable join when no authorization required

        foreach($queries as $i => $query) {
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.'.$query->getAttribute(), $query->getOperator(), ':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value);
            }
            $condition = implode(' OR ', $conditions);
            $where[] = empty($condition) ? '' : '('.$condition.')';
        }

        $order = (!empty($orders)) ? 'ORDER BY '.implode(', ', $orders) : '';

        $this->getPDO()->beginTransaction();

        $stmt = $this->getPDO()->prepare("DELETE FROM {$this->getNamespace()}.{$name} table_main
            WHERE {$permissions} AND ".implode(' AND ', $where)."
            {$order}
            LIMIT :limit;
        ");

        foreach($queries as $i => $query) {
            if($query->getOperator() === Query::TYPE_SEARCH) continue;
            foreach($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);

        if(!$stmt->execute()) {
            $this->getPDO()->rollBack();
            throw new Exception('Failed to delete records');
        }
        
        if(!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return true;
    }
}