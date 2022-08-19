<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Database;

class MySQL extends MariaDB
{
    /**
     * Returns the attribute type for read permissions
     *
     * @return string
     */
    protected function getTypeForReadPermission(): string
    {
        return "JSON";
    }

    /**
     * Returns the index type for read permissions
     *
     * @return string
     */
    protected function getIndexTypeForReadPermission(): string
    {
        return Database::INDEX_ARRAY;
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
                throw new Exception('Unknown Index Type:' . $type);
                break;
        }

        return 'CREATE '.$type.' `'.$id.'` ON `'.$this->getDefaultDatabase().'`.`'.$this->getNamespace().'_'.$collection.'` ( '.implode(', ', $attributes).' );';
    }

    /**
     * Get SQL query to aggregate permissions as JSON array
     *
     * @param string $collection
     * @return string 
     * @throws Exception 
     */
    protected function getSQLPermissionsQuery(string $collection): string
    {
        $permissions = '';
        foreach (Database::PERMISSIONS as $i => $type) {
            $permissions .= "(
                    SELECT JSON_ARRAYAGG(_permission)
                    FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$collection}_perms`
                    WHERE
                        _document = table_main._uid
                        AND _type = {$this->getPDO()->quote($type)}
                ) as _{$type}";

            if ($i !== \array_key_last(Database::PERMISSIONS)) {
                $permissions .= ",\n";
            }
        }
        return $permissions;
    }
}
