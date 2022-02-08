<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Database;

class Postgres extends MariaDB
{
    /**
     * Create Database
     *
     * @param string $name
     * 
     * @return bool
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        // $results = $this->getPDO()->prepare("SELECT 1 FROM pg_database WHERE datname='{$name}'");
        // $results->execute();
        // $results = $results->fetchAll();

        // if(!empty($results))
        //     return false;

        return $this->getPDO()
            ->prepare("CREATE SCHEMA IF NOT EXISTS \"{$name}\"")
            ->execute();
    }

    /**
     * Get SQL Type
     * 
     * @param string $type
     * @param int $size in chars
     * 
     * @return string
     */
    protected function getSQLType(string $type, int $size, bool $signed = true): string
    {
        switch ($type) {
            case Database::VAR_STRING:
                // $size = $size * 4; // Convert utf8mb4 size to bytes
                if($size > 16383) {
                    return 'TEXT';
                }

                return "VARCHAR({$size})";
            break;

            case Database::VAR_INTEGER:  // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554

                if($size >= 8) { // INT = 4 bytes, BIGINT = 8 bytes
                    return 'BIGINT';
                }

                return 'INTEGER';
            break;

            case Database::VAR_FLOAT:
                return 'REAL';
            break;

            case Database::VAR_BOOLEAN:
                return 'BOOLEAN';
            break;

            case Database::VAR_DOCUMENT:
                return 'VARCHAR';
            break;

            default:
                throw new Exception('Unknown Type');
            break;
        }
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
    protected function getSQLIndex(string $collection, string $id,  string $type, array $attributes): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
            case Database::INDEX_ARRAY:
                $type = 'INDEX';
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
     * Create Collection
     * 
     * @param string $name
     * @param Document[] $attributes (optional)
     * @param Document[] $indexes (optional)
     * @return bool
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $database = $this->getDefaultDatabase();
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        if (!empty($attributes) || !empty($indexes)) {
            foreach ($attributes as &$attribute) {
                $attrId = $this->filter($attribute->getId());
                $attrType = $this->getSQLType($attribute->getAttribute('type'), $attribute->getAttribute('size', 0), $attribute->getAttribute('signed', true));

                if($attribute->getAttribute('array')) {
                    $attrType = 'TEXT';
                }

                $attribute = "\"{$attrId}\" {$attrType}, ";
            }

            foreach ($indexes as &$index) {
                $indexId = $this->filter($index->getId()); 
                $indexType = $this->getSQLIndexType($index->getAttribute('type'));

                $indexAttributes = $index->getAttribute('attributes');
                foreach ($indexAttributes as $key => &$attribute) {
                    $indexLength = $index->getAttribute('lengths')[$key] ?? '';
                    $indexLength = (empty($indexLength)) ? '' : '('.(int)$indexLength.')';
                    $indexOrder = $index->getAttribute('orders')[$key] ?? '';
                    $indexAttribute = $this->filter($attribute);

                    if ($indexType === Database::INDEX_FULLTEXT) {
                        $indexOrder = '';
                    }

                    $attribute = "\"{$indexAttribute}\"{$indexLength} {$indexOrder}";
                }

                $index = "{$indexType} \"{$indexId}\" (" . \implode(", ", $indexAttributes) . " ),";

            }

            $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS \"{$database}\".\"{$namespace}_{$id}\" (
                    \"_id\" SERIAL NOT NULL,
                    \"_uid\" CHAR(255) NOT NULL,
                    \"_read\" " . $this->getTypeForReadPermission() . " NOT NULL,
                    \"_write\" TEXT NOT NULL,
                    " . \implode(' ', $attributes) . "
                    PRIMARY KEY (\"_id\"),
                    " . \implode(' ', $indexes) . "
                    CONSTRAINT \"index_{$namespace}_{$id}\" UNIQUE (\"_uid\")
                  )")
                ->execute();

        } else {
            $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS \"{$database}\".\"{$namespace}_{$id}\" (
                    \"_id\" SERIAL(11) NOT NULL,
                    \"_uid\" CHAR(255) NOT NULL,
                    \"_read\" " . $this->getTypeForReadPermission() . " NOT NULL,
                    \"_write\" TEXT NOT NULL,
                    PRIMARY KEY (\"_id\"),
                    CONSTRAINT \"index_{$namespace}_{$id}\" UNIQUE (\"_uid\")
                  )")
                ->execute();
        }

        // Update $this->getIndexCount when adding another default index
        return $this->createIndex($id, '_index2', $this->getIndexTypeForReadPermission(), ['_read'], [], []);
    }
}
