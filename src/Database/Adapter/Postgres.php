<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use PDOException;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;

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
     * Create Index
     * 
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array $attributes
     * @param array $lengths
     * @param array $orders
     * 
     * @return bool
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        foreach($attributes as $key => &$attribute) {
            $length = $lengths[$key] ?? '';
            $length = (empty($length)) ? '' : '('.(int)$length.')';
            $order = $orders[$key] ?? '';
            $attribute = $this->filter($attribute);

            if(Database::INDEX_FULLTEXT === $type) {
                $order = '';
            }

            $attribute = "\"{$attribute}\"{$length} {$order}";
        }

        return $this->getPDO()
            ->prepare($this->getSQLIndex($name, $id, $type, $attributes))
            ->execute();
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
                $type = 'UNIQUE INDEX';
            break;

            default:
                throw new Exception('Unknown Index Type:' . $type);
            break;
        }

        return 'CREATE '.$type.' "'.$id.'" ON "'.$this->getDefaultDatabase().'"."'.$this->getNamespace().'_'.$collection.'" ( '.implode(', ', $attributes).' );';
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string $database database name
     * @param string $collection (optional) collection name
     *
     * @return bool
     */
    public function exists(string $database, string $collection = null): bool
    {
        $database = $this->filter($database);

        if (!\is_null($collection)) {
            $collection = $this->filter($collection);

            $select = 'TABLE_NAME';
            $from = 'INFORMATION_SCHEMA.TABLES' ;
            $where = 'TABLE_SCHEMA = :schema AND TABLE_NAME = :table';
            $match = "{$this->getNamespace()}_{$collection}";
        } else {
            $select = 'SCHEMA_NAME';
            $from = 'INFORMATION_SCHEMA.SCHEMATA' ;
            $where = 'SCHEMA_NAME = :schema';
            $match = $database;
        }

        $stmt = $this->getPDO()
            ->prepare("SELECT {$select}
                FROM {$from}
                WHERE {$where};");

        $stmt->bindValue(':schema', $database, PDO::PARAM_STR);

        if (!\is_null($collection)) {
            $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", PDO::PARAM_STR);
        }

        $stmt->execute();

        $document = $stmt->fetch(PDO::FETCH_ASSOC);

        return (($document[strtolower($select)] ?? '') === $match);
    }


    /**
     * Delete Database
     * 
     * @param string $name
     *
     * @return bool
     */
    public function delete(string $name): bool
    {
        $name = $this->filter($name);

        return $this->getPDO()
            ->prepare("DROP SCHEMA \"{$name}\" CASCADE;")
            ->execute();
    }

    /**
     * Create Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $attributes = $document->getAttributes();
        $name = $this->filter($collection);
        $columns = '';

        $this->getPDO()->beginTransaction();

        /**
         * Insert Attributes
         */
        foreach ($attributes as $attribute => $value) { // Parse statement
            $column = $this->filter($attribute);
            $columns .= "\"{$column}\"" . '=:' . $column . ',';
        }

        $stmt = $this->getPDO()
            ->prepare("INSERT INTO \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\"
                VALUES( {$columns} _uid = :_uid, _read = :_read, _write = :_write)");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);
        $stmt->bindValue(':_read', json_encode($document->getRead()), PDO::PARAM_STR);
        $stmt->bindValue(':_write', json_encode($document->getWrite()), PDO::PARAM_STR);

        foreach ($attributes as $attribute => $value) {
            if(is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
        }

        var_dump($stmt);

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            switch ($e->getCode()) {
                case 1062:
                case 23000:
                    $this->getPDO()->rollBack();
                    throw new Duplicate('Duplicated document: '.$e->getMessage());
                    break;

                default:
                    throw $e;
                    break;
            }
        }

        if(!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return $document;
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
                    \"_id\" SERIAL NOT NULL,
                    \"_uid\" CHAR(255) NOT NULL,
                    \"_read\" " . $this->getTypeForReadPermission() . " NOT NULL,
                    \"_write\" TEXT NOT NULL,
                    PRIMARY KEY (\"_id\"),
                    CONSTRAINT \"index_{$namespace}_{$id}\" UNIQUE (\"_uid\")
                  )")
                ->execute();
        }

        // Update $this->getIndexCount when adding another default index
        return $this->createIndex($id, "_index2_{$namespace}_{$id}", $this->getIndexTypeForReadPermission(), ['_read'], [], []);
    }
}
