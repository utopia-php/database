<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

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

        return $this->getPDO()
            ->prepare("CREATE SCHEMA IF NOT EXISTS \"{$name}\"")
            ->execute();
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

        $document = $stmt->fetch();

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
     * Create Collection
     *
     * @param string $name
     * @param Document[] $attributes (optional)
     * @param Document[] $indexes (optional)
     * @return bool
     * @throws Exception
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $database = $this->getDefaultDatabase();
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        $this->getPDO()->beginTransaction();

        foreach ($attributes as &$attribute) {
            $attrId = $this->filter($attribute->getId());
            $attrType = $this->getSQLType($attribute->getAttribute('type'), $attribute->getAttribute('size', 0), $attribute->getAttribute('signed', true));

            if($attribute->getAttribute('array')) {
                $attrType = 'TEXT';
            }

            $attribute = "\"{$attrId}\" {$attrType}, ";
        }

        // todo: why TEXT[] for _read _write?

        $stmt = $this->getPDO()->prepare("CREATE TABLE IF NOT EXISTS \"{$database}\".\"{$namespace}_{$id}\" (
                \"_id\" SERIAL PRIMARY KEY,
                \"_uid\" VARCHAR(255) NOT NULL,
                " . \implode(' ', $attributes) . "
                \"_read\" TEXT[] NOT NULL,
                \"_write\" TEXT[] NOT NULL
                )");

        $stmtIndex = $this->getPDO()->prepare("CREATE UNIQUE INDEX IF NOT EXISTS \"{$namespace}_{$id}_uid\" on \"{$database}\".\"{$namespace}_{$id}\" (LOWER(_uid));");

        try {
            $stmt->execute();
            $stmtIndex->execute();
            foreach ($indexes as &$index) {
                $indexId = $this->filter($index->getId()); 
                $indexAttributes = $index->getAttribute('attributes');
                $this->createIndex($id, $indexId, $index->getAttribute('type'), $indexAttributes, [], $index->getAttribute("orders"));
            }
        }
        catch(Exception $e){
            // todo: remove this line
            var_dump($e->getMessage());
            $this->getPDO()->rollBack();
            throw new Exception('Failed to create collection');
        }
        
        if(!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        // Update $this->getIndexCount when adding another default index
        return $this->createIndex($id, "read", $this->getIndexTypeForReadPermission(), ['_read'], [], []);

        }

    /**
     * Delete Collection
     * 
     * @param string $id
     * @return bool
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        return $this->getPDO()
            ->prepare("DROP TABLE \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$id}\";")
            ->execute();
    }

    /**
     * Create Attribute
     * 
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $array
     * 
     * @return bool
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $type = $this->getSQLType($type, $size, $signed);

        if($array) {
            $type = 'TEXT';
        }

        return $this->getPDO()
            ->prepare("ALTER TABLE \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\"
                ADD COLUMN \"{$id}\" {$type};")
            ->execute();
    }

    /**
     * Delete Attribute
     * 
     * @param string $collection
     * @param string $id
     * @param bool $array
     * 
     * @return bool
     */
    public function deleteAttribute(string $collection, string $id, bool $array = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        return $this->getPDO()
            ->prepare("ALTER TABLE \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\"
                DROP COLUMN \"{$id}\";")
            ->execute();
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
            //$length = $lengths[$key] ?? '';
            //$length = (empty($length)) ? '' : '('.(int)$length.')'; // not in use ?//
            $order = $orders[$key] ?? '';
            $attribute = $this->filter($attribute);
            $attribute = '"' . $attribute . '"';

            if(Database::INDEX_UNIQUE === $type) {
                $attribute = 'lower('.$attribute.')'; // case insensitive
            }

            if(Database::INDEX_FULLTEXT === $type) {
                $order = '';
            }

            $attribute .= !empty($order) ? ' ' . $order : '';
        }

        return $this->getPDO()
            ->prepare($this->getSQLIndex($name, $id, $type, $attributes))
            ->execute();
    }

    /**
     * Delete Index
     * 
     * @param string $collection
     * @param string $id
     * 
     * @return bool
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $id = $this->filter($id);
        $schemaName = $this->getDefaultDatabase();

        return $this->getPDO()
            ->prepare("DROP INDEX IF EXISTS \"{$schemaName}\".{$id};")
            ->execute();
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return Document
     * @throws Exception
     */
    public function getDocument(string $collection, string $id): Document
    {
        $name = $this->filter($collection);

        $stmt = $this->getPDO()->prepare("SELECT *
            FROM \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\"
            WHERE _uid = :_uid
            LIMIT 1;
        ");

        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);
        $stmt->execute();

        /** @var array $document */
        $document = $stmt->fetch();

        if(empty($document)) {
            return new Document([]);
        }

        $document['$id'] = $document['_uid'];
        $document['$internalId'] = $document['_id']; // we use ATTR_STRINGIFY_FETCHES no need to to cast
        $document['$read'] = (isset($document['_read'])) ? $this->encodeArray($document['_read']) : [];
        $document['$write'] = (isset($document['_write'])) ? $this->encodeArray($document['_write']) : [];

        unset($document['_id']);
        unset($document['_uid']);
        unset($document['_read']);
        unset($document['_write']);

        return new Document($document);
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
        $columnNames = '';
        $columns = '';

        $this->getPDO()->beginTransaction();

        /**
         * Insert Attributes
         */
        foreach ($attributes as $attribute => $value) { // Parse statement
            $column = $this->filter($attribute);
            $columnNames .= "\"{$column}\"" . ', ';
            $columns .= ":" . $column . ', ';
        }

        $stmt = $this->getPDO()
            ->prepare("INSERT INTO \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\"
                ({$columnNames} _uid, _read, _write)
                VALUES ({$columns} :_uid, :_read, :_write)");

        $read = array_map(fn($role) => '"'.$role.'"', $document->getRead());
        $write = array_map(fn($role) => '"'.$role.'"', $document->getWrite());
        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);
        $stmt->bindValue(':_read', $this->decodeArray($read), PDO::PARAM_STR);
        $stmt->bindValue(':_write', $this->decodeArray($write), PDO::PARAM_STR);

        foreach ($attributes as $attribute => $value) {
            if(is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? ($value == true ? "true" : "false") : $value;
            $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
        }

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            switch ($e->getCode()) {
                case 1062:
                case 23505:
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
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     */
    public function updateDocument(string $collection, Document $document): Document
    {
        $attributes = $document->getAttributes();
        $name = $this->filter($collection);
        $columns = '';

        $this->getPDO()->beginTransaction();

        /**
         * Update Attributes
         */
        foreach ($attributes as $attribute => $value) { // Parse statement
            $column = $this->filter($attribute);
            $columns .= "\"{$column}\"" . '=:' . $column . ',';
        }

        $stmt = $this->getPDO()
            ->prepare("UPDATE \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\"
                SET {$columns} _uid = :_uid, _read = :_read, _write = :_write WHERE _uid = :_uid");

        $read = array_map(fn($role) => '"'.$role.'"', $document->getRead());
        $write = array_map(fn($role) => '"'.$role.'"', $document->getWrite());
        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);
        $stmt->bindValue(':_read', $this->decodeArray($read), PDO::PARAM_STR);
        $stmt->bindValue(':_write', $this->decodeArray($write), PDO::PARAM_STR);

        foreach ($attributes as $attribute => $value) {
            if(is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? ($value == true ? "true" : "false") : $value;
            $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
        }

        if(!empty($attributes)) {
            try {
                $stmt->execute();
            } catch (PDOException $e) {
                switch ($e->getCode()) {
                    case 1062:
                    case 23505:
                        $this->getPDO()->rollBack();
                        throw new Duplicate('Duplicated document: '.$e->getMessage());
                        break;

                    default:
                        throw $e;
                        break;
                }
            }
        }

        if(!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return $document;
    }

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $name = $this->filter($collection);

        $this->getPDO()->beginTransaction();

        $stmt = $this->getPDO()
            ->prepare("DELETE FROM \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\"
                WHERE _uid = :_uid");

        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);

        if(!$stmt->execute()) {
            $this->getPDO()->rollBack();
            throw new Exception('Failed to clean document');
        }

        if(!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return true;
    }

         /**
     * Find Documents
     *
     * Find data sets using chosen queries
     *
     * @param string $collection
     * @param array $queries
     * @param int $limit
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     * @param array $cursor
     * @param string $cursorDirection
     *
     * @return array 
     * @throws Exception 
     * @throws PDOException 
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = ['1=1'];
        $orders = [];

        $orderAttributes = \array_map(function($orderAttribute) {
            return $orderAttribute === '$id' ? '_uid' : $orderAttribute;
        }, $orderAttributes);

        $hasIdAttribute = false;
        foreach($orderAttributes as $i => $attribute) {
            if($attribute === '_uid') {
                $hasIdAttribute = true;
            }
            $attribute = $this->filter($attribute);
            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);

            // Get most dominant/first order attribute
            if ($i === 0 && !empty($cursor)) {
                $orderOperatorInternalId = Query::TYPE_GREATER; // To preserve natural order
                $orderOperator = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;

                if ($cursorDirection === Database::CURSOR_BEFORE) {
                    $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                    $orderOperatorInternalId = $orderType === Database::ORDER_ASC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
                    $orderOperator = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
                }

                $where[] = "(
                        {$attribute} {$this->getSQLOperator($orderOperator)} :cursor 
                        OR (
                            {$attribute} = :cursor 
                            AND
                            _id {$this->getSQLOperator($orderOperatorInternalId)} {$cursor['$internalId']}
                        )
                    )";
            } else if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }

            $orders[] = $attribute.' '.$orderType;
        }

        // Allow after pagination without any order
        if (empty($orderAttributes) && !empty($cursor)) {
            $orderType = $orderTypes[0] ?? Database::ORDER_ASC;
            $orderOperator = $cursorDirection === Database::CURSOR_AFTER ? (
                $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER
            ) : (
                $orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER
            );
            $where[] = "( _id {$this->getSQLOperator($orderOperator)} {$cursor['$internalId']} )";
        }

        // Allow order type without any order attribute, fallback to the natural order (_id)
        if(!$hasIdAttribute) {
            if (empty($orderAttributes) && !empty($orderTypes)) {
                $order = $orderTypes[0] ?? Database::ORDER_ASC;
                if ($cursorDirection === Database::CURSOR_BEFORE) {
                    $order = $order === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                }
    
                $orders[] = 'table_main._id ' . $this->filter($order);
            } else {
                $orders[] = 'table_main._id ' . ($cursorDirection === Database::CURSOR_AFTER ? Database::ORDER_ASC : Database::ORDER_DESC); // Enforce last ORDER by '_id'
            }
        }

        $permissions = (Authorization::$status) ? $this->getSQLPermissions($roles) : '1=1'; // Disable join when no authorization required
        foreach($queries as $i => $query) {
            if($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('"table_main"."'.$query->getAttribute().'"', $query->getOperator(), ':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value);
            }
            $condition = implode(' OR ', $conditions);
            $where[] = empty($condition) ? '' : '('.$condition.')';
        }

        $order = 'ORDER BY '.implode(', ', $orders);

        $stmt = $this->getPDO()->prepare("SELECT table_main.* FROM \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\" table_main
            WHERE {$permissions} AND ".implode(' AND ', $where)."
            {$order}
            LIMIT :limit OFFSET :offset;
        ");

        foreach($queries as $i => $query) {
            if($query->getOperator() === Query::TYPE_SEARCH) continue;
            foreach($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        if (!empty($cursor) && !empty($orderAttributes) && array_key_exists(0, $orderAttributes)) {
            $attribute = $orderAttributes[0];
            if (is_null($cursor[$attribute] ?? null)) {
                throw new Exception("Order attribute '{$attribute}' is empty.");
            }
            $stmt->bindValue(':cursor', $cursor[$attribute], $this->getPDOType($cursor[$attribute]));
        }

        $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $stmt->execute();

        $results = $stmt->fetchAll();

        foreach ($results as &$value) {
            $value['$id'] = $value['_uid'];
            $value['$internalId'] = strval($value['_id']);
            $value['$read'] = (isset($value['_read'])) ? $this->encodeArray($value['_read']) : [];
            $value['$write'] = (isset($value['_write'])) ? $this->encodeArray($value['_write']) : [];
            unset($value['_uid']);
            unset($value['_id']);
            unset($value['_read']);
            unset($value['_write']);

            $value = new Document($value);
        }

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $results = array_reverse($results); //TODO: check impact on array_reverse
        }

        return $results;
    }

    

    /**
     * Count Documents
     *
     * Count data set size using chosen queries
     *
     * @param string $collection
     * @param array $queries
     * @param int $max
     *
     * @return int
     */
    public function count(string $collection, array $queries = [], int $max = 0): int
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = ['1=1'];
        $limit = ($max === 0) ? '' : 'LIMIT :max';

        $permissions = (Authorization::$status) ? $this->getSQLPermissions($roles) : '1=1'; // Disable join when no authorization required

        foreach($queries as $i => $query) {
            if($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('"table_main"."'.$query->getAttribute().'"', $query->getOperator(), ':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value);
            }

            $condition = implode(' OR ', $conditions);
            $where[] = empty($condition) ? '' : '('.$condition.')';
        }

        $stmt = $this->getPDO()->prepare("SELECT COUNT(1) as sum FROM (SELECT 1 FROM \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\" table_main
            WHERE {$permissions} AND ".implode(' AND ', $where)."
            {$limit}) table_count
        ");

        foreach($queries as $i => $query) {
            if($query->getOperator() === Query::TYPE_SEARCH) continue;
            foreach($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        if($max !== 0) {
            $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        }

        $stmt->execute();

        /** @var array $result */
        $result = $stmt->fetch();

        return $result['sum'] ?? 0;
    }

    /**
     * Sum an Attribute
     *
     * Sum an attribute using chosen queries
     *
     * @param string $collection
     * @param string $attribute
     * @param Query[] $queries
     * @param int $max
     *
     * @return int|float
     */
    public function sum(string $collection, string $attribute, array $queries = [], int $max = 0): int|float
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = ['1=1'];
        $limit = ($max === 0) ? '' : 'LIMIT :max';

        $permissions = (Authorization::$status) ? $this->getSQLPermissions($roles) : '1=1'; // Disable join when no authorization required

        foreach($queries as $i => $query) {
            if($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('"table_main"."'.$query->getAttribute().'"', $query->getOperator(), ':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value);
            }

            $where[] = implode(' OR ', $conditions);
        }

        $stmt = $this->getPDO()->prepare("SELECT SUM({$attribute}) as sum
            FROM (
                SELECT {$attribute}
                FROM \"{$this->getDefaultDatabase()}\".\"{$this->getNamespace()}_{$name}\" table_main
                WHERE {$permissions} AND ".implode(' AND ', $where)."
                {$limit}
            ) table_count");

        foreach($queries as $i => $query) {
            if($query->getOperator() === Query::TYPE_SEARCH) continue;
            foreach($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        if($max !== 0) {
            $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        }

        $stmt->execute();

        /** @var array $result */
        $result = $stmt->fetch();

        return $result['sum'] ?? 0;
    }

    /**
     * Get SQL Type
     *
     * @param string $type
     * @param int $size in chars
     *
     * @return string
     * @throws Exception
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

            case Database::VAR_INTEGER:  // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554

                if($size >= 8) { // INT = 4 bytes, BIGINT = 8 bytes
                    return 'BIGINT';
                }

                return 'INTEGER';

            case Database::VAR_FLOAT:
                return 'REAL';

            case Database::VAR_BOOLEAN:
                return 'BOOLEAN';

            case Database::VAR_DOCUMENT:
                return 'VARCHAR';

            default:
                throw new Exception('Unknown Type');
        }
    }

    /**
     * Get SQL Condtions
     * 
     * @param string $attribute
     * @param string $operator
     * @param string $placeholder
     * @param mixed $value
     * 
     * @return string
     */
    protected function getSQLCondition(string $attribute, string $operator, string $placeholder, $value): string
    {
        switch ($operator) {
            case Query::TYPE_SEARCH:
                $value = "'".$value."'";
                return "to_tsvector({$attribute}) @@ to_tsquery({$value})";
            break;

            default:
                return $attribute.' '.$this->getSQLOperator($operator).' '.$placeholder; // Using \"attrubute_\" to avoid conflicts with custom names;
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
     * @throws Exception
     */
    protected function getSQLIndex(string $collection, string $id,  string $type, array $attributes): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
            case Database::INDEX_FULLTEXT:
            case Database::INDEX_ARRAY:
                $type = 'INDEX';
            break;

            case Database::INDEX_UNIQUE:
                $type = 'UNIQUE INDEX';
            break;

            default:
                throw new Exception('Unknown Index Type:' . $type);
        }

        // TODO: for index UNIQUE becuase of case insensitive add lower()

        var_dump('CREATE '.$type.' "'.$this->getNamespace().'_'.$collection.'_'.$id.'" ON "'.$this->getDefaultDatabase().'"."'.$this->getNamespace().'_'.$collection.'" ('.implode(', ', $attributes).')');

        return 'CREATE '.$type.' "'.$this->getNamespace().'_'.$collection.'_'.$id.'" ON "'.$this->getDefaultDatabase().'"."'.$this->getNamespace().'_'.$collection.'" ('.implode(', ', $attributes).')';
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
        $roles = array_map(fn($role) => "'".$role."'", $roles);
        return "(table_main._read && ARRAY[".implode(',', $roles)."])";
    }

    /**
     * Get PDO Type
     * 
     * @param mixed $value
     * 
     * @return int
     */
    protected function getPDOType($value): int
    {
        switch (gettype($value)) {
            case 'string':
                return PDO::PARAM_STR;
            break;

            case 'boolean':
                return PDO::PARAM_BOOL;
            break;

            //case 'float': // (for historical reasons "double" is returned in case of a float, and not simply "float")
            case 'double':
                return PDO::PARAM_STR;
            break;

            case 'integer':
                return PDO::PARAM_INT;
            break;

            case 'NULL':
                return PDO::PARAM_NULL;
            break;

            default:
                throw new Exception('Unknown PDO Type for ' . gettype($value));
            break;
        }
    }

    /**
     * Encode array
     * 
     * @param string $value
     * 
     * @return array
     */
    protected function encodeArray(string $value): array
    {
        return explode(',', substr($value, 1, -1));
    }

    /**
     * Decode array
     * 
     * @param array $value
     * 
     * @return string
     */
    protected function decodeArray(array $value): string
    {
        return '{'.implode(",", $value).'}';
    }
}
