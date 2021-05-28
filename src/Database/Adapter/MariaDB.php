<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use PDOException;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class MariaDB extends Adapter
{
    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param PDO $pdo
     */
    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }
    
    /**
     * Create Database
     * 
     * @return bool
     */
    public function create(): bool
    {
        $name = $this->getNamespace();

        return $this->getPDO()
            ->prepare("CREATE DATABASE {$name} /*!40100 DEFAULT CHARACTER SET utf8mb4 */;")
            ->execute();
    }

    /**
     * Check if database exists
     *
     * @return bool
     */
    public function exists(): bool
    {
        $name = $this->getNamespace();

        $stmt = $this->getPDO()
            ->prepare("SELECT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME = :schema;");
            
        $stmt->bindValue(':schema', $name, PDO::PARAM_STR);

        $stmt->execute();
        
        $document = $stmt->fetch();

        return (($document['SCHEMA_NAME'] ?? '') == $name);
    }

    /**
     * List Databases
     * 
     * @return array
     */
    public function list(): array
    {
        $list = [];
        return $list;
    }

    /**
     * Delete Database
     * 
     * @return bool
     */
    public function delete(): bool
    {
        $name = $this->getNamespace();

        return $this->getPDO()
            ->prepare("DROP DATABASE {$name};")
            ->execute();
    }

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
            ->prepare("CREATE TABLE IF NOT EXISTS {$this->getNamespace()}.{$id}_permissions (
                `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `_uid` CHAR(255) NOT NULL,
                `_role` CHAR(128) NOT NULL,
                PRIMARY KEY (`_id`),
                INDEX `_index1` (`_uid`),
                INDEX `_index2` (`_role` ASC)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
            ->execute();

        return $this->getPDO()
            ->prepare("CREATE TABLE IF NOT EXISTS {$this->getNamespace()}.{$id} (
                `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `_uid` CHAR(255) NOT NULL,
                `_permissions` TEXT NOT NULL,
                PRIMARY KEY (`_id`),
                UNIQUE KEY `_index1` (`_uid`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
            ->execute();
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
        
        $this->getPDO()
            ->prepare("DROP TABLE {$this->getNamespace()}.{$id}_permissions;")
            ->execute();

        return $this->getPDO()
            ->prepare("DROP TABLE {$this->getNamespace()}.{$id};")
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
            $type = 'LONGTEXT';
        }

        return $this->getPDO()
            ->prepare("ALTER TABLE {$this->getNamespace()}.{$name}
                ADD COLUMN `{$id}` {$type};")
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
            ->prepare("ALTER TABLE {$this->getNamespace()}.{$name}
                DROP COLUMN `{$id}`;")
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
            $length = $lengths[$key] ?? '';
            $length = (empty($length)) ? '' : '('.(int)$length.')';
            $order = $orders[$key] ?? 'ASC';
            $attribute = $this->filter($attribute);

            if(Database::INDEX_FULLTEXT === $type) {
                $order = '';
            }

            $attribute = "`{$attribute}`{$length} {$order}";
        }

        return $this->getPDO()
            ->prepare("CREATE ".$this->getSQLIndex($type)." `{$id}` ON {$this->getNamespace()}.{$name} (".implode(', ', $attributes).");")
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
        $name = $this->filter($collection);
        $id = $this->filter($id);

        return $this->getPDO()
            ->prepare("ALTER TABLE {$this->getNamespace()}.{$name}
                DROP INDEX `{$id}`;")
            ->execute();
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return Document
     */
    public function getDocument(string $collection, string $id): Document
    {
        $name = $this->filter($collection);
        
        $stmt = $this->getPDO()->prepare("SELECT * FROM {$this->getNamespace()}.{$name}
            WHERE _uid = :_uid
            LIMIT 1;
        ");

        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);

        $stmt->execute();

        $document = $stmt->fetch();

        if(empty($document)) {
            return new Document([]);
        }

        $permissions = (isset($document['_permissions'])) ? json_decode($document['_permissions'], true) : [];
        $document['$id'] = $document['_uid'];
        $document['$read'] = $permissions[Database::PERMISSION_READ] ?? [];
        $document['$write'] = $permissions[Database::PERMISSION_WRITE] ?? [];
        unset($document['_id']);
        unset($document['_uid']);
        unset($document['_permissions']);

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
        $columns = '';

        $this->getPDO()->beginTransaction();

        /**
         * Insert Attributes
         */
        foreach ($attributes as $attribute => $value) { // Parse statement
            $column = $this->filter($attribute);
            $columns .= "`{$column}`" . '=:' . $column . ',';
        }

        $stmt = $this->getPDO()
            ->prepare("INSERT INTO {$this->getNamespace()}.{$name}
                SET {$columns} _uid = :_uid, _permissions = :_permissions");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);
        $stmt->bindValue(':_permissions', json_encode([Database::PERMISSION_READ => $document->getRead(), Database::PERMISSION_WRITE => $document->getWrite()]), PDO::PARAM_STR);

        foreach ($attributes as $attribute => $value) {
            if(is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
        }

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            switch ($e->getCode()) {
                case 1062:
                case 23000:
                    $this->getPDO()->rollBack();
                    throw new Duplicate('Duplicated document: '.$e->getMessage()); // TODO add test for catching this exception
                    break;
                
                default:
                    throw $e;
                    break;
            }
        }

        /**
         * Insert Permissions
         * 
         * Following performance inhancment tips from this discussions:
         * https://stackoverflow.com/a/4559320/2299554
         * https://stackoverflow.com/a/9088630/2299554
         */
        $query = "INSERT INTO {$this->getNamespace()}.{$name}_permissions
                (_uid, _role) VALUES ";
        $values = [];

        foreach ($document->getRead() as $key => $role) {
            $query .= '(?, ?), ';
            $values[] = $document->getId();
            $values[] = $role;
        }

        $stmt = $this->getPDO()->prepare(substr($query, 0, -2)); // Removes the last `, ` from the prepared statement

        if(!$stmt->execute($values)) {
            $this->getPDO()->rollBack();
            throw new Exception('Failed to save permission');
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
            $columns .= "`{$column}`" . '=:' . $column . ',';
        }

        $stmt = $this->getPDO()
            ->prepare("UPDATE {$this->getNamespace()}.{$name}
                SET {$columns} _uid = :_uid, _permissions = :_permissions WHERE _uid = :_uid");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);
        $stmt->bindValue(':_permissions', json_encode([Database::PERMISSION_READ => $document->getRead(), Database::PERMISSION_WRITE => $document->getWrite()]), PDO::PARAM_STR);

        foreach ($attributes as $attribute => $value) {
            if(is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
        }

        if(!empty($attributes)) {
            $stmt->execute();
        }

        /**
         * Update Permissions
         */
        $stmt = $this->getPDO() // Clean all old permissions to avoid any duplications
            ->prepare("DELETE FROM {$this->getNamespace()}.{$name}_permissions
                WHERE _uid = :_uid");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

        if(!$stmt->execute()) {
            $this->getPDO()->rollBack();
            throw new Exception('Failed to clean permissions');
        }

        /**
         * Insert Permissions
         * 
         * Following performance inhancment tips from this discussions:
         * https://stackoverflow.com/a/4559320/2299554
         * https://stackoverflow.com/a/9088630/2299554
         */
        $query = "INSERT INTO {$this->getNamespace()}.{$name}_permissions
                (_uid, _role) VALUES ";
        $values = [];

        foreach ($document->getRead() as $key => $role) {
            $query .= '(?, ?), ';
            $values[] = $document->getId();
            $values[] = $role;
        }

        $stmt = $this->getPDO()->prepare(substr($query, 0, -2)); // Removes the last `, ` from the prepared statement

        if(!$stmt->execute($values)) {
            $this->getPDO()->rollBack();
            throw new Exception('Failed to save permission');
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
            ->prepare("DELETE FROM {$this->getNamespace()}.{$name}
                WHERE _uid = :_uid LIMIT 1");

        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);

        if(!$stmt->execute()) {
            $this->getPDO()->rollBack();
            throw new Exception('Failed to clean document');
        }

        $stmt = $this->getPDO()
            ->prepare("DELETE FROM {$this->getNamespace()}.{$name}_permissions
                WHERE _uid = :_uid");

        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);

        if(!$stmt->execute()) {
            $this->getPDO()->rollBack();
            throw new Exception('Failed to clean permissions');
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
     * @param \Utopia\Database\Query[] $queries
     * @param int $limit
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     * @param bool $count
     *
     * @return Document[]
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = []): array
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = ['1=1'];
        $orders = [];

        foreach($roles as &$role) {
            $role = $this->getPDO()->quote($role, PDO::PARAM_STR);
        }

        foreach($orderAttributes as $i => $attribute) {
            $attribute = $this->filter($attribute);
            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);
            $orders[] = $attribute.' '.$orderType;
        }

        $permissions = (Authorization::$status) ? "INNER JOIN {$this->getNamespace()}.{$name}_permissions as table_permissions
            ON table_main._uid = table_permissions._uid" : ''; // Disable join when no authorization required
        $permissions2 = (Authorization::$status) ? " AND table_permissions._role IN (".implode(',', $roles).")" : ''; // Disable join when no authorization required

        foreach($queries as $i => $query) {
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.'.$query->getAttribute(), $query->getOperator(), ':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value);
            }

            $where[] = implode(' OR ', $conditions);
        }

        $order = (!empty($orders)) ? 'ORDER BY '.implode(', ', $orders) : '';

        $stmt = $this->getPDO()->prepare("SELECT table_main.* FROM {$this->getNamespace()}.{$name} table_main
            {$permissions}
            WHERE ".implode(' AND ', $where)."
            {$permissions2}
            GROUP BY table_main._uid 
            {$order}
            LIMIT :offset, :limit;
        ");

        foreach($queries as $i => $query) {
            if($query->getOperator() === Query::TYPE_SEARCH) continue;
            foreach($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $stmt->execute();

        $results = $stmt->fetchAll();

        foreach ($results as &$value) {
            $permissions = (isset($value['_permissions'])) ? json_decode($value['_permissions'], true) : [];
            $value['$id'] = $value['_uid'];
            $value['$read'] = $permissions[Database::PERMISSION_READ] ?? [];
            $value['$write'] = $permissions[Database::PERMISSION_WRITE] ?? [];
            unset($value['_id']);
            unset($value['_uid']);
            unset($value['_permissions']);

            $value = new Document($value);
        }

        return $results;
    }

    /**
     * Count Documents
     *
     * Count data set size using chosen queries
     *
     * @param string $collection
     * @param \Utopia\Database\Query[] $queries
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

        foreach($roles as &$role) {
            $role = $this->getPDO()->quote($role, PDO::PARAM_STR);
        }

        $permissions = (Authorization::$status) ? "INNER JOIN {$this->getNamespace()}.{$name}_permissions as table_permissions
            ON table_main._uid = table_permissions._uid" : ''; // Disable join when no authorization required
        $permissions2 = (Authorization::$status) ? " AND table_permissions._role IN (".implode(',', $roles).")" : ''; // Disable join when no authorization required

        foreach($queries as $i => $query) {
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.'.$query->getAttribute(), $query->getOperator(), ':attribute_'.$i.'_'.$key.'_'.$query->getAttribute(), $value);
            }

            $where[] = implode(' OR ', $conditions);
        }

        $stmt = $this->getPDO()->prepare("SELECT COUNT(1) as sum FROM (SELECT 1 FROM {$this->getNamespace()}.{$name} table_main
            {$permissions}
            WHERE ".implode(' AND ', $where)."
            {$permissions2}
            GROUP BY table_main._uid 
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

        $result = $stmt->fetch();

        return $result['sum'] ?? 0;
    }

    /**
     * Get max STRING limit
     * 
     * @return int
     */
    public function getStringLimit(): int
    {
        return 4294967295;
    }

    /**
     * Get max INT limit
     * 
     * @return int
     */
    public function getIntLimit(): int
    {
        return 4294967295;
    }

    /**
     * Is index supported?
     * 
     * @return bool
     */
    public function getSupportForIndex(): bool
    {
        return true;
    }

    /**
     * Is unique index supported?
     * 
     * @return bool
     */
    public function getSupportForUniqueIndex(): bool
    {
        return true;
    }

    /**
     * Is fulltext index supported?
     * 
     * @return bool
     */
    public function getSupportForFulltextIndex(): bool
    {
        return true;
    }

    /**
     * Does the adapter handle casting?
     * 
     * @return bool
     */
    public function getSupportForCasting(): bool
    {
        return false;
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
                if($size > 16777215) {
                    return 'LONGTEXT';
                }
                
                if($size > 65535) {
                    return 'MEDIUMTEXT';
                }

                if($size > 16383) {
                    return 'TEXT';
                }
                
                return "VARCHAR({$size})";
            break;

            case Database::VAR_INTEGER:  // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554
                $signed = ($signed) ? '' : ' UNSIGNED';
                return 'INT'.$signed;
            break;

            case Database::VAR_FLOAT:
                $signed = ($signed) ? '' : ' UNSIGNED';
                return 'FLOAT'.$signed;
            break;

            case Database::VAR_BOOLEAN:
                return 'TINYINT(1)';
            break;

            case Database::VAR_DOCUMENT:
                return 'CHAR(255)';
            break;
            
            default:
                throw new Exception('Unknown Type');
            break;
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
                return 'MATCH('.$attribute.') AGAINST('.$this->getPDO()->quote($value).')';
            break;

            default:
                return $attribute.' '.$this->getSQLOperator($operator).' '.$placeholder; // Using `attrubute_` to avoid conflicts with custom names;
            break;
        }
    }

    /**
     * Get SQL Operator
     * 
     * @param string $operator
     * 
     * @return string
     */
    protected function getSQLOperator(string $operator): string
    {
        switch ($operator) {
            case Query::TYPE_EQUAL:
                return '=';
            break;

            case Query::TYPE_NOTEQUAL:
                return '!=';
            break;

            case Query::TYPE_LESSER:
                return '<';
            break;

            case Query::TYPE_LESSEREQUAL:
                return '<=';
            break;

            case Query::TYPE_GREATER:
                return '>';
            break;

            case Query::TYPE_GREATEREQUAL:
                return '>=';
            break;

            default:
                throw new Exception('Unknown Operator:' . $operator);
            break;
        }
    }

    /**
     * Get SQL Index
     * 
     * @param string $operator
     * 
     * @return string
     */
    protected function getSQLIndex(string $type): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
                return 'INDEX';
            break;
            
            case Database::INDEX_UNIQUE:
                return 'UNIQUE INDEX';
            break;
            
            case Database::INDEX_FULLTEXT:
                return 'FULLTEXT INDEX';
            break;

            default:
                throw new Exception('Unknown Index Type:' . $type);
            break;
        }
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
                return PDO::PARAM_INT;
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
     * @return PDO
     *
     * @throws Exception
     */
    protected function getPDO()
    {
        return $this->pdo;
    }
}