<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use PDOException;
use Throwable;
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
    public function __construct($pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * Create Database
     *
     * @param string $name
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        return $this->getPDO()
            ->prepare("CREATE DATABASE IF NOT EXISTS `{$name}` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;")
            ->execute();
    }

    /**
     * Check if Database exists
     * Optionally check if collection exists in Database
     *
     * @param string $database
     * @param string $collection
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function exists(string $database, ?string $collection): bool
    {
        $database = $this->filter($database);

        if (!\is_null($collection)) {
            $collection = $this->filter($collection);

            $select = 'TABLE_NAME';
            $from = 'INFORMATION_SCHEMA.TABLES';
            $where = 'TABLE_SCHEMA = :schema AND TABLE_NAME = :table';
            $match = "{$this->getNamespace()}_{$collection}";
        } else {
            $select = 'SCHEMA_NAME';
            $from = 'INFORMATION_SCHEMA.SCHEMATA';
            $where = 'SCHEMA_NAME = :schema';
            $match = $database;
        }

        $stmt = $this->getPDO()->prepare("SELECT {$select} FROM {$from} WHERE {$where}");

        $stmt->bindValue(':schema', $database, PDO::PARAM_STR);

        if (!\is_null($collection)) {
            $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", PDO::PARAM_STR);
        }

        $stmt->execute();

        $document = $stmt->fetch(PDO::FETCH_ASSOC);

        return (($document[$select] ?? '') === $match);
    }

    /**
     * List Databases
     * 
     * @return array
     */
    public function list(): array
    {
        return [];
    }

    /**
     * Delete Database
     *
     * @param string $name
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function delete(string $name): bool
    {
        $name = $this->filter($name);

        return $this->getPDO()
            ->prepare("DROP DATABASE `{$name}`;")
            ->execute();
    }

    /**
     * Create Collection
     * 
     * @param string $name
     * @param Document[] $attributes
     * @param Document[] $indexes
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $database = $this->getDefaultDatabase();
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        foreach ($attributes as $key => $attribute) {
            $attrId = $this->filter($attribute->getId());
            $attrType = $this->getSQLType($attribute->getAttribute('type'), $attribute->getAttribute('size', 0), $attribute->getAttribute('signed', true));

            if ($attribute->getAttribute('array')) {
                $attrType = 'LONGTEXT';
            }

            $attributes[$key] = "`{$attrId}` {$attrType}, ";
        }

        foreach ($indexes as $key => $index) {
            $indexId = $this->filter($index->getId());
            $indexType = $this->getSQLIndexType($index->getAttribute('type'));

            $indexAttributes = $index->getAttribute('attributes');
            foreach ($indexAttributes as $nested => $attribute) {
                $indexLength = $index->getAttribute('lengths')[$key] ?? '';
                $indexLength = (empty($indexLength)) ? '' : '('.(int)$indexLength.')';
                $indexOrder = $index->getAttribute('orders')[$key] ?? '';
                $indexAttribute = $this->filter($attribute);

                if ($indexType === Database::INDEX_FULLTEXT) {
                    $indexOrder = '';
                }

                $indexAttributes[$nested] = "`{$indexAttribute}`{$indexLength} {$indexOrder}";
            }

            $indexes[$key] = "{$indexType} `{$indexId}` (" . \implode(", ", $indexAttributes) . " ),";
        }

        try {
            $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS `{$database}`.`{$namespace}_{$id}` (
                        `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                        `_uid` CHAR(255) NOT NULL,
                        " . \implode(' ', $attributes) . "
                        PRIMARY KEY (`_id`),
                        " . \implode(' ', $indexes) . "
                        UNIQUE KEY `_index1` (`_uid`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                ->execute();

            $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS `{$database}`.`{$namespace}_{$id}_perms` (
                        `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                        `_type` VARCHAR(12) NOT NULL,
                        `_permission` VARCHAR(255) NOT NULL,
                        `_document` VARCHAR(255) NOT NULL,
                        PRIMARY KEY (`_id`),
                        UNIQUE INDEX `_index1` (`_type`,`_document`,`_permission`),
                        INDEX `_index2` (`_permission`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                ->execute();
        } catch (\Exception $th) {
            $this->getPDO()
                ->prepare("DROP TABLE IF EXISTS `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$id}`, `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$id}_perms`;")
                ->execute();
            throw $th;
        }

        // Update $this->getIndexCount when adding another default index
        return true;
    }

    /**
     * Delete Collection
     * @param string $id
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        return $this->getPDO()
            ->prepare("DROP TABLE `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$id}`, `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$id}_perms`;")
            ->execute();
    }

    /**
     * Create Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @param bool $array
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $type = $this->getSQLType($type, $size, $signed);

        if ($array) {
            $type = 'LONGTEXT';
        }

        return $this->getPDO()
            ->prepare("ALTER TABLE `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}`
                ADD COLUMN `{$id}` {$type};")
            ->execute();
    }

    /**
     * Rename Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $name
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function renameAttribute(string $collection, string $id, string $name): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($id);
        $name = $this->filter($name);

        return $this->getPDO()
            ->prepare("ALTER TABLE `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$collection}` RENAME COLUMN `{$id}` TO `{$name}`;")
            ->execute();
    }

    /**
     * Delete Attribute
     *
     * @param string $collection
     * @param string $id
     * @param bool $array
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteAttribute(string $collection, string $id, bool $array = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        return $this->getPDO()
            ->prepare("ALTER TABLE `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}`
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
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        foreach ($attributes as $key => $attribute) {
            $length = $lengths[$key] ?? '';
            $length = (empty($length)) ? '' : '(' . (int)$length . ')';
            $order = $orders[$key] ?? '';
            $attribute = $this->filter($attribute);

            if (Database::INDEX_FULLTEXT === $type) {
                $order = '';
            }

            $attributes[$key] = "`{$attribute}`{$length} {$order}";
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
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        return $this->getPDO()
            ->prepare("ALTER TABLE `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}`
                DROP INDEX `{$id}`;")
            ->execute();
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     * @return Document
     * @throws Exception
     * @throws PDOException
     */
    public function getDocument(string $collection, string $id): Document
    {
        $name = $this->filter($collection);

        $stmt = $this->getPDO()->prepare("
            SELECT 
                table_main.*,
                {$this->getSQLPermissionsQuery($name, 'read', '$read')},
                {$this->getSQLPermissionsQuery($name, 'write', '$write')}
            FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}` table_main
            WHERE _uid = :_uid
            LIMIT 1;
        ");

        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);

        $stmt->execute();

        /** @var array $document */
        $document = $stmt->fetch(PDO::FETCH_ASSOC);
        if (empty($document)) {
            return new Document([]);
        }

        $document['$id'] = $document['_uid'];
        $document['$internalId'] = (string)$document['_id'];
        $document['$read'] = json_decode($document['$read'], true) ?? [];
        $document['$write'] = json_decode($document['$write'], true) ?? [];

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
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws Duplicate
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
            ->prepare("INSERT INTO `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}`
                SET {$columns} _uid = :_uid");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

        foreach ($attributes as $attribute => $value) {
            if (is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
        }

        $permissions = [];
        foreach ($document->getRead() as $permission) {
            $permissions[] = "('read', '{$permission}', '{$document->getId()}')";
        }

        foreach ($document->getWrite() as $permission) {
            $permissions[] = "('write', '{$permission}', '{$document->getId()}')";
        }

        if (!empty($permissions)) {
            $queryPermissions = "INSERT INTO `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}_perms` (_type, _permission, _document) VALUES " . implode(', ', $permissions);
            $stmtPermissions = $this->getPDO()->prepare($queryPermissions);
        }

        try {
            $stmt->execute();
            if (isset($stmtPermissions)) {
                $stmtPermissions->execute();
            }
        } catch (PDOException $e) {
            switch ($e->getCode()) {
                case 1062:
                case 23000:
                    $this->getPDO()->rollBack();
                    throw new Duplicate('Duplicated document: ' . $e->getMessage());
                    break;

                default:
                    throw $e;
                    break;
            }
        }

        if (!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return $document;
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws Duplicate
     */
    public function updateDocument(string $collection, Document $document): Document
    {
        $attributes = $document->getAttributes();
        $name = $this->filter($collection);
        $columns = '';

        /**
         * Get current permissions from the database
         */
        $permissionsStmt = $this->getPDO()->prepare("
                SELECT _type, _permission
                FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}_perms` p
                WHERE p._document = :_uid
        ");
        $permissionsStmt->bindValue(':_uid', $document->getId());
        $permissionsStmt->execute();
        $permissions = $permissionsStmt->fetchAll(PDO::FETCH_ASSOC);

        $permissions = array_reduce($permissions, function (array $carry, array $item) {
            $carry[$item['_type']][] = $item['_permission'];

            return $carry;
        }, ['read' => [], 'write' => []]);

        $this->getPDO()->beginTransaction();

        /**
         * Get removed Permissions
         */
        $readRemoved = array_diff($permissions['read'], $document->getRead());
        $writeRemoved = array_diff($permissions['write'], $document->getWrite());

        /**
         * Get added Permissions
         */
        $readAdded = array_diff($document->getRead(), $permissions['read']);
        $writeAdded = array_diff($document->getWrite(), $permissions['write']);

        /**
         * Query to remove permissions
         */
        if (!empty($readRemoved) || !empty($writeRemoved)) {
            $removeQuery = 'AND (';
            if ($readRemoved) {
                $removeQuery .= "(
                    _type = 'read'
                    AND _permission IN (" . implode(', ', array_map(fn (string $i) => ":_remove_read_{$i}", array_keys($readRemoved))) . ")
                )";
            }

            if ($readRemoved && $writeRemoved) {
                $removeQuery .= ' OR ';
            }

            if ($writeRemoved) {
                $removeQuery .= "(
                    _type = 'write'
                    AND _permission IN (" . implode(', ', array_map(fn (string $i) => ":_remove_write_{$i}", array_keys($writeRemoved))) . ")
                )";
            }
            $removeQuery .= ')';
        }

        if (isset($removeQuery)) {
            $stmtRemovePermissions = $this->getPDO()
                ->prepare("
                DELETE
                FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}_perms`
                WHERE
                    _document = :_uid
                    {$removeQuery}
            ");
            $stmtRemovePermissions->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

            foreach ($readRemoved as $i => $role) {
                $stmtRemovePermissions->bindValue(":_remove_read_{$i}", $role);
            }
            foreach ($writeRemoved as $i => $role) {
                $stmtRemovePermissions->bindValue(":_remove_write_{$i}", $role);
            }
        }

        /**
         * Query to add permissions
         */
        if ($readAdded || $writeAdded) {
            $stmtAddPermissions = $this->getPDO()
                ->prepare(
                    "
                    INSERT INTO `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}_perms`
                    (_document, _type, _permission) VALUES " . implode(', ', [
                        ...array_map(fn (string $i) => "( :_uid, 'read', :_add_read_{$i} )", array_keys($readAdded)),
                        ...array_map(fn (string $i) => "( :_uid, 'write', :_add_write_{$i} )", array_keys($writeAdded))
                    ])
                );

            $stmtAddPermissions->bindValue(":_uid", $document->getId());
            foreach ($readAdded as $i => $role) {
                $stmtAddPermissions->bindValue(":_add_read_{$i}", $role);
            }
            foreach ($writeAdded as $i => $role) {
                $stmtAddPermissions->bindValue(":_add_write_{$i}", $role);
            }
        }

        /**
         * Update Attributes
         */
        foreach ($attributes as $attribute => $value) {
            $column = $this->filter($attribute);
            $columns .= "`{$column}`" . '=:' . $column . ',';
        }

        $stmt = $this->getPDO()
            ->prepare("UPDATE `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}`
                SET {$columns} _uid = :_uid WHERE _uid = :_uid");

        $stmt->bindValue(':_uid', $document->getId());

        foreach ($attributes as $attribute => $value) {
            if (is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
        }

        if (!empty($attributes)) {
            try {
                $stmt->execute();
                if (isset($stmtRemovePermissions)) {
                    $stmtRemovePermissions->execute();
                }
                if (isset($stmtAddPermissions)) {
                    $stmtAddPermissions->execute();
                }
            } catch (PDOException $e) {
                switch ($e->getCode()) {
                    case 1062:
                    case 23000:
                        $this->getPDO()->rollBack();
                        throw new Duplicate('Duplicated document: ' . $e->getMessage());
                        break;

                    default:
                        throw $e;
                        break;
                }
            }
        }

        if (!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return $document;
    }

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $name = $this->filter($collection);

        $this->getPDO()->beginTransaction();

        $stmt = $this->getPDO()->prepare("DELETE FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}` WHERE _uid = :_uid LIMIT 1");
        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);

        $stmtPermissions = $this->getPDO()->prepare("DELETE FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}_perms` WHERE _document = :_uid");
        $stmtPermissions->bindValue(':_uid', $id, PDO::PARAM_STR);

        try {
            $stmt->execute() || throw new Exception('Failed to delete document');
            $stmtPermissions->execute() || throw new Exception('Failed to clean permissions');
        } catch (\Throwable $th) {
            $this->getPDO()->rollBack();
            throw new Exception($th->getMessage());
        }

        if (!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return true;
    }

    /**
     * Find Documents
     *
     * @param string $collection
     * @param Query[] $queries
     * @param int $limit
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     * @param array $cursor
     * @param string $cursorDirection
     * @return Document[]
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
                        table_main.{$attribute} {$this->getSQLOperator($orderOperator)} :cursor 
                        OR (
                            table_main.{$attribute} = :cursor 
                            AND
                            table_main._id {$this->getSQLOperator($orderOperatorInternalId)} {$cursor['$internalId']}
                        )
                    )";
            } else if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }

            $orders[] = $attribute . ' ' . $orderType;
        }

        // Allow after pagination without any order
        if (empty($orderAttributes) && !empty($cursor)) {
            $orderType = $orderTypes[0] ?? Database::ORDER_ASC;
            $orderOperator = $cursorDirection === Database::CURSOR_AFTER ? ($orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER
            ) : ($orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER
            );
            $where[] = "( table_main._id {$this->getSQLOperator($orderOperator)} {$cursor['$internalId']} )";
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

        foreach ($queries as $i => $query) {
            if ($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.' . $query->getAttribute(), $query->getOperator(), ':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value);
            }
            $condition = implode(' OR ', $conditions);
            $where[] = empty($condition) ? '' : '(' . $condition . ')';
        }

        $order = 'ORDER BY ' . implode(', ', $orders);

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $stmt = $this->getPDO()->prepare("
            SELECT
                table_main.*,
                {$this->getSQLPermissionsQuery($name, 'read', '_read')},
                {$this->getSQLPermissionsQuery($name, 'write', '_write')}
            FROM 
                `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}` table_main
            WHERE " . implode(' AND ', $where) . "
            GROUP BY table_main._uid
            {$order}
            LIMIT :offset, :limit;
        ");

        foreach ($queries as $i => $query) {
            if ($query->getOperator() === Query::TYPE_SEARCH) continue;
            foreach ($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value, $this->getPDOType($value));
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

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

        foreach ($results as $key => $value) {
            $results[$key]['$id'] = $value['_uid'];
            $results[$key]['$internalId'] = $value['_id'];
            $results[$key]['$read'] = json_decode($value['_read'], true) ?? [];
            $results[$key]['$write'] = json_decode($value['_write'], true) ?? [];

            unset($results[$key]['_uid']);
            unset($results[$key]['_id']);
            unset($results[$key]['_read']);
            unset($results[$key]['_write']);

            $results[$key] = new Document($results[$key]);
        }

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $results = array_reverse($results);
        }

        return $results;
    }

    /**
     * Count Documents
     *
     * @param string $collection
     * @param Query[] $queries
     * @param int $max
     * @return int
     * @throws Exception
     * @throws PDOException
     */
    public function count(string $collection, array $queries = [], int $max = 0): int
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = ['1=1'];
        $limit = ($max === 0) ? '' : 'LIMIT :max';

        foreach ($queries as $i => $query) {
            if ($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.' . $query->getAttribute(), $query->getOperator(), ':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value);
            }

            $condition = implode(' OR ', $conditions);
            $where[] = empty($condition) ? '' : '(' . $condition . ')';
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $stmt = $this->getPDO()->prepare("
            SELECT COUNT(1) as sum
            FROM
                (
                    SELECT 1
                    FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}` table_main
                    WHERE " . implode(' AND ', $where) . "
                    {$limit}
                ) table_count
        ");

        foreach ($queries as $i => $query) {
            if ($query->getOperator() === Query::TYPE_SEARCH) continue;
            foreach ($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        if ($max !== 0) {
            $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        }

        $stmt->execute();

        /** @var array $result */
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        return $result['sum'] ?? 0;
    }

    /**
     * Sum an Attribute
     *
     * @param string $collection
     * @param string $attribute
     * @param Query[] $queries
     * @param int $max
     * @return int|float
     * @throws Exception
     * @throws PDOException
     */
    public function sum(string $collection, string $attribute, array $queries = [], int $max = 0): int|float
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = ['1=1'];
        $limit = ($max === 0) ? '' : 'LIMIT :max';

        foreach ($queries as $i => $query) {
            if ($query->getAttribute() === '$id') {
                $query->setAttribute('_uid');
            }
            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.' . $query->getAttribute(), $query->getOperator(), ':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value);
            }

            $where[] = implode(' OR ', $conditions);
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $stmt = $this->getPDO()->prepare("
            SELECT SUM({$attribute}) as sum
            FROM (
                SELECT {$attribute}
                FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}` table_main
                WHERE " . implode(' AND ', $where) . "
                {$limit}
            ) table_count
        ");

        foreach ($queries as $i => $query) {
            if ($query->getOperator() === Query::TYPE_SEARCH) continue;
            foreach ($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        if ($max !== 0) {
            $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        }

        $stmt->execute();

        /** @var array $result */
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

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
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getIndexCount(Document $collection): int
    {
        $indexes = \count($collection->getAttribute('indexes') ?? []);
        return $indexes + static::getNumberOfDefaultIndexes();
    }

    /**
     * Get maximum index limit.
     * https://mariadb.com/kb/en/innodb-limitations/#limitations-on-schema
     *
     * @return int
     */
    public function getIndexLimit(): int
    {
        return 64;
    }

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getAttributeCount(Document $collection): int
    {
        $attributes = \count($collection->getAttribute('attributes') ?? []);

        // +1 ==> virtual columns count as total, so add as buffer
        return $attributes + static::getNumberOfDefaultAttributes() + 1;
    }

    /**
     * Get maximum column limit.
     * https://mariadb.com/kb/en/innodb-limitations/#limitations-on-schema
     * Can be inherited by MySQL since we utilize the InnoDB engine
     *
     * @return int
     */
    public function getAttributeLimit(): int
    {
        return 1017;
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    public static function getRowLimit(): int
    {
        return 65535;
    }

    public static function getNumberOfDefaultAttributes(): int
    {
        return 4;
    }

    public static function getNumberOfDefaultIndexes(): int
    {
        return 3;
    }

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     *
     * @param Document $collection
     * @return int
     */
    public function getAttributeWidth(Document $collection): int
    {
        // Default collection has:
        // `_id` int(11) => 4 bytes
        // `_uid` char(255) => 1020 (255 bytes * 4 for utf8mb4)
        // but this number seems to vary, so we give a +500 byte buffer
        $total = 1500;

        /** @var array $attributes */
        $attributes = $collection->getAttributes()['attributes'];
        foreach ($attributes as $attribute) {
            switch ($attribute['type']) {
                case Database::VAR_STRING:
                    switch (true) {
                        case ($attribute['size'] > 16777215):
                            // 8 bytes length + 4 bytes for LONGTEXT
                            $total += 12;
                            break;

                        case ($attribute['size'] > 65535):
                            // 8 bytes length + 3 bytes for MEDIUMTEXT
                            $total += 11;
                            break;

                        case ($attribute['size'] > 16383):
                            // 8 bytes length + 2 bytes for TEXT
                            $total += 10;
                            break;

                        case ($attribute['size'] > 255):
                            // $size = $size * 4; // utf8mb4 up to 4 bytes per char
                            // 8 bytes length + 2 bytes for VARCHAR(>255)
                            $total += ($attribute['size'] * 4) + 2;
                            break;

                        default:
                            // $size = $size * 4; // utf8mb4 up to 4 bytes per char
                            // 8 bytes length + 1 bytes for VARCHAR(<=255)
                            $total += ($attribute['size'] * 4) + 1;
                            break;
                    }
                    break;

                case Database::VAR_INTEGER:
                    if ($attribute['size'] >= 8) {
                        $total += 8; // BIGINT takes 8 bytes
                    } else {
                        $total += 4; // INT takes 4 bytes
                    }
                    break;
                case Database::VAR_FLOAT:
                    // FLOAT(p) takes 4 bytes when p <= 24, 8 otherwise
                    $total += 4;
                    break;

                case Database::VAR_BOOLEAN:
                    // TINYINT(1) takes one byte
                    $total += 1;
                    break;

                case Database::VAR_DOCUMENT:
                    // CHAR(255)
                    $total += 255;
                    break;

                default:
                    throw new Exception('Unknown Type');
                    break;
            }
        }

        return $total;
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
     * Returns the attribute type for read permissions
     *
     * @return string
     */
    protected function getTypeForReadPermission(): string
    {
        return "TEXT";
    }

    /**
     * Returns the index type for read permissions
     *
     * @return string
     */
    protected function getIndexTypeForReadPermission(): string
    {
        return Database::INDEX_FULLTEXT;
    }

    /**
     * Get SQL Type
     *
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @return string
     * @throws Exception
     */
    protected function getSQLType(string $type, int $size, bool $signed = true): string
    {
        switch ($type) {
            case Database::VAR_STRING:
                // $size = $size * 4; // Convert utf8mb4 size to bytes
                if ($size > 16777215) {
                    return 'LONGTEXT';
                }

                if ($size > 65535) {
                    return 'MEDIUMTEXT';
                }

                if ($size > 16383) {
                    return 'TEXT';
                }

                return "VARCHAR({$size})";
                break;

            case Database::VAR_INTEGER:  // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554
                $signed = ($signed) ? '' : ' UNSIGNED';

                if ($size >= 8) { // INT = 4 bytes, BIGINT = 8 bytes
                    return 'BIGINT' . $signed;
                }

                return 'INT' . $signed;
                break;

            case Database::VAR_FLOAT:
                $signed = ($signed) ? '' : ' UNSIGNED';
                return 'FLOAT' . $signed;
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
     * Get SQL Conditions
     *
     * @param string $attribute
     * @param string $operator
     * @param string $placeholder
     * @param mixed $value
     * @return string
     * @throws Exception
     */
    protected function getSQLCondition(string $attribute, string $operator, string $placeholder, $value): string
    {
        switch ($operator) {
            case Query::TYPE_SEARCH:
                /**
                 * Replace reserved chars with space.
                 */
                $value = trim(str_replace(['@', '+', '-', '*'], ' ', $value));

                /**
                 * Prepend wildcard by default on the back.
                 */
                $value = "'{$value}*'";

                return 'MATCH(' . $attribute . ') AGAINST(' . $this->getPDO()->quote($value) . ' IN BOOLEAN MODE)';
                break;

            default:
                return $attribute . ' ' . $this->getSQLOperator($operator) . ' ' . $placeholder; // Using `attrubute_` to avoid conflicts with custom names;
                break;
        }
    }

    /**
     * Get SQL Operator
     *
     * @param string $operator
     * @return string
     * @throws Exception
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
     * Get SQL Index Type
     *
     * @param string $type
     * @return string
     * @throws Exception
     */
    protected function getSQLIndexType(string $type): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
            case Database::INDEX_ARRAY:
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
     * Get SQL Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array $attributes
     * @return string
     * @throws Exception
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

        return "CREATE {$type} `{$id}` ON `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$collection}` ( ". implode(', ', $attributes) ." )";
    }

    /**
     * Get SQL condition for permissions
     *
     * @param string $collection 
     * @param array $roles 
     * @return string 
     * @throws Exception 
     */
    protected function getSQLPermissionsCondition(string $collection, array $roles): string
    {
        $roles = array_map(fn (string $role) => $this->getPDO()->quote($role), $roles);

        return "table_main._uid IN 
                (
                    SELECT * FROM 
                    (
                        SELECT _document
                        FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$collection}_perms`
                        WHERE _permission IN (" . implode(', ', $roles) . ")
                        AND _type = 'read'
                    ) as subquery
                )";
    }

    /**
     * Get SQL query to aggregate permissions as JSON array
     *
     * @param string $collection 
     * @param string $type 
     * @param string $alias 
     * @return string 
     * @throws Exception 
     */
    protected function getSQLPermissionsQuery(string $collection, string $type, string $alias): string
    {
        return "(
                    SELECT JSON_ARRAYAGG(DISTINCT _permission)
                    FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$collection}_perms`
                    WHERE
                        _document = table_main._uid
                        AND _type = {$this->getPDO()->quote($type)}
                ) as {$alias}";
    }

    /**
     * Get PDO Type
     *
     * @param mixed $value
     * @return int
     * @throws Exception 
     */
    protected function getPDOType(mixed $value): int
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
     * Returns the current PDO object
     * @return PDO 
     */
    protected function getPDO()
    {
        return $this->pdo;
    }
}
