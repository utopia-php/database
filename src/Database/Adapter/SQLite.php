<?php

namespace Utopia\Database\Adapter;

use PDO;
use Utopia\Database\Database;
use Exception;
use PDOException;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\ID;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;


/**
 * Main differences from MariaDB and MySQL:
 * 
 * 1. No concept of a schema. All tables are in the same schema.
 * 2. AUTO_INCREMENT is AUTOINCREAMENT.
 * 3. Can't create indexes in the same statement as creating a table.
 * 4. Can't use SET to bind values on INSERT
 * 5. last_insert_id is last_insert_rowid
 * 6. Can only drop one table at a time
 * 7. no index length support?
 * 8. DELETE doesn't support ORDER BY or LIMIT
 * 9. MODIFY COLUMN is not supported
 * 10. Can't rename an index directly
 */
class SQLite extends MySQL
{
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

        if (\is_null($collection)) {
            return false;
        }

        $collection = $this->filter($collection);

        $stmt = $this->getPDO()->prepare("SELECT name FROM sqlite_master WHERE type='table'  AND name = :table");

        $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", PDO::PARAM_STR);

        $stmt->execute();

        $document = $stmt->fetch();

        return (($document['name'] ?? '') === "{$this->getNamespace()}_{$collection}");
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
        return true;
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
        return true;
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

        // foreach ($indexes as $key => $index) {
        //     $indexId = $this->filter($index->getId());
        //     $indexType = $index->getAttribute('type');

        //     $indexAttributes = $index->getAttribute('attributes');
        //     foreach ($indexAttributes as $nested => $attribute) {
        //         $indexLength = $index->getAttribute('lengths')[$key] ?? '';
        //         $indexLength = (empty($indexLength)) ? '' : '(' . (int)$indexLength . ')';
        //         $indexOrder = $index->getAttribute('orders')[$key] ?? '';
        //         $indexAttribute = $this->filter($attribute);

        //         if ($indexType === Database::INDEX_FULLTEXT) {
        //             $indexOrder = '';
        //         }

        //         $indexAttributes[$nested] = "`{$indexAttribute}`{$indexLength} {$indexOrder}";
        //     }

        //     $indexes[$key] = "{$indexType} `{$indexId}` (" . \implode(", ", $indexAttributes) . " ),";
        // }

        $this->getPDO()
            ->prepare("CREATE TABLE IF NOT EXISTS `{$namespace}_{$id}` (
                    `_id` INTEGER PRIMARY KEY AUTOINCREMENT,
                    `_uid` CHAR(255) NOT NULL,
                    `_createdAt` datetime(3) DEFAULT NULL,
                    `_updatedAt` datetime(3) DEFAULT NULL,
                    `_permissions` MEDIUMTEXT DEFAULT NULL".((!empty($attributes)) ? ',' : '')."
                    " . substr(\implode(' ', $attributes), 0, -2) . "
                )")
            ->execute();

        $this->createIndex($id, '_index1', Database::INDEX_UNIQUE, ['_uid'], [], []);
        $this->createIndex($id, '_created_at', Database::INDEX_KEY, ['_createdAt'], [], []);
        $this->createIndex($id, '_updated_at', Database::INDEX_KEY, ['_updatedAt'], [], []);

        try {
            $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS `{$namespace}_{$id}_perms` (
                        `_id` INTEGER PRIMARY KEY AUTOINCREMENT,
                        `_type` VARCHAR(12) NOT NULL,
                        `_permission` VARCHAR(255) NOT NULL,
                        `_document` VARCHAR(255) NOT NULL
                    )")
                ->execute();
        } catch (\Throwable $th) {
            var_dump($th->getMessage());
        }
        
        $this->createIndex("{$id}_perms", '_index_1', Database::INDEX_UNIQUE, ['_document', '_type', '_permission'], [], []);
        $this->createIndex("{$id}_perms", '_index_2', Database::INDEX_KEY, ['_permission'], [], []);
        
        // Update $this->getCountOfIndexes when adding another default index
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

        $this->getPDO()
            ->prepare("DROP TABLE `{$this->getNamespace()}_{$id}`;")
            ->execute();
        $this->getPDO()
            ->prepare("DROP TABLE `{$this->getNamespace()}_{$id}_perms`;")
            ->execute();

            return true;
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
            ->prepare("ALTER TABLE `{$this->getNamespace()}_{$name}`
                ADD COLUMN `{$id}` {$type};")
            ->execute();
    }

    /**
     * Update Attribute
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
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        return true;
    }

    /**
     * Rename Attribute
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $old = $this->filter($old);
        $new = $this->filter($new);

        return $this->getPDO()
            ->prepare("ALTER TABLE `{$this->getNamespace()}_{$collection}` RENAME COLUMN `{$old}` TO `{$new}`;")
            ->execute();
    }

    /**
     * Rename Index
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $collectionDocument = $this->getDocument(Database::METADATA, $collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $index = $collectionDocument->find('key', $old, 'indexes'); /** @var Document $index */

        if ($this->deleteIndex($collection, $old)
            && $this->createIndex(
                $collection,
                $new,
                $index->getAttribute('type'),
                $index->getAttribute('attributes', []),
                $index->getAttribute('legnths', []),
                $index->getAttribute('orders', []))) {
            return true;
        }

        return false;
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
            ->prepare("ALTER TABLE `{$this->getNamespace()}_{$name}`
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

        try {
            $x = $this->getPDO()
                ->prepare($this->getSQLIndex($name, $id, $type, $attributes))
                ->execute();
        } catch (\Throwable $th) {
            var_dump($this->getSQLIndex($name, $id, $type, $attributes));
            var_dump($th->getMessage());
        }
        
        return $x;
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
            ->prepare("DROP INDEX `{$this->getNamespace()}_{$name}_{$id}`;")
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
            SELECT * 
            FROM `{$this->getNamespace()}_{$name}` 
            WHERE _uid = :_uid;
        ");

        $stmt->bindValue(':_uid', $id);
        $stmt->execute();

        /** @var array $document */
        $document = $stmt->fetch();
        if (empty($document)) {
            return new Document([]);
        }

        $document['$id'] = $document['_uid'];
        $document['$internalId'] = $document['_id'];
        $document['$createdAt'] = $document['_createdAt'];
        $document['$updatedAt'] = $document['_updatedAt'];
        $document['$permissions'] = json_decode($document['_permissions'] ?? '[]', true);

        unset($document['_id']);
        unset($document['_uid']);
        unset($document['_createdAt']);
        unset($document['_updatedAt']);
        unset($document['_permissions']);

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
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = json_encode($document->getPermissions());

        $name = $this->filter($collection);
        $columns = ['_uid'];
        $values = ['_uid'];

        $this->getPDO()->beginTransaction();

        /**
         * Insert Attributes
         */
        $bindIndex = 0;
        foreach ($attributes as $attribute => $value) { // Parse statement
            $column = $this->filter($attribute);
            $values[] = 'value_' . $bindIndex;
            $columns[] = "`{$column}`";
            $bindIndex++;
        }

        $stmt = $this->getPDO()
            ->prepare("INSERT INTO `{$this->getNamespace()}_{$name}`
                (".implode(', ', $columns).") VALUES (:".implode(', :', $values).");");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

        $attributeIndex = 0;
        foreach ($attributes as $attribute => $value) {
            if (is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $bindKey = 'value_' . $attributeIndex;
            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $bindKey, $value, $this->getPDOType($value));
            $attributeIndex++;
        }

        $permissions = [];
        foreach (Database::PERMISSIONS as $type) {
            foreach ($document->getPermissionsByType($type) as $permission) {
                $permission = \str_replace('"', '', $permission);
                $permissions[] = "('{$type}', '{$permission}', '{$document->getId()}')";
            }
        }

        if (!empty($permissions)) {
            $queryPermissions = "INSERT INTO `{$this->getNamespace()}_{$name}_perms` (_type, _permission, _document) VALUES " . implode(', ', $permissions);
            $stmtPermissions = $this->getPDO()->prepare($queryPermissions);
        }

        try {
            $stmt->execute();

            $statment = $this->getPDO()->prepare("select last_insert_rowid() as id");
            $statment->execute();
            $last = $statment->fetch();
            $document['$internalId'] = $last['id'];

            if (isset($stmtPermissions)) {
                $stmtPermissions->execute();
            }
        } catch (PDOException $e) {
            $this->getPDO()->rollBack();
            switch ($e->getCode()) {
                case "1062":
                case "23000":
                    throw new Duplicate('Duplicated document: ' . $e->getMessage());
                break;
                default:
                    throw $e;
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
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = json_encode($document->getPermissions());

        $name = $this->filter($collection);
        $columns = '';

        /**
         * Get current permissions from the database
         */
        $permissionsStmt = $this->getPDO()->prepare("
                SELECT _type, _permission
                FROM `{$this->getNamespace()}_{$name}_perms` p
                WHERE p._document = :_uid
        ");
        $permissionsStmt->bindValue(':_uid', $document->getId());
        $permissionsStmt->execute();
        $permissions = $permissionsStmt->fetchAll();

        $initial = [];
        foreach (Database::PERMISSIONS as $type) {
            $initial[$type] = [];
        }

        $permissions = array_reduce($permissions, function (array $carry, array $item) {
            $carry[$item['_type']][] = $item['_permission'];

            return $carry;
        }, $initial);

        $this->getPDO()->beginTransaction();

        /**
         * Get removed Permissions
         */
        $removals = [];
        foreach(Database::PERMISSIONS as $type) {
            $diff = \array_diff($permissions[$type], $document->getPermissionsByType($type));
            if (!empty($diff)) {
                $removals[$type] = $diff;
            }
        }

        /**
         * Get added Permissions
         */
        $additions = [];
        foreach(Database::PERMISSIONS as $type) {
            $diff = \array_diff($document->getPermissionsByType($type), $permissions[$type]);
            if (!empty($diff)) {
                $additions[$type] = $diff;
            }
        }

        /**
         * Query to remove permissions
         */
        $removeQuery = '';
        if (!empty($removals)) {
            $removeQuery = 'AND (';
            foreach ($removals as $type => $permissions) {
                $removeQuery .= "(
                    _type = '{$type}'
                    AND _permission IN (" . implode(', ', \array_map(fn(string $i) => ":_remove_{$type}_{$i}", \array_keys($permissions))) . ")
                )";
                if ($type !== \array_key_last($removals)) {
                    $removeQuery .= ' OR ';
                }
            }
        }
        if (!empty($removeQuery)) {
            $removeQuery .= ')';
            $stmtRemovePermissions = $this->getPDO()
                ->prepare("
                DELETE
                FROM `{$this->getNamespace()}_{$name}_perms`
                WHERE
                    _document = :_uid
                    {$removeQuery}
            ");
            $stmtRemovePermissions->bindValue(':_uid', $document->getId());

            foreach ($removals as $type => $permissions) {
                foreach ($permissions as $i => $permission) {
                    $stmtRemovePermissions->bindValue(":_remove_{$type}_{$i}", $permission);
                }
            }
        }

        /**
         * Query to add permissions
         */
        if (!empty($additions)) {
            $values = [];
            foreach ($additions as $type => $permissions) {
                foreach ($permissions as $i => $_) {
                    $values[] = "( :_uid, '{$type}', :_add_{$type}_{$i} )";
                }
            }

            $stmtAddPermissions = $this->getPDO()
                ->prepare(
                    "
                    INSERT INTO `{$this->getNamespace()}_{$name}_perms`
                    (_document, _type, _permission) VALUES " . \implode(', ', $values)
                );

            $stmtAddPermissions->bindValue(":_uid", $document->getId());
            foreach ($additions as $type => $permissions) {
                foreach ($permissions as $i => $permission) {
                    $stmtAddPermissions->bindValue(":_add_{$type}_{$i}", $permission);
                }
            }
        }

        /**
         * Update Attributes
         */

        $bindIndex = 0;
        foreach ($attributes as $attribute => $value) {
            $column = $this->filter($attribute);
            $bindKey = 'key_' . $bindIndex;
            $columns .= "`{$column}`" . '=:' . $bindKey . ',';
            $bindIndex++;
        }

        $stmt = $this->getPDO()
            ->prepare("UPDATE `{$this->getNamespace()}_{$name}`
                SET {$columns} _uid = :_uid WHERE _uid = :_uid");

        $stmt->bindValue(':_uid', $document->getId());

        $attributeIndex = 0;
        foreach ($attributes as $attribute => $value) {
            if (is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $bindKey = 'key_' . $attributeIndex;
            $attribute = $this->filter($attribute);
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $bindKey, $value, $this->getPDOType($value));
            $attributeIndex++;
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
                $this->getPDO()->rollBack();
                switch ($e->getCode()) {
                    case '1062':
                    case '23000':
                        throw new Duplicate('Duplicated document: ' . $e->getMessage());

                    default:
                        throw $e;
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

        $stmt = $this->getPDO()->prepare("DELETE FROM `{$this->getNamespace()}_{$name}` WHERE _uid = :_uid");
        $stmt->bindValue(':_uid', $id);

        $stmtPermissions = $this->getPDO()->prepare("DELETE FROM `{$this->getNamespace()}_{$name}_perms` WHERE _document = :_uid");
        $stmtPermissions->bindValue(':_uid', $id);

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
     *
     * @return Document[]
     * @throws Exception
     * @throws PDOException
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = [];
        $orders = [];

        $orderAttributes = \array_map(fn ($orderAttribute) => match ($orderAttribute) {
            '$id' => ID::custom('_uid'),
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $orderAttribute
        }, $orderAttributes);

        $hasIdAttribute = false;
        foreach ($orderAttributes as $i => $attribute) {
            if ($attribute === '_uid') {
                $hasIdAttribute = true;
            }

            $attribute = $this->filter($attribute);
            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);

            // Get most dominant/first order attribute
            if ($i === 0 && !empty($cursor)) {
                $orderMethodInternalId = Query::TYPE_GREATER; // To preserve natural order
                $orderMethod = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;

                if ($cursorDirection === Database::CURSOR_BEFORE) {
                    $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                    $orderMethodInternalId = $orderType === Database::ORDER_ASC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
                    $orderMethod = $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER;
                }

                $where[] = "(
                        table_main.{$attribute} {$this->getSQLOperator($orderMethod)} :cursor 
                        OR (
                            table_main.{$attribute} = :cursor 
                            AND
                            table_main._id {$this->getSQLOperator($orderMethodInternalId)} {$cursor['$internalId']}
                        )
                    )";
            } else if ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }

            $orders[] = "`${attribute}` ${orderType}";
        }

        // Allow after pagination without any order
        if (empty($orderAttributes) && !empty($cursor)) {
            $orderType = $orderTypes[0] ?? Database::ORDER_ASC;
            $orderMethod = $cursorDirection === Database::CURSOR_AFTER ? ($orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER
            ) : ($orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER
            );
            $where[] = "( table_main._id {$this->getSQLOperator($orderMethod)} {$cursor['$internalId']} )";
        }

        // Allow order type without any order attribute, fallback to the natural order (_id)
        if (!$hasIdAttribute) {
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
            $query->setAttribute(match ($query->getAttribute()) {
                '$id' => ID::custom('_uid'),
                '$createdAt' => '_createdAt',
                '$updatedAt' => '_updatedAt',
                default => $query->getAttribute()
            });

            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.' . $query->getAttribute(), $query->getMethod(), ':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value);
            }
            $condition = implode(' OR ', $conditions);
            $where[] = empty($condition) ? '' : '(' . $condition . ')';
        }

        $order = 'ORDER BY ' . implode(', ', $orders);

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $sqlWhere = !empty($where) ? 'where ' . implode(' AND ', $where) : '';

        $sql = "
            SELECT table_main.*
            FROM `{$this->getNamespace()}_{$name}` as table_main
            " . $sqlWhere . "
            GROUP BY _uid
            {$order}
            LIMIT :offset, :limit;
        ";

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($queries as $i => $query) {
            if ($query->getMethod() === Query::TYPE_SEARCH) continue;
            foreach ($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        if (!empty($cursor) && !empty($orderAttributes) && array_key_exists(0, $orderAttributes)) {
            $attribute = $orderAttributes[0];

            $attribute = match ($attribute) {
                '_uid' => '$id',
                '_createdAt' => '$createdAt',
                '_updatedAt' => '$updatedAt',
                default => $attribute
            };

            if (is_null($cursor[$attribute] ?? null)) {
                throw new Exception("Order attribute '{$attribute}' is empty.");
            }
            $stmt->bindValue(':cursor', $cursor[$attribute], $this->getPDOType($cursor[$attribute]));
        }

        $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $stmt->execute();

        $results = $stmt->fetchAll();

        foreach ($results as $key => $value) {
            $results[$key]['$id'] = $value['_uid'];
            $results[$key]['$internalId'] = $value['_id'];
            $results[$key]['$createdAt'] = $value['_createdAt'];
            $results[$key]['$updatedAt'] = $value['_updatedAt'];
            $results[$key]['$permissions'] = json_decode($value['_permissions'] ?? '[]', true);

            unset($results[$key]['_uid']);
            unset($results[$key]['_id']);
            unset($results[$key]['_createdAt']);
            unset($results[$key]['_updatedAt']);
            unset($results[$key]['_permissions']);

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
        $where = [];
        $limit = ($max === 0) ? '' : 'LIMIT :max';

        foreach ($queries as $i => $query) {
            $query->setAttribute(match ($query->getAttribute()) {
                '$id' => ID::custom('_uid'),
                '$createdAt' => '_createdAt',
                '$updatedAt' => '_updatedAt',
                default => $query->getAttribute()
            });

            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.' . $query->getAttribute(), $query->getMethod(), ':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value);
            }

            $condition = implode(' OR ', $conditions);
            $where[] = empty($condition) ? '' : '(' . $condition . ')';
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $sqlWhere = !empty($where) ? 'where ' . implode(' AND ', $where) : '';
        $sql = "SELECT COUNT(1) as sum
            FROM
                (
                    SELECT 1
                    FROM `{$this->getNamespace()}_{$name}` table_main
                    " . $sqlWhere . "
                    {$limit}
                ) table_count
        ";
        $stmt = $this->getPDO()->prepare($sql);

        foreach ($queries as $i => $query) {
            if ($query->getMethod() === Query::TYPE_SEARCH) continue;
            foreach ($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        if ($max !== 0) {
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
        $where = [];
        $limit = ($max === 0) ? '' : 'LIMIT :max';

        foreach ($queries as $i => $query) {
            $query->setAttribute(match ($query->getAttribute()) {
                '$id' => ID::custom('_uid'),
                '$createdAt' => '_createdAt',
                '$updatedAt' => '_updatedAt',
                default => $query->getAttribute()
            });

            $conditions = [];
            foreach ($query->getValues() as $key => $value) {
                $conditions[] = $this->getSQLCondition('table_main.' . $query->getAttribute(), $query->getMethod(), ':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value);
            }

            $where[] = implode(' OR ', $conditions);
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $sqlWhere = !empty($where) ? 'where ' . implode(' AND ', $where) : '';

        $stmt = $this->getPDO()->prepare("
            SELECT SUM({$attribute}) as sum
            FROM (
                SELECT {$attribute}
                FROM `{$this->getNamespace()}_{$name}` table_main
                 " . $sqlWhere . "
                {$limit}
            ) table_count
        ");

        foreach ($queries as $i => $query) {
            if ($query->getMethod() === Query::TYPE_SEARCH) continue;
            foreach ($query->getValues() as $key => $value) {
                $stmt->bindValue(':attribute_' . $i . '_' . $key . '_' . $query->getAttribute(), $value, $this->getPDOType($value));
            }
        }

        if ($max !== 0) {
            $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        }

        $stmt->execute();

        /** @var array $result */
        $result = $stmt->fetch();

        return $result['sum'] ?? 0;
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

                case Database::VAR_DATETIME:
                    $total += 19; // 2022-06-26 14:46:24
                    break;
                default:
                    throw new Exception('Unknown Type');
                    break;
            }
        }

        return $total;
    }

    /**
     * Is fulltext index supported?
     *
     * @return bool
     */
    public function getSupportForFulltextIndex(): bool
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

            case Database::VAR_INTEGER:  // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554
                if ($size >= 8) { // INT = 4 bytes, BIGINT = 8 bytes
                    return 'BIGINT';
                }

                return 'INT';

            case Database::VAR_FLOAT:
                return 'FLOAT';

            case Database::VAR_BOOLEAN:
                return 'TINYINT(1)';

            case Database::VAR_DOCUMENT:
                return 'CHAR(255)';

            case Database::VAR_DATETIME:
                return 'DATETIME(3)';
                break;
            default:
                throw new Exception('Unknown Type');
        }
    }

    /**
     * Get SQL Conditions
     *
     * @param string $attribute
     * @param string $method
     * @param string $placeholder
     * @param mixed $value
     * @return string
     * @throws Exception
     */
    protected function getSQLCondition(string $attribute, string $method, string $placeholder, $value): string
    {
        switch ($method) {
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

            default:
                return $attribute . ' ' . $this->getSQLOperator($method) . ' ' . $placeholder; // Using `attrubute_` to avoid conflicts with custom names;
                break;
        }
    }

    /**
     * Get SQL Operator
     *
     * @param string $method
     * @return string
     * @throws Exception
     */
    protected function getSQLOperator(string $method): string
    {
        switch ($method) {
            case Query::TYPE_EQUAL:
                return '=';

            case Query::TYPE_NOTEQUAL:
                return '!=';

            case Query::TYPE_LESSER:
                return '<';

            case Query::TYPE_LESSEREQUAL:
                return '<=';

            case Query::TYPE_GREATER:
                return '>';

            case Query::TYPE_GREATEREQUAL:
                return '>=';

            default:
                throw new Exception('Unknown method:' . $method);
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

            case Database::INDEX_UNIQUE:
                return 'UNIQUE INDEX';

            default:
                throw new Exception('Unknown Index Type:' . $type);
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
        $postfix = '';

        switch ($type) {
            case Database::INDEX_KEY:
            case Database::INDEX_ARRAY:
                $type = 'INDEX';
                break;

            case Database::INDEX_UNIQUE:
                $type = 'UNIQUE INDEX';
                $postfix = ' COLLATE NOCASE';

                break;

            default:
                throw new Exception('Unknown Index Type:' . $type);
                break;
        }

        $attributes = \array_map(fn ($attribute) => match ($attribute) {
            '$id' => ID::custom('_uid'),
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $attribute
        }, $attributes);

        foreach ($attributes as $key => $attribute) {
            $length = $lengths[$key] ?? '';
            $length = (empty($length)) ? '' : '(' . (int)$length . ')';
            $order = $orders[$key] ?? '';
            $attribute = $this->filter($attribute);

            $attributes[$key] = "`{$attribute}`{$postfix} {$order}";
        }

        return "CREATE {$type} `{$this->getNamespace()}_{$collection}_{$id}` ON `{$this->getNamespace()}_{$collection}` ( " . implode(', ', $attributes) . ")";
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
        return "table_main._uid IN (
                    SELECT distinct(_document)
                    FROM `{$this->getNamespace()}_{$collection}_perms`
                    WHERE _permission IN (" . implode(', ', $roles) . ")
                    AND _type = 'read'
                )";
    }

    /**
     * Get list of keywords that cannot be used
     *  Refference: https://www.sqlite.org/lang_keywords.html
     * 
     * @return string[]
     */
    public function getKeywords(): array
    {
        return [
            'ABORT',
            'ACTION',
            'ADD',
            'AFTER',
            'ALL',
            'ALTER',
            'ALWAYS',
            'ANALYZE',
            'AND',
            'AS',
            'ASC',
            'ATTACH',
            'AUTOINCREMENT',
            'BEFORE',
            'BEGIN',
            'BETWEEN',
            'BY',
            'CASCADE',
            'CASE',
            'CAST',
            'CHECK',
            'COLLATE',
            'COLUMN',
            'COMMIT',
            'CONFLICT',
            'CONSTRAINT',
            'CREATE',
            'CROSS',
            'CURRENT',
            'CURRENT_DATE',
            'CURRENT_TIME',
            'CURRENT_TIMESTAMP',
            'DATABASE',
            'DEFAULT',
            'DEFERRABLE',
            'DEFERRED',
            'DELETE',
            'DESC',
            'DETACH',
            'DISTINCT',
            'DO',
            'DROP',
            'EACH',
            'ELSE',
            'END',
            'ESCAPE',
            'EXCEPT',
            'EXCLUDE',
            'EXCLUSIVE',
            'EXISTS',
            'EXPLAIN',
            'FAIL',
            'FILTER',
            'FIRST',
            'FOLLOWING',
            'FOR',
            'FOREIGN',
            'FROM',
            'FULL',
            'GENERATED',
            'GLOB',
            'GROUP',
            'GROUPS',
            'HAVING',
            'IF',
            'IGNORE',
            'IMMEDIATE',
            'IN',
            'INDEX',
            'INDEXED',
            'INITIALLY',
            'INNER',
            'INSERT',
            'INSTEAD',
            'INTERSECT',
            'INTO',
            'IS',
            'ISNULL',
            'JOIN',
            'KEY',
            'LAST',
            'LEFT',
            'LIKE',
            'LIMIT',
            'MATCH',
            'MATERIALIZED',
            'NATURAL',
            'NO',
            'NOT',
            'NOTHING',
            'NOTNULL',
            'NULL',
            'NULLS',
            'OF',
            'OFFSET',
            'ON',
            'OR',
            'ORDER',
            'OTHERS',
            'OUTER',
            'OVER',
            'PARTITION',
            'PLAN',
            'PRAGMA',
            'PRECEDING',
            'PRIMARY',
            'QUERY',
            'RAISE',
            'RANGE',
            'RECURSIVE',
            'REFERENCES',
            'REGEXP',
            'REINDEX',
            'RELEASE',
            'RENAME',
            'REPLACE',
            'RESTRICT',
            'RETURNING',
            'RIGHT',
            'ROLLBACK',
            'ROW',
            'ROWS',
            'SAVEPOINT',
            'SELECT',
            'SET',
            'TABLE',
            'TEMP',
            'TEMPORARY',
            'THEN',
            'TIES',
            'TO',
            'TRANSACTION',
            'TRIGGER',
            'UNBOUNDED',
            'UNION',
            'UNIQUE',
            'UPDATE',
            'USING',
            'VACUUM',
            'VALUES',
            'VIEW',
            'VIRTUAL',
            'WHEN',
            'WHERE',
            'WINDOW',
            'WITH',
            'WITHOUT',
        ];
    }
}
