<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Exception\Duplicate;

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
class SQLite extends MariaDB
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
     * @param array<Document> $attributes
     * @param array<Document> $indexes
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        $this->getPDO()->beginTransaction();

        /** @var array<string> $attributeStrings */
        $attributeStrings = [];

        foreach ($attributes as $key => $attribute) {
            $attrId = $this->filter($attribute->getId());
            $attrType = $this->getSQLType($attribute->getAttribute('type'), $attribute->getAttribute('size', 0), $attribute->getAttribute('signed', true));

            if ($attribute->getAttribute('array')) {
                $attrType = 'LONGTEXT';
            }

            $attributeStrings[$key] = "`{$attrId}` {$attrType}, ";
        }

        $this->getPDO()
            ->prepare("CREATE TABLE IF NOT EXISTS `{$namespace}_{$id}` (
                    `_id` INTEGER PRIMARY KEY AUTOINCREMENT,
                    `_uid` CHAR(255) NOT NULL,
                    `_createdAt` datetime(3) DEFAULT NULL,
                    `_updatedAt` datetime(3) DEFAULT NULL,
                    `_permissions` MEDIUMTEXT DEFAULT NULL".((!empty($attributes)) ? ',' : '')."
                    " . substr(\implode(' ', $attributeStrings), 0, -2) . "
                )")
            ->execute();

        $this->createIndex($id, '_index1', Database::INDEX_UNIQUE, ['_uid'], [], []);
        $this->createIndex($id, '_created_at', Database::INDEX_KEY, ['_createdAt'], [], []);
        $this->createIndex($id, '_updated_at', Database::INDEX_KEY, ['_updatedAt'], [], []);

        foreach ($indexes as $key => $index) {
            $indexId = $this->filter($index->getId());
            $indexType = $index->getAttribute('type');
            $indexAttributes = $index->getAttribute('attributes', []);
            $indexLengths = $index->getAttribute('lengths', []);
            $indexOrders = $index->getAttribute('orders', []);

            $this->createIndex($id, $indexId, $indexType, $indexAttributes, $indexLengths, $indexOrders);
        }

        $this->getPDO()
            ->prepare("CREATE TABLE IF NOT EXISTS `{$namespace}_{$id}_perms` (
                    `_id` INTEGER PRIMARY KEY AUTOINCREMENT,
                    `_type` VARCHAR(12) NOT NULL,
                    `_permission` VARCHAR(255) NOT NULL,
                    `_document` VARCHAR(255) NOT NULL
                )")
            ->execute();

        $this->createIndex("{$id}_perms", '_index_1', Database::INDEX_UNIQUE, ['_document', '_type', '_permission'], [], []);
        $this->createIndex("{$id}_perms", '_index_2', Database::INDEX_KEY, ['_permission'], [], []);

        $this->getPDO()->commit();

        // Update $this->getCountOfIndexes when adding another default index
        return true;
    }

      /**
     * Get Collection Size
     * @param string $name
     * @return int
     * @throws Exception
     * 
     */


     public function getCollectionSize(string $name): string
{
    $name = $this->filter($name);

    $query = $this->getPDO()->prepare("
        SELECT sum(LENGTH(name) + LENGTH(sql)) AS total_size
        FROM sqlite_master
        WHERE type = 'table' AND name = '{$name}'
    ");
    $query->execute();

    try {
        $size = $query->fetchColumn();
        return strval($size);
    } catch (PDOException $e) {
        throw new Exception($e->getMessage());
    }
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

        $this->getPDO()->beginTransaction();

        $this->getPDO()
            ->prepare("DROP TABLE `{$this->getNamespace()}_{$id}`;")
            ->execute();

        $this->getPDO()
            ->prepare("DROP TABLE `{$this->getNamespace()}_{$id}_perms`;")
            ->execute();

        $this->getPDO()->commit();

        return true;
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

        $collection = $this->getDocument(Database::METADATA, $name);

        if ($collection->isEmpty()) {
            throw new Exception('Collection not found');
        }

        $indexes = \json_decode($collection->getAttribute('indexes', []), true);

        foreach ($indexes as $index) {
            $attributes = $index['attributes'];
            if ($attributes === [$id]) {
                $this->deleteIndex($name, $index['$id']);
            } elseif (\in_array($id, $attributes)) {
                $this->deleteIndex($name, $index['$id']);
                $this->createIndex($name, $index['$id'], $index['type'], \array_diff($attributes, [$id]), $index['lengths'], $index['orders']);
            }
        }

        return $this->getPDO()
            ->prepare("ALTER TABLE {$this->getSQLTable($name)}
                DROP COLUMN `{$id}`;")
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
        $indexes = json_decode($collectionDocument['indexes'], true);
        $index = null;

        foreach ($indexes as $node) {
            if ($node['key'] === $old) {
                $index = $node;
                break;
            }
        }

        if ($index
            && $this->deleteIndex($collection, $old)
            && $this->createIndex(
                $collection,
                $new,
                $index['type'],
                $index['attributes'],
                $index['lengths'],
                $index['orders'],
            )) {
            return true;
        }

        return false;
    }

    /**
     * Create Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     * @param array<int> $lengths
     * @param array<string> $orders
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

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
            ->prepare("DROP INDEX `{$this->getNamespace()}_{$name}_{$id}`;")
            ->execute();
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
            throw match ($e->getCode()) {
                "1062", "23000" => new Duplicate('Duplicated document: ' . $e->getMessage()),
                default => $e,
            };
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
        foreach (Database::PERMISSIONS as $type) {
            $diff = \array_diff($permissions[$type], $document->getPermissionsByType($type));
            if (!empty($diff)) {
                $removals[$type] = $diff;
            }
        }

        /**
         * Get added Permissions
         */
        $additions = [];
        foreach (Database::PERMISSIONS as $type) {
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
                    AND _permission IN (" . implode(', ', \array_map(fn (string $i) => ":_remove_{$type}_{$i}", \array_keys($permissions))) . ")
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

            throw match ($e->getCode()) {
                '1062', '23000' => new Duplicate('Duplicated document: ' . $e->getMessage()),
                default => $e,
            };
        }

        if (!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return $document;
    }

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    public function getSupportForSchemas(): bool
    {
        return false;
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
     * Is fulltext Wildcard index supported?
     *
     * @return bool
     */
    public function getSupportForFulltextWildcardIndex(): bool
    {
        return false;
    }

    /**
     * Are timeouts supported?
     *
     * @return bool
     */
    public function getSupportForTimeouts(): bool
    {
        return false;
    }

    public function getSupportForRelationships(): bool
    {
        return false;
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
     * @param array<string> $attributes
     * @return string
     * @throws Exception
     */
    protected function getSQLIndex(string $collection, string $id, string $type, array $attributes): string
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
        }

        $attributes = \array_map(fn ($attribute) => match ($attribute) {
            '$id' => ID::custom('_uid'),
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $attribute
        }, $attributes);

        foreach ($attributes as $key => $attribute) {
            $order = '';
            $attribute = $this->filter($attribute);

            $attributes[$key] = "`$attribute`$postfix $order";
        }

        return "CREATE {$type} `{$this->getNamespace()}_{$collection}_{$id}` ON `{$this->getNamespace()}_{$collection}` ( " . implode(', ', $attributes) . ")";
    }

    /**
     * Get SQL condition for permissions
     *
     * @param string $collection
     * @param array<string> $roles
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
     * @return array<string>
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
