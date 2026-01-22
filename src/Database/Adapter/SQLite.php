<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Swoole\Database\PDOStatementProxy;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Operator;

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
     * @inheritDoc
     */
    public function startTransaction(): bool
    {
        try {
            if ($this->inTransaction === 0) {
                if ($this->getPDO()->inTransaction()) {
                    $this->getPDO()->rollBack();
                }

                $result = $this->getPDO()->beginTransaction();
            } else {
                $result = $this->getPDO()
                    ->prepare('SAVEPOINT transaction' . $this->inTransaction)
                    ->execute();
            }
        } catch (PDOException $e) {
            throw new TransactionException('Failed to start transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        if (!$result) {
            throw new TransactionException('Failed to start transaction');
        }

        $this->inTransaction++;

        return $result;
    }

    /**
     * Check if Database exists
     * Optionally check if collection exists in Database
     *
     * @param string $database
     * @param string|null $collection
     * @return bool
     * @throws DatabaseException
     */
    public function exists(string $database, ?string $collection = null): bool
    {
        $database = $this->filter($database);

        if (\is_null($collection)) {
            return false;
        }

        $collection = $this->filter($collection);

        $sql = "
			SELECT name FROM sqlite_master 
			WHERE type='table' AND name = :table
		";

        $sql = $this->trigger(Database::EVENT_DATABASE_CREATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", PDO::PARAM_STR);

        $stmt->execute();

        $document = $stmt->fetchAll();
        $stmt->closeCursor();
        if (!empty($document)) {
            $document = $document[0];
        }

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
        $id = $this->filter($name);

        /** @var array<string> $attributeStrings */
        $attributeStrings = [];

        foreach ($attributes as $key => $attribute) {
            $attrId = $this->filter($attribute->getId());

            $attrType = $this->getSQLType(
                $attribute->getAttribute('type'),
                $attribute->getAttribute('size', 0),
                $attribute->getAttribute('signed', true),
                $attribute->getAttribute('array', false),
                $attribute->getAttribute('required', false)
            );

            $attributeStrings[$key] = "`{$attrId}` {$attrType}, ";
        }

        $tenantQuery = $this->sharedTables ? '`_tenant` INTEGER DEFAULT NULL,' : '';

        $collection = "
			CREATE TABLE {$this->getSQLTable($id)} (
				`_id` INTEGER PRIMARY KEY AUTOINCREMENT,
				`_uid` VARCHAR(36) NOT NULL,
				{$tenantQuery}
				`_createdAt` DATETIME(3) DEFAULT NULL,
				`_updatedAt` DATETIME(3) DEFAULT NULL,
				`_permissions` MEDIUMTEXT DEFAULT NULL".(!empty($attributes) ? ',' : '')."
				" . \substr(\implode(' ', $attributeStrings), 0, -2) . "
			)
		";

        $collection = $this->trigger(Database::EVENT_COLLECTION_CREATE, $collection);

        $permissions = "
			CREATE TABLE {$this->getSQLTable($id . '_perms')} (
				`_id` INTEGER PRIMARY KEY AUTOINCREMENT,
				{$tenantQuery}
				`_type` VARCHAR(12) NOT NULL,
				`_permission` VARCHAR(255) NOT NULL,
				`_document` VARCHAR(255) NOT NULL
			)
		";

        $permissions = $this->trigger(Database::EVENT_COLLECTION_CREATE, $permissions);

        try {
            $this->getPDO()
                ->prepare($collection)
                ->execute();

            $this->getPDO()
                ->prepare($permissions)
                ->execute();

            $this->createIndex($id, '_index1', Database::INDEX_UNIQUE, ['_uid'], [], []);
            $this->createIndex($id, '_created_at', Database::INDEX_KEY, [ '_createdAt'], [], []);
            $this->createIndex($id, '_updated_at', Database::INDEX_KEY, [ '_updatedAt'], [], []);

            $this->createIndex("{$id}_perms", '_index_1', Database::INDEX_UNIQUE, ['_document', '_type', '_permission'], [], []);
            $this->createIndex("{$id}_perms", '_index_2', Database::INDEX_KEY, ['_permission', '_type'], [], []);

            if ($this->sharedTables) {
                $this->createIndex($id, '_tenant_id', Database::INDEX_KEY, [ '_id'], [], []);
            }

            foreach ($indexes as $index) {
                $indexId = $this->filter($index->getId());
                $indexType = $index->getAttribute('type');
                $indexAttributes = $index->getAttribute('attributes', []);
                $indexLengths = $index->getAttribute('lengths', []);
                $indexOrders = $index->getAttribute('orders', []);
                $indexTtl = $index->getAttribute('ttl', 0);

                $this->createIndex($id, $indexId, $indexType, $indexAttributes, $indexLengths, $indexOrders, [], [], $indexTtl);
            }

            $this->createIndex("{$id}_perms", '_index_1', Database::INDEX_UNIQUE, ['_document', '_type', '_permission'], [], []);
            $this->createIndex("{$id}_perms", '_index_2', Database::INDEX_KEY, ['_permission', '_type'], [], []);

        } catch (PDOException $e) {
            throw $this->processException($e);
        }
        return true;
    }


    /**
     * Get Collection Size of raw data
     * @param string $collection
     * @return int
     * @throws DatabaseException
     *
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->filter($collection);
        $namespace = $this->getNamespace();
        $name = $namespace . '_' . $collection;
        $permissions = $namespace . '_' . $collection . '_perms';

        $collectionSize = $this->getPDO()->prepare("
             SELECT SUM(\"pgsize\") 
             FROM \"dbstat\" 
             WHERE name = :name;
        ");

        $permissionsSize = $this->getPDO()->prepare("
             SELECT SUM(\"pgsize\") 
             FROM \"dbstat\"
             WHERE name = :name;
        ");

        $collectionSize->bindParam(':name', $name);
        $permissionsSize->bindParam(':name', $permissions);

        try {
            $collectionSize->execute();
            $permissionsSize->execute();
            $size = $collectionSize->fetchColumn() + $permissionsSize->fetchColumn();
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get collection size: ' . $e->getMessage());
        }

        return $size;
    }

    /**
     * Get Collection Size on disk
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->getSizeOfCollection($collection);
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

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id)}";
        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        $this->getPDO()
            ->prepare($sql)
            ->execute();

        $sql = "DROP TABLE IF EXISTS {$this->getSQLTable($id . '_perms')}";
        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        $this->getPDO()
            ->prepare($sql)
            ->execute();

        return true;
    }

    /**
     * Analyze a collection updating it's metadata on the database engine
     *
     * @param string $collection
     * @return bool
     */
    public function analyzeCollection(string $collection): bool
    {
        return false;
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
     * @param string|null $newKey
     * @param bool $required
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false): bool
    {
        if (!empty($newKey) && $newKey !== $id) {
            return $this->renameAttribute($collection, $id, $newKey);
        }

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
        $metadataCollection = new Document(['$id' => Database::METADATA]);
        $collection = $this->getDocument($metadataCollection, $name);

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
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

        $sql = "ALTER TABLE {$this->getSQLTable($name)} DROP COLUMN `{$id}`";

        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            if (str_contains($e->getMessage(), 'no such column')) {
                return true;
            }

            throw $e;
        }
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
        $metadataCollection = new Document(['$id' => Database::METADATA]);
        $collection = $this->getDocument($metadataCollection, $collection);

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $old = $this->filter($old);
        $new = $this->filter($new);
        $indexes = \json_decode($collection->getAttribute('indexes', []), true);
        $index = null;

        foreach ($indexes as $node) {
            if ($node['key'] === $old) {
                $index = $node;
                break;
            }
        }

        if ($index
            && $this->deleteIndex($collection->getId(), $old)
            && $this->createIndex(
                $collection->getId(),
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
     * @param array<string,string> $indexAttributeTypes
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = [], int $ttl = 1): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        // Workaround for no support for CREATE INDEX IF NOT EXISTS
        $stmt = $this->getPDO()->prepare("
			SELECT name 
			FROM sqlite_master 
			WHERE type='index' AND name=:_index;
		");
        $stmt->bindValue(':_index', "{$this->getNamespace()}_{$this->tenant}_{$name}_{$id}");
        $stmt->execute();
        $index = $stmt->fetch();
        if (!empty($index)) {
            return true;
        }

        $sql = $this->getSQLIndex($name, $id, $type, $attributes);

        $sql = $this->trigger(Database::EVENT_INDEX_CREATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
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

        $sql = "DROP INDEX `{$this->getNamespace()}_{$this->tenant}_{$name}_{$id}`";
        $sql = $this->trigger(Database::EVENT_INDEX_DELETE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            if (str_contains($e->getMessage(), 'no such index')) {
                return true;
            }

            throw $e;
        }
    }

    /**
     * Create Document
     *
     * @param Document $collection
     * @param Document $document
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws DuplicateException
     */
    public function createDocument(Document $collection, Document $document): Document
    {
        $collection = $collection->getId();
        $attributes = $document->getAttributes();
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = json_encode($document->getPermissions());

        if ($this->sharedTables) {
            $attributes['_tenant'] = $this->tenant;
        }

        $name = $this->filter($collection);
        $columns = ['_uid'];
        $values = ['_uid'];

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

        // Insert manual id if set
        if (!empty($document->getSequence())) {
            $values[] = '_id';
            $columns[] = "_id";
        }

        $sql = "
			INSERT INTO `{$this->getNamespace()}_{$name}` (".\implode(', ', $columns).") 
			VALUES (:".\implode(', :', $values).");
		";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_CREATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

        // Bind internal id if set
        if (!empty($document->getSequence())) {
            $stmt->bindValue(':_id', $document->getSequence(), PDO::PARAM_STR);
        }

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
                $tenantQuery = $this->sharedTables ? ', :_tenant' : '';
                $permissions[] = "('{$type}', '{$permission}', '{$document->getId()}' {$tenantQuery})";
            }
        }

        if (!empty($permissions)) {
            $tenantQuery = $this->sharedTables ? ', _tenant' : '';

            $queryPermissions = "
				INSERT INTO `{$this->getNamespace()}_{$name}_perms` (_type, _permission, _document {$tenantQuery})
				VALUES " . \implode(', ', $permissions);

            $queryPermissions = $this->trigger(Database::EVENT_PERMISSIONS_CREATE, $queryPermissions);

            $stmtPermissions = $this->getPDO()->prepare($queryPermissions);

            if ($this->sharedTables) {
                $stmtPermissions->bindValue(':_tenant', $this->tenant);
            }
        }

        try {
            $stmt->execute();

            $statment = $this->getPDO()->prepare("SELECT last_insert_rowid() AS id");
            $statment->execute();
            $last = $statment->fetch();

            $document['$sequence'] = $last['id'];

            if (isset($stmtPermissions)) {
                $stmtPermissions->execute();
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }


        return $document;
    }

    /**
     * Update Document
     *
     * @param Document $collection
     * @param string $id
     * @param Document $document
     * @param bool $skipPermissions
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws DuplicateException
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        $spatialAttributes = $this->getSpatialAttributes($collection);
        $collection = $collection->getId();
        $attributes = $document->getAttributes();
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = json_encode($document->getPermissions());

        if ($this->sharedTables) {
            $attributes['_tenant'] = $this->tenant;
        }

        $name = $this->filter($collection);
        $columns = '';

        if (!$skipPermissions) {
            $sql = "
			SELECT _type, _permission
			FROM `{$this->getNamespace()}_{$name}_perms`
			WHERE _document = :_uid
			{$this->getTenantQuery($collection)}
		";

            $sql = $this->trigger(Database::EVENT_PERMISSIONS_READ, $sql);

            /**
             * Get current permissions from the database
             */
            $permissionsStmt = $this->getPDO()->prepare($sql);
            $permissionsStmt->bindValue(':_uid', $document->getId());

            if ($this->sharedTables) {
                $permissionsStmt->bindValue(':_tenant', $this->tenant);
            }

            $permissionsStmt->execute();
            $permissions = $permissionsStmt->fetchAll();
            $permissionsStmt->closeCursor();

            $initial = [];
            foreach (Database::PERMISSIONS as $type) {
                $initial[$type] = [];
            }

            $permissions = array_reduce($permissions, function (array $carry, array $item) {
                $carry[$item['_type']][] = $item['_permission'];

                return $carry;
            }, $initial);

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
                $removeQuery = ' AND (';
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
                $sql = "
				DELETE
                FROM `{$this->getNamespace()}_{$name}_perms`
                WHERE _document = :_uid
                {$this->getTenantQuery($collection)}
			";

                $removeQuery = $sql . $removeQuery;
                $removeQuery = $this->trigger(Database::EVENT_PERMISSIONS_DELETE, $removeQuery);

                $stmtRemovePermissions = $this->getPDO()->prepare($removeQuery);
                $stmtRemovePermissions->bindValue(':_uid', $document->getId());

                if ($this->sharedTables) {
                    $stmtRemovePermissions->bindValue(':_tenant', $this->tenant);
                }

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
                        $tenantQuery = $this->sharedTables ? ', :_tenant' : '';
                        $values[] = "(:_uid, '{$type}', :_add_{$type}_{$i} {$tenantQuery})";
                    }
                }

                $tenantQuery = $this->sharedTables ? ', _tenant' : '';

                $sql = "
			   INSERT INTO `{$this->getNamespace()}_{$name}_perms` (_document, _type, _permission {$tenantQuery})
			   VALUES " . \implode(', ', $values);

                $sql = $this->trigger(Database::EVENT_PERMISSIONS_CREATE, $sql);

                $stmtAddPermissions = $this->getPDO()->prepare($sql);

                $stmtAddPermissions->bindValue(":_uid", $document->getId());
                if ($this->sharedTables) {
                    $stmtAddPermissions->bindValue(":_tenant", $this->tenant);
                }

                foreach ($additions as $type => $permissions) {
                    foreach ($permissions as $i => $permission) {
                        $stmtAddPermissions->bindValue(":_add_{$type}_{$i}", $permission);
                    }
                }
            }
        }

        /**
         * Update Attributes
         */
        $keyIndex = 0;
        $opIndex = 0;
        $operators = [];

        // Separate regular attributes from operators
        foreach ($attributes as $attribute => $value) {
            if (Operator::isOperator($value)) {
                $operators[$attribute] = $value;
            }
        }

        foreach ($attributes as $attribute => $value) {
            $column = $this->filter($attribute);

            // Check if this is an operator, spatial attribute, or regular attribute
            if (isset($operators[$attribute])) {
                $operatorSQL = $this->getOperatorSQL($column, $operators[$attribute], $opIndex);
                $columns .= $operatorSQL;
            } elseif ($this->getSupportForSpatialAttributes() && \in_array($attribute, $spatialAttributes, true)) {
                $bindKey = 'key_' . $keyIndex;
                $columns .= "`{$column}` = " . $this->getSpatialGeomFromText(':' . $bindKey);
                $keyIndex++;
            } else {
                $bindKey = 'key_' . $keyIndex;
                $columns .= "`{$column}`" . '=:' . $bindKey;
                $keyIndex++;
            }

            $columns .= ',';
        }

        // Remove trailing comma
        $columns = rtrim($columns, ',');

        $sql = "
			UPDATE `{$this->getNamespace()}_{$name}`
			SET {$columns}, _uid = :_newUid
			WHERE _uid = :_existingUid
			{$this->getTenantQuery($collection)}
		";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_UPDATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_existingUid', $id);
        $stmt->bindValue(':_newUid', $document->getId());

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
        }

        // Bind values for non-operator attributes and operator parameters
        $keyIndex = 0;
        $opIndexForBinding = 0;
        foreach ($attributes as $attribute => $value) {
            // Handle operators separately
            if (isset($operators[$attribute])) {
                $this->bindOperatorParams($stmt, $operators[$attribute], $opIndexForBinding);
                continue;
            }

            // Convert spatial arrays to WKT, json_encode non-spatial arrays
            if (\in_array($attribute, $spatialAttributes, true)) {
                if (\is_array($value)) {
                    $value = $this->convertArrayToWKT($value);
                }
            } elseif (is_array($value)) { // arrays & objects should be saved as strings
                $value = json_encode($value);
            }

            $bindKey = 'key_' . $keyIndex;
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $bindKey, $value, $this->getPDOType($value));
            $keyIndex++;
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
            throw $this->processException($e);
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

    public function getSupportForQueryContains(): bool
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

    public function getSupportForUpdateLock(): bool
    {
        return false;
    }

    /**
     * Is attribute resizing supported?
     *
     * @return bool
     */
    public function getSupportForAttributeResizing(): bool
    {
        return false;
    }

    /**
     * Is get connection id supported?
     *
     * @return bool
     */
    public function getSupportForGetConnectionId(): bool
    {
        return false;
    }

    /**
     * Is get schema attributes supported?
     *
     * @return bool
     */
    public function getSupportForSchemaAttributes(): bool
    {
        return false;
    }

    /**
     * Is upsert supported?
     *
     * @return bool
     */
    public function getSupportForUpserts(): bool
    {
        return false;
    }

    /**
     * Is hostname supported?
     *
     * @return bool
     */
    public function getSupportForHostname(): bool
    {
        return false;
    }

    /**
     * Is batch create attributes supported?
     *
     * @return bool
     */
    public function getSupportForBatchCreateAttributes(): bool
    {
        return false;
    }

    public function getSupportForSpatialAttributes(): bool
    {
        return false; // SQLite doesn't have native spatial support
    }

    public function getSupportForObject(): bool
    {
        return false;
    }

    /**
     * Are object (JSON) indexes supported?
     *
     * @return bool
     */
    public function getSupportForObjectIndexes(): bool
    {
        return false;
    }

    public function getSupportForSpatialIndexNull(): bool
    {
        return false; // SQLite doesn't have native spatial support
    }

    /**
     * Override getSpatialGeomFromText to return placeholder unchanged for SQLite
     * SQLite does not support ST_GeomFromText, so we return the raw placeholder
     *
     * @param string $wktPlaceholder
     * @param int|null $srid
     * @return string
     */
    protected function getSpatialGeomFromText(string $wktPlaceholder, ?int $srid = null): string
    {
        return $wktPlaceholder;
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
                return 'INDEX';

            case Database::INDEX_UNIQUE:
                return 'UNIQUE INDEX';

            default:
                throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_FULLTEXT);
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
                $type = 'INDEX';
                break;

            case Database::INDEX_UNIQUE:
                $type = 'UNIQUE INDEX';
                $postfix = 'COLLATE NOCASE';

                break;

            default:
                throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_FULLTEXT);
        }

        $attributes = \array_map(fn ($attribute) => match ($attribute) {
            '$id' => ID::custom('_uid'),
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $attribute
        }, $attributes);

        foreach ($attributes as $key => $attribute) {
            $attribute = $this->filter($attribute);

            $attributes[$key] = "`{$attribute}` {$postfix}";
        }

        $key = "`{$this->getNamespace()}_{$this->tenant}_{$collection}_{$id}`";
        $attributes = implode(', ', $attributes);

        if ($this->sharedTables) {
            $attributes = "`_tenant` {$postfix}, {$attributes}";
        }

        return "CREATE {$type} {$key} ON `{$this->getNamespace()}_{$collection}` ({$attributes})";
    }

    /**
     * Get SQL condition for permissions
     *
     * @param string $collection
     * @param array<string> $roles
     * @return string
     * @throws Exception
     */
    protected function getSQLPermissionsCondition(string $collection, array $roles, string $alias, string $type = Database::PERMISSION_READ): string
    {
        $roles = array_map(fn (string $role) => $this->getPDO()->quote($role), $roles);

        return "{$this->quote($alias)}.{$this->quote('_uid')} IN (
                    SELECT distinct(_document)
                    FROM `{$this->getNamespace()}_{$collection}_perms`
                    WHERE _permission IN (" . implode(', ', $roles) . ")
                    AND _type = '{$type}'
                )";
    }

    /**
     * Get SQL table
     *
     * @param string $name
     * @return string
     */
    protected function getSQLTable(string $name): string
    {
        return $this->quote("{$this->getNamespace()}_{$this->filter($name)}");
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

    protected function processException(PDOException $e): \Exception
    {
        // Timeout
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Duplicate - SQLite uses various error codes for constraint violations:
        // - Error code 19 is SQLITE_CONSTRAINT (includes UNIQUE violations)
        // - Error code 1 is also used for some duplicate cases
        // - SQL state '23000' is integrity constraint violation
        if (
            ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && ($e->errorInfo[1] === 1 || $e->errorInfo[1] === 19)) ||
            $e->getCode() === '23000'
        ) {
            // Check if it's actually a duplicate/unique constraint violation
            $message = $e->getMessage();
            if (
                (isset($e->errorInfo[1]) && $e->errorInfo[1] === 19) ||
                $e->getCode() === '23000' ||
                stripos($message, 'unique') !== false ||
                stripos($message, 'duplicate') !== false
            ) {
                return new DuplicateException('Document already exists', $e->getCode(), $e);
            }
        }

        // String or BLOB exceeds size limit
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 18) {
            return new LimitException('Value too large', $e->getCode(), $e);
        }

        return $e;
    }

    public function getSupportForSpatialIndexOrder(): bool
    {
        return false;
    }
    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return false;
    }

    /**
     * Does the adapter support calculating distance(in meters) between multidimension geometry(line, polygon,etc)?
     *
     * @return bool
     */
    public function getSupportForDistanceBetweenMultiDimensionGeometryInMeters(): bool
    {
        return false;
    }

    /**
     * Does the adapter support spatial axis order specification?
     *
     * @return bool
     */
    public function getSupportForSpatialAxisOrder(): bool
    {
        return false;
    }

    /**
     * Adapter supports optional spatial attributes with existing rows.
     *
     * @return bool
     */
    public function getSupportForOptionalSpatialAttributeWithExistingRows(): bool
    {
        return true;
    }

    /**
     * Get the SQL function for random ordering
     *
     * @return string
     */
    protected function getRandomOrder(): string
    {
        return 'RANDOM()';
    }

    /**
     * Check if SQLite math functions (like POWER) are available
     * SQLite must be compiled with -DSQLITE_ENABLE_MATH_FUNCTIONS
     *
     * @return bool
     */
    private function getSupportForMathFunctions(): bool
    {
        static $available = null;

        if ($available !== null) {
            return $available;
        }

        try {
            // Test if POWER function exists by attempting to use it
            $stmt = $this->getPDO()->query('SELECT POWER(2, 3) as test');
            $result = $stmt->fetch();
            $available = ($result['test'] == 8);
            return $available;
        } catch (PDOException $e) {
            // Function doesn't exist
            $available = false;
            return false;
        }
    }

    /**
     * Bind operator parameters to statement
     * Override to handle SQLite-specific operator bindings
     *
     * @param \PDOStatement|PDOStatementProxy $stmt
     * @param Operator $operator
     * @param int &$bindIndex
     * @return void
     */
    protected function bindOperatorParams(\PDOStatement|PDOStatementProxy $stmt, Operator $operator, int &$bindIndex): void
    {
        $method = $operator->getMethod();

        // For operators that SQLite doesn't use bind parameters for, skip binding entirely
        // Note: The bindIndex increment happens in getOperatorSQL(), NOT here
        if (in_array($method, [Operator::TYPE_TOGGLE, Operator::TYPE_DATE_SET_NOW, Operator::TYPE_ARRAY_UNIQUE])) {
            // These operators don't bind any parameters - they're handled purely in SQL
            // DO NOT increment bindIndex here as it's already handled in getOperatorSQL()
            return;
        }

        // For ARRAY_FILTER, bind the filter value if present
        if ($method === Operator::TYPE_ARRAY_FILTER) {
            $values = $operator->getValues();
            if (!empty($values) && count($values) >= 2) {
                $filterType = $values[0];
                $filterValue = $values[1];

                // Only bind if we support this filter type (all comparison operators need binding)
                $comparisonTypes = ['equal', 'notEqual', 'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual'];
                if (in_array($filterType, $comparisonTypes)) {
                    $bindKey = "op_{$bindIndex}";
                    $value = (is_bool($filterValue)) ? (int)$filterValue : $filterValue;
                    $stmt->bindValue(":{$bindKey}", $value, $this->getPDOType($value));
                    $bindIndex++;
                }
            }
            return;
        }

        // For all other operators, use parent implementation
        parent::bindOperatorParams($stmt, $operator, $bindIndex);
    }

    /**
     * Get SQL expression for operator
     *
     * IMPORTANT: SQLite JSON Limitations
     * -----------------------------------
     * Array operators using json_each() and json_group_array() have type conversion behavior:
     * - Numbers are preserved but may lose precision (e.g., 1.0 becomes 1)
     * - Booleans become integers (true→1, false→0)
     * - Strings remain strings
     * - Objects and nested arrays are converted to JSON strings
     *
     * This is inherent to SQLite's JSON implementation and affects: ARRAY_APPEND, ARRAY_PREPEND,
     * ARRAY_UNIQUE, ARRAY_INTERSECT, ARRAY_DIFF, ARRAY_INSERT, and ARRAY_REMOVE.
     *
     * @param string $column
     * @param Operator $operator
     * @param int &$bindIndex
     * @return ?string
     */
    protected function getOperatorSQL(string $column, Operator $operator, int &$bindIndex): ?string
    {
        $quotedColumn = $this->quote($column);
        $method = $operator->getMethod();

        switch ($method) {
            // Numeric operators
            case Operator::TYPE_INCREMENT:
                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN COALESCE({$quotedColumn}, 0) > :$maxKey - :$bindKey THEN :$maxKey
                        ELSE COALESCE({$quotedColumn}, 0) + :$bindKey
                    END";
                }
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) + :$bindKey";

            case Operator::TYPE_DECREMENT:
                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $minKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) <= :$minKey THEN :$minKey
                        WHEN COALESCE({$quotedColumn}, 0) < :$minKey + :$bindKey THEN :$minKey
                        ELSE COALESCE({$quotedColumn}, 0) - :$bindKey
                    END";
                }
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) - :$bindKey";

            case Operator::TYPE_MULTIPLY:
                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN :$bindKey > 0 AND COALESCE({$quotedColumn}, 0) > :$maxKey / :$bindKey THEN :$maxKey
                        WHEN :$bindKey < 0 AND COALESCE({$quotedColumn}, 0) < :$maxKey / :$bindKey THEN :$maxKey
                        ELSE COALESCE({$quotedColumn}, 0) * :$bindKey
                    END";
                }
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) * :$bindKey";

            case Operator::TYPE_DIVIDE:
                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $minKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN :$bindKey != 0 AND COALESCE({$quotedColumn}, 0) / :$bindKey <= :$minKey THEN :$minKey
                        ELSE COALESCE({$quotedColumn}, 0) / :$bindKey
                    END";
                }
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) / :$bindKey";

            case Operator::TYPE_MODULO:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                return "{$quotedColumn} = COALESCE({$quotedColumn}, 0) % :$bindKey";

            case Operator::TYPE_POWER:
                if (!$this->getSupportForMathFunctions()) {
                    throw new DatabaseException(
                        'SQLite POWER operator requires math functions. ' .
                        'Compile SQLite with -DSQLITE_ENABLE_MATH_FUNCTIONS or use multiply operators instead.'
                    );
                }

                $values = $operator->getValues();
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                if (isset($values[1])) {
                    $maxKey = "op_{$bindIndex}";
                    $bindIndex++;
                    return "{$quotedColumn} = CASE
                        WHEN COALESCE({$quotedColumn}, 0) >= :$maxKey THEN :$maxKey
                        WHEN COALESCE({$quotedColumn}, 0) <= 1 THEN COALESCE({$quotedColumn}, 0)
                        WHEN :$bindKey * LN(COALESCE({$quotedColumn}, 1)) > LN(:$maxKey) THEN :$maxKey
                        ELSE POWER(COALESCE({$quotedColumn}, 0), :$bindKey)
                    END";
                }
                return "{$quotedColumn} = POWER(COALESCE({$quotedColumn}, 0), :$bindKey)";

                // String operators
            case Operator::TYPE_STRING_CONCAT:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                return "{$quotedColumn} = IFNULL({$quotedColumn}, '') || :$bindKey";

            case Operator::TYPE_STRING_REPLACE:
                $searchKey = "op_{$bindIndex}";
                $bindIndex++;
                $replaceKey = "op_{$bindIndex}";
                $bindIndex++;
                return "{$quotedColumn} = REPLACE({$quotedColumn}, :$searchKey, :$replaceKey)";

                // Boolean operators
            case Operator::TYPE_TOGGLE:
                // SQLite: toggle boolean (0 or 1), treat NULL as 0
                return "{$quotedColumn} = CASE WHEN COALESCE({$quotedColumn}, 0) = 0 THEN 1 ELSE 0 END";

                // Array operators
            case Operator::TYPE_ARRAY_APPEND:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: merge arrays by using json_group_array on extracted elements
                // We use json_each to extract elements from both arrays and combine them
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value FROM json_each(IFNULL({$quotedColumn}, '[]'))
                        UNION ALL
                        SELECT value FROM json_each(:$bindKey)
                    )
                )";

            case Operator::TYPE_ARRAY_PREPEND:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: prepend by extracting and recombining with new elements first
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value FROM json_each(:$bindKey)
                        UNION ALL
                        SELECT value FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    )
                )";

            case Operator::TYPE_ARRAY_UNIQUE:
                // SQLite: get distinct values from JSON array
                return "{$quotedColumn} = (
                    SELECT json_group_array(DISTINCT value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                )";

            case Operator::TYPE_ARRAY_REMOVE:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: remove specific value from array
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value != :$bindKey
                )";

            case Operator::TYPE_ARRAY_INSERT:
                $indexKey = "op_{$bindIndex}";
                $bindIndex++;
                $valueKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: Insert element at specific index by:
                // 1. Take elements before index (0 to index-1)
                // 2. Add new element
                // 3. Take elements from index to end
                // The bound value is JSON-encoded by parent, json() parses it back to a value,
                // then we wrap it in json_array() and extract to get the same format as json_each()
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value, rownum
                        FROM (
                            SELECT value, (ROW_NUMBER() OVER ()) - 1 as rownum
                            FROM json_each(IFNULL({$quotedColumn}, '[]'))
                        )
                        WHERE rownum < :$indexKey
                        UNION ALL
                        SELECT value, :$indexKey as rownum
                        FROM json_each(json_array(json(:$valueKey)))
                        UNION ALL
                        SELECT value, rownum + 1 as rownum
                        FROM (
                            SELECT value, (ROW_NUMBER() OVER ()) - 1 as rownum
                            FROM json_each(IFNULL({$quotedColumn}, '[]'))
                        )
                        WHERE rownum >= :$indexKey
                        ORDER BY rownum
                    )
                )";

            case Operator::TYPE_ARRAY_INTERSECT:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: keep only values that exist in both arrays
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value IN (SELECT value FROM json_each(:$bindKey))
                )";

            case Operator::TYPE_ARRAY_DIFF:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;
                // SQLite: remove values that exist in the comparison array
                return "{$quotedColumn} = (
                    SELECT json_group_array(value)
                    FROM json_each(IFNULL({$quotedColumn}, '[]'))
                    WHERE value NOT IN (SELECT value FROM json_each(:$bindKey))
                )";

            case Operator::TYPE_ARRAY_FILTER:
                $values = $operator->getValues();
                if (empty($values)) {
                    // No filter criteria, return array unchanged
                    return "{$quotedColumn} = {$quotedColumn}";
                }

                $filterType = $values[0]; // 'equal', 'notEqual', 'isNull', 'isNotNull', 'greaterThan', etc.

                switch ($filterType) {
                    case 'isNull':
                        // Filter for null values - no bind parameter needed
                        return "{$quotedColumn} = (
                            SELECT json_group_array(value)
                            FROM json_each(IFNULL({$quotedColumn}, '[]'))
                            WHERE value IS NULL
                        )";

                    case 'isNotNull':
                        // Filter out null values - no bind parameter needed
                        return "{$quotedColumn} = (
                            SELECT json_group_array(value)
                            FROM json_each(IFNULL({$quotedColumn}, '[]'))
                            WHERE value IS NOT NULL
                        )";

                    case 'equal':
                    case 'notEqual':
                    case 'greaterThan':
                    case 'greaterThanEqual':
                    case 'lessThan':
                    case 'lessThanEqual':
                        if (\count($values) < 2) {
                            return "{$quotedColumn} = {$quotedColumn}";
                        }

                        $bindKey = "op_{$bindIndex}";
                        $bindIndex++;

                        $operator = match ($filterType) {
                            'equal' => '=',
                            'notEqual' => '!=',
                            'greaterThan' => '>',
                            'greaterThanEqual' => '>=',
                            'lessThan' => '<',
                            'lessThanEqual' => '<=',
                            default => throw new OperatorException('Unsupported filter type: ' . $filterType),
                        };

                        // For numeric comparisons, cast to REAL; for equal/notEqual, use text comparison
                        $isNumericComparison = \in_array($filterType, ['greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual']);
                        if ($isNumericComparison) {
                            return "{$quotedColumn} = (
                                SELECT json_group_array(value)
                                FROM json_each(IFNULL({$quotedColumn}, '[]'))
                                WHERE CAST(value AS REAL) $operator CAST(:$bindKey AS REAL)
                            )";
                        } else {
                            return "{$quotedColumn} = (
                                SELECT json_group_array(value)
                                FROM json_each(IFNULL({$quotedColumn}, '[]'))
                                WHERE value $operator :$bindKey
                            )";
                        }

                        // no break
                    default:
                        return "{$quotedColumn} = {$quotedColumn}";
                }

                // Date operators
                // no break
            case Operator::TYPE_DATE_ADD_DAYS:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = datetime({$quotedColumn}, :$bindKey || ' days')";

            case Operator::TYPE_DATE_SUB_DAYS:
                $bindKey = "op_{$bindIndex}";
                $bindIndex++;

                return "{$quotedColumn} = datetime({$quotedColumn}, '-' || abs(:$bindKey) || ' days')";

            case Operator::TYPE_DATE_SET_NOW:
                return "{$quotedColumn} = datetime('now')";

            default:
                // Fall back to parent implementation for other operators
                return parent::getOperatorSQL($column, $operator, $bindIndex);
        }
    }

    /**
     * Override getUpsertStatement to use SQLite's ON CONFLICT syntax instead of MariaDB's ON DUPLICATE KEY UPDATE
     *
     * @param string $tableName
     * @param string $columns
     * @param array<string> $batchKeys
     * @param array<string> $attributes
     * @param array<mixed> $bindValues
     * @param string $attribute
     * @param array<string, Operator> $operators
     * @return mixed
     */
    public function getUpsertStatement(
        string $tableName,
        string $columns,
        array $batchKeys,
        array $attributes,
        array $bindValues,
        string $attribute = '',
        array $operators = [],
    ): mixed {
        $getUpdateClause = function (string $attribute, bool $increment = false): string {
            $attribute = $this->quote($this->filter($attribute));
            if ($increment) {
                $new = "{$attribute} + excluded.{$attribute}";
            } else {
                $new = "excluded.{$attribute}";
            }

            if ($this->sharedTables) {
                return "{$attribute} = CASE WHEN _tenant = excluded._tenant THEN {$new} ELSE {$attribute} END";
            }

            return "{$attribute} = {$new}";
        };

        $updateColumns = [];
        $opIndex = 0;

        if (!empty($attribute)) {
            // Increment specific column by its new value in place
            $updateColumns = [
                $getUpdateClause($attribute, increment: true),
                $getUpdateClause('_updatedAt'),
            ];
        } else {
            // Update all columns, handling operators separately
            foreach (\array_keys($attributes) as $attr) {
                /**
                 * @var string $attr
                 */
                $filteredAttr = $this->filter($attr);

                // Check if this attribute has an operator
                if (isset($operators[$attr])) {
                    $operatorSQL = $this->getOperatorSQL($filteredAttr, $operators[$attr], $opIndex);
                    if ($operatorSQL !== null) {
                        $updateColumns[] = $operatorSQL;
                    }
                } else {
                    if (!in_array($attr, ['_uid', '_id', '_createdAt', '_tenant'])) {
                        $updateColumns[] = $getUpdateClause($filteredAttr);
                    }
                }
            }
        }

        $conflictKeys = $this->sharedTables ? '(_uid, _tenant)' : '(_uid)';

        $stmt = $this->getPDO()->prepare(
            "
            INSERT INTO {$this->getSQLTable($tableName)} {$columns}
            VALUES " . \implode(', ', $batchKeys) . "
            ON CONFLICT {$conflictKeys} DO UPDATE
                SET " . \implode(', ', $updateColumns)
        );

        // Bind regular attribute values
        foreach ($bindValues as $key => $binding) {
            $stmt->bindValue($key, $binding, $this->getPDOType($binding));
        }

        $opIndexForBinding = 0;

        // Bind operator parameters in the same order used to build SQL
        foreach (array_keys($attributes) as $attr) {
            if (isset($operators[$attr])) {
                $this->bindOperatorParams($stmt, $operators[$attr], $opIndexForBinding);
            }
        }

        return $stmt;
    }

    public function getSupportForAlterLocks(): bool
    {
        return false;
    }

    public function getSupportNonUtfCharacters(): bool
    {
        return false;
    }

    /**
     * Is PCRE regex supported?
     * SQLite does not have native REGEXP support - it requires compile-time option or user-defined function
     *
     * @return bool
     */
    public function getSupportForPCRERegex(): bool
    {
        return false;
    }

    /**
     * Is POSIX regex supported?
     * SQLite does not have native REGEXP support - it requires compile-time option or user-defined function
     *
     * @return bool
     */
    public function getSupportForPOSIXRegex(): bool
    {
        return false;
    }

    public function getSupportForTTLIndexes(): bool
    {
        return false;
    }
}
