<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Helpers\ID;

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

                $this->createIndex($id, $indexId, $indexType, $indexAttributes, $indexLengths, $indexOrders);
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
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null): bool
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
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = []): bool
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
     * @throws Duplicate
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
            throw match ($e->getCode()) {
                "1062", "23000" => new Duplicate('Duplicated document: ' . $e->getMessage()),
                default => $e,
            };
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
     * @throws Duplicate
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
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
        $bindIndex = 0;
        foreach ($attributes as $attribute => $value) {
            $column = $this->filter($attribute);
            $bindKey = 'key_' . $bindIndex;
            $columns .= "`{$column}`" . '=:' . $bindKey . ',';
            $bindIndex++;
        }

        $sql = "
			UPDATE `{$this->getNamespace()}_{$name}`
			SET {$columns} _uid = :_newUid 
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
            throw match ($e->getCode()) {
                '1062',
                '23000' => new Duplicate('Duplicated document: ' . $e->getMessage()),
                default => $e,
            };
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

    public function getSupportForSpatialIndexNull(): bool
    {
        return false; // SQLite doesn't have native spatial support
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

        // Duplicate
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1) {
            return new DuplicateException('Document already exists', $e->getCode(), $e);
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
}
