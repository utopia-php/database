<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDOException;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Query;

abstract class SQL extends Adapter
{
    protected mixed $pdo;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param mixed $pdo
     */
    public function __construct(mixed $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @inheritDoc
     */
    public function startTransaction(): bool
    {
        try {
            if ($this->inTransaction === 0) {
                if ($this->getPDO()->inTransaction()) {
                    $this->getPDO()->rollBack();
                } else {
                    // If no active transaction, this has no effect.
                    $this->getPDO()->prepare('ROLLBACK')->execute();
                }

                $result = $this->getPDO()->beginTransaction();
            } else {
                $result = $this->getPDO()->exec('SAVEPOINT transaction' . $this->inTransaction);
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
     * @inheritDoc
     */
    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        } elseif ($this->inTransaction > 1) {
            $this->inTransaction--;
            return true;
        }

        if (!$this->getPDO()->inTransaction()) {
            $this->inTransaction = 0;
            return false;
        }

        try {
            $result = $this->getPDO()->commit();
            $this->inTransaction = 0;
        } catch (PDOException $e) {
            throw new TransactionException('Failed to commit transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        if (!$result) {
            throw new TransactionException('Failed to commit transaction');
        }

        return $result;
    }

    /**
     * @inheritDoc
     */
    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        try {
            if ($this->inTransaction > 1) {
                $result = $this->getPDO()->exec('ROLLBACK TO transaction' . ($this->inTransaction - 1));
                $this->inTransaction--;
            } else {
                $result = $this->getPDO()->rollBack();
                $this->inTransaction = 0;
            }
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to rollback transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        if (!$result) {
            throw new TransactionException('Failed to rollback transaction');
        }

        return $result;
    }

    /**
     * Ping Database
     *
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function ping(): bool
    {
        return $this->getPDO()
            ->prepare("SELECT 1;")
            ->execute();
    }

    public function reconnect(): void
    {
        $this->getPDO()->reconnect();
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

        if (!\is_null($collection)) {
            $collection = $this->filter($collection);
            $stmt = $this->getPDO()->prepare("
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = :schema 
                  AND TABLE_NAME = :table
            ");
            $stmt->bindValue(':schema', $database, \PDO::PARAM_STR);
            $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", \PDO::PARAM_STR);
        } else {
            $stmt = $this->getPDO()->prepare("
                SELECT SCHEMA_NAME FROM
                INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME = :schema
            ");
            $stmt->bindValue(':schema', $database, \PDO::PARAM_STR);
        }

        try {
            $stmt->execute();
            $document = $stmt->fetchAll();
            $stmt->closeCursor();
        } catch (PDOException $e) {
            $e = $this->processException($e);

            if ($e instanceof NotFoundException) {
                return false;
            }

            throw $e;
        }

        if (empty($document)) {
            return false;
        }

        return true;
    }

    /**
     * List Databases
     *
     * @return array<Document>
     */
    public function list(): array
    {
        return [];
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
        $id = $this->quote($this->filter($id));
        $type = $this->getSQLType($type, $size, $signed, $array);

        $sql = "ALTER TABLE {$this->getSQLTable($collection)} ADD COLUMN {$id} {$type};";
        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Create Attributes
     *
     * @param string $collection
     * @param array<array<string, mixed>> $attributes
     * @return bool
     * @throws DatabaseException
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        $parts = [];
        foreach ($attributes as $attribute) {
            $id = $this->quote($this->filter($attribute['$id']));
            $type = $this->getSQLType(
                $attribute['type'],
                $attribute['size'],
                $attribute['signed'] ?? true,
                $attribute['array'] ?? false,
            );
            $parts[] = "{$id} {$type}";
        }

        $columns = \implode(', ADD COLUMN ', $parts);

        $sql = "ALTER TABLE {$this->getSQLTable($collection)} ADD COLUMN {$columns};";
        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
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
        $old = $this->quote($this->filter($old));
        $new = $this->quote($this->filter($new));

        $sql = "ALTER TABLE {$this->getSQLTable($collection)} RENAME COLUMN {$old} TO {$new};";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
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
        $id = $this->quote($this->filter($id));
        $sql = "ALTER TABLE {$this->getSQLTable($collection)} DROP COLUMN {$id};";
        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     * @param Query[] $queries
     * @param bool $forUpdate
     * @return Document
     * @throws DatabaseException
     */
    public function getDocument(string $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $name = $this->filter($collection);
        $selections = $this->getAttributeSelections($queries);

        $forUpdate = $forUpdate ? 'FOR UPDATE' : '';

        $alias = Query::DEFAULT_ALIAS;

        $sql = "
		    SELECT {$this->getAttributeProjection($selections, $alias)}
            FROM {$this->getSQLTable($name)} AS {$this->quote($alias)}
            WHERE {$this->quote($alias)}.{$this->quote('_uid')} = :_uid 
            {$this->getTenantQuery($collection, $alias)}
		";

        if ($this->getSupportForUpdateLock()) {
            $sql .= " {$forUpdate}";
        }

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_uid', $id);

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->getTenant());
        }

        $stmt->execute();
        $document = $stmt->fetchAll();
        $stmt->closeCursor();

        if (empty($document)) {
            return new Document([]);
        }

        $document = $document[0];

        if (\array_key_exists('_id', $document)) {
            $document['$sequence'] = $document['_id'];
            unset($document['_id']);
        }
        if (\array_key_exists('_uid', $document)) {
            $document['$id'] = $document['_uid'];
            unset($document['_uid']);
        }
        if (\array_key_exists('_tenant', $document)) {
            $document['$tenant'] = $document['_tenant'];
            unset($document['_tenant']);
        }
        if (\array_key_exists('_createdAt', $document)) {
            $document['$createdAt'] = $document['_createdAt'];
            unset($document['_createdAt']);
        }
        if (\array_key_exists('_updatedAt', $document)) {
            $document['$updatedAt'] = $document['_updatedAt'];
            unset($document['_updatedAt']);
        }
        if (\array_key_exists('_permissions', $document)) {
            $document['$permissions'] = json_decode($document['_permissions'] ?? '[]', true);
            unset($document['_permissions']);
        }

        return new Document($document);
    }

    /**
     * Update documents
     *
     * Updates all documents which match the given query.
     *
     * @param string $collection
     * @param Document $updates
     * @param array<Document> $documents
     *
     * @return int
     *
     * @throws DatabaseException
     */
    public function updateDocuments(string $collection, Document $updates, array $documents): int
    {
        if (empty($documents)) {
            return 0;
        }

        $attributes = $updates->getAttributes();

        if (!empty($updates->getUpdatedAt())) {
            $attributes['_updatedAt'] = $updates->getUpdatedAt();
        }

        if (!empty($updates->getPermissions())) {
            $attributes['_permissions'] = json_encode($updates->getPermissions());
        }

        if (empty($attributes)) {
            return 0;
        }

        $bindIndex = 0;
        $columns = '';
        foreach ($attributes as $attribute => $value) {
            $column = $this->filter($attribute);
            $columns .= "{$this->quote($column)} = :key_{$bindIndex}";

            if ($attribute !== \array_key_last($attributes)) {
                $columns .= ',';
            }

            $bindIndex++;
        }

        $name = $this->filter($collection);
        $sequences = \array_map(fn ($document) => $document->getSequence(), $documents);

        $sql = "
            UPDATE {$this->getSQLTable($name)}
            SET {$columns}
            WHERE _id IN (" . \implode(', ', \array_map(fn ($index) => ":_id_{$index}", \array_keys($sequences))) . ")
            {$this->getTenantQuery($collection)}
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENTS_UPDATE, $sql);
        $stmt = $this->getPDO()->prepare($sql);

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
        }

        foreach ($sequences as $id => $value) {
            $stmt->bindValue(":_id_{$id}", $value);
        }

        $attributeIndex = 0;
        foreach ($attributes as $value) {
            if (is_array($value)) {
                $value = json_encode($value);
            }

            $bindKey = 'key_' . $attributeIndex;
            $value = (is_bool($value)) ? (int)$value : $value;
            $stmt->bindValue(':' . $bindKey, $value, $this->getPDOType($value));
            $attributeIndex++;
        }

        $stmt->execute();
        $affected = $stmt->rowCount();

        // Permissions logic
        if (!empty($updates->getPermissions())) {
            $removeQueries = [];
            $removeBindValues = [];

            $addQuery = '';
            $addBindValues = [];

            foreach ($documents as $index => $document) {
                // Permissions logic
                $sql = "
                    SELECT _type, _permission
                    FROM {$this->getSQLTable($name . '_perms')}
                    WHERE _document = :_uid
                    {$this->getTenantQuery($collection)}
                ";

                $sql = $this->trigger(Database::EVENT_PERMISSIONS_READ, $sql);

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

                $permissions = \array_reduce($permissions, function (array $carry, array $item) {
                    $carry[$item['_type']][] = $item['_permission'];
                    return $carry;
                }, $initial);

                // Get removed Permissions
                $removals = [];
                foreach (Database::PERMISSIONS as $type) {
                    $diff = array_diff($permissions[$type], $updates->getPermissionsByType($type));
                    if (!empty($diff)) {
                        $removals[$type] = $diff;
                    }
                }

                // Build inner query to remove permissions
                if (!empty($removals)) {
                    foreach ($removals as $type => $permissionsToRemove) {
                        $bindKey = '_uid_' . $index;
                        $removeBindKeys[] = ':_uid_' . $index;
                        $removeBindValues[$bindKey] = $document->getId();

                        $removeQueries[] = "(
                            _document = :_uid_{$index}
                            {$this->getTenantQuery($collection)}
                            AND _type = '{$type}'
                            AND _permission IN (" . \implode(', ', \array_map(function (string $i) use ($permissionsToRemove, $index, $type, &$removeBindKeys, &$removeBindValues) {
                            $bindKey = 'remove_' . $type . '_' . $index . '_' . $i;
                            $removeBindKeys[] = ':' . $bindKey;
                            $removeBindValues[$bindKey] = $permissionsToRemove[$i];

                            return ':' . $bindKey;
                        }, \array_keys($permissionsToRemove))) .
                            ")
                        )";
                    }
                }

                // Get added Permissions
                $additions = [];
                foreach (Database::PERMISSIONS as $type) {
                    $diff = \array_diff($updates->getPermissionsByType($type), $permissions[$type]);
                    if (!empty($diff)) {
                        $additions[$type] = $diff;
                    }
                }

                // Build inner query to add permissions
                if (!empty($additions)) {
                    foreach ($additions as $type => $permissionsToAdd) {
                        foreach ($permissionsToAdd as $i => $permission) {
                            $bindKey = '_uid_' . $index;
                            $addBindValues[$bindKey] = $document->getId();

                            $bindKey = 'add_' . $type . '_' . $index . '_' . $i;
                            $addBindValues[$bindKey] = $permission;

                            $addQuery .= "(:_uid_{$index}, '{$type}', :{$bindKey}";

                            if ($this->sharedTables) {
                                $addQuery .= ", :_tenant)";
                            } else {
                                $addQuery .= ")";
                            }

                            if ($i !== \array_key_last($permissionsToAdd) || $type !== \array_key_last($additions)) {
                                $addQuery .= ', ';
                            }
                        }
                    }
                    if ($index !== \array_key_last($documents)) {
                        $addQuery .= ', ';
                    }
                }
            }

            if (!empty($removeQueries)) {
                $removeQuery = \implode(' OR ', $removeQueries);

                $stmtRemovePermissions = $this->getPDO()->prepare("
                    DELETE
                    FROM {$this->getSQLTable($name . '_perms')}
                    WHERE ({$removeQuery})
                ");

                foreach ($removeBindValues as $key => $value) {
                    $stmtRemovePermissions->bindValue($key, $value, $this->getPDOType($value));
                }

                if ($this->sharedTables) {
                    $stmtRemovePermissions->bindValue(':_tenant', $this->tenant);
                }
                $stmtRemovePermissions->execute();
            }

            if (!empty($addQuery)) {
                $sqlAddPermissions = "
                    INSERT INTO {$this->getSQLTable($name . '_perms')} (_document, _type, _permission
                ";

                if ($this->sharedTables) {
                    $sqlAddPermissions .= ', _tenant)';
                } else {
                    $sqlAddPermissions .= ')';
                }

                $sqlAddPermissions .=  " VALUES {$addQuery}";

                $stmtAddPermissions = $this->getPDO()->prepare($sqlAddPermissions);

                foreach ($addBindValues as $key => $value) {
                    $stmtAddPermissions->bindValue($key, $value, $this->getPDOType($value));
                }

                if ($this->sharedTables) {
                    $stmtAddPermissions->bindValue(':_tenant', $this->tenant);
                }

                $stmtAddPermissions->execute();
            }
        }

        return $affected;
    }


    /**
     * Delete Documents
     *
     * @param string $collection
     * @param array<string> $sequences
     * @param array<string> $permissionIds
     *
     * @return int
     * @throws DatabaseException
     */
    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        if (empty($sequences)) {
            return 0;
        }

        try {
            $name = $this->filter($collection);

            $sql = "
            DELETE FROM {$this->getSQLTable($name)} 
            WHERE _id IN (" . \implode(', ', \array_map(fn ($index) => ":_id_{$index}", \array_keys($sequences))) . ")
            {$this->getTenantQuery($collection)}
            ";

            $sql = $this->trigger(Database::EVENT_DOCUMENTS_DELETE, $sql);

            $stmt = $this->getPDO()->prepare($sql);

            foreach ($sequences as $id => $value) {
                $stmt->bindValue(":_id_{$id}", $value);
            }

            if ($this->sharedTables) {
                $stmt->bindValue(':_tenant', $this->tenant);
            }

            if (!$stmt->execute()) {
                throw new DatabaseException('Failed to delete documents');
            }

            if (!empty($permissionIds)) {
                $sql = "
                DELETE FROM {$this->getSQLTable($name . '_perms')} 
                WHERE _document IN (" . \implode(', ', \array_map(fn ($index) => ":_id_{$index}", \array_keys($permissionIds))) . ")
                {$this->getTenantQuery($collection)}
                ";

                $sql = $this->trigger(Database::EVENT_PERMISSIONS_DELETE, $sql);

                $stmtPermissions = $this->getPDO()->prepare($sql);

                foreach ($permissionIds as $id => $value) {
                    $stmtPermissions->bindValue(":_id_{$id}", $value);
                }

                if ($this->sharedTables) {
                    $stmtPermissions->bindValue(':_tenant', $this->tenant);
                }

                if (!$stmtPermissions->execute()) {
                    throw new DatabaseException('Failed to delete permissions');
                }
            }
        } catch (\Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        return $stmt->rowCount();
    }

    /**
     * Assign internal IDs for the given documents
     *
     * @param string $collection
     * @param array<Document> $documents
     * @return array<Document>
     * @throws DatabaseException
     */
    public function getSequences(string $collection, array $documents): array
    {
        $documentIds = [];
        $keys = [];
        $binds = [];

        foreach ($documents as $i => $document) {
            if (empty($document->getSequence())) {
                $documentIds[] = $document->getId();

                $key = ":uid_{$i}";

                $binds[$key] = $document->getId();
                $keys[] = $key;

                if ($this->sharedTables) {
                    $binds[':_tenant_'.$i] = $document->getTenant();
                }
            }
        }

        if (empty($documentIds)) {
            return $documents;
        }

        $placeholders = implode(',', array_values($keys));

        $sql = "
            SELECT _uid, _id
            FROM {$this->getSQLTable($collection)}
            WHERE {$this->quote('_uid')} IN ({$placeholders})
            {$this->getTenantQuery($collection, tenantCount: \count($documentIds))}
            ";

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($binds as $key => $value) {
            $stmt->bindValue($key, $value);
        }

        $stmt->execute();
        $sequences = $stmt->fetchAll(\PDO::FETCH_KEY_PAIR); // Fetch as [documentId => sequence]
        $stmt->closeCursor();

        foreach ($documents as $document) {
            if (isset($sequences[$document->getId()])) {
                $document['$sequence'] = $sequences[$document->getId()];
            }
        }

        return $documents;
    }

    /**
     * Get max STRING limit
     *
     * @return int
     */
    public function getLimitForString(): int
    {
        return 4294967295;
    }

    /**
     * Get max INT limit
     *
     * @return int
     */
    public function getLimitForInt(): int
    {
        return 4294967295;
    }

    /**
     * Get maximum column limit.
     * https://mariadb.com/kb/en/innodb-limitations/#limitations-on-schema
     * Can be inherited by MySQL since we utilize the InnoDB engine
     *
     * @return int
     */
    public function getLimitForAttributes(): int
    {
        return 1017;
    }

    /**
     * Get maximum index limit.
     * https://mariadb.com/kb/en/innodb-limitations/#limitations-on-schema
     *
     * @return int
     */
    public function getLimitForIndexes(): int
    {
        return 64;
    }

    /**
     * Is schemas supported?
     *
     * @return bool
     */
    public function getSupportForSchemas(): bool
    {
        return true;
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
     * Are attributes supported?
     *
     * @return bool
     */
    public function getSupportForAttributes(): bool
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
     * Are FOR UPDATE locks supported?
     *
     * @return bool
     */
    public function getSupportForUpdateLock(): bool
    {
        return true;
    }

    /**
     * Is Attribute Resizing Supported?
     *
     * @return bool
     */
    public function getSupportForAttributeResizing(): bool
    {
        return true;
    }

    /**
     * Are batch operations supported?
     *
     * @return bool
     */
    public function getSupportForBatchOperations(): bool
    {
        return true;
    }

    /**
     * Is get connection id supported?
     *
     * @return bool
     */
    public function getSupportForGetConnectionId(): bool
    {
        return true;
    }

    /**
     * Is cache fallback supported?
     *
     * @return bool
     */
    public function getSupportForCacheSkipOnFailure(): bool
    {
        return true;
    }

    /**
     * Is hostname supported?
     *
     * @return bool
     */
    public function getSupportForHostname(): bool
    {
        return true;
    }

    /**
     * Get current attribute count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfAttributes(Document $collection): int
    {
        $attributes = \count($collection->getAttribute('attributes') ?? []);

        return $attributes + $this->getCountOfDefaultAttributes();
    }

    /**
     * Get current index count from collection document
     *
     * @param Document $collection
     * @return int
     */
    public function getCountOfIndexes(Document $collection): int
    {
        $indexes = \count($collection->getAttribute('indexes') ?? []);
        return $indexes + $this->getCountOfDefaultIndexes();
    }

    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    public function getCountOfDefaultAttributes(): int
    {
        return \count(Database::INTERNAL_ATTRIBUTES);
    }

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    public function getCountOfDefaultIndexes(): int
    {
        return \count(Database::INTERNAL_INDEXES);
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    public function getDocumentSizeLimit(): int
    {
        return 65535;
    }

    /**
     * Estimate maximum number of bytes required to store a document in $collection.
     * Byte requirement varies based on column type and size.
     * Needed to satisfy MariaDB/MySQL row width limit.
     *
     * @param Document $collection
     * @return int
     * @throws DatabaseException
     */
    public function getAttributeWidth(Document $collection): int
    {
        /**
         * @link https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
         *
         * `_id` bigint => 8 bytes
         * `_uid` varchar(255) => 1021 (4 * 255 + 1) bytes
         * `_tenant` int => 4 bytes
         * `_createdAt` datetime(3) => 7 bytes
         * `_updatedAt` datetime(3) => 7 bytes
         * `_permissions` mediumtext => 20
         */

        $total = 1067;

        $attributes = $collection->getAttributes()['attributes'];

        foreach ($attributes as $attribute) {
            /**
             * Json / Longtext
             * only the pointer contributes 20 bytes
             * data is stored externally
             */

            if ($attribute['array'] ?? false) {
                $total += 20;
                continue;
            }

            switch ($attribute['type']) {
                case Database::VAR_ID:
                    $total += 8; //  BIGINT 8 bytes
                    break;

                case Database::VAR_STRING:
                    /**
                     * Text / Mediumtext / Longtext
                     * only the pointer contributes 20 bytes to the row size
                     * data is stored externally
                     */

                    $total += match (true) {
                        $attribute['size'] > $this->getMaxVarcharLength() => 20,
                        $attribute['size'] > 255 => $attribute['size'] * 4 + 2, //  VARCHAR(>255) + 2 length
                        default => $attribute['size'] * 4 + 1, //  VARCHAR(<=255) + 1 length
                    };

                    break;

                case Database::VAR_INTEGER:
                    if ($attribute['size'] >= 8) {
                        $total += 8; //  BIGINT 8 bytes
                    } else {
                        $total += 4; // INT 4 bytes
                    }
                    break;

                case Database::VAR_FLOAT:
                    $total += 8; // DOUBLE 8 bytes
                    break;

                case Database::VAR_BOOLEAN:
                    $total += 1; // TINYINT(1) 1 bytes
                    break;

                case Database::VAR_RELATIONSHIP:
                    $total += Database::LENGTH_KEY * 4 + 1; // VARCHAR(<=255)
                    break;

                case Database::VAR_DATETIME:
                    /**
                     * 1 byte year + month
                     * 1 byte for the day
                     * 3 bytes for the hour, minute, and second
                     * 2 bytes miliseconds DATETIME(3)
                     */
                    $total += 7;
                    break;
                default:
                    throw new DatabaseException('Unknown type: ' . $attribute['type']);
            }
        }

        return $total;
    }

    /**
     * Get list of keywords that cannot be used
     *  Refference: https://mariadb.com/kb/en/reserved-words/
     *
     * @return array<string>
     */
    public function getKeywords(): array
    {
        return [
            'ACCESSIBLE',
            'ADD',
            'ALL',
            'ALTER',
            'ANALYZE',
            'AND',
            'AS',
            'ASC',
            'ASENSITIVE',
            'BEFORE',
            'BETWEEN',
            'BIGINT',
            'BINARY',
            'BLOB',
            'BOTH',
            'BY',
            'CALL',
            'CASCADE',
            'CASE',
            'CHANGE',
            'CHAR',
            'CHARACTER',
            'CHECK',
            'COLLATE',
            'COLUMN',
            'CONDITION',
            'CONSTRAINT',
            'CONTINUE',
            'CONVERT',
            'CREATE',
            'CROSS',
            'CURRENT_DATE',
            'CURRENT_ROLE',
            'CURRENT_TIME',
            'CURRENT_TIMESTAMP',
            'CURRENT_USER',
            'CURSOR',
            'DATABASE',
            'DATABASES',
            'DAY_HOUR',
            'DAY_MICROSECOND',
            'DAY_MINUTE',
            'DAY_SECOND',
            'DEC',
            'DECIMAL',
            'DECLARE',
            'DEFAULT',
            'DELAYED',
            'DELETE',
            'DELETE_DOMAIN_ID',
            'DESC',
            'DESCRIBE',
            'DETERMINISTIC',
            'DISTINCT',
            'DISTINCTROW',
            'DIV',
            'DO_DOMAIN_IDS',
            'DOUBLE',
            'DROP',
            'DUAL',
            'EACH',
            'ELSE',
            'ELSEIF',
            'ENCLOSED',
            'ESCAPED',
            'EXCEPT',
            'EXISTS',
            'EXIT',
            'EXPLAIN',
            'FALSE',
            'FETCH',
            'FLOAT',
            'FLOAT4',
            'FLOAT8',
            'FOR',
            'FORCE',
            'FOREIGN',
            'FROM',
            'FULLTEXT',
            'GENERAL',
            'GRANT',
            'GROUP',
            'HAVING',
            'HIGH_PRIORITY',
            'HOUR_MICROSECOND',
            'HOUR_MINUTE',
            'HOUR_SECOND',
            'IF',
            'IGNORE',
            'IGNORE_DOMAIN_IDS',
            'IGNORE_SERVER_IDS',
            'IN',
            'INDEX',
            'INFILE',
            'INNER',
            'INOUT',
            'INSENSITIVE',
            'INSERT',
            'INT',
            'INT1',
            'INT2',
            'INT3',
            'INT4',
            'INT8',
            'INTEGER',
            'INTERSECT',
            'INTERVAL',
            'INTO',
            'IS',
            'ITERATE',
            'JOIN',
            'KEY',
            'KEYS',
            'KILL',
            'LEADING',
            'LEAVE',
            'LEFT',
            'LIKE',
            'LIMIT',
            'LINEAR',
            'LINES',
            'LOAD',
            'LOCALTIME',
            'LOCALTIMESTAMP',
            'LOCK',
            'LONG',
            'LONGBLOB',
            'LONGTEXT',
            'LOOP',
            'LOW_PRIORITY',
            'MASTER_HEARTBEAT_PERIOD',
            'MASTER_SSL_VERIFY_SERVER_CERT',
            'MATCH',
            'MAXVALUE',
            'MEDIUMBLOB',
            'MEDIUMINT',
            'MEDIUMTEXT',
            'MIDDLEINT',
            'MINUTE_MICROSECOND',
            'MINUTE_SECOND',
            'MOD',
            'MODIFIES',
            'NATURAL',
            'NOT',
            'NO_WRITE_TO_BINLOG',
            'NULL',
            'NUMERIC',
            'OFFSET',
            'ON',
            'OPTIMIZE',
            'OPTION',
            'OPTIONALLY',
            'OR',
            'ORDER',
            'OUT',
            'OUTER',
            'OUTFILE',
            'OVER',
            'PAGE_CHECKSUM',
            'PARSE_VCOL_EXPR',
            'PARTITION',
            'POSITION',
            'PRECISION',
            'PRIMARY',
            'PROCEDURE',
            'PURGE',
            'RANGE',
            'READ',
            'READS',
            'READ_WRITE',
            'REAL',
            'RECURSIVE',
            'REF_SYSTEM_ID',
            'REFERENCES',
            'REGEXP',
            'RELEASE',
            'RENAME',
            'REPEAT',
            'REPLACE',
            'REQUIRE',
            'RESIGNAL',
            'RESTRICT',
            'RETURN',
            'RETURNING',
            'REVOKE',
            'RIGHT',
            'RLIKE',
            'ROWS',
            'SCHEMA',
            'SCHEMAS',
            'SECOND_MICROSECOND',
            'SELECT',
            'SENSITIVE',
            'SEPARATOR',
            'SET',
            'SHOW',
            'SIGNAL',
            'SLOW',
            'SMALLINT',
            'SPATIAL',
            'SPECIFIC',
            'SQL',
            'SQLEXCEPTION',
            'SQLSTATE',
            'SQLWARNING',
            'SQL_BIG_RESULT',
            'SQL_CALC_FOUND_ROWS',
            'SQL_SMALL_RESULT',
            'SSL',
            'STARTING',
            'STATS_AUTO_RECALC',
            'STATS_PERSISTENT',
            'STATS_SAMPLE_PAGES',
            'STRAIGHT_JOIN',
            'TABLE',
            'TERMINATED',
            'THEN',
            'TINYBLOB',
            'TINYINT',
            'TINYTEXT',
            'TO',
            'TRAILING',
            'TRIGGER',
            'TRUE',
            'UNDO',
            'UNION',
            'UNIQUE',
            'UNLOCK',
            'UNSIGNED',
            'UPDATE',
            'USAGE',
            'USE',
            'USING',
            'UTC_DATE',
            'UTC_TIME',
            'UTC_TIMESTAMP',
            'VALUES',
            'VARBINARY',
            'VARCHAR',
            'VARCHARACTER',
            'VARYING',
            'WHEN',
            'WHERE',
            'WHILE',
            'WINDOW',
            'WITH',
            'WRITE',
            'XOR',
            'YEAR_MONTH',
            'ZEROFILL',
            'ACTION',
            'BIT',
            'DATE',
            'ENUM',
            'NO',
            'TEXT',
            'TIME',
            'TIMESTAMP',
            'BODY',
            'ELSIF',
            'GOTO',
            'HISTORY',
            'MINUS',
            'OTHERS',
            'PACKAGE',
            'PERIOD',
            'RAISE',
            'ROWNUM',
            'ROWTYPE',
            'SYSDATE',
            'SYSTEM',
            'SYSTEM_TIME',
            'VERSIONING',
            'WITHOUT'
        ];
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

    public function getSupportForNumericCasting(): bool
    {
        return false;
    }


    /**
     * Does the adapter handle Query Array Contains?
     *
     * @return bool
     */
    public function getSupportForQueryContains(): bool
    {
        return true;
    }

    /**
     * Does the adapter handle array Overlaps?
     *
     * @return bool
     */
    abstract public function getSupportForJSONOverlaps(): bool;

    public function getSupportForIndexArray(): bool
    {
        return true;
    }

    public function getSupportForCastIndexArray(): bool
    {
        return false;
    }

    public function getSupportForRelationships(): bool
    {
        return true;
    }

    public function getSupportForReconnection(): bool
    {
        return true;
    }

    public function getSupportForBatchCreateAttributes(): bool
    {
        return true;
    }

    /**
     * @param string $value
     * @return string
     */
    protected function getFulltextValue(string $value): string
    {
        $exact = str_ends_with($value, '"') && str_starts_with($value, '"');

        /** Replace reserved chars with space. */
        $specialChars = '@,+,-,*,),(,<,>,~,"';
        $value = str_replace(explode(',', $specialChars), ' ', $value);
        $value = preg_replace('/\s+/', ' ', $value); // Remove multiple whitespaces
        $value = trim($value);

        if (empty($value)) {
            return '';
        }

        if ($exact) {
            $value = '"' . $value . '"';
        } else {
            /** Prepend wildcard by default on the back. */
            $value .= '*';
        }

        return $value;
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
            case Query::TYPE_NOT_EQUAL:
                return '!=';
            case Query::TYPE_LESSER:
                return '<';
            case Query::TYPE_LESSER_EQUAL:
                return '<=';
            case Query::TYPE_GREATER:
                return '>';
            case Query::TYPE_GREATER_EQUAL:
                return '>=';
            case Query::TYPE_IS_NULL:
                return 'IS NULL';
            case Query::TYPE_IS_NOT_NULL:
                return 'IS NOT NULL';
            case Query::TYPE_STARTS_WITH:
            case Query::TYPE_ENDS_WITH:
            case Query::TYPE_CONTAINS:
                return $this->getLikeOperator();
            default:
                throw new DatabaseException('Unknown method: ' . $method);
        }
    }

    abstract protected function getSQLType(
        string $type,
        int $size,
        bool $signed = true,
        bool $array = false
    ): string;

    /**
     * Get SQL Index Type
     *
     * @param string $type
     * @return string
     * @throws Exception
     */
    protected function getSQLIndexType(string $type): string
    {
        return match ($type) {
            Database::INDEX_KEY => 'INDEX',
            Database::INDEX_UNIQUE => 'UNIQUE INDEX',
            Database::INDEX_FULLTEXT => 'FULLTEXT INDEX',
            default => throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_FULLTEXT),
        };
    }

    /**
     * Get SQL condition for permissions
     *
     * @param string $collection
     * @param array<string> $roles
     * @param string $alias
     * @param string $type
     * @return string
     * @throws DatabaseException
     */
    protected function getSQLPermissionsCondition(
        string $collection,
        array $roles,
        string $alias,
        string $type = Database::PERMISSION_READ
    ): string {
        if (!\in_array($type, Database::PERMISSIONS)) {
            throw new DatabaseException('Unknown permission type: ' . $type);
        }

        $roles = \array_map(fn ($role) => $this->getPDO()->quote($role), $roles);
        $roles = \implode(', ', $roles);

        return "{$this->quote($alias)}.{$this->quote('_uid')} IN (
            SELECT _document
            FROM {$this->getSQLTable($collection . '_perms')}
            WHERE _permission IN ({$roles})
              AND _type = '{$type}'
              {$this->getTenantQuery($collection)}
        )";
    }

    /**
     * Get SQL table
     *
     * @param string $name
     * @return string
     * @throws DatabaseException
     */
    protected function getSQLTable(string $name): string
    {
        return "{$this->quote($this->getDatabase())}.{$this->quote($this->getNamespace().'_'.$this->filter($name))}";
    }

    /**
     * Returns the current PDO object
     * @return mixed
     */
    protected function getPDO(): mixed
    {
        return $this->pdo;
    }

    /**
     * Get PDO Type
     *
     * @param mixed $value
     * @return int
     * @throws Exception
     */
    abstract protected function getPDOType(mixed $value): int;

    /**
     * Returns default PDO configuration
     *
     * @return array<int, mixed>
     */
    public static function getPDOAttributes(): array
    {
        return [
            \PDO::ATTR_TIMEOUT => 3, // Specifies the timeout duration in seconds. Takes a value of type int.
            \PDO::ATTR_PERSISTENT => true, // Create a persistent connection
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC, // Fetch a result row as an associative array.
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION, // PDO will throw a PDOException on errors
            \PDO::ATTR_EMULATE_PREPARES => true, // Emulate prepared statements
            \PDO::ATTR_STRINGIFY_FETCHES => true // Returns all fetched data as Strings
        ];
    }

    public function getHostname(): string
    {
        try {
            return $this->pdo->getHostname();
        } catch (\Throwable) {
            return '';
        }
    }

    /**
     * @return int
     */
    public function getMaxVarcharLength(): int
    {
        return 16381; // Floor value for Postgres:16383 | MySQL:16381 | MariaDB:16382
    }

    /**
     * @return string
     */
    public function getIdAttributeType(): string
    {
        return Database::VAR_ID_INT;
    }

    /**
     * @return int
     */
    public function getMaxIndexLength(): int
    {
        /**
         * $tenant int = 1
         */
        return $this->sharedTables ? 767 : 768;
    }

    /**
     * @param Query $query
     * @param array<string, mixed> $binds
     * @return string
     * @throws Exception
     */
    abstract protected function getSQLCondition(Query $query, array &$binds): string;

    /**
     * @param array<Query> $queries
     * @param array<string, mixed> $binds
     * @param string $separator
     * @return string
     * @throws Exception
     */
    public function getSQLConditions(array $queries, array &$binds, string $separator = 'AND'): string
    {
        $conditions = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                continue;
            }

            if ($query->isNested()) {
                $conditions[] = $this->getSQLConditions($query->getValues(), $binds, $query->getMethod());
            } else {
                $conditions[] = $this->getSQLCondition($query, $binds);
            }
        }

        $tmp = implode(' ' . $separator . ' ', $conditions);
        return empty($tmp) ? '' : '(' . $tmp . ')';
    }

    /**
     * @return string
     */
    public function getLikeOperator(): string
    {
        return 'LIKE';
    }

    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    public function getSchemaAttributes(string $collection): array
    {
        return [];
    }

    public function getTenantQuery(
        string $collection,
        string $alias = '',
        int $tenantCount = 0,
        string $condition = 'AND'
    ): string {
        if (!$this->sharedTables) {
            return '';
        }

        $dot = '';
        if ($alias !== '') {
            $dot = '.';
            $alias = $this->quote($alias);
        }

        $bindings = [];
        if ($tenantCount === 0) {
            $bindings[] = ':_tenant';
        } else {
            for ($index = 0; $index < $tenantCount; $index++) {
                $bindings[] = ":_tenant_{$index}";
            }
        }
        $bindings = \implode(',', $bindings);

        $orIsNull = '';
        if ($collection === Database::METADATA) {
            $orIsNull = " OR {$alias}{$dot}_tenant IS NULL";
        }

        return "{$condition} ({$alias}{$dot}_tenant IN ({$bindings}) {$orIsNull})";
    }

    /**
     * Get the SQL projection given the selected attributes
     *
     * @param array<string> $selections
     * @param string $prefix
     * @return mixed
     * @throws Exception
     */
    protected function getAttributeProjection(array $selections, string $prefix): mixed
    {
        if (empty($selections) || \in_array('*', $selections)) {
            return "{$this->quote($prefix)}.*";
        }

        $internalKeys = [
            '$id',
            '$sequence',
            '$permissions',
            '$createdAt',
            '$updatedAt',
        ];

        $selections = \array_diff($selections, [...$internalKeys, '$collection']);

        foreach ($internalKeys as $internalKey) {
            $selections[] = $this->getInternalKeyForAttribute($internalKey);
        }

        foreach ($selections as &$selection) {
            $selection = "{$this->quote($prefix)}.{$this->quote($this->filter($selection))}";
        }

        return \implode(',', $selections);
    }

    protected function getInternalKeyForAttribute(string $attribute): string
    {
        return match ($attribute) {
            '$id' => '_uid',
            '$sequence' => '_id',
            '$collection' => '_collection',
            '$tenant' => '_tenant',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            '$permissions' => '_permissions',
            default => $attribute
        };
    }

    protected function escapeWildcards(string $value): string
    {
        $wildcards = ['%', '_', '[', ']', '^', '-', '.', '*', '+', '?', '(', ')', '{', '}', '|'];

        foreach ($wildcards as $wildcard) {
            $value = \str_replace($wildcard, "\\$wildcard", $value);
        }

        return $value;
    }

    protected function processException(PDOException $e): \Exception
    {
        return $e;
    }

    /**
     * @param mixed $stmt
     * @return bool
     */
    protected function execute(mixed $stmt): bool
    {
        return $stmt->execute();
    }

    /**
     * Create Documents in batches
     *
     * @param string $collection
     * @param array<Document> $documents
     *
     * @return array<Document>
     *
     * @throws DuplicateException
     * @throws \Throwable
     */
    public function createDocuments(string $collection, array $documents): array
    {
        if (empty($documents)) {
            return $documents;
        }

        try {
            $name = $this->filter($collection);

            $attributeKeys = Database::INTERNAL_ATTRIBUTE_KEYS;

            $hasSequence = null;
            foreach ($documents as $document) {
                $attributes = $document->getAttributes();
                $attributeKeys = [...$attributeKeys, ...\array_keys($attributes)];

                if ($hasSequence === null) {
                    $hasSequence = !empty($document->getSequence());
                } elseif ($hasSequence == empty($document->getSequence())) {
                    throw new DatabaseException('All documents must have an sequence if one is set');
                }
            }

            $attributeKeys = array_unique($attributeKeys);

            if ($hasSequence) {
                $attributeKeys[] = '_id';
            }

            if ($this->sharedTables) {
                $attributeKeys[] = '_tenant';
            }

            $columns = [];
            foreach ($attributeKeys as $key => $attribute) {
                $columns[$key] = $this->quote($this->filter($attribute));
            }

            $columns = '(' . \implode(', ', $columns) . ')';

            $bindIndex = 0;
            $batchKeys = [];
            $bindValues = [];
            $permissions = [];

            foreach ($documents as $index => $document) {
                $attributes = $document->getAttributes();
                $attributes['_uid'] = $document->getId();
                $attributes['_createdAt'] = $document->getCreatedAt();
                $attributes['_updatedAt'] = $document->getUpdatedAt();
                $attributes['_permissions'] = \json_encode($document->getPermissions());

                if (!empty($document->getSequence())) {
                    $attributes['_id'] = $document->getSequence();
                }

                if ($this->sharedTables) {
                    $attributes['_tenant'] = $document->getTenant();
                }

                $bindKeys = [];

                foreach ($attributeKeys as $key) {
                    $value = $attributes[$key] ?? null;
                    if (\is_array($value)) {
                        $value = \json_encode($value);
                    }
                    $value = (\is_bool($value)) ? (int)$value : $value;
                    $bindKey = 'key_' . $bindIndex;
                    $bindKeys[] = ':' . $bindKey;
                    $bindValues[$bindKey] = $value;
                    $bindIndex++;
                }

                $batchKeys[] = '(' . \implode(', ', $bindKeys) . ')';

                foreach (Database::PERMISSIONS as $type) {
                    foreach ($document->getPermissionsByType($type) as $permission) {
                        $tenantBind = $this->sharedTables ? ", :_tenant_{$index}" : '';
                        $permission = \str_replace('"', '', $permission);
                        $permission = "('{$type}', '{$permission}', :_uid_{$index} {$tenantBind})";
                        $permissions[] = $permission;
                    }
                }
            }

            $batchKeys = \implode(', ', $batchKeys);

            $stmt = $this->getPDO()->prepare("
                INSERT INTO {$this->getSQLTable($name)} {$columns}
                VALUES {$batchKeys}
            ");

            foreach ($bindValues as $key => $value) {
                $stmt->bindValue($key, $value, $this->getPDOType($value));
            }

            $this->execute($stmt);

            if (!empty($permissions)) {
                $tenantColumn = $this->sharedTables ? ', _tenant' : '';
                $permissions = \implode(', ', $permissions);

                $sqlPermissions = "
                    INSERT INTO {$this->getSQLTable($name . '_perms')} (_type, _permission, _document {$tenantColumn})
                    VALUES {$permissions};
                ";

                $stmtPermissions = $this->getPDO()->prepare($sqlPermissions);

                foreach ($documents as $index => $document) {
                    $stmtPermissions->bindValue(":_uid_{$index}", $document->getId());
                    if ($this->sharedTables) {
                        $stmtPermissions->bindValue(":_tenant_{$index}", $document->getTenant());
                    }
                }

                $this->execute($stmtPermissions);
            }

        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $documents;
    }
}
