<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Throwable;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class MariaDB extends SQL
{
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

        if ($this->exists($name)) {
            return true;
        }

        $sql = "CREATE DATABASE `{$name}` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;";

        $sql = $this->trigger(Database::EVENT_DATABASE_CREATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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

        $sql = "DROP DATABASE `{$name}`;";

        $sql = $this->trigger(Database::EVENT_DATABASE_DELETE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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
        $database = $this->getDefaultDatabase();
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        /** @var array<string> $attributeStrings */
        $attributeStrings = [];

        /** @var array<string> $indexStrings */
        $indexStrings = [];

        foreach ($attributes as $key => $attribute) {
            $attrId = $this->filter($attribute->getId());
            $attrType = $this->getSQLType($attribute->getAttribute('type'), $attribute->getAttribute('size', 0), $attribute->getAttribute('signed', true));

            if ($attribute->getAttribute('array')) {
                $attrType = 'LONGTEXT';
            }

            $attributeStrings[$key] = "`{$attrId}` {$attrType}, ";
        }

        foreach ($indexes as $key => $index) {
            $indexId = $this->filter($index->getId());
            $indexType = $index->getAttribute('type');

            $indexAttributes = $index->getAttribute('attributes');
            foreach ($indexAttributes as $nested => $attribute) {
                $indexLength = $index->getAttribute('lengths')[$nested] ?? '';
                $indexLength = (empty($indexLength)) ? '' : '(' . (int)$indexLength . ')';
                $indexOrder = $index->getAttribute('orders')[$nested] ?? '';
                $indexAttribute = $this->filter($attribute);

                if ($indexType === Database::INDEX_FULLTEXT) {
                    $indexOrder = '';
                }

                $indexAttributes[$nested] = "`{$indexAttribute}`{$indexLength} {$indexOrder}";
            }

            $indexStrings[$key] = "{$indexType} `{$indexId}` (" . \implode(", ", $indexAttributes) . " ),";
        }

        $sql = "
			CREATE TABLE IF NOT EXISTS `{$database}`.`{$namespace}_{$id}` (
				`_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
				`_uid` VARCHAR(255) NOT NULL,
				`_createdAt` datetime(3) DEFAULT NULL,
				`_updatedAt` datetime(3) DEFAULT NULL,
				`_permissions` MEDIUMTEXT DEFAULT NULL,
				" . \implode(' ', $attributeStrings) . "
				PRIMARY KEY (`_id`),
				" . \implode(' ', $indexStrings) . "
				UNIQUE KEY `_uid` (`_uid`),
				KEY `_created_at` (`_createdAt`),
				KEY `_updated_at` (`_updatedAt`)
			)
		";

        $sql = $this->trigger(Database::EVENT_COLLECTION_CREATE, $sql);

        try {
            $this->getPDO()
                ->prepare($sql)
                ->execute();

            $sql = "
				CREATE TABLE IF NOT EXISTS `{$database}`.`{$namespace}_{$id}_perms` (
					`_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
					`_type` VARCHAR(12) NOT NULL,
					`_permission` VARCHAR(255) NOT NULL,
					`_document` VARCHAR(255) NOT NULL,
					PRIMARY KEY (`_id`),
					UNIQUE INDEX `_index1` (`_document`,`_type`,`_permission`),
					INDEX `_permission` (`_permission`,`_type`,`_document`)
				)
			";

            $sql = $this->trigger(Database::EVENT_COLLECTION_CREATE, $sql);

            $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (\Exception $th) {
            $this->getPDO()
                ->prepare("DROP TABLE IF EXISTS {$this->getSQLTable($id)}, {$this->getSQLTable($id . '_perms')};")
                ->execute();
            throw $th;
        }

        return true;
    }

    /**
     * Get Collection Size
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->filter($collection);
        $collection = $this->getNamespace() . '_' . $collection;
        $database = $this->getDefaultDatabase();
        $name = $database . '/' . $collection;
        $permissions = $database . '/' . $collection . '_perms';

        $collectionSize = $this->getPDO()->prepare("
            SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
            FROM INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES
            WHERE NAME = :name
         ");

        $permissionsSize = $this->getPDO()->prepare("
            SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
            FROM INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES
            WHERE NAME = :permissions
        ");

        $collectionSize->bindParam(':name', $name);
        $permissionsSize->bindParam(':permissions', $permissions);

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
     * Delete Collection
     * @param string $id
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);

        $sql = "DROP TABLE {$this->getSQLTable($id)}, {$this->getSQLTable($id . '_perms')};";

        $sql = $this->trigger(Database::EVENT_COLLECTION_DELETE, $sql);

        return $this->getPDO()
            ->prepare($sql)
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

        $sql = "ALTER TABLE {$this->getSQLTable($name)} ADD COLUMN `{$id}` {$type};";
        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
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
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $type = $this->getSQLType($type, $size, $signed);

        if ($array) {
            $type = 'LONGTEXT';
        }

        $sql = "ALTER TABLE {$this->getSQLTable($name)} MODIFY `{$id}` {$type};";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
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

        $sql = "ALTER TABLE {$this->getSQLTable($name)} DROP COLUMN `{$id}`;";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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

        $sql = "ALTER TABLE {$this->getSQLTable($collection)} RENAME COLUMN `{$old}` TO `{$new}`;";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
    }

    /**
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param string $relatedCollection
     * @param bool $twoWay
     * @param string $twoWayKey
     * @return bool
     * @throws Exception
     */
    public function createRelationship(
        string $collection,
        string $relatedCollection,
        string $type,
        bool $twoWay = false,
        string $id = '',
        string $twoWayKey = ''
    ): bool {
        $name = $this->filter($collection);
        $relatedName = $this->filter($relatedCollection);
        $table = $this->getSQLTable($name);
        $relatedTable = $this->getSQLTable($relatedName);
        $id = $this->filter($id);
        $twoWayKey = $this->filter($twoWayKey);
        $sqlType = $this->getSQLType(Database::VAR_RELATIONSHIP, 0, false);

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                $sql = "ALTER TABLE {$table} ADD COLUMN `{$id}` {$sqlType} DEFAULT NULL;";

                if ($twoWay) {
                    $sql .= "ALTER TABLE {$relatedTable} ADD COLUMN `{$twoWayKey}` {$sqlType} DEFAULT NULL;";
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                $sql = "ALTER TABLE {$relatedTable} ADD COLUMN `{$twoWayKey}` {$sqlType} DEFAULT NULL;";
                break;
            case Database::RELATION_MANY_TO_ONE:
                $sql = "ALTER TABLE {$table} ADD COLUMN `{$id}` {$sqlType} DEFAULT NULL;";
                break;
            case Database::RELATION_MANY_TO_MANY:
                return true;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
    }

    /**
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $key
     * @param string $twoWayKey
     * @param string|null $newKey
     * @param string|null $newTwoWayKey
     * @return bool
     * @throws Exception
     */
    public function updateRelationship(
        string $collection,
        string $relatedCollection,
        string $type,
        bool $twoWay,
        string $key,
        string $twoWayKey,
        ?string $newKey = null,
        ?string $newTwoWayKey = null,
    ): bool {
        $name = $this->filter($collection);
        $relatedName = $this->filter($relatedCollection);
        $table = $this->getSQLTable($name);
        $relatedTable = $this->getSQLTable($relatedName);
        $key = $this->filter($key);
        $twoWayKey = $this->filter($twoWayKey);

        if (!\is_null($newKey)) {
            $newKey = $this->filter($newKey);
        }
        if (!\is_null($newTwoWayKey)) {
            $newTwoWayKey = $this->filter($newTwoWayKey);
        }

        $sql = '';

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                if (!\is_null($newKey)) {
                    $sql = "ALTER TABLE {$table} RENAME COLUMN `{$key}` TO `{$newKey}`;";
                }
                if ($twoWay && !\is_null($newTwoWayKey)) {
                    $sql .= "ALTER TABLE {$relatedTable} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`;";
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($twoWay && !\is_null($newTwoWayKey)) {
                    $sql = "ALTER TABLE {$relatedTable} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`;";
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if (!\is_null($newKey)) {
                    $sql = "ALTER TABLE {$table} RENAME COLUMN `{$key}` TO `{$newKey}`;";
                }
                break;
            case Database::RELATION_MANY_TO_MANY:
                $collection = $this->getDocument(Database::METADATA, $collection);
                $relatedCollection = $this->getDocument(Database::METADATA, $relatedCollection);

                $junction = $this->getSQLTable('_' . $collection->getInternalId() . '_' . $relatedCollection->getInternalId());

                if (!\is_null($newKey)) {
                    $sql = "ALTER TABLE {$junction} RENAME COLUMN `{$key}` TO `{$newKey}`;";
                }
                if ($twoWay && !\is_null($newTwoWayKey)) {
                    $sql .= "ALTER TABLE {$junction} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`;";
                }
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        if (empty($sql)) {
            return true;
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
    }

    public function deleteRelationship(
        string $collection,
        string $relatedCollection,
        string $type,
        bool $twoWay,
        string $key,
        string $twoWayKey,
        string $side
    ): bool {
        $name = $this->filter($collection);
        $relatedName = $this->filter($relatedCollection);
        $table = $this->getSQLTable($name);
        $relatedTable = $this->getSQLTable($relatedName);
        $key = $this->filter($key);
        $twoWayKey = $this->filter($twoWayKey);

        $sql = '';

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                $sql = "ALTER TABLE {$table} DROP COLUMN `{$key}`;";
                if ($twoWay) {
                    $sql .= "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`;";
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $sql = "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`;";
                } elseif ($twoWay) {
                    $sql = "ALTER TABLE {$table} DROP COLUMN `{$key}`;";
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($twoWay && $side === Database::RELATION_SIDE_CHILD) {
                    $sql = "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`;";
                } else {
                    $sql = "ALTER TABLE {$table} DROP COLUMN `{$key}`;";
                }
                break;
            case Database::RELATION_MANY_TO_MANY:
                $collection = $this->getDocument(Database::METADATA, $collection);
                $relatedCollection = $this->getDocument(Database::METADATA, $relatedCollection);

                $junction = $side === Database::RELATION_SIDE_PARENT
                    ? $this->getSQLTable('_' . $collection->getInternalId() . '_' . $relatedCollection->getInternalId())
                    : $this->getSQLTable('_' . $relatedCollection->getInternalId() . '_' . $collection->getInternalId());

                $perms = $side === Database::RELATION_SIDE_PARENT
                    ? $this->getSQLTable('_' . $collection->getInternalId() . '_' . $relatedCollection->getInternalId() . '_perms')
                    : $this->getSQLTable('_' . $relatedCollection->getInternalId() . '_' . $collection->getInternalId() . '_perms');

                $sql = "DROP TABLE {$junction}; DROP TABLE {$perms}";
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        if (empty($sql)) {
            return true;
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $sql);

        return $this->getPDO()
            ->prepare($sql)
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
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $old = $this->filter($old);
        $new = $this->filter($new);

        $sql = "ALTER TABLE {$this->getSQLTable($collection)} RENAME INDEX `{$old}` TO `{$new}`;";

        $sql = $this->trigger(Database::EVENT_INDEX_RENAME, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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

        $attributes = \array_map(fn ($attribute) => match ($attribute) {
            '$id' => '_uid',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $attribute
        }, $attributes);

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

        $sql = "ALTER TABLE {$this->getSQLTable($name)} DROP INDEX `{$id}`;";

        $sql = $this->trigger(Database::EVENT_INDEX_DELETE, $sql);

        return $this->getPDO()
            ->prepare($sql)
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
     * @throws DuplicateException
     * @throws Throwable
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $attributes = $document->getAttributes();
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = json_encode($document->getPermissions());

        $name = $this->filter($collection);
        $columns = '';
        $columnNames = '';

        try {
            if (!$this->getPDO()->beginTransaction()) {
                throw new Exception('Failed to begin transaction');
            }

            /**
             * Insert Attributes
             */
            $bindIndex = 0;
            foreach ($attributes as $attribute => $value) { // Parse statement
                $column = $this->filter($attribute);
                $bindKey = 'key_' . $bindIndex;
                $columns .= "`{$column}`, ";
                $columnNames .= ':' . $bindKey . ', ';
                $bindIndex++;
            }

            // Insert manual id if set
            if (!empty($document->getInternalId())) {
                $bindKey = '_id';
                $columns .= "_id, ";
                $columnNames .= ':' . $bindKey . ', ';
            }

            $sql = "
                INSERT INTO {$this->getSQLTable($name)}({$columns} _uid)
                VALUES ({$columnNames} :_uid)
            ";

            $sql = $this->trigger(Database::EVENT_DOCUMENT_CREATE, $sql);

            $stmt = $this->getPDO()->prepare($sql);

            $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

            // Bind manual internal id if set
            if (!empty($document->getInternalId())) {
                $stmt->bindValue(':_id', $document->getInternalId(), PDO::PARAM_STR);
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

            $permissions = [];
            foreach (Database::PERMISSIONS as $type) {
                foreach ($document->getPermissionsByType($type) as $permission) {
                    $permission = \str_replace('"', '', $permission);
                    $permissions[] = "('{$type}', '{$permission}', '{$document->getId()}')";
                }
            }

            if (!empty($permissions)) {
                $strPermissions = \implode(', ', $permissions);

                $sqlPermissions = "
                    INSERT INTO {$this->getSQLTable($name . '_perms')} (_type, _permission, _document) 
                    VALUES {$strPermissions}
                ";
                $sqlPermissions = $this->trigger(Database::EVENT_PERMISSIONS_CREATE, $sqlPermissions);
                $stmtPermissions = $this->getPDO()->prepare($sqlPermissions);
            }

            $stmt->execute();

            if(empty($document->getInternalId())) {
                $statement = $this->getPDO()->prepare("select last_insert_id() as id");
                $statement->execute();
                $last = $statement->fetch();
                $document['$internalId'] = $last['id'];
            }

            if (isset($stmtPermissions)) {
                $stmtPermissions->execute();
            }

            if (!$this->getPDO()->commit()) {
                throw new DatabaseException('Failed to commit transaction');
            }
        } catch (Throwable $th) {
            if (!$this->getPDO()->rollBack()) {
                throw new DatabaseException('Failed to rollBack transaction');
            }
            throw match ($th->getCode()) {
                1062, 23000 => new DuplicateException('Duplicated document: ' . $th->getMessage()),
                default => $th,
            };
        }

        return $document;
    }

    /**
     * Create Documents in batches
     *
     * @param string $collection
     * @param array<Document> $documents
     * @param int $batchSize
     *
     * @return array<Document>
     *
     * @throws DuplicateException
     * @throws Exception
     * @throws Throwable
     */
    public function createDocuments(string $collection, array $documents, int $batchSize = Database::INSERT_BATCH_SIZE): array
    {
        if (empty($documents)) {
            return $documents;
        }

        try {
            if (!$this->getPDO()->beginTransaction()) {
                throw new Exception('Failed to begin transaction');
            }

            $name = $this->filter($collection);
            $batches = \array_chunk($documents, max(1, $batchSize));

            foreach ($batches as $batch) {
                $bindIndex = 0;
                $batchKeys = [];
                $bindValues = [];
                $permissions = [];

                foreach ($batch as $document) {
                    $attributes = $document->getAttributes();
                    $attributes['_uid'] = $document->getId();
                    $attributes['_createdAt'] = $document->getCreatedAt();
                    $attributes['_updatedAt'] = $document->getUpdatedAt();
                    $attributes['_permissions'] = \json_encode($document->getPermissions());

                    $columns = [];
                    foreach (\array_keys($attributes) as $key => $attribute) {
                        $columns[$key] = "`{$this->filter($attribute)}`";
                    }

                    $columns = '(' . \implode(', ', $columns) . ')';

                    $bindKeys = [];

                    foreach ($attributes as $value) {
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
                            $permission = \str_replace('"', '', $permission);
                            $permissions[] = "('{$type}', '{$permission}', '{$document->getId()}')";
                        }
                    }
                }

                $stmt = $this->getPDO()->prepare(
                    "
                    INSERT INTO {$this->getSQLTable($name)} {$columns}
                    VALUES " . \implode(', ', $batchKeys)
                );

                foreach ($bindValues as $key => $value) {
                    $stmt->bindValue($key, $value, $this->getPDOType($value));
                }

                $stmt->execute();

                if (!empty($permissions)) {
                    $stmtPermissions = $this->getPDO()->prepare(
                        "
                        INSERT INTO {$this->getSQLTable($name . '_perms')} (_type, _permission, _document) 
                        VALUES " . \implode(', ', $permissions)
                    );
                    $stmtPermissions?->execute();
                }
            }

            if (!$this->getPDO()->commit()) {
                throw new Exception('Failed to commit transaction');
            }

            return $documents;

        } catch (Throwable $th) {
            if (!$this->getPDO()->rollBack()) {
                throw new DatabaseException('Failed to rollBack transaction');
            }

            throw match ($th->getCode()) {
                1062, 23000 => new DuplicateException('Duplicated document: ' . $th->getMessage()),
                default => $th,
            };
        }
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param Document $document
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws DuplicateException
     * @throws Throwable
     */
    public function updateDocument(string $collection, Document $document): Document
    {
        $attributes = $document->getAttributes();
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = json_encode($document->getPermissions());

        $name = $this->filter($collection);
        $columns = '';

        try {
            if (!$this->getPDO()->beginTransaction()) {
                throw new Exception('Failed to begin transaction');
            }

            $sql = "
                SELECT _type, _permission
                FROM {$this->getSQLTable($name . '_perms')} p
                WHERE p._document = :_uid FOR UPDATE
            ";

            $sql = $this->trigger(Database::EVENT_PERMISSIONS_READ, $sql);

            /**
             * Get current permissions from the database
             */
            $sqlPermissions = $this->getPDO()->prepare($sql);
            $sqlPermissions->bindValue(':_uid', $document->getId());
            $sqlPermissions->execute();
            $permissions = $sqlPermissions->fetchAll();
            $sqlPermissions->closeCursor();

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
                $removeQuery = "
                    DELETE
                    FROM {$this->getSQLTable($name . '_perms')}
                    WHERE
                        _document = :_uid
                        {$removeQuery}
                ";

                $removeQuery = $this->trigger(Database::EVENT_PERMISSIONS_DELETE, $removeQuery);

                $stmtRemovePermissions = $this->getPDO()->prepare($removeQuery);
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

                $sql = "
                    INSERT INTO {$this->getSQLTable($name . '_perms')}
                    (_document, _type, _permission) VALUES " . \implode(', ', $values)
                ;

                $sql = $this->trigger(Database::EVENT_PERMISSIONS_CREATE, $sql);

                $stmtAddPermissions = $this->getPDO()->prepare($sql);

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

            $sql = "
                UPDATE {$this->getSQLTable($name)}
                SET {$columns} _uid = :_uid 
                WHERE _uid = :_uid
            ";

            $sql = $this->trigger(Database::EVENT_DOCUMENT_UPDATE, $sql);

            $stmt = $this->getPDO()->prepare($sql);

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

            $stmt->execute();

            if (isset($stmtRemovePermissions)) {
                $stmtRemovePermissions->execute();
            }
            if (isset($stmtAddPermissions)) {
                $stmtAddPermissions->execute();
            }

            if (!$this->getPDO()->commit()) {
                throw new DatabaseException('Failed to commit transaction');
            }
        } catch (Throwable $th) {
            if (!$this->getPDO()->rollBack()) {
                throw new DatabaseException('Failed to rollBack transaction');
            }
            throw match ($th->getCode()) {
                1062, 23000 => new DuplicateException('Duplicated document: ' . $th->getMessage()),
                default => $th,
            };
        }

        return $document;
    }

    /**
     * Update Documents in batches
     *
     * @param string $collection
     * @param array<Document> $documents
     * @param int $batchSize
     *
     * @return array<Document>
     *
     * @throws DuplicateException
     * @throws Exception
     * @throws Throwable
     */
    public function updateDocuments(string $collection, array $documents, int $batchSize = Database::INSERT_BATCH_SIZE): array
    {
        if (empty($documents)) {
            return $documents;
        }

        try {
            if (!$this->getPDO()->beginTransaction()) {
                throw new Exception('Failed to begin transaction');
            }

            $name = $this->filter($collection);
            $batches = \array_chunk($documents, max(1, $batchSize));

            foreach ($batches as $batch) {
                $bindIndex = 0;
                $batchKeys = [];
                $bindValues = [];

                $removeQuery = '';
                $removeBindValues = [];

                $addQuery = '';
                $addBindValues = [];

                foreach ($batch as $index => $document) {
                    $attributes = $document->getAttributes();
                    $attributes['_uid'] = $document->getId();
                    $attributes['_createdAt'] = $document->getCreatedAt();
                    $attributes['_updatedAt'] = $document->getUpdatedAt();
                    $attributes['_permissions'] = json_encode($document->getPermissions());

                    $columns = \array_map(function ($attribute) {
                        return "`" . $this->filter($attribute) . "`";
                    }, \array_keys($attributes));

                    $bindKeys = [];

                    foreach ($attributes as $value) {
                        if (\is_array($value)) {
                            $value = json_encode($value);
                        }
                        $value = (is_bool($value)) ? (int)$value : $value;
                        $bindKey = 'key_' . $bindIndex;
                        $bindKeys[] = ':' . $bindKey;
                        $bindValues[$bindKey] = $value;
                        $bindIndex++;
                    }

                    $batchKeys[] = '(' . implode(', ', $bindKeys) . ')';

                    // Permissions logic
                    $permissionsStmt = $this->getPDO()->prepare("
                        SELECT _type, _permission
                        FROM {$this->getSQLTable($name . '_perms')}
                        WHERE _document = :_uid
                    ");
                    $permissionsStmt->bindValue(':_uid', $document->getId());
                    $permissionsStmt->execute();
                    $permissions = $permissionsStmt->fetchAll(PDO::FETCH_ASSOC);

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
                        $diff = array_diff($permissions[$type], $document->getPermissionsByType($type));
                        if (!empty($diff)) {
                            $removals[$type] = $diff;
                        }
                    }

                    // Build inner query to remove permissions
                    if (!empty($removals)) {
                        foreach ($removals as $type => $permissionsToRemove) {
                            $bindKey = 'uid_' . $index;
                            $removeBindKeys[] = ':uid_' . $index;
                            $removeBindValues[$bindKey] = $document->getId();

                            $removeQuery .= "(
                                _document = :uid_{$index}
                                AND _type = '{$type}'
                                AND _permission IN (" . implode(', ', \array_map(function (string $i) use ($permissionsToRemove, $index, $type, &$removeBindKeys, &$removeBindValues) {
                                $bindKey = 'remove_' . $type . '_' . $index . '_' . $i;
                                $removeBindKeys[] = ':' . $bindKey;
                                $removeBindValues[$bindKey] = $permissionsToRemove[$i];

                                return ':' . $bindKey;
                            }, \array_keys($permissionsToRemove))) .
                                ")
                            )";

                            if ($type !== \array_key_last($removals)) {
                                $removeQuery .= ' OR ';
                            }
                        }

                        if ($index !== \array_key_last($batch)) {
                            $removeQuery .= ' OR ';
                        }
                    }

                    // Get added Permissions
                    $additions = [];
                    foreach (Database::PERMISSIONS as $type) {
                        $diff = \array_diff($document->getPermissionsByType($type), $permissions[$type]);
                        if (!empty($diff)) {
                            $additions[$type] = $diff;
                        }
                    }

                    // Build inner query to add permissions
                    if (!empty($additions)) {
                        foreach ($additions as $type => $permissionsToAdd) {
                            foreach ($permissionsToAdd as $i => $permission) {
                                $bindKey = 'uid_' . $index;
                                $addBindValues[$bindKey] = $document->getId();

                                $bindKey = 'add_' . $type . '_' . $index . '_' . $i;
                                $addBindValues[$bindKey] = $permission;

                                $addQuery .= "(:uid_{$index}, '{$type}', :{$bindKey})";

                                if ($i !== \array_key_last($permissionsToAdd) || $type !== \array_key_last($additions)) {
                                    $addQuery .= ', ';
                                }
                            }
                        }
                        if ($index !== \array_key_last($batch)) {
                            $addQuery .= ', ';
                        }
                    }
                }

                $updateClause = '';
                for ($i = 0; $i < \count($columns); $i++) {
                    $column = $columns[$i];
                    if (!empty($updateClause)) {
                        $updateClause .= ', ';
                    }
                    $updateClause .= "{$column} = VALUES({$column})";
                }

                $stmt = $this->getPDO()->prepare("
                    INSERT INTO {$this->getSQLTable($name)} (" . \implode(", ", $columns) . ") 
                    VALUES " . \implode(', ', $batchKeys) . "
                    ON DUPLICATE KEY UPDATE $updateClause
                ");

                foreach ($bindValues as $key => $value) {
                    $stmt->bindValue($key, $value, $this->getPDOType($value));
                }

                $stmt->execute();

                if (!empty($removeQuery)) {
                    $stmtRemovePermissions = $this->getPDO()->prepare("
                        DELETE
                        FROM {$this->getSQLTable($name . '_perms')}
                        WHERE ({$removeQuery})
                    ");

                    foreach ($removeBindValues as $key => $value) {
                        $stmtRemovePermissions->bindValue($key, $value, $this->getPDOType($value));
                    }

                    $stmtRemovePermissions->execute();
                }

                if (!empty($addQuery)) {
                    $stmtAddPermissions = $this->getPDO()->prepare("
                        INSERT INTO {$this->getSQLTable($name . '_perms')} (`_document`, `_type`, `_permission`)
                        VALUES {$addQuery}
                    ");

                    foreach ($addBindValues as $key => $value) {
                        $stmtAddPermissions->bindValue($key, $value, $this->getPDOType($value));
                    }

                    $stmtAddPermissions->execute();
                }
            }

            if (!$this->getPDO()->commit()) {
                throw new Exception('Failed to commit transaction');
            }

            return $documents;
        } catch (Throwable $th) {
            if (!$this->getPDO()->rollBack()) {
                throw new DatabaseException('Failed to rollBack transaction');
            }

            throw match ($th->getCode()) {
                1062, 23000 => new DuplicateException('Duplicated document: ' . $th->getMessage()),
                default => $th,
            };
        }
    }

    /**
     * Increase or decrease an attribute value
     *
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param int|float|null $min
     * @param int|float|null $max
     * @return bool
     * @throws Exception
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, int|float|null $min = null, int|float|null $max = null): bool
    {
        $name = $this->filter($collection);
        $attribute = $this->filter($attribute);

        $sqlMax = $max ? " AND `{$attribute}` <= {$max}" : '';
        $sqlMin = $min ? " AND `{$attribute}` >= {$min}" : '';

        $sql = "
			UPDATE {$this->getSQLTable($name)} 
			SET `{$attribute}` = `{$attribute}` + :val 
			WHERE 
			    _uid = :_uid 
				{$sqlMax}
				{$sqlMin}	
		";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_UPDATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);
        $stmt->bindValue(':_uid', $id);
        $stmt->bindValue(':val', $value);

        $stmt->execute() || throw new DatabaseException('Failed to update attribute');
        return true;
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

        try {
            if (!$this->getPDO()->beginTransaction()) {
                throw new Exception('Failed to begin transaction');
            }

            $sql = "
                DELETE FROM {$this->getSQLTable($name)} 
                WHERE _uid = :_uid
            ";

            $sql = $this->trigger(Database::EVENT_DOCUMENT_DELETE, $sql);

            $stmt = $this->getPDO()->prepare($sql);

            $stmt->bindValue(':_uid', $id);

            $sql = "
                DELETE FROM {$this->getSQLTable($name . '_perms')} 
                WHERE _document = :_uid
            ";

            $sql = $this->trigger(Database::EVENT_PERMISSIONS_DELETE, $sql);

            $stmtPermissions = $this->getPDO()->prepare($sql);
            $stmtPermissions->bindValue(':_uid', $id);

            if (!$stmt->execute()) {
                throw new DatabaseException('Failed to delete document');
            }
            if (!$stmtPermissions->execute()) {
                throw new DatabaseException('Failed to clean permissions');
            }

            if (!$this->getPDO()->commit()) {
                throw new DatabaseException('Failed to commit transaction');
            }
        } catch (Throwable $th) {
            if (!$this->getPDO()->rollBack()) {
                throw new DatabaseException('Failed to rollBack transaction');
            }
            throw new DatabaseException($th->getMessage());
        }

        return true;
    }

    /**
     * Find Documents
     *
     * @param string $collection
     * @param array<Query> $queries
     * @param int|null $limit
     * @param int|null $offset
     * @param array<string> $orderAttributes
     * @param array<string> $orderTypes
     * @param array<string, mixed> $cursor
     * @param string $cursorDirection
     * @return array<Document>
     * @throws DatabaseException
     * @throws TimeoutException
     */
    public function find(string $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER): array
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = [];
        $orders = [];

        $orderAttributes = \array_map(fn ($orderAttribute) => match ($orderAttribute) {
            '$id' => '_uid',
            '$internalId' => '_id',
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
                        table_main.`{$attribute}` {$this->getSQLOperator($orderMethod)} :cursor 
                        OR (
                            table_main.`{$attribute}` = :cursor 
                            AND
                            table_main._id {$this->getSQLOperator($orderMethodInternalId)} {$cursor['$internalId']}
                        )
                    )";
            } elseif ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }

            $orders[] = "`${attribute}` ${orderType}";
        }

        // Allow after pagination without any order
        if (empty($orderAttributes) && !empty($cursor)) {
            $orderType = $orderTypes[0] ?? Database::ORDER_ASC;

            if ($cursorDirection === Database::CURSOR_AFTER) {
                $orderMethod = $orderType === Database::ORDER_DESC
                    ? Query::TYPE_LESSER
                    : Query::TYPE_GREATER;
            } else {
                $orderMethod = $orderType === Database::ORDER_DESC
                    ? Query::TYPE_GREATER
                    : Query::TYPE_LESSER;
            }

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

        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                continue;
            }
            $where[] = $this->getSQLCondition($query);
        }


        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $sqlWhere = !empty($where) ? 'WHERE ' . implode(' AND ', $where) : '';
        $sqlOrder = 'ORDER BY ' . implode(', ', $orders);
        $sqlLimit = \is_null($limit) ? '' : 'LIMIT :limit';
        $sqlLimit .= \is_null($offset) ? '' : ' OFFSET :offset';

        $selections = $this->getAttributeSelections($queries);

        $sql = "
            SELECT {$this->getAttributeProjection($selections, 'table_main')}
            FROM {$this->getSQLTable($name)} as table_main
            {$sqlWhere}
            {$sqlOrder}
            {$sqlLimit};
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_FIND, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($queries as $query) {
            $this->bindConditionValue($stmt, $query);
        }

        if (!empty($cursor) && !empty($orderAttributes) && array_key_exists(0, $orderAttributes)) {
            $attribute = $orderAttributes[0];

            $attribute = match ($attribute) {
                '_uid' => '$id',
                '_id' => '$internalId',
                '_createdAt' => '$createdAt',
                '_updatedAt' => '$updatedAt',
                default => $attribute
            };

            if (\is_null($cursor[$attribute] ?? null)) {
                throw new DatabaseException("Order attribute '{$attribute}' is empty");
            }
            $stmt->bindValue(':cursor', $cursor[$attribute], $this->getPDOType($cursor[$attribute]));
        }

        if (!\is_null($limit)) {
            $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        }
        if (!\is_null($offset)) {
            $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        }

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            $this->processException($e);
        }

        $results = $stmt->fetchAll();
        $stmt->closeCursor();

        foreach ($results as $index => $document) {
            if (\array_key_exists('_uid', $document)) {
                $results[$index]['$id'] = $document['_uid'];
                unset($results[$index]['_uid']);
            }
            if (\array_key_exists('_id', $document)) {
                $results[$index]['$internalId'] = $document['_id'];
                unset($results[$index]['_id']);
            }
            if (\array_key_exists('_createdAt', $document)) {
                $results[$index]['$createdAt'] = $document['_createdAt'];
                unset($results[$index]['_createdAt']);
            }
            if (\array_key_exists('_updatedAt', $document)) {
                $results[$index]['$updatedAt'] = $document['_updatedAt'];
                unset($results[$index]['_updatedAt']);
            }
            if (\array_key_exists('_permissions', $document)) {
                $results[$index]['$permissions'] = \json_decode($document['_permissions'] ?? '[]', true);
                unset($results[$index]['_permissions']);
            }

            $results[$index] = new Document($results[$index]);
        }

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $results = \array_reverse($results);
        }

        return $results;
    }

    /**
     * Count Documents
     *
     * @param string $collection
     * @param array<Query> $queries
     * @param int|null $max
     * @return int
     * @throws Exception
     * @throws PDOException
     */
    public function count(string $collection, array $queries = [], ?int $max = null): int
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = [];
        $limit = \is_null($max) ? '' : 'LIMIT :max';

        foreach ($queries as $query) {
            $where[] = $this->getSQLCondition($query);
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $sqlWhere = !empty($where)
            ? 'WHERE ' . \implode(' AND ', $where)
            : '';

        $sql = "
			SELECT COUNT(1) as sum FROM (
				SELECT 1
				FROM {$this->getSQLTable($name)} table_main
				" . $sqlWhere . "
				{$limit}
			) table_count
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_COUNT, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($queries as $query) {
            $this->bindConditionValue($stmt, $query);
        }

        if (!\is_null($max)) {
            $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        }

        $stmt->execute();

        $result = $stmt->fetchAll();
        $stmt->closeCursor();
        if (!empty($result)) {
            $result = $result[0];
        }

        return $result['sum'] ?? 0;
    }

    /**
     * Sum an Attribute
     *
     * @param string $collection
     * @param string $attribute
     * @param array<Query> $queries
     * @param int|null $max
     * @return int|float
     * @throws Exception
     * @throws PDOException
     */
    public function sum(string $collection, string $attribute, array $queries = [], ?int $max = null): int|float
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = [];
        $limit = \is_null($max) ? '' : 'LIMIT :max';

        foreach ($queries as $query) {
            $where[] = $this->getSQLCondition($query);
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $sqlWhere = !empty($where)
            ? 'WHERE ' . \implode(' AND ', $where)
            : '';

        $sql = "
			SELECT SUM({$attribute}) as sum FROM (
				SELECT {$attribute}
				FROM {$this->getSQLTable($name)} table_main
				{$sqlWhere}
				{$limit}
			) table_count
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_SUM, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($queries as $query) {
            $this->bindConditionValue($stmt, $query);
        }

        if (!\is_null($max)) {
            $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        }

        $stmt->execute();

        $result = $stmt->fetchAll();
        $stmt->closeCursor();
        if (!empty($result)) {
            $result = $result[0];
        }

        return $result['sum'] ?? 0;
    }

    /**
     * Get the SQL projection given the selected attributes
     *
     * @param array<string> $selections
     * @param string $prefix
     * @return mixed
     * @throws Exception
     */
    protected function getAttributeProjection(array $selections, string $prefix = ''): mixed
    {
        if (empty($selections) || \in_array('*', $selections)) {
            if (!empty($prefix)) {
                return "`{$prefix}`.*";
            }
            return '*';
        }

        // Remove $id, $permissions and $collection if present since it is always selected by default
        $selections = \array_diff($selections, ['$id', '$permissions', '$collection']);

        $selections[] = '_uid';
        $selections[] = '_permissions';

        if (\in_array('$internalId', $selections)) {
            $selections[] = '_id';
            $selections = \array_diff($selections, ['$internalId']);
        }
        if (\in_array('$createdAt', $selections)) {
            $selections[] = '_createdAt';
            $selections = \array_diff($selections, ['$createdAt']);
        }
        if (\in_array('$updatedAt', $selections)) {
            $selections[] = '_updatedAt';
            $selections = \array_diff($selections, ['$updatedAt']);
        }

        if (!empty($prefix)) {
            foreach ($selections as &$selection) {
                $selection = "`{$prefix}`.`{$this->filter($selection)}`";
            }
        } else {
            foreach ($selections as &$selection) {
                $selection = "`{$this->filter($selection)}`";
            }
        }

        return \implode(', ', $selections);
    }

    /*
     * Get SQL Condition
     *
     * @param Query $query
     * @return string
     * @throws Exception
     */
    protected function getSQLCondition(Query $query): string
    {
        $query->setAttribute(match ($query->getAttribute()) {
            '$id' => '_uid',
            '$internalId' => '_id',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $query->getAttribute()
        });

        $attribute = "`{$query->getAttribute()}`";
        $placeholder = $this->getSQLPlaceholder($query);

        switch ($query->getMethod()) {
            case Query::TYPE_SEARCH:
                return "MATCH(table_main.{$attribute}) AGAINST (:{$placeholder}_0 IN BOOLEAN MODE)";

            case Query::TYPE_BETWEEN:
                return "table_main.{$attribute} BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Query::TYPE_IS_NULL:
            case Query::TYPE_IS_NOT_NULL:
                return "table_main.{$attribute} {$this->getSQLOperator($query->getMethod())}";

            default:
                $conditions = [];
                foreach ($query->getValues() as $key => $value) {
                    $conditions[] = $attribute . ' ' . $this->getSQLOperator($query->getMethod()) . ' :' . $placeholder . '_' . $key;
                }
                $condition = implode(' OR ', $conditions);
                return empty($condition) ? '' : '(' . $condition . ')';
        }
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

                if ($size > $this->getMaxVarcharLength()) {
                    return 'TEXT';
                }

                return "VARCHAR({$size})";

            case Database::VAR_INTEGER:  // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554
                $signed = ($signed) ? '' : ' UNSIGNED';

                if ($size >= 8) { // INT = 4 bytes, BIGINT = 8 bytes
                    return 'BIGINT' . $signed;
                }

                return 'INT' . $signed;

            case Database::VAR_FLOAT:
                $signed = ($signed) ? '' : ' UNSIGNED';
                return 'DOUBLE' . $signed;

            case Database::VAR_BOOLEAN:
                return 'TINYINT(1)';

            case Database::VAR_RELATIONSHIP:
                return 'VARCHAR(255)';

            case Database::VAR_DATETIME:
                return 'DATETIME(3)';

            default:
                throw new DatabaseException('Unknown type: ' . $type . '. Must be one of ' . Database::VAR_STRING . ', ' . Database::VAR_INTEGER .  ', ' . Database::VAR_FLOAT . ', ' . Database::VAR_BOOLEAN . ', ' . Database::VAR_DATETIME . ', ' . Database::VAR_RELATIONSHIP);
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
        $type = match ($type) {
            Database::INDEX_KEY,
            Database::INDEX_ARRAY => 'INDEX',
            Database::INDEX_UNIQUE => 'UNIQUE INDEX',
            Database::INDEX_FULLTEXT => 'FULLTEXT INDEX',
            default => throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_ARRAY . ', ' . Database::INDEX_FULLTEXT),
        };

        return "CREATE {$type} `{$id}` ON {$this->getSQLTable($collection)} ( " . implode(', ', $attributes) . " )";
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
        return match (gettype($value)) {
            'double', 'string' => PDO::PARAM_STR,
            'integer', 'boolean' => PDO::PARAM_INT,
            'NULL' => PDO::PARAM_NULL,
            default => throw new DatabaseException('Unknown PDO Type for ' . \gettype($value)),
        };
    }

    /**
     * Is fulltext Wildcard index supported?
     *
     * @return bool
     */
    public function getSupportForFulltextWildcardIndex(): bool
    {
        return true;
    }

    /**
     * Are timeouts supported?
     *
     * @return bool
     */
    public function getSupportForTimeouts(): bool
    {
        return true;
    }

    /**
     * Set max execution time
     * @param int $milliseconds
     * @param string $event
     * @return void
     * @throws DatabaseException
     */
    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        if (!$this->getSupportForTimeouts()) {
            return;
        }
        if ($milliseconds <= 0) {
            throw new DatabaseException('Timeout must be greater than 0');
        }

        $seconds = $milliseconds / 1000;

        $this->before($event, 'timeout', function ($sql) use ($seconds) {
            return "SET STATEMENT max_statement_time = {$seconds} FOR " . $sql;
        });
    }

    /**
     * @param PDOException $e
     * @throws TimeoutException
     */
    protected function processException(PDOException $e): void
    {
        // Regular PDO
        if ($e->getCode() === '70100' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1969) {
            throw new TimeoutException($e->getMessage());
        }

        // PDOProxy switches errorInfo PDOProxy.php line 64
        if ($e->getCode() === 1969 && isset($e->errorInfo[0]) && $e->errorInfo[0] === '70100') {
            throw new TimeoutException($e->getMessage());
        }

        throw $e;
    }
}
