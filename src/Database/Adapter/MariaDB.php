<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Truncate as TruncateException;
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
        $id = $this->filter($name);

        /** @var array<string> $attributeStrings */
        $attributeStrings = [];

        /** @var array<string> $indexStrings */
        $indexStrings = [];

        foreach ($attributes as $key => $attribute) {
            $attrId = $this->filter($attribute->getId());

            $attrType = $this->getSQLType(
                $attribute->getAttribute('type'),
                $attribute->getAttribute('size', 0),
                $attribute->getAttribute('signed', true),
                $attribute->getAttribute('array', false)
            );

            // Ignore relationships with virtual attributes
            if ($attribute->getAttribute('type') === Database::VAR_RELATIONSHIP) {
                $options = $attribute->getAttribute('options', []);
                $relationType = $options['relationType'] ?? null;
                $twoWay = $options['twoWay'] ?? false;
                $side = $options['side'] ?? null;

                if (
                    $relationType === Database::RELATION_MANY_TO_MANY
                    || ($relationType === Database::RELATION_ONE_TO_ONE && !$twoWay && $side === Database::RELATION_SIDE_CHILD)
                    || ($relationType === Database::RELATION_ONE_TO_MANY && $side === Database::RELATION_SIDE_PARENT)
                    || ($relationType === Database::RELATION_MANY_TO_ONE && $side === Database::RELATION_SIDE_CHILD)
                ) {
                    continue;
                }
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

                $indexAttribute = match ($attribute) {
                    '$id' => '_uid',
                    '$createdAt' => '_createdAt',
                    '$updatedAt' => '_updatedAt',
                    default => $attribute
                };
                $indexAttribute = $this->filter($indexAttribute);

                if ($indexType === Database::INDEX_FULLTEXT) {
                    $indexOrder = '';
                }

                $indexAttributes[$nested] = "`{$indexAttribute}`{$indexLength} {$indexOrder}";
            }

            $indexAttributes = \implode(", ", $indexAttributes);

            if ($this->sharedTables && $indexType !== Database::INDEX_FULLTEXT) {
                // Add tenant as first index column for best performance
                $indexAttributes = "_tenant, {$indexAttributes}";
            }

            $indexStrings[$key] = "{$indexType} `{$indexId}` ({$indexAttributes}),";
        }

        $collection = "
			CREATE TABLE {$this->getSQLTable($id)} (
				_id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
				_uid VARCHAR(255) NOT NULL,
				_createdAt DATETIME(3) DEFAULT NULL,
				_updatedAt DATETIME(3) DEFAULT NULL,
				_permissions MEDIUMTEXT DEFAULT NULL,
				PRIMARY KEY (_id),
				" . \implode(' ', $attributeStrings) . "
				" . \implode(' ', $indexStrings) . "
		";

        if ($this->sharedTables) {
            $collection .= "
            	_tenant INT(11) UNSIGNED DEFAULT NULL,
				UNIQUE KEY _uid (_uid, _tenant),
				KEY _created_at (_tenant, _createdAt),
				KEY _updated_at (_tenant, _updatedAt),
				KEY _tenant_id (_tenant, _id)
			";
        } else {
            $collection .= "
				UNIQUE KEY _uid (_uid),
				KEY _created_at (_createdAt),
				KEY _updated_at (_updatedAt)
			";
        }

        $collection .= ")";
        $collection = $this->trigger(Database::EVENT_COLLECTION_CREATE, $collection);

        $permissions = "
            CREATE TABLE {$this->getSQLTable($id . '_perms')} (
                _id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
                _type VARCHAR(12) NOT NULL,
                _permission VARCHAR(255) NOT NULL,
                _document VARCHAR(255) NOT NULL,
                PRIMARY KEY (_id),
        ";

        if ($this->sharedTables) {
            $permissions .= "
                _tenant INT(11) UNSIGNED DEFAULT NULL,
                UNIQUE INDEX _index1 (_document, _tenant, _type, _permission),
                INDEX _permission (_tenant, _permission, _type)
            ";
        } else {
            $permissions .= "
                UNIQUE INDEX _index1 (_document, _type, _permission),
                INDEX _permission (_permission, _type)
            ";
        }

        $permissions .= ")";
        $permissions = $this->trigger(Database::EVENT_COLLECTION_CREATE, $permissions);

        try {
            $this->getPDO()
                ->prepare($collection)
                ->execute();

            $this->getPDO()
                ->prepare($permissions)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return true;
    }

    /**
     * Get collection size on disk
     *
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        $collection = $this->filter($collection);
        $collection = $this->getNamespace() . '_' . $collection;
        $database = $this->getDatabase();
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
     * Get Collection Size of the raw data
     *
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->filter($collection);
        $collection = $this->getNamespace() . '_' . $collection;
        $database = $this->getDatabase();
        $permissions = $collection . '_perms';

        $collectionSize = $this->getPDO()->prepare("
            SELECT SUM(data_length + index_length)  
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = :name AND
            table_schema = :database
         ");

        $permissionsSize = $this->getPDO()->prepare("
            SELECT SUM(data_length + index_length)  
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = :permissions AND
            table_schema = :database
        ");

        $collectionSize->bindParam(':name', $collection);
        $collectionSize->bindParam(':database', $database);
        $permissionsSize->bindParam(':permissions', $permissions);
        $permissionsSize->bindParam(':database', $database);

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
     * Delete collection
     *
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
     * Analyze a collection updating it's metadata on the database engine
     *
     * @param string $collection
     * @return bool
     * @throws DatabaseException
     */
    public function analyzeCollection(string $collection): bool
    {
        $name = $this->filter($collection);

        $sql = "ANALYZE TABLE {$this->getSQLTable($name)}";

        $stmt = $this->getPDO()->prepare($sql);
        return $stmt->execute();
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
        $type = $this->getSQLType($type, $size, $signed, $array);

        $sql = "ALTER TABLE {$this->getSQLTable($name)} ADD COLUMN `{$id}` {$type};";
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
     * @throws DatabaseException
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $newKey = empty($newKey) ? null : $this->filter($newKey);
        $type = $this->getSQLType($type, $size, $signed, $array);

        if (!empty($newKey)) {
            $sql = "ALTER TABLE {$this->getSQLTable($name)} CHANGE COLUMN `{$id}` `{$newKey}` {$type};";
        } else {
            $sql = "ALTER TABLE {$this->getSQLTable($name)} MODIFY `{$id}` {$type};";
        }

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
        $name = $this->filter($collection);
        $id = $this->filter($id);

        $sql = "ALTER TABLE {$this->getSQLTable($name)} DROP COLUMN `{$id}`;";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            if ($e->getCode() === "42000" && $e->errorInfo[1] === 1091) {
                return true;
            }

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
        $old = $this->filter($old);
        $new = $this->filter($new);

        $sql = "ALTER TABLE {$this->getSQLTable($collection)} RENAME COLUMN `{$old}` TO `{$new}`;";

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
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param string $relatedCollection
     * @param bool $twoWay
     * @param string $twoWayKey
     * @return bool
     * @throws DatabaseException
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
     * @param string $side
     * @param string|null $newKey
     * @param string|null $newTwoWayKey
     * @return bool
     * @throws DatabaseException
     */
    public function updateRelationship(
        string $collection,
        string $relatedCollection,
        string $type,
        bool $twoWay,
        string $key,
        string $twoWayKey,
        string $side,
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
                if ($key !== $newKey) {
                    $sql = "ALTER TABLE {$table} RENAME COLUMN `{$key}` TO `{$newKey}`;";
                }
                if ($twoWay && $twoWayKey !== $newTwoWayKey) {
                    $sql .= "ALTER TABLE {$relatedTable} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`;";
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    if ($twoWayKey !== $newTwoWayKey) {
                        $sql = "ALTER TABLE {$relatedTable} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`;";
                    }
                } else {
                    if ($key !== $newKey) {
                        $sql = "ALTER TABLE {$table} RENAME COLUMN `{$key}` TO `{$newKey}`;";
                    }
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    if ($twoWayKey !== $newTwoWayKey) {
                        $sql = "ALTER TABLE {$relatedTable} RENAME COLUMN `{$twoWayKey}` TO `{$newTwoWayKey}`;";
                    }
                } else {
                    if ($key !== $newKey) {
                        $sql = "ALTER TABLE {$table} RENAME COLUMN `{$key}` TO `{$newKey}`;";
                    }
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

    /**
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string $key
     * @param string $twoWayKey
     * @param string $side
     * @return bool
     * @throws DatabaseException
     */
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

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $sql = "ALTER TABLE {$table} DROP COLUMN `{$key}`;";
                    if ($twoWay) {
                        $sql .= "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`;";
                    }
                } elseif ($side === Database::RELATION_SIDE_CHILD) {
                    $sql = "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`;";
                    if ($twoWay) {
                        $sql .= "ALTER TABLE {$table} DROP COLUMN `{$key}`;";
                    }
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $sql = "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`;";
                } else {
                    $sql = "ALTER TABLE {$table} DROP COLUMN `{$key}`;";
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $sql = "ALTER TABLE {$table} DROP COLUMN `{$key}`;";
                } else {
                    $sql = "ALTER TABLE {$relatedTable} DROP COLUMN `{$twoWayKey}`;";
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
     * @throws DatabaseException
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool
    {
        $collection = $this->getDocument(Database::METADATA, $collection);

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $collectionAttributes = \json_decode($collection->getAttribute('attributes', []), true);

        $id = $this->filter($id);

        foreach ($attributes as $i => $attr) {
            $collectionAttribute = \array_filter($collectionAttributes, fn ($collectionAttribute) => array_key_exists('key', $collectionAttribute) && $collectionAttribute['key'] === $attr);
            $collectionAttribute = end($collectionAttribute);
            $order = empty($orders[$i]) || Database::INDEX_FULLTEXT === $type ? '' : $orders[$i];
            $length = empty($lengths[$i]) ? '' : '(' . (int)$lengths[$i] . ')';

            $attr = match ($attr) {
                '$id' => '_uid',
                '$createdAt' => '_createdAt',
                '$updatedAt' => '_updatedAt',
                default => $this->filter($attr),
            };

            $attributes[$i] = "`{$attr}`{$length} {$order}";

            if (!empty($collectionAttribute['array']) && $this->getSupportForCastIndexArray()) {
                $attributes[$i] = '(CAST(`' . $attr . '` AS char(' . Database::ARRAY_INDEX_LENGTH . ') ARRAY))';
            }
        }

        $sqlType = match ($type) {
            Database::INDEX_KEY => 'INDEX',
            Database::INDEX_UNIQUE => 'UNIQUE INDEX',
            Database::INDEX_FULLTEXT => 'FULLTEXT INDEX',
            default => throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_FULLTEXT),
        };

        $attributes = \implode(', ', $attributes);

        if ($this->sharedTables && $type !== Database::INDEX_FULLTEXT) {
            // Add tenant as first index column for best performance
            $attributes = "_tenant, {$attributes}";
        }

        $sql =  "CREATE {$sqlType} `{$id}` ON {$this->getSQLTable($collection->getId())} ({$attributes})";
        $sql = $this->trigger(Database::EVENT_INDEX_CREATE, $sql);

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            $this->processException($e);
            return false;
        }
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

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            if ($e->getCode() === "42000" && $e->errorInfo[1] === 1091) {
                return true;
            }

            throw $e;
        }
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
     * @throws \Throwable
     */
    public function createDocument(string $collection, Document $document): Document
    {
        try {
            $attributes = $document->getAttributes();
            $attributes['_createdAt'] = $document->getCreatedAt();
            $attributes['_updatedAt'] = $document->getUpdatedAt();
            $attributes['_permissions'] = \json_encode($document->getPermissions());

            if ($this->sharedTables) {
                $attributes['_tenant'] = $document->getTenant();
            }

            $name = $this->filter($collection);
            $columns = '';
            $columnNames = '';

            /**
             * Insert Attributes
             */
            $bindIndex = 0;
            foreach ($attributes as $attribute => $value) {
                $column = $this->filter($attribute);
                $bindKey = 'key_' . $bindIndex;
                $columns .= "`{$column}`, ";
                $columnNames .= ':' . $bindKey . ', ';
                $bindIndex++;
            }

            // Insert internal ID if set
            if (!empty($document->getInternalId())) {
                $bindKey = '_id';
                $columns .= "_id, ";
                $columnNames .= ':' . $bindKey . ', ';
            }

            $sql = "
			    INSERT INTO {$this->getSQLTable($name)} ({$columns} _uid)
			    VALUES ({$columnNames} :_uid)
			";

            $sql = $this->trigger(Database::EVENT_DOCUMENT_CREATE, $sql);

            $stmt = $this->getPDO()->prepare($sql);

            $stmt->bindValue(':_uid', $document->getId());

            if (!empty($document->getInternalId())) {
                $stmt->bindValue(':_id', $document->getInternalId());
            }

            $attributeIndex = 0;
            foreach ($attributes as $value) {
                if (\is_array($value)) {
                    $value = \json_encode($value);
                }

                $bindKey = 'key_' . $attributeIndex;
                $attribute = $this->filter($attribute);
                $value = (\is_bool($value)) ? (int)$value : $value;
                $stmt->bindValue(':' . $bindKey, $value, $this->getPDOType($value));
                $attributeIndex++;
            }

            $permissions = [];
            foreach (Database::PERMISSIONS as $type) {
                foreach ($document->getPermissionsByType($type) as $permission) {
                    $tenantBind = $this->sharedTables ? ", :_tenant" : '';
                    $permission = \str_replace('"', '', $permission);
                    $permission = "('{$type}', '{$permission}', :_uid {$tenantBind})";
                    $permissions[] = $permission;
                }
            }

            if (!empty($permissions)) {
                $tenantColumn = $this->sharedTables ? ', _tenant' : '';
                $permissions = \implode(', ', $permissions);

                $sqlPermissions = "
                    INSERT INTO {$this->getSQLTable($name . '_perms')} (_type, _permission, _document {$tenantColumn})
                    VALUES {$permissions};
                ";

                $stmtPermissions = $this->getPDO()->prepare($sqlPermissions);
                $stmtPermissions->bindValue(':_uid', $document->getId());
                if ($this->sharedTables) {
                    $stmtPermissions->bindValue(':_tenant', $document->getTenant());
                }
            }

            $stmt->execute();

            $document['$internalId'] = $this->pdo->lastInsertId();

            if (empty($document['$internalId'])) {
                throw new DatabaseException('Error creating document empty "$internalId"');
            }

            if (isset($stmtPermissions)) {
                $stmtPermissions->execute();
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
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

            $hasInternalId = null;
            foreach ($documents as $document) {
                $attributes = $document->getAttributes();
                $attributeKeys = array_merge($attributeKeys, array_keys($attributes));

                if ($hasInternalId === null) {
                    $hasInternalId = !empty($document->getInternalId());
                } elseif ($hasInternalId == empty($document->getInternalId())) {
                    throw new DatabaseException('All documents must have an internalId if one is set');
                }
            }
            $attributeKeys = array_unique($attributeKeys);

            if ($this->sharedTables) {
                $attributeKeys[] = '_tenant';
            }

            $columns = [];
            foreach ($attributeKeys as $key => $attribute) {
                $columns[$key] = "`{$this->filter($attribute)}`";
            }
            $columns = '(' . \implode(', ', $columns) . ')';

            $bindIndex = 0;
            $batchKeys = [];
            $bindValues = [];
            $permissions = [];
            $documentIds = [];

            foreach ($documents as $index => $document) {
                $attributes = $document->getAttributes();
                $attributes['_uid'] = $document->getId();
                $attributes['_createdAt'] = $document->getCreatedAt();
                $attributes['_updatedAt'] = $document->getUpdatedAt();
                $attributes['_permissions'] = \json_encode($document->getPermissions());

                if (! empty($document->getInternalId())) {
                    $attributes['_id'] = $document->getInternalId();
                    $attributeKeys[] = '_id';
                } else {
                    $documentIds[] = $document->getId();
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

            $stmt->execute();

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

                $stmtPermissions?->execute();
            }

            $internalIds = $this->getInternalIds($collection, $documentIds);

            foreach ($documents as $document) {
                if (isset($internalIds[$document->getId()])) {
                    $document['$internalId'] = $internalIds[$document->getId()];
                }
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $documents;
    }

    /**
     * Get internal IDs for the given documents
     *
     * @param string $collection
     * @param array<string> $documentIds
     * @return array<string>
     * @throws DatabaseException
     */
    private function getInternalIds(string $collection, array $documentIds): array
    {
        $internalIds = [];

        /**
         * UID, _tenant bottleneck is ~ 5000 rows since we use _uid IN query
         */
        foreach (\array_chunk($documentIds, 1000) as $documentIdsChunk) {
            // Get internal IDs
            $sql = "
                SELECT _uid, _id
                FROM {$this->getSQLTable($collection)}
                WHERE _uid IN (" . implode(',', array_map(fn ($index) => ":_key_{$index}", array_keys($documentIdsChunk))) . ")
                {$this->getTenantQuery($collection)}
            ";
            $stmt = $this->getPDO()->prepare($sql);

            foreach ($documentIdsChunk as $index => $id) {
                $stmt->bindValue(":_key_{$index}", $id);
            }

            if ($this->sharedTables) {
                $stmt->bindValue(':_tenant', $this->tenant);
            }

            $stmt->execute();
            $results = $stmt->fetchAll(PDO::FETCH_KEY_PAIR); // Fetch as [documentId => internalId]
            $stmt->closeCursor();

            $internalIds = array_merge($internalIds, $results);
        }

        return $internalIds;
    }
    /**
     * Update Document
     *
     * @param string $collection
     * @param string $id
     * @param Document $document
     * @return Document
     * @throws Exception
     * @throws PDOException
     * @throws DuplicateException
     * @throws \Throwable
     */
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        try {
            $attributes = $document->getAttributes();
            $attributes['_createdAt'] = $document->getCreatedAt();
            $attributes['_updatedAt'] = $document->getUpdatedAt();
            $attributes['_permissions'] = json_encode($document->getPermissions());

            if ($this->sharedTables) {
                $attributes['_tenant'] = $this->tenant;
            }

            $name = $this->filter($collection);
            $columns = '';

            $sql = "
			    SELECT _type, _permission
			    FROM {$this->getSQLTable($name . '_perms')}
			    WHERE _document = :_uid
			    {$this->getTenantQuery($collection)}
			";

            $sql = $this->trigger(Database::EVENT_PERMISSIONS_READ, $sql);

            /**
             * Get current permissions from the database
             */
            $sqlPermissions = $this->getPDO()->prepare($sql);
            $sqlPermissions->bindValue(':_uid', $document->getId());

            if ($this->sharedTables) {
                $sqlPermissions->bindValue(':_tenant', $this->tenant);
            }

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
                    FROM {$this->getSQLTable($name . '_perms')}
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
                        $value = "( :_uid, '{$type}', :_add_{$type}_{$i}";

                        if ($this->sharedTables) {
                            $value .= ", :_tenant)";
                        } else {
                            $value .= ")";
                        }

                        $values[] = $value;
                    }
                }

                $sql = "
				    INSERT INTO {$this->getSQLTable($name . '_perms')} (_document, _type, _permission
				";

                if ($this->sharedTables) {
                    $sql .= ', _tenant)';
                } else {
                    $sql .= ')';
                }

                $sql .= " VALUES " . \implode(', ', $values);

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
                if (is_array($value)) {
                    $value = json_encode($value);
                }

                $bindKey = 'key_' . $attributeIndex;
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

        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
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

        $name = $this->filter($collection);

        $columns = '';

        $where = [];

        $ids = \array_map(fn ($document) => $document->getId(), $documents);
        $where[] = "_uid IN (" . \implode(', ', \array_map(fn ($index) => ":_id_{$index}", \array_keys($ids))) . ")";

        if ($this->sharedTables) {
            $where[] = "_tenant = :_tenant";
        }

        $sqlWhere = 'WHERE ' . implode(' AND ', $where);

        $bindIndex = 0;
        foreach ($attributes as $attribute => $value) {
            $column = $this->filter($attribute);
            $bindKey = 'key_' . $bindIndex;
            $columns .= "`{$column}`" . '=:' . $bindKey;

            if ($attribute !== \array_key_last($attributes)) {
                $columns .= ',';
            }

            $bindIndex++;
        }

        $sql = "
            UPDATE {$this->getSQLTable($name)}
            SET {$columns}
            {$sqlWhere}
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENTS_UPDATE, $sql);
        $stmt = $this->getPDO()->prepare($sql);

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
        }

        foreach ($ids as $id => $value) {
            $stmt->bindValue(":_id_{$id}", $value);
        }

        $attributeIndex = 0;
        foreach ($attributes as $attribute => $value) {
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

            /* @var $document Document */
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
                        $bindKey = 'uid_' . $index;
                        $removeBindKeys[] = ':uid_' . $index;
                        $removeBindValues[$bindKey] = $document->getId();

                        $removeQueries[] = "(
                            _document = :uid_{$index}
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
                            $bindKey = 'uid_' . $index;
                            $addBindValues[$bindKey] = $document->getId();

                            $bindKey = 'add_' . $type . '_' . $index . '_' . $i;
                            $addBindValues[$bindKey] = $permission;

                            $addQuery .= "(:uid_{$index}, '{$type}', :{$bindKey}";

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
                    INSERT INTO {$this->getSQLTable($name . '_perms')} (`_document`, `_type`, `_permission`
                ";

                if ($this->sharedTables) {
                    $sqlAddPermissions .= ', `_tenant`)';
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
     * @param string $collection
     * @param string $attribute
     * @param array<Document> $documents
     * @return array<Document>
     * @throws DatabaseException
     */
    public function createOrUpdateDocuments(
        string $collection,
        string $attribute,
        array $documents
    ): array {
        if (empty($documents)) {
            return $documents;
        }

        try {
            $name = $this->filter($collection);
            $attribute = $this->filter($attribute);

            $bindIndex = 0;
            $batchKeys = [];
            $bindValues = [];
            $attributes = [];
            $documentIds = [];

            foreach ($documents as $document) {
                /**
                 * @var array<string, mixed> $attributes
                 */
                $attributes = $document->getAttributes();
                $documentIds[] = $attributes['_uid'] = $document->getId();
                $attributes['_createdAt'] = $document->getCreatedAt();
                $attributes['_updatedAt'] = $document->getUpdatedAt();
                $attributes['_permissions'] = \json_encode($document->getPermissions());

                if (!empty($document->getInternalId())) {
                    $attributes['_id'] = $document->getInternalId();
                }

                if ($this->sharedTables) {
                    $attributes['_tenant'] = $this->tenant;
                }

                $columns = [];
                foreach (\array_keys($attributes) as $key => $attr) {
                    $columns[$key] = "`{$this->filter($attr)}`";
                }

                $columns = '(' . \implode(', ', $columns) . ')';

                $bindKeys = [];

                foreach ($attributes as $attrValue) {
                    if (\is_array($attrValue)) {
                        $attrValue = \json_encode($attrValue);
                    }
                    $attrValue = (\is_bool($attrValue)) ? (int)$attrValue : $attrValue;
                    $bindKey = 'key_' . $bindIndex;
                    $bindKeys[] = ':' . $bindKey;
                    $bindValues[$bindKey] = $attrValue;
                    $bindIndex++;
                }

                $batchKeys[] = '(' . \implode(', ', $bindKeys) . ')';
            }

            if (!empty($attribute)) {
                // Increment specific column by its new value in place
                $updateColumns = [
                    "`{$attribute}` = `{$attribute}` + VALUES(`{$attribute}`)",
                    "`_updatedAt` = VALUES(`_updatedAt`)"
                ];
            } else {
                // Update all columns
                $updateColumns = [];
                foreach (\array_keys($attributes) as $attr) {
                    $updateColumns[] = "`{$this->filter($attr)}` = VALUES(`{$this->filter($attr)}`)";
                }
            }

            $stmt = $this->getPDO()->prepare(
                "
                INSERT INTO {$this->getSQLTable($name)} {$columns}
                VALUES " . \implode(', ', $batchKeys) . "
                ON DUPLICATE KEY UPDATE
                    " . \implode(', ', $updateColumns)
            );

            foreach ($bindValues as $key => $binding) {
                $stmt->bindValue($key, $binding, $this->getPDOType($binding));
            }

            $stmt->execute();

            // Fetch existing permissions in bulk after data updates
            $sql = "
                SELECT _document, _type, _permission
                FROM {$this->getSQLTable($name . '_perms')}
                WHERE _document IN (" . \implode(',', \array_map(fn ($index) => ":_key_{$index}", \array_keys($documentIds))) . ")
                {$this->getTenantQuery($collection)}
            ";

            $stmt = $this->getPDO()->prepare($sql);

            foreach ($documentIds as $index => $id) {
                $stmt->bindValue(":_key_{$index}", $id);
            }

            if ($this->sharedTables) {
                $stmt->bindValue(':_tenant', $this->tenant);
            }

            $stmt->execute();
            $existing = $stmt->fetchAll();
            $stmt->closeCursor();

            // Group permissions by document
            $permissionsByDocument = [];
            foreach ($existing as $row) {
                $permissionsByDocument[$row['_document']][$row['_type']][] = $row['_permission'];
            }

            foreach ($documentIds as $id) {
                foreach (Database::PERMISSIONS as $type) {
                    $permissionsByDocument[$id][$type] = $permissionsByDocument[$id][$type] ?? [];
                }
            }

            $removeQueries = [];
            $removeBindValues = [];
            $addQueries = [];
            $addBindValues = [];

            foreach ($documents as $index => $document) {
                $currentPermissions = $permissionsByDocument[$document->getId()] ?? [];

                // Calculate removals
                foreach (Database::PERMISSIONS as $type) {
                    $toRemove = \array_diff($currentPermissions[$type], $document->getPermissionsByType($type));
                    if (!empty($toRemove)) {
                        $removeQueries[] = "(
                            _document = :uid_{$index}
                            {$this->getTenantQuery($collection)}
                            AND _type = '{$type}'
                            AND _permission IN (" . \implode(',', \array_map(fn ($i) => ":remove_{$type}_{$index}_{$i}", \array_keys($toRemove))) . ")
                        )";
                        $removeBindValues[":uid_{$index}"] = $document->getId();
                        foreach ($toRemove as $i => $perm) {
                            $removeBindValues[":remove_{$type}_{$index}_{$i}"] = $perm;
                        }
                    }
                }

                // Calculate additions
                foreach (Database::PERMISSIONS as $type) {
                    $toAdd = \array_diff($document->getPermissionsByType($type), $currentPermissions[$type]);
                    foreach ($toAdd as $i => $permission) {
                        $addQuery = "(:uid_{$index}, '{$type}', :add_{$type}_{$index}_{$i}";

                        if ($this->sharedTables) {
                            $addQuery .= ", :_tenant)";
                        } else {
                            $addQuery .= ")";
                        }

                        $addQueries[] = $addQuery;
                        $addBindValues[":uid_{$index}"] = $document->getId();
                        $addBindValues[":add_{$type}_{$index}_{$i}"] = $permission;
                    }
                }
            }

            // Execute permission removals
            if (!empty($removeQueries)) {
                $removeQuery = \implode(' OR ', $removeQueries);
                $stmtRemovePermissions = $this->getPDO()->prepare("DELETE FROM {$this->getSQLTable($name . '_perms')} WHERE {$removeQuery}");
                foreach ($removeBindValues as $key => $value) {
                    $stmtRemovePermissions->bindValue($key, $value, $this->getPDOType($value));
                }
                if ($this->sharedTables) {
                    $stmtRemovePermissions->bindValue(':_tenant', $this->tenant);
                }
                $stmtRemovePermissions->execute();
            }

            // Execute permission additions
            if (!empty($addQuery)) {
                $sqlAddPermissions = "INSERT INTO {$this->getSQLTable($name . '_perms')} (_document, _type, _permission";
                if ($this->sharedTables) {
                    $sqlAddPermissions .= ", _tenant)";
                } else {
                    $sqlAddPermissions .= ")";
                }
                $addQuery = \implode(', ', $addQueries);
                $sqlAddPermissions .= " VALUES {$addQuery}";
                $stmtAddPermissions = $this->getPDO()->prepare($sqlAddPermissions);
                foreach ($addBindValues as $key => $value) {
                    $stmtAddPermissions->bindValue($key, $value, $this->getPDOType($value));
                }
                if ($this->sharedTables) {
                    $stmtAddPermissions->bindValue(':_tenant', $this->tenant);
                }

                $stmtAddPermissions->execute();
            }

            $internalIds = $this->getInternalIds($collection, $documentIds);

            foreach ($documents as $document) {
                if (isset($internalIds[$document->getId()])) {
                    $document['$internalId'] = $internalIds[$document->getId()];
                }
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $documents;
    }

    /**
     * Increase or decrease an attribute value
     *
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param string $updatedAt
     * @param int|float|null $min
     * @param int|float|null $max
     * @return bool
     * @throws DatabaseException
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, string $updatedAt, int|float|null $min = null, int|float|null $max = null): bool
    {
        $name = $this->filter($collection);
        $attribute = $this->filter($attribute);

        $sqlMax = $max ? " AND `{$attribute}` <= {$max}" : '';
        $sqlMin = $min ? " AND `{$attribute}` >= {$min}" : '';

        $sql = "
			UPDATE {$this->getSQLTable($name)} 
			SET 
			    `{$attribute}` = `{$attribute}` + :val,
			    `_updatedAt` = :updatedAt
			WHERE _uid = :_uid
			{$this->getTenantQuery($collection)}
		";

        $sql .= $sqlMax . $sqlMin;

        $sql = $this->trigger(Database::EVENT_DOCUMENT_UPDATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);
        $stmt->bindValue(':_uid', $id);
        $stmt->bindValue(':val', $value);
        $stmt->bindValue(':updatedAt', $updatedAt);

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
        }

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
        try {
            $name = $this->filter($collection);

            $sql = "
                DELETE FROM {$this->getSQLTable($name)} 
                WHERE _uid = :_uid
                {$this->getTenantQuery($collection)}
		    ";

            $sql = $this->trigger(Database::EVENT_DOCUMENT_DELETE, $sql);

            $stmt = $this->getPDO()->prepare($sql);

            $stmt->bindValue(':_uid', $id);

            if ($this->sharedTables) {
                $stmt->bindValue(':_tenant', $this->tenant);
            }

            $sql = "
			    DELETE FROM {$this->getSQLTable($name . '_perms')} 
		        WHERE _document = :_uid
		        {$this->getTenantQuery($collection)}
		    ";

            $sql = $this->trigger(Database::EVENT_PERMISSIONS_DELETE, $sql);

            $stmtPermissions = $this->getPDO()->prepare($sql);
            $stmtPermissions->bindValue(':_uid', $id);

            if ($this->sharedTables) {
                $stmtPermissions->bindValue(':_tenant', $this->tenant);
            }

            if (!$stmt->execute()) {
                throw new DatabaseException('Failed to delete document');
            }

            $deleted = $stmt->rowCount();

            if (!$stmtPermissions->execute()) {
                throw new DatabaseException('Failed to delete permissions');
            }
        } catch (\Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        return $deleted;
    }

    /**
     * Delete Documents
     *
     * @param string $collection
     * @param array<string> $ids
     *
     * @return int
     */
    public function deleteDocuments(string $collection, array $ids): int
    {
        if (empty($ids)) {
            return 0;
        }

        try {
            $name = $this->filter($collection);
            $where = [];

            if ($this->sharedTables) {
                $where[] = "_tenant = :_tenant";
            }

            $where[] = "_uid IN (" . \implode(', ', \array_map(fn ($index) => ":_id_{$index}", \array_keys($ids))) . ")";

            $sql = "DELETE FROM {$this->getSQLTable($name)} WHERE " . \implode(' AND ', $where);

            $sql = $this->trigger(Database::EVENT_DOCUMENTS_DELETE, $sql);

            $stmt = $this->getPDO()->prepare($sql);

            foreach ($ids as $id => $value) {
                $stmt->bindValue(":_id_{$id}", $value);
            }

            if ($this->sharedTables) {
                $stmt->bindValue(':_tenant', $this->tenant);
            }

            $sql = "
                DELETE FROM {$this->getSQLTable($name . '_perms')} 
                WHERE _document IN (" . \implode(', ', \array_map(fn ($index) => ":_id_{$index}", \array_keys($ids))) . ")
            ";

            if ($this->sharedTables) {
                $sql .= ' AND _tenant = :_tenant';
            }

            $sql = $this->trigger(Database::EVENT_PERMISSIONS_DELETE, $sql);

            $stmtPermissions = $this->getPDO()->prepare($sql);

            foreach ($ids as $id => $value) {
                $stmtPermissions->bindValue(":_id_{$id}", $value);
            }

            if ($this->sharedTables) {
                $stmtPermissions->bindValue(':_tenant', $this->tenant);
            }

            if (!$stmt->execute()) {
                throw new DatabaseException('Failed to delete documents');
            }

            if (!$stmtPermissions->execute()) {
                throw new DatabaseException('Failed to delete permissions');
            }
        } catch (\Throwable $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        return $stmt->rowCount();
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
     * @param string $forPermission
     * @return array<Document>
     * @throws DatabaseException
     * @throws TimeoutException
     * @throws Exception
     */
    public function find(string $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = [];
        $orders = [];

        $queries = array_map(fn ($query) => clone $query, $queries);

        $orderAttributes = \array_map(fn ($orderAttribute) => match ($orderAttribute) {
            '$id' => '_uid',
            '$internalId' => '_id',
            '$tenant' => '_tenant',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $orderAttribute
        }, $orderAttributes);

        $hasIdAttribute = false;
        foreach ($orderAttributes as $i => $attribute) {
            if (\in_array($attribute, ['_uid', '_id'])) {
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

            $orders[] = "`{$attribute}` {$orderType}";
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

        $conditions = $this->getSQLConditions($queries);
        if (!empty($conditions)) {
            $where[] = $conditions;
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles, $forPermission);
        }

        if ($this->sharedTables) {
            $orIsNull = '';

            if ($collection === Database::METADATA) {
                $orIsNull = " OR table_main._tenant IS NULL";
            }

            $where[] = "(table_main._tenant = :_tenant {$orIsNull})";
        }

        $sqlWhere = !empty($where) ? 'WHERE ' . implode(' AND ', $where) : '';
        $sqlOrder = 'ORDER BY ' . implode(', ', $orders);
        $sqlLimit = \is_null($limit) ? '' : 'LIMIT :limit';
        $sqlLimit .= \is_null($offset) ? '' : ' OFFSET :offset';

        $selections = $this->getAttributeSelections($queries);

        $sql = "
            SELECT {$this->getAttributeProjection($selections, 'table_main')}
            FROM {$this->getSQLTable($name)} AS table_main
            {$sqlWhere}
            {$sqlOrder}
            {$sqlLimit};
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_FIND, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($queries as $query) {
            $this->bindConditionValue($stmt, $query);
        }
        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
        }

        if (!empty($cursor) && !empty($orderAttributes) && array_key_exists(0, $orderAttributes)) {
            $attribute = $orderAttributes[0];

            $attribute = match ($attribute) {
                '_uid' => '$id',
                '_id' => '$internalId',
                '_tenant' => '$tenant',
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
            throw $this->processException($e);
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
            if (\array_key_exists('_tenant', $document)) {
                $document['$tenant'] = $document['_tenant'] === null ? null : (int)$document['_tenant'];
                unset($results[$index]['_tenant']);
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

        $queries = array_map(fn ($query) => clone $query, $queries);

        $conditions = $this->getSQLConditions($queries);
        if (!empty($conditions)) {
            $where[] = $conditions;
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        if ($this->sharedTables) {
            $orIsNull = '';

            if ($collection === Database::METADATA) {
                $orIsNull = " OR table_main._tenant IS NULL";
            }

            $where[] = "(table_main._tenant = :_tenant {$orIsNull})";
        }

        $sqlWhere = !empty($where)
            ? 'WHERE ' . \implode(' AND ', $where)
            : '';

        $sql = "
			SELECT COUNT(1) as sum FROM (
				SELECT 1
				FROM {$this->getSQLTable($name)} table_main
				{$sqlWhere}
				{$limit}
			) table_count
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_COUNT, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($queries as $query) {
            $this->bindConditionValue($stmt, $query);
        }

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
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

        $queries = array_map(fn ($query) => clone $query, $queries);

        foreach ($queries as $query) {
            $where[] = $this->getSQLCondition($query);
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        if ($this->sharedTables) {
            $orIsNull = '';

            if ($collection === Database::METADATA) {
                $orIsNull = " OR table_main._tenant IS NULL";
            }

            $where[] = "(table_main._tenant = :_tenant {$orIsNull})";
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

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
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

    /**
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
            '$tenant' => '_tenant',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            default => $query->getAttribute()
        });

        $attribute = "`{$query->getAttribute()}`";
        $placeholder = $this->getSQLPlaceholder($query);

        switch ($query->getMethod()) {
            case Query::TYPE_OR:
            case Query::TYPE_AND:
                $conditions = [];
                /* @var $q Query */
                foreach ($query->getValue() as $q) {
                    $conditions[] = $this->getSQLCondition($q);
                }

                $method = strtoupper($query->getMethod());
                return empty($conditions) ? '' : ' '. $method .' (' . implode(' AND ', $conditions) . ')';

            case Query::TYPE_SEARCH:
                return "MATCH(`table_main`.{$attribute}) AGAINST (:{$placeholder}_0 IN BOOLEAN MODE)";

            case Query::TYPE_BETWEEN:
                return "`table_main`.{$attribute} BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Query::TYPE_IS_NULL:
            case Query::TYPE_IS_NOT_NULL:
                return "`table_main`.{$attribute} {$this->getSQLOperator($query->getMethod())}";

            case Query::TYPE_CONTAINS:
                if ($this->getSupportForJSONOverlaps() && $query->onArray()) {
                    return "JSON_OVERLAPS(`table_main`.{$attribute}, :{$placeholder}_0)";
                }

                // no break
            default:
                $conditions = [];
                foreach ($query->getValues() as $key => $value) {
                    $conditions[] = "{$attribute} {$this->getSQLOperator($query->getMethod())} :{$placeholder}_{$key}";
                }
                return empty($conditions) ? '' : '(' . implode(' OR ', $conditions) . ')';
        }
    }

    /**
     * Get SQL Type
     *
     * @param string $type
     * @param int $size
     * @param bool $signed
     * @param bool $array
     * @return string
     * @throws DatabaseException
     */
    protected function getSQLType(string $type, int $size, bool $signed = true, bool $array = false): string
    {
        if ($array === true) {
            return 'JSON';
        }

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

    public function getMinDateTime(): \DateTime
    {
        return new \DateTime('1000-01-01 00:00:00');
    }

    public function getMaxDateTime(): \DateTime
    {
        return new \DateTime('9999-12-31 23:59:59');
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
     * Does the adapter handle Query Array Overlaps?
     *
     * @return bool
     */
    public function getSupportForJSONOverlaps(): bool
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

    public function getSupportForUpserts(): bool
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
     * @return string
     */
    public function getConnectionId(): string
    {
        $stmt = $this->getPDO()->query("SELECT CONNECTION_ID();");
        return $stmt->fetchColumn();
    }

    public function getInternalIndexesKeys(): array
    {
        return ['primary', '_created_at', '_updated_at', '_tenant_id'];
    }

    protected function processException(PDOException $e): \Exception
    {
        // Timeout
        if ($e->getCode() === '70100' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1969) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Duplicate table
        if ($e->getCode() === '42S01' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1050) {
            return new DuplicateException('Collection already exists', $e->getCode(), $e);
        }

        // Duplicate column
        if ($e->getCode() === '42S21' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1060) {
            return new DuplicateException('Attribute already exists', $e->getCode(), $e);
        }

        // Duplicate index
        if ($e->getCode() === '42000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1061) {
            return new DuplicateException('Index already exists', $e->getCode(), $e);
        }

        // Duplicate row
        if ($e->getCode() === '23000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1062) {
            return new DuplicateException('Document already exists', $e->getCode(), $e);
        }

        // Data is too big for column resize
        if (($e->getCode() === '22001' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1406) ||
            ($e->getCode() === '01000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1265)) {
            return new TruncateException('Resize would result in data truncation', $e->getCode(), $e);
        }

        // Unknown database
        if ($e->getCode() === '42000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1049) {
            return new NotFoundException('Database not found', $e->getCode(), $e);
        }

        // Unknown collection
        if ($e->getCode() === '42S02' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 1049) {
            return new NotFoundException('Collection not found', $e->getCode(), $e);
        }

        return $e;
    }

    /**
     * Get Schema Attributes
     *
     * @param string $collection
     * @return array<Document>
     * @throws DatabaseException
     */
    public function getSchemaAttributes(string $collection): array
    {
        $schema = $this->getDatabase();
        $collection = $this->getNamespace().'_'.$this->filter($collection);

        try {
            $stmt = $this->getPDO()->prepare('
                SELECT
                COLUMN_NAME as columnName,
                COLUMN_DEFAULT as columnDefault,
                IS_NULLABLE as isNullable,
                DATA_TYPE as dataType,
                CHARACTER_MAXIMUM_LENGTH as characterMaximumLength,
                NUMERIC_PRECISION as numericPrecision,
                NUMERIC_SCALE as numericScale,
                DATETIME_PRECISION as datetimePrecision,
                COLUMN_TYPE as columnType,
                COLUMN_KEY as columnKey,
                EXTRA as extra
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            ');
            $stmt->bindParam(':schema', $schema);
            $stmt->bindParam(':table', $collection);
            $stmt->execute();
            $results = $stmt->fetchAll();
            $stmt->closeCursor();

            foreach ($results as $index => $document) {
                $results[$index] = new Document($document);
            }

            return $results;

        } catch (PDOException $e) {
            throw new DatabaseException('Failed to get schema attributes', $e->getCode(), $e);
        }
    }

    public function getSupportForSchemaAttributes(): bool
    {
        return true;
    }

}
