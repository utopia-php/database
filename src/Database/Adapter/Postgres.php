<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Throwable;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Timeout;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class Postgres extends SQL
{
    /**
     * Differences between MariaDB and Postgres
     *
     * 1. Need to use CASCADE to DROP schema
     * 2. Quotes are different ` vs "
     * 3. DATETIME is TIMESTAMP
     * 4. Full-text search is different - to_tsvector() and to_tsquery()
     */

    /**
     * Create Database
     *
     * @param string $name
     *
     * @return bool
     * @throws DatabaseException
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        if ($this->exists($name)) {
            return true;
        }

        $sql = "CREATE SCHEMA \"{$name}\"";
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

        $sql = "DROP SCHEMA IF EXISTS \"{$name}\" CASCADE";
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
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        /** @var array<string> $attributeStrings */
        $attributeStrings = [];

        $this->getPDO()->beginTransaction();

        /** @var array<string> $attributeStrings */
        $attributeStrings = [];
        foreach ($attributes as $attribute) {
            $attrId = $this->filter($attribute->getId());

            $attrType = $this->getSQLType(
                $attribute->getAttribute('type'),
                $attribute->getAttribute('size', 0),
                $attribute->getAttribute('signed', true),
                $attribute->getAttribute('array', false)
            );

            $attributeStrings[] = "\"{$attrId}\" {$attrType}, ";
        }

        $sqlTenant = $this->sharedTables ? '_tenant INTEGER DEFAULT NULL,' : '';

        $sql = "
            CREATE TABLE IF NOT EXISTS {$this->getSQLTable($id)} (
                _id SERIAL NOT NULL,
                _uid VARCHAR(255) NOT NULL,
                ". $sqlTenant ."
                \"_createdAt\" TIMESTAMP(3) DEFAULT NULL,
                \"_updatedAt\" TIMESTAMP(3) DEFAULT NULL,
                _permissions TEXT DEFAULT NULL,
                " . \implode(' ', $attributeStrings) . "
                PRIMARY KEY (_id)
            );
        ";
        if ($this->sharedTables) {
            $sql .= "
				CREATE UNIQUE INDEX \"{$namespace}_{$this->tenant}_{$id}_uid\" ON {$this->getSQLTable($id)} (LOWER(_uid), _tenant);
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_created\" ON {$this->getSQLTable($id)} (_tenant, \"_createdAt\");
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_updated\" ON {$this->getSQLTable($id)} (_tenant, \"_updatedAt\");
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_tenant_id\" ON {$this->getSQLTable($id)} (_tenant, _id);
			";
        } else {
            $sql .= "
				CREATE UNIQUE INDEX \"{$namespace}_{$id}_uid\" ON {$this->getSQLTable($id)} (LOWER(_uid));
            	CREATE INDEX \"{$namespace}_{$id}_created\" ON {$this->getSQLTable($id)} (\"_createdAt\");
            	CREATE INDEX \"{$namespace}_{$id}_updated\" ON {$this->getSQLTable($id)} (\"_updatedAt\");
			";
        }

        $sql = $this->trigger(Database::EVENT_COLLECTION_CREATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        try {
            $stmt->execute();

            $sql = "
				CREATE TABLE IF NOT EXISTS {$this->getSQLTable($id . '_perms')} (
					_id SERIAL NOT NULL,
					_tenant INTEGER DEFAULT NULL,
					_type VARCHAR(12) NOT NULL,
					_permission VARCHAR(255) NOT NULL,
					_document VARCHAR(255) NOT NULL,
					PRIMARY KEY (_id)
				);   
			";

            if ($this->sharedTables) {
                $sql .= "
					CREATE UNIQUE INDEX \"{$namespace}_{$this->tenant}_{$id}_ukey\" 
				    	ON {$this->getSQLTable($id. '_perms')} USING btree (_tenant,_document,_type,_permission);
					CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_permission\" 
				    	ON {$this->getSQLTable($id. '_perms')} USING btree (_tenant,_permission,_type); 
				";
            } else {
                $sql .= "
					CREATE UNIQUE INDEX \"{$namespace}_{$id}_ukey\" 
				    	ON {$this->getSQLTable($id. '_perms')} USING btree (_document,_type,_permission);
					CREATE INDEX \"{$namespace}_{$id}_permission\" 
				    	ON {$this->getSQLTable($id. '_perms')} USING btree (_permission,_type); 
				";
            }

            $sql = $this->trigger(Database::EVENT_COLLECTION_CREATE, $sql);

            $this->getPDO()
                ->prepare($sql)
                ->execute();

            foreach ($indexes as $index) {
                $indexId = $this->filter($index->getId());
                $indexType = $index->getAttribute('type');
                $indexAttributes = $index->getAttribute('attributes', []);
                $indexOrders = $index->getAttribute('orders', []);

                $this->createIndex(
                    $id,
                    $indexId,
                    $indexType,
                    $indexAttributes,
                    [],
                    $indexOrders
                );
            }
        } catch (Exception $e) {
            $this->getPDO()->rollBack();
            throw new DatabaseException('Failed to create collection: ' . $e->getMessage());
        }

        if (!$this->getPDO()->commit()) {
            throw new DatabaseException('Failed to commit transaction');
        }

        return true;
    }

    /**
     * Get Collection Size
     * @param string $collection
     * @return int
     * @throws DatabaseException
     *
     */
    public function getSizeOfCollection(string $collection): int
    {
        $collection = $this->filter($collection);
        $name = $this->getSQLTable($collection);
        $permissions = $this->getSQLTable($collection . '_perms');

        $collectionSize = $this->getPDO()->prepare("
             SELECT pg_total_relation_size(:name);
        ");

        $permissionsSize = $this->getPDO()->prepare("
             SELECT pg_total_relation_size(:permissions);
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

        return  $size;
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

        $sql = "DROP TABLE {$this->getSQLTable($id)}, {$this->getSQLTable($id . '_perms')}";
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
     * @param bool $array
     *
     * @return bool
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $type = $this->getSQLType($type, $size, $signed, $array);

        $sql = "
			ALTER TABLE {$this->getSQLTable($name)}
			ADD COLUMN \"{$id}\" {$type}
		";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

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
     *
     * @return bool
     */
    public function deleteAttribute(string $collection, string $id, bool $array = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        $sql = "
			ALTER TABLE {$this->getSQLTable($name)}
			DROP COLUMN \"{$id}\";
		";

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

        $sql = "
			ALTER TABLE {$this->getSQLTable($collection)} 
			RENAME COLUMN \"{$old}\" TO \"{$new}\"
		";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

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
     * @param string $newKey
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, string $newKey = null): bool
    {
        try {
            $name = $this->filter($collection);
            $id = $this->filter($id);
            $type = $this->getSQLType($type, $size, $signed, $array);

            $this->getPDO()->beginTransaction();

            if ($type == 'TIMESTAMP(3)') {
                $type = "TIMESTAMP(3) without time zone USING TO_TIMESTAMP(\"$id\", 'YYYY-MM-DD HH24:MI:SS.MS')";
            }

            if (!empty($newKey) && $id !== $newKey) {
                $newKey = $this->filter($newKey);

                $sql = "
                    ALTER TABLE {$this->getSQLTable($name)}
                    RENAME COLUMN \"{$id}\" TO \"{$newKey}\"
                ";

                $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

                $result = $this->getPDO()
                    ->prepare($sql)
                    ->execute();

                if (!$result) {
                    return false;
                }

                $id = $newKey;
            }

            $sql = "
                ALTER TABLE {$this->getSQLTable($name)}
                ALTER COLUMN \"{$id}\" TYPE {$type}
            ";

            $sql = $this->trigger(Database::EVENT_ATTRIBUTE_UPDATE, $sql);

            $result = $this->getPDO()
                ->prepare($sql)
                ->execute();

            if (!$this->getPDO()->commit()) {
                throw new DatabaseException('Failed to commit transaction');
            }

            return $result;
        } catch (\Throwable $e) {
            if($this->getPDO()->inTransaction()) {
                $this->getPDO()->rollBack();
            }

            throw $e;
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
                $sql = "ALTER TABLE {$table} ADD COLUMN \"{$id}\" {$sqlType} DEFAULT NULL;";

                if ($twoWay) {
                    $sql .= "ALTER TABLE {$relatedTable} ADD COLUMN \"{$twoWayKey}\" {$sqlType} DEFAULT NULL;";
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                $sql = "ALTER TABLE {$relatedTable} ADD COLUMN \"{$twoWayKey}\" {$sqlType} DEFAULT NULL;";
                break;
            case Database::RELATION_MANY_TO_ONE:
                $sql = "ALTER TABLE {$table} ADD COLUMN \"{$id}\" {$sqlType} DEFAULT NULL;";
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
                    $sql = "ALTER TABLE {$table} RENAME COLUMN \"{$key}\" TO \"{$newKey}\";";
                }
                if ($twoWay && $twoWayKey !== $newTwoWayKey) {
                    $sql .= "ALTER TABLE {$relatedTable} RENAME COLUMN \"{$twoWayKey}\" TO \"{$newTwoWayKey}\";";
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    if ($twoWayKey !== $newTwoWayKey) {
                        $sql = "ALTER TABLE {$relatedTable} RENAME COLUMN \"{$twoWayKey}\" TO \"{$newTwoWayKey}\";";
                    }
                } else {
                    if ($key !== $newKey) {
                        $sql = "ALTER TABLE {$table} RENAME COLUMN \"{$key}\" TO \"{$newKey}\";";
                    }
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    if ($twoWayKey !== $newTwoWayKey) {
                        $sql = "ALTER TABLE {$relatedTable} RENAME COLUMN \"{$twoWayKey}\" TO \"{$newTwoWayKey}\";";
                    }
                } else {
                    if ($key !== $newKey) {
                        $sql = "ALTER TABLE {$table} RENAME COLUMN \"{$key}\" TO \"{$newKey}\";";
                    }
                }
                break;
            case Database::RELATION_MANY_TO_MANY:
                $collection = $this->getDocument(Database::METADATA, $collection);
                $relatedCollection = $this->getDocument(Database::METADATA, $relatedCollection);

                $junction = $this->getSQLTable('_' . $collection->getInternalId() . '_' . $relatedCollection->getInternalId());

                if (!\is_null($newKey)) {
                    $sql = "ALTER TABLE {$junction} RENAME COLUMN \"{$key}\" TO \"{$newKey}\";";
                }
                if ($twoWay && !\is_null($newTwoWayKey)) {
                    $sql .= "ALTER TABLE {$junction} RENAME COLUMN \"{$twoWayKey}\" TO \"{$newTwoWayKey}\";";
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

        $sql = '';

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $sql = "ALTER TABLE {$table} DROP COLUMN \"{$key}\";";
                    if ($twoWay) {
                        $sql .= "ALTER TABLE {$relatedTable} DROP COLUMN \"{$twoWayKey}\";";
                    }
                } elseif ($side === Database::RELATION_SIDE_CHILD) {
                    $sql = "ALTER TABLE {$relatedTable} DROP COLUMN \"{$twoWayKey}\";";
                    if ($twoWay) {
                        $sql .= "ALTER TABLE {$table} DROP COLUMN \"{$key}\";";
                    }
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $sql = "ALTER TABLE {$relatedTable} DROP COLUMN \"{$twoWayKey}\";";
                } else {
                    $sql = "ALTER TABLE {$table} DROP COLUMN \"{$key}\";";
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    $sql = "ALTER TABLE {$relatedTable} DROP COLUMN \"{$twoWayKey}\";";
                } else {
                    $sql = "ALTER TABLE {$table} DROP COLUMN \"{$key}\";";
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
     * Create Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     * @param array<int> $lengths
     * @param array<string> $orders
     *
     * @return bool
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($id);

        foreach ($attributes as $i => $attr) {
            $order = empty($orders[$i]) || Database::INDEX_FULLTEXT === $type ? '' : $orders[$i];

            $attr = match ($attr) {
                '$id' => '_uid',
                '$createdAt' => '_createdAt',
                '$updatedAt' => '_updatedAt',
                default => $this->filter($attr),
            };

            if (Database::INDEX_UNIQUE === $type) {
                $attributes[$i] = "LOWER(\"{$attr}\"::text) {$order}";
            } else {
                $attributes[$i] = "\"{$attr}\" {$order}";
            }
        }

        $sqlType = match ($type) {
            Database::INDEX_KEY,
            Database::INDEX_FULLTEXT => 'INDEX',
            Database::INDEX_UNIQUE => 'UNIQUE INDEX',
            default => throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_FULLTEXT),
        };

        $key = "\"{$this->getNamespace()}_{$this->tenant}_{$collection}_{$id}\"";
        $attributes = \implode(', ', $attributes);

        if ($this->sharedTables && $type !== Database::INDEX_FULLTEXT) {
            // Add tenant as first index column for best performance
            $attributes = "_tenant, {$attributes}";
        }

        $sql = "CREATE {$sqlType} {$key} ON {$this->getSQLTable($collection)} ({$attributes});";
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
     *
     * @return bool
     * @throws Exception
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $schemaName = $this->getDatabase();

        $key = "\"{$this->getNamespace()}_{$this->tenant}_{$collection}_{$id}\"";

        $sql = "DROP INDEX IF EXISTS \"{$schemaName}\".{$key}";
        $sql = $this->trigger(Database::EVENT_INDEX_DELETE, $sql);

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
     * @throws PDOException
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $namespace = $this->getNamespace();
        $old = $this->filter($old);
        $new = $this->filter($new);
        $oldIndexName = "{$this->tenant}_{$collection}_{$old}";
        $newIndexName = "{$namespace}_{$this->tenant}_{$collection}_{$new}";

        $sql = "ALTER INDEX {$this->getSQLTable($oldIndexName)} RENAME TO \"{$newIndexName}\"";
        $sql = $this->trigger(Database::EVENT_INDEX_RENAME, $sql);

        return $this->getPDO()
            ->prepare($sql)
            ->execute();
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
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = \json_encode($document->getPermissions());

        if ($this->sharedTables) {
            $attributes['_tenant'] = $this->tenant;
        }

        $name = $this->filter($collection);
        $columns = '';
        $columnNames = '';

        $this->getPDO()->beginTransaction();

        /**
         * Insert Attributes
         */

        // Insert internal id if set
        if (!empty($document->getInternalId())) {
            $bindKey = '_id';
            $columns .= "\"_id\", ";
            $columnNames .= ':' . $bindKey . ', ';
        }

        $bindIndex = 0;
        foreach ($attributes as $attribute => $value) {
            $column = $this->filter($attribute);
            $bindKey = 'key_' . $bindIndex;
            $columns .= "\"{$column}\", ";
            $columnNames .= ':' . $bindKey . ', ';
            $bindIndex++;
        }

        $sql = "
			INSERT INTO {$this->getSQLTable($name)} ({$columns} \"_uid\")
			VALUES ({$columnNames} :_uid)
			RETURNING _id
		";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_CREATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

        if (!empty($document->getInternalId())) {
            $stmt->bindValue(':_id', $document->getInternalId(), PDO::PARAM_STR);
        }

        $attributeIndex = 0;
        foreach ($attributes as $value) {
            if (is_array($value)) {
                $value = \json_encode($value);
            }

            $bindKey = 'key_' . $attributeIndex;
            $value = (is_bool($value)) ? ($value ? "true" : "false") : $value;
            $stmt->bindValue(':' . $bindKey, $value, $this->getPDOType($value));
            $attributeIndex++;
        }

        $permissions = [];
        foreach (Database::PERMISSIONS as $type) {
            foreach ($document->getPermissionsByType($type) as $permission) {
                $permission = \str_replace('"', '', $permission);
                $sqlTenant = $this->sharedTables ? ', :_tenant' : '';
                $permissions[] = "('{$type}', '{$permission}', '{$document->getId()}' {$sqlTenant})";
            }
        }


        if (!empty($permissions)) {
            $permissions = \implode(', ', $permissions);
            $sqlTenant = $this->sharedTables ? ', _tenant' : '';

            $queryPermissions = "
				INSERT INTO {$this->getSQLTable($name . '_perms')} (_type, _permission, _document {$sqlTenant})
				VALUES {$permissions}
			";

            $queryPermissions = $this->trigger(Database::EVENT_PERMISSIONS_CREATE, $queryPermissions);
            $stmtPermissions = $this->getPDO()->prepare($queryPermissions);
            if($sqlTenant) {
                $stmtPermissions->bindValue(':_tenant', $this->tenant);
            }
        }

        try {
            $stmt->execute();

            $document['$internalId'] = $stmt->fetch()["_id"];

            if (isset($stmtPermissions)) {
                $stmtPermissions->execute();
            }
        } catch (Throwable $e) {
            switch ($e->getCode()) {
                case 23505:
                    $this->getPDO()->rollBack();
                    throw new Duplicate('Duplicated document: ' . $e->getMessage());
                default:
                    throw $e;
            }
        }

        if (!$this->getPDO()->commit()) {
            throw new DatabaseException('Failed to commit transaction');
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
     * @throws Duplicate
     */
    public function createDocuments(string $collection, array $documents, int $batchSize = Database::INSERT_BATCH_SIZE): array
    {
        if (empty($documents)) {
            return $documents;
        }

        $this->getPDO()->beginTransaction();

        try {
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

                    if($this->sharedTables) {
                        $attributes['_tenant'] = $this->tenant;
                    }

                    $columns = [];
                    foreach (\array_keys($attributes) as $key => $attribute) {
                        $columns[$key] = "\"{$this->filter($attribute)}\"";
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
                            $permissions[] = "('{$type}', '{$permission}', '{$document->getId()}', :_tenant)";
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
                        INSERT INTO {$this->getSQLTable($name . '_perms')} (_type, _permission, _document, _tenant) 
                        VALUES " . \implode(', ', $permissions)
                    );
                    $stmtPermissions->bindValue(':_tenant', $this->tenant);
                    $stmtPermissions?->execute();
                }
            }

            if (!$this->getPDO()->commit()) {
                throw new DatabaseException('Failed to commit transaction');
            }

            return $documents;

        } catch (PDOException $e) {
            $this->getPDO()->rollBack();

            throw match ($e->getCode()) {
                1062, 23000 => new Duplicate('Duplicated document: ' . $e->getMessage()),
                default => $e,
            };
        }
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
		";

        if ($this->sharedTables) {
            $sql .= ' AND _tenant = :_tenant';
        }

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
			";

            if ($this->sharedTables) {
                $sql .= ' AND _tenant = :_tenant';
            }

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
                    $sqlTenant = $this->sharedTables ? ', :_tenant' : '';
                    $values[] = "( :_uid, '{$type}', :_add_{$type}_{$i} {$sqlTenant})";
                }
            }

            $sqlTenant = $this->sharedTables ? ', _tenant' : '';

            $sql = "
				INSERT INTO {$this->getSQLTable($name . '_perms')} (_document, _type, _permission {$sqlTenant})
				VALUES" . \implode(', ', $values);

            $sql = $this->trigger(Database::EVENT_PERMISSIONS_CREATE, $sql);

            $stmtAddPermissions = $this->getPDO()->prepare($sql);
            $stmtAddPermissions->bindValue(":_uid", $document->getId());
            if ($this->sharedTables) {
                $stmtAddPermissions->bindValue(':_tenant', $this->tenant);
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
            $columns .= "\"{$column}\"" . '=:' . $bindKey . ',';
            $bindIndex++;
        }

        $sql = "
			UPDATE {$this->getSQLTable($name)}
			SET {$columns} _uid = :_uid 
			WHERE _uid = :_uid
		";

        if ($this->sharedTables) {
            $sql .= ' AND _tenant = :_tenant';
        }

        $sql = $this->trigger(Database::EVENT_DOCUMENT_UPDATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_uid', $document->getId());

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
        }

        $attributeIndex = 0;
        foreach ($attributes as $attribute => $value) {
            if (is_array($value)) {
                $value = json_encode($value);
            }

            $bindKey = 'key_' . $attributeIndex;
            $value = (is_bool($value)) ? ($value == true ? "true" : "false") : $value;
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
            switch ($e->getCode()) {
                case 1062:
                case 23505:
                    throw new Duplicate('Duplicated document: ' . $e->getMessage());

                default:
                    throw $e;
            }
        }

        if (!$this->getPDO()->commit()) {
            throw new DatabaseException('Failed to commit transaction');
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
     * @throws Duplicate
     */
    public function updateDocuments(string $collection, array $documents, int $batchSize = Database::INSERT_BATCH_SIZE): array
    {
        if (empty($documents)) {
            return $documents;
        }

        $this->getPDO()->beginTransaction();

        try {
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

                    if($this->sharedTables) {
                        $attributes['_tenant'] = $this->tenant;
                    }

                    $columns = \array_map(function ($attribute) {
                        return '"' . $this->filter($attribute) . '"';
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
                    $sql = "
                        SELECT _type, _permission
                        FROM {$this->getSQLTable($name . '_perms')}
                        WHERE _document = :_uid
                    ";

                    if ($this->sharedTables) {
                        $sql .= ' AND _tenant = :_tenant';
                    }

                    $sql = $this->trigger(Database::EVENT_PERMISSIONS_READ, $sql);

                    $permissionsStmt = $this->getPDO()->prepare($sql);
                    $permissionsStmt->bindValue(':_uid', $document->getId());

                    if ($this->sharedTables) {
                        $permissionsStmt->bindValue(':_tenant', $this->tenant);
                    }

                    $permissionsStmt->execute();
                    $permissions = $permissionsStmt->fetchAll();

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

                            $tenantQuery = '';
                            if ($this->sharedTables) {
                                $tenantQuery = ' AND _tenant = :_tenant';
                            }

                            $removeQuery .= "(
                                _document = :uid_{$index}
                                {$tenantQuery}
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

                                $tenantQuery = $this->sharedTables ? ', :_tenant' : '';

                                $addQuery .= "(:uid_{$index}, '{$type}', :{$bindKey} {$tenantQuery})";

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
                    $updateClause .= "{$column} = excluded.{$column}";
                }

                $sql = "
                    INSERT INTO {$this->getSQLTable($name)} (" . \implode(", ", $columns) . ") 
                    VALUES " . \implode(', ', $batchKeys) . "
                ";

                if ($this->sharedTables) {
                    $sql .= "ON CONFLICT (_tenant, LOWER(_uid)) DO UPDATE SET $updateClause";
                } else {
                    $sql .= "ON CONFLICT (LOWER(_uid)) DO UPDATE SET $updateClause";
                }

                $stmt = $this->getPDO()->prepare($sql);

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
                    if ($this->sharedTables) {
                        $stmtRemovePermissions->bindValue(':_tenant', $this->tenant);
                    }

                    $stmtRemovePermissions->execute();
                }

                if (!empty($addQuery)) {
                    $tenantQuery = $this->sharedTables ? ', _tenant' : '';

                    $stmtAddPermissions = $this->getPDO()->prepare("
                        INSERT INTO {$this->getSQLTable($name . '_perms')} (_document, _type, _permission {$tenantQuery})
                        VALUES {$addQuery}
                    ");

                    foreach ($addBindValues as $key => $value) {
                        $stmtAddPermissions->bindValue($key, $value, $this->getPDOType($value));
                    }

                    if($this->sharedTables) {
                        $stmtAddPermissions->bindValue(':_tenant', $this->tenant);
                    }

                    $stmtAddPermissions->execute();
                }
            }

            if (!$this->getPDO()->commit()) {
                throw new DatabaseException('Failed to commit transaction');
            }

            return $documents;
        } catch (PDOException $e) {
            $this->getPDO()->rollBack();

            throw match ($e->getCode()) {
                1062, 23000 => new Duplicate('Duplicated document: ' . $e->getMessage()),
                default => $e,
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

        $sqlMax = $max ? " AND \"{$attribute}\" <= {$max}" : "";
        $sqlMin = $min ? " AND \"{$attribute}\" >= {$min}" : "";

        $sql = "
			UPDATE {$this->getSQLTable($name)} 
			SET 
			    \"{$attribute}\" = \"{$attribute}\" + :val,
                \"_updatedAt\" = :updatedAt
			WHERE _uid = :_uid 
		";

        if ($this->sharedTables) {
            $sql .= ' AND _tenant = :_tenant';
        }

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
     *
     * @return bool
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $name = $this->filter($collection);

        $this->getPDO()->beginTransaction();

        $sql = "
			DELETE FROM {$this->getSQLTable($name)} 
			WHERE _uid = :_uid
		";

        if ($this->sharedTables) {
            $sql .= ' AND _tenant = :_tenant';
        }

        $sql = $this->trigger(Database::EVENT_DOCUMENT_DELETE, $sql);
        $stmt = $this->getPDO()->prepare($sql);
        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);

        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
        }

        $sql = "
			DELETE FROM {$this->getSQLTable($name . '_perms')} 
			WHERE _document = :_uid
		";

        if ($this->sharedTables) {
            $sql .= ' AND _tenant = :_tenant';
        }

        $sql = $this->trigger(Database::EVENT_PERMISSIONS_DELETE, $sql);

        $stmtPermissions = $this->getPDO()->prepare($sql);
        $stmtPermissions->bindValue(':_uid', $id);

        if ($this->sharedTables) {
            $stmtPermissions->bindValue(':_tenant', $this->tenant);
        }

        $deleted = false;

        try {
            if (!$stmt->execute()) {
                throw new DatabaseException('Failed to delete document');
            }

            $deleted = $stmt->rowCount();

            if (!$stmtPermissions->execute()) {
                throw new DatabaseException('Failed to delete permissions');
            }
        } catch (\Throwable $th) {
            $this->getPDO()->rollBack();
            throw new DatabaseException($th->getMessage());
        }

        if (!$this->getPDO()->commit()) {
            throw new DatabaseException('Failed to commit transaction');
        }

        return $deleted;
    }

    /**
     * Find Documents
     *
     * Find data sets using chosen queries
     *
     * @param string $collection
     * @param array<Query> $queries
     * @param int|null $limit
     * @param int|null $offset
     * @param array<string> $orderAttributes
     * @param array<string> $orderTypes
     * @param array<string, mixed> $cursor
     * @param string $cursorDirection
     *
     * @return array<Document>
     * @throws Exception
     * @throws PDOException
     * @throws Timeout
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
            '$tenant' => '_tenant',
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
                        table_main.\"{$attribute}\" {$this->getSQLOperator($orderMethod)} :cursor 
                        OR (
                            table_main.\"{$attribute}\" = :cursor 
                            AND
                            table_main._id {$this->getSQLOperator($orderMethodInternalId)} {$cursor['$internalId']}
                        )
                    )";
            } elseif ($cursorDirection === Database::CURSOR_BEFORE) {
                $orderType = $orderType === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
            }

            $orders[] = '"' . $attribute . '" ' . $orderType;
        }

        // Allow after pagination without any order
        if (empty($orderAttributes) && !empty($cursor)) {
            $orderType = $orderTypes[0] ?? Database::ORDER_ASC;
            $orderMethod = $cursorDirection === Database::CURSOR_AFTER ? (
                $orderType === Database::ORDER_DESC ? Query::TYPE_LESSER : Query::TYPE_GREATER
            ) : (
                $orderType === Database::ORDER_DESC ? Query::TYPE_GREATER : Query::TYPE_LESSER
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

        $conditions = $this->getSQLConditions($queries);
        if(!empty($conditions)) {
            $where[] = $conditions;
        }

        if ($this->sharedTables) {
            $where[] = "table_main._tenant = :_tenant";
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
                throw new DatabaseException("Order attribute '{$attribute}' is empty.");
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
            if (\array_key_exists('_tenant', $document)) {
                $results[$index]['$tenant'] = $document['_tenant'];
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
            $results = array_reverse($results);
        }

        return $results;
    }

    /**
     * Count Documents
     *
     * Count data set size using chosen queries
     *
     * @param string $collection
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int
     */
    public function count(string $collection, array $queries = [], ?int $max = null): int
    {
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = [];
        $limit = \is_null($max) ? '' : 'LIMIT :max';

        $conditions = $this->getSQLConditions($queries);
        if(!empty($conditions)) {
            $where[] = $conditions;
        }

        if ($this->sharedTables) {
            $where[] = "table_main._tenant = :_tenant";
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles);
        }

        $sqlWhere = !empty($where) ? 'WHERE ' . implode(' AND ', $where) : '';
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
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int|float
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

        if ($this->sharedTables) {
            $where[] = "table_main._tenant = :_tenant";
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
        if ($this->sharedTables) {
            $stmt->bindValue(':_tenant', $this->tenant);
        }

        if (!\is_null($max)) {
            $stmt->bindValue(':max', $max, PDO::PARAM_INT);
        }

        $stmt->execute();

        $result = $stmt->fetch();

        return $result['sum'] ?? 0;
    }

    /**
     * Get the SQL projection given the selected attributes
     *
     * @param string[] $selections
     * @param string $prefix
     * @return string
     * @throws Exception
     */
    protected function getAttributeProjection(array $selections, string $prefix = ''): string
    {
        if (empty($selections) || \in_array('*', $selections)) {
            if (!empty($prefix)) {
                return "\"{$prefix}\".*";
            }
            return '*';
        }

        // Remove $id ,$permissions and $collection from selections if present since they are always selected
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
                $selection = "\"{$prefix}\".\"{$this->filter($selection)}\"";
            }
        } else {
            foreach ($selections as &$selection) {
                $selection = "\"{$this->filter($selection)}\"";
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

        $attribute = "\"{$query->getAttribute()}\"";
        $placeholder = $this->getSQLPlaceholder($query);
        $operator = null;

        switch ($query->getMethod()) {
            case Query::TYPE_SEARCH:
                return "to_tsvector(regexp_replace({$attribute}, '[^\w]+',' ','g')) @@ websearch_to_tsquery(:{$placeholder}_0)";

            case Query::TYPE_BETWEEN:
                return "table_main.{$attribute} BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Query::TYPE_IS_NULL:
            case Query::TYPE_IS_NOT_NULL:
                return "table_main.{$attribute} {$this->getSQLOperator($query->getMethod())}";

            case Query::TYPE_CONTAINS:
                $operator = $query->onArray() ? '@>' : null;

                // no break
            default:
                $conditions = [];
                $operator = $operator ?? $this->getSQLOperator($query->getMethod());
                foreach ($query->getValues() as $key => $value) {
                    $conditions[] = $attribute.' '.$operator.' :'.$placeholder.'_'.$key;
                }
                $condition = implode(' OR ', $conditions);
                return empty($condition) ? '' : '(' . $condition . ')';
        }
    }

    /**
     * @param string $value
     * @return string
     */
    protected function getFulltextValue(string $value): string
    {
        $exact = str_ends_with($value, '"') && str_starts_with($value, '"');
        $value = str_replace(['@', '+', '-', '*', '.', "'", '"'], ' ', $value);
        $value = preg_replace('/\s+/', ' ', $value); // Remove multiple whitespaces
        $value = trim($value);

        if (!$exact) {
            $value = str_replace(' ', ' or ', $value);
        }

        return "'" . $value . "'";
    }

    /**
     * Get SQL Type
     *
     * @param string $type
     * @param int $size in chars
     * @param bool $signed
     * @param bool $array
     * @return string
     * @throws DatabaseException
     */
    protected function getSQLType(string $type, int $size, bool $signed = true, bool $array = false): string
    {
        if($array === true) {
            return 'JSONB';
        }

        switch ($type) {
            case Database::VAR_STRING:
                // $size = $size * 4; // Convert utf8mb4 size to bytes
                if ($size > $this->getMaxVarcharLength()) {
                    return 'TEXT';
                }

                return "VARCHAR({$size})";

            case Database::VAR_INTEGER:  // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554

                if ($size >= 8) { // INT = 4 bytes, BIGINT = 8 bytes
                    return 'BIGINT';
                }

                return 'INTEGER';

            case Database::VAR_FLOAT:
                return 'DOUBLE PRECISION';

            case Database::VAR_BOOLEAN:
                return 'BOOLEAN';

            case Database::VAR_RELATIONSHIP:
                return 'VARCHAR(255)';

            case Database::VAR_DATETIME:
                return 'TIMESTAMP(3)';

            default:
                throw new DatabaseException('Unknown Type: ' . $type);
        }
    }

    /**
     * Get SQL schema
     *
     * @return string
     */
    protected function getSQLSchema(): string
    {
        if (!$this->getSupportForSchemas()) {
            return '';
        }

        return "\"{$this->getDatabase()}\".";
    }

    /**
     * Get SQL table
     *
     * @param string $name
     * @return string
     */
    protected function getSQLTable(string $name): string
    {
        return "\"{$this->getDatabase()}\".\"{$this->getNamespace()}_{$name}\"";
    }

    /**
     * Get PDO Type
     *
     * @param mixed $value
     *
     * @return int
     * @throws DatabaseException
     */
    protected function getPDOType(mixed $value): int
    {
        return match (\gettype($value)) {
            'string', 'double' => PDO::PARAM_STR,
            'boolean' => PDO::PARAM_BOOL,
            'integer' => PDO::PARAM_INT,
            'NULL' => PDO::PARAM_NULL,
            default => throw new DatabaseException('Unknown PDO Type for ' . \gettype($value)),
        };
    }

    /**
     * Encode array
     *
     * @param string $value
     *
     * @return array<string>
     */
    protected function encodeArray(string $value): array
    {
        $string = substr($value, 1, -1);
        if (empty($string)) {
            return [];
        } else {
            return explode(',', $string);
        }
    }

    /**
     * Decode array
     *
     * @param array<string> $value
     *
     * @return string
     */
    protected function decodeArray(array $value): string
    {
        if (empty($value)) {
            return '{}';
        }

        foreach ($value as &$item) {
            $item = '"' . str_replace(['"', '(', ')'], ['\"', '\(', '\)'], $item) . '"';
        }

        return '{' . implode(",", $value) . '}';
    }

    /**
     * Is fulltext Wildcard index supported?
     *
     * @return bool
     */
    // TODO: Fix full-text search logic for postgres and MariaDB
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
        return true;
    }

    /**
     * Does the adapter handle Query Array Overlaps?
     *
     * @return bool
     */
    public function getSupportForJSONOverlaps(): bool
    {
        return false;
    }

    /**
     * Returns Max Execution Time
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
        $this->before($event, 'timeout', function ($sql) use ($milliseconds) {
            return "
				SET statement_timeout = {$milliseconds};
				{$sql};
				SET statement_timeout = 0;
			";
        });
    }

    /**
     * @param PDOException $e
     * @throws Timeout
     */
    protected function processException(PDOException $e): void
    {
        // Regular PDO
        if ($e->getCode() === '57014' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            throw new Timeout($e->getMessage(), $e->getCode(), $e);
        }

        // PDOProxy switches errorInfo PDOProxy.php line 64
        if ($e->getCode() === 7 && isset($e->errorInfo[0]) && $e->errorInfo[0] === '57014') {
            throw new Timeout($e->getMessage(), $e->getCode(), $e);
        }

        throw $e;
    }

    /**
     * @return string
     */
    public function getLikeOperator(): string
    {
        return 'ILIKE';
    }
}
