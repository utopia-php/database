<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Truncate as TruncateException;
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
                $result = true;
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
    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        try {
            $result = $this->getPDO()->rollBack();
            $this->inTransaction = 0;
        } catch (PDOException $e) {
            throw new DatabaseException('Failed to rollback transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }

        if (!$result) {
            throw new TransactionException('Failed to rollback transaction');
        }

        return $result;
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

        $this->timeout = $milliseconds;

        $this->before($event, 'timeout', function ($sql) use ($milliseconds) {
            return "
				SET statement_timeout = {$milliseconds};
				{$sql};
				SET statement_timeout = 0;
			";
        });
    }

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
     * @throws DuplicateException
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        /** @var array<string> $attributeStrings */
        $attributeStrings = [];

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

            $attributeStrings[] = "\"{$attrId}\" {$attrType}, ";
        }

        $sqlTenant = $this->sharedTables ? '_tenant INTEGER DEFAULT NULL,' : '';

        $collection = "
            CREATE TABLE {$this->getSQLTable($id)} (
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
            $collection .= "
				CREATE UNIQUE INDEX \"{$namespace}_{$this->tenant}_{$id}_uid\" ON {$this->getSQLTable($id)} (LOWER(_uid), _tenant);
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_created\" ON {$this->getSQLTable($id)} (_tenant, \"_createdAt\");
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_updated\" ON {$this->getSQLTable($id)} (_tenant, \"_updatedAt\");
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_tenant_id\" ON {$this->getSQLTable($id)} (_tenant, _id);
			";
        } else {
            $collection .= "
				CREATE UNIQUE INDEX \"{$namespace}_{$id}_uid\" ON {$this->getSQLTable($id)} (LOWER(_uid));
            	CREATE INDEX \"{$namespace}_{$id}_created\" ON {$this->getSQLTable($id)} (\"_createdAt\");
            	CREATE INDEX \"{$namespace}_{$id}_updated\" ON {$this->getSQLTable($id)} (\"_updatedAt\");
			";
        }

        $collection = $this->trigger(Database::EVENT_COLLECTION_CREATE, $collection);

        $permissions = "
            CREATE TABLE {$this->getSQLTable($id . '_perms')} (
                _id SERIAL NOT NULL,
                _tenant INTEGER DEFAULT NULL,
                _type VARCHAR(12) NOT NULL,
                _permission VARCHAR(255) NOT NULL,
                _document VARCHAR(255) NOT NULL,
                PRIMARY KEY (_id)
            );   
        ";

        if ($this->sharedTables) {
            $permissions .= "
                CREATE UNIQUE INDEX \"{$namespace}_{$this->tenant}_{$id}_ukey\" 
                    ON {$this->getSQLTable($id. '_perms')} USING btree (_tenant,_document,_type,_permission);
                CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_permission\" 
                    ON {$this->getSQLTable($id. '_perms')} USING btree (_tenant,_permission,_type); 
            ";
        } else {
            $permissions .= "
                CREATE UNIQUE INDEX \"{$namespace}_{$id}_ukey\" 
                    ON {$this->getSQLTable($id. '_perms')} USING btree (_document,_type,_permission);
                CREATE INDEX \"{$namespace}_{$id}_permission\" 
                    ON {$this->getSQLTable($id. '_perms')} USING btree (_permission,_type); 
            ";
        }

        $permissions = $this->trigger(Database::EVENT_COLLECTION_CREATE, $permissions);

        try {
            $this->getPDO()
                ->prepare($collection)
                ->execute();

            $this->getPDO()
                ->prepare($permissions)
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
        } catch (PDOException $e) {
            $e = $this->processException($e);

            if (!($e instanceof DuplicateException)) {
                $this->getPDO()
                    ->prepare("DROP TABLE IF EXISTS {$this->getSQLTable($id)}, {$this->getSQLTable($id . '_perms')};")
                    ->execute();
            }

            throw $e;
        }

        return true;
    }

    /**
     * Get Collection Size on disk
     * @param string $collection
     * @return int
     * @throws DatabaseException
     */
    public function getSizeOfCollectionOnDisk(string $collection): int
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
     * Get Collection Size of raw data
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
             SELECT pg_relation_size(:name);
        ");

        $permissionsSize = $this->getPDO()->prepare("
             SELECT pg_relation_size(:permissions);
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
     * Create Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $array
     *
     * @return bool
     * @throws Exception
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
     *
     * @return bool
     * @throws DatabaseException
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

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            if ($e->getCode() === "42703" && $e->errorInfo[1] === 7) {
                return true;
            }

            throw $e;
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
     * @param string|null $newKey
     * @return bool
     * @throws Exception
     * @throws PDOException
     */
    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $newKey = empty($newKey) ? null : $this->filter($newKey);
        $type = $this->getSQLType($type, $size, $signed, $array);

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

        try {
            $result = $this->getPDO()
            ->prepare($sql)
            ->execute();

            return $result;
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

        try {
            return $this->getPDO()
                ->prepare($sql)
                ->execute();
        } catch (PDOException $e) {
            throw $this->processException($e);
        }
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
            $attributes['_tenant'] = $document->getTenant();
        }

        $name = $this->filter($collection);
        $columns = '';
        $columnNames = '';

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
            if (\is_array($value)) {
                $value = \json_encode($value);
            }

            $bindKey = 'key_' . $attributeIndex;
            $value = (\is_bool($value)) ? ($value ? "true" : "false") : $value;
            $stmt->bindValue(':' . $bindKey, $value, $this->getPDOType($value));
            $attributeIndex++;
        }

        $permissions = [];
        foreach (Database::PERMISSIONS as $type) {
            foreach ($document->getPermissionsByType($type) as $permission) {
                $permission = \str_replace('"', '', $permission);
                $sqlTenant = $this->sharedTables ? ', :_tenant' : '';
                $permissions[] = "('{$type}', '{$permission}', :_uid {$sqlTenant})";
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
            $stmtPermissions->bindValue(':_uid', $document->getId());
            if ($sqlTenant) {
                $stmtPermissions->bindValue(':_tenant', $document->getTenant());
            }
        }

        try {
            $stmt->execute();

            $document['$internalId'] = $stmt->fetch()["_id"];

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
                $columns[$key] = "\"{$this->filter($attribute)}\"";
            }
            $columns = '(' . \implode(', ', $columns) . ')';

            $internalIds = [];

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

                if (!empty($document->getInternalId())) {
                    $internalIds[$document->getId()] = true;
                    $attributes['_id'] = $document->getInternalId();
                    $attributeKeys[] = '_id';
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
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        foreach ($documents as $document) {
            if (!isset($internalIds[$document->getId()])) {
                $document['$internalId'] = $this->getDocument(
                    $collection,
                    $document->getId(),
                    [Query::select(['$internalId'])]
                )->getInternalId();
            }
        }

        return $documents;
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param string $id
     * @param Document $document
     *
     * @return Document
     * @throws DatabaseException
     * @throws DuplicateException
     */
    public function updateDocument(string $collection, string $id, Document $document): Document
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
            $whereTenant = "(_tenant = :_tenant";

            if ($collection === Database::METADATA) {
                $whereTenant .= " OR _tenant IS NULL";
            }

            $where[] = $whereTenant . ')';
        }

        $sqlWhere = 'WHERE ' . implode(' AND ', $where);

        $bindIndex = 0;
        foreach ($attributes as $attribute => $value) {
            $column = $this->filter($attribute);
            $bindKey = 'key_' . $bindIndex;
            $columns .= "\"{$column}\"" . '=:' . $bindKey;

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
                    INSERT INTO {$this->getSQLTable($name . '_perms')} (\"_document\", \"_type\", \"_permission\"
                ";

                if ($this->sharedTables) {
                    $sqlAddPermissions .= ', "_tenant")';
                } else {
                    $sqlAddPermissions .= ')';
                }

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
        }

        return $affected;
    }

    /**
     * @param string $collection
     * @param string $attribute
     * @param array<Document> $documents
     * @return array<Document>
     */
    public function createOrUpdateDocuments(string $collection, string $attribute, array $documents): array
    {
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

        $sqlMax = $max ? " AND \"{$attribute}\" <= {$max}" : "";
        $sqlMin = $min ? " AND \"{$attribute}\" >= {$min}" : "";

        $sql = "
			UPDATE {$this->getSQLTable($name)} 
			SET 
			    \"{$attribute}\" = \"{$attribute}\" + :val,
                \"_updatedAt\" = :updatedAt
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
     *
     * @return bool
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $name = $this->filter($collection);

        $sql = "
			DELETE FROM {$this->getSQLTable($name)} 
			WHERE _uid = :_uid
			{$this->getTenantQuery($collection)}
		";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_DELETE, $sql);
        $stmt = $this->getPDO()->prepare($sql);
        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);

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
            throw new DatabaseException($th->getMessage());
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
                WHERE _document IN (" . \implode(', ', \array_map(fn ($id) => ":_id_{$id}", \array_keys($ids))) . ")
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
     * @param string $forPermission
     *
     * @return array<Document>
     * @throws DatabaseException
     * @throws TimeoutException

     * @throws TimeoutException
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
        if (!empty($conditions)) {
            $where[] = $conditions;
        }

        if ($this->sharedTables) {
            $orIsNull = '';

            if ($collection === Database::METADATA) {
                $orIsNull = " OR table_main._tenant IS NULL";
            }

            $where[] = "(table_main._tenant = :_tenant {$orIsNull})";
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles, $forPermission);
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
                $results[$index]['$tenant'] = $document['_tenant'] === null ? null : (int)$document['_tenant'];
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

        $queries = array_map(fn ($query) => clone $query, $queries);

        $conditions = $this->getSQLConditions($queries);
        if (!empty($conditions)) {
            $where[] = $conditions;
        }

        if ($this->sharedTables) {
            $orIsNull = '';

            if ($collection === Database::METADATA) {
                $orIsNull = " OR table_main._tenant IS NULL";
            }

            $where[] = "(table_main._tenant = :_tenant {$orIsNull})";
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

        $queries = array_map(fn ($query) => clone $query, $queries);

        foreach ($queries as $query) {
            $where[] = $this->getSQLCondition($query);
        }

        if ($this->sharedTables) {
            $orIsNull = '';

            if ($collection === Database::METADATA) {
                $orIsNull = " OR table_main._tenant IS NULL";
            }

            $where[] = "(table_main._tenant = :_tenant {$orIsNull})";
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
        if ($array === true) {
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

    public function getMinDateTime(): \DateTime
    {
        return new \DateTime('-4713-01-01 00:00:00');
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
     * Is get schema attributes supported?
     *
     * @return bool
     */
    public function getSupportForSchemaAttributes(): bool
    {
        return false;
    }

    public function getSupportForUpserts(): bool
    {
        return false;
    }

    /**
     * @return string
     */
    public function getLikeOperator(): string
    {
        return 'ILIKE';
    }



    protected function processException(PDOException $e): \Exception
    {
        // Timeout
        if ($e->getCode() === '57014' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new TimeoutException('Query timed out', $e->getCode(), $e);
        }

        // Duplicate table
        if ($e->getCode() === '42P07' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new DuplicateException('Collection already exists', $e->getCode(), $e);
        }

        // Duplicate column
        if ($e->getCode() === '42701' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new DuplicateException('Attribute already exists', $e->getCode(), $e);
        }

        // Duplicate row
        if ($e->getCode() === '23505' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new DuplicateException('Document already exists', $e->getCode(), $e);
        }

        // Data is too big for column resize
        if ($e->getCode() === '22001' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new TruncateException('Resize would result in data truncation', $e->getCode(), $e);
        }

        return $e;
    }

    /**
     * @return string
     */
    public function getConnectionId(): string
    {
        $stmt = $this->getPDO()->query("SELECT pg_backend_pid();");
        return $stmt->fetchColumn();
    }
}
