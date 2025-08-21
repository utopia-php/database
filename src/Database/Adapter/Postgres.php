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
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Helpers\ID;
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

    protected function execute(mixed $stmt): bool
    {
        $pdo = $this->getPDO();

        // Choose the right SET command based on transaction state
        $sql = $this->inTransaction === 0
            ? "SET statement_timeout = '{$this->timeout}ms'"
            : "SET LOCAL statement_timeout = '{$this->timeout}ms'";

        // Apply timeout
        $pdo->exec($sql);

        try {
            return $stmt->execute();
        } finally {
            // Only reset the global timeout when not in a transaction
            if ($this->inTransaction === 0) {
                $pdo->exec("RESET statement_timeout");
            }
        }
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

        $dbCreation = $this->getPDO()
            ->prepare($sql)
            ->execute();

        // extension for supporting spatial types
        $this->getPDO()->prepare('CREATE EXTENSION IF NOT EXISTS postgis;')->execute();

        $collation = "
            CREATE COLLATION IF NOT EXISTS utf8_ci (
            provider = icu,
            locale   = 'und-u-ks-primary',
            deterministic = false
            );
        ";
        $this->getPDO()->prepare($collation)->execute();
        return $dbCreation;
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

        return $this->getPDO()->prepare($sql)->execute();
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
                $attribute->getAttribute('array', false),
                $attribute->getAttribute('required', false)
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
                " . $sqlTenant . "
                \"_createdAt\" TIMESTAMP(3) DEFAULT NULL,
                \"_updatedAt\" TIMESTAMP(3) DEFAULT NULL,
                _permissions TEXT DEFAULT NULL,
                " . \implode(' ', $attributeStrings) . "
                PRIMARY KEY (_id)
            );
        ";

        if ($this->sharedTables) {
            $collection .= "
				CREATE UNIQUE INDEX \"{$namespace}_{$this->tenant}_{$id}_uid\" ON {$this->getSQLTable($id)} (\"_uid\", \"_tenant\");
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_created\" ON {$this->getSQLTable($id)} (_tenant, \"_createdAt\");
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_updated\" ON {$this->getSQLTable($id)} (_tenant, \"_updatedAt\");
            	CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_tenant_id\" ON {$this->getSQLTable($id)} (_tenant, _id);
			";
        } else {
            $collection .= "
				CREATE UNIQUE INDEX \"{$namespace}_{$id}_uid\" ON {$this->getSQLTable($id)} (\"_uid\");
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
                    ON {$this->getSQLTable($id . '_perms')} USING btree (_tenant,_document,_type,_permission);
                CREATE INDEX \"{$namespace}_{$this->tenant}_{$id}_permission\" 
                    ON {$this->getSQLTable($id . '_perms')} USING btree (_tenant,_permission,_type); 
            ";
        } else {
            $permissions .= "
                CREATE UNIQUE INDEX \"{$namespace}_{$id}_ukey\" 
                    ON {$this->getSQLTable($id . '_perms')} USING btree (_document,_type,_permission);
                CREATE INDEX \"{$namespace}_{$id}_permission\" 
                    ON {$this->getSQLTable($id . '_perms')} USING btree (_permission,_type); 
            ";
        }

        $permissions = $this->trigger(Database::EVENT_COLLECTION_CREATE, $permissions);

        try {
            $this->getPDO()->prepare($collection)->execute();

            $this->getPDO()->prepare($permissions)->execute();

            foreach ($indexes as $index) {
                $indexId = $this->filter($index->getId());
                $indexType = $index->getAttribute('type');
                $indexAttributes = $index->getAttribute('attributes', []);
                $indexAttributesWithType = [];
                foreach ($indexAttributes as $indexAttribute) {
                    foreach ($attributes as $attribute) {
                        if ($attribute->getId() === $indexAttribute) {
                            $indexAttributesWithType[$indexAttribute] = $attribute->getAttribute('type');
                        }
                    }
                }
                $indexOrders = $index->getAttribute('orders', []);
                if ($indexType === Database::INDEX_SPATIAL && count($indexOrders)) {
                    throw new DatabaseException('Spatial indexes with explicit orders are not supported. Remove the orders to create this index.');
                }
                $this->createIndex(
                    $id,
                    $indexId,
                    $indexType,
                    $indexAttributes,
                    [],
                    $indexOrders,
                    $indexAttributesWithType
                );
            }
        } catch (PDOException $e) {
            $e = $this->processException($e);

            if (!($e instanceof DuplicateException)) {
                $this->execute($this->getPDO()
                    ->prepare("DROP TABLE IF EXISTS {$this->getSQLTable($id)}, {$this->getSQLTable($id . '_perms')};"));
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
            $this->execute($collectionSize);
            $this->execute($permissionsSize);
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
            $this->execute($collectionSize);
            $this->execute($permissionsSize);
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

        return $this->getPDO()->prepare($sql)->execute();
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
     * @param bool $signed
     * @param bool $array
     *
     * @return bool
     * @throws DatabaseException
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);
        $type = $this->getSQLType($type, $size, $signed, $array, $required);

        $sql = "
			ALTER TABLE {$this->getSQLTable($name)}
			ADD COLUMN \"{$id}\" {$type}
		";

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_CREATE, $sql);

        try {
            return $this->execute($this->getPDO()
                ->prepare($sql));
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
            return $this->execute($this->getPDO()
                ->prepare($sql));
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

        return $this->execute($this->getPDO()
            ->prepare($sql));
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
        $type = $this->getSQLType($type, $size, $signed, $array, false);

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

            $result = $this->execute($this->getPDO()
                ->prepare($sql));

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
            $result = $this->execute($this->getPDO()
                ->prepare($sql));

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
        $sqlType = $this->getSQLType(Database::VAR_RELATIONSHIP, 0, false, false, false);

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

        return $this->execute($this->getPDO()
            ->prepare($sql));
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
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collection = $this->getDocument($metadataCollection, $collection);
                $relatedCollection = $this->getDocument($metadataCollection, $relatedCollection);

                $junction = $this->getSQLTable('_' . $collection->getSequence() . '_' . $relatedCollection->getSequence());

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

        return $this->execute($this->getPDO()
            ->prepare($sql));
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
                $metadataCollection = new Document(['$id' => Database::METADATA]);
                $collection = $this->getDocument($metadataCollection, $collection);
                $relatedCollection = $this->getDocument($metadataCollection, $relatedCollection);

                $junction = $side === Database::RELATION_SIDE_PARENT
                    ? $this->getSQLTable('_' . $collection->getSequence() . '_' . $relatedCollection->getSequence())
                    : $this->getSQLTable('_' . $relatedCollection->getSequence() . '_' . $collection->getSequence());

                $perms = $side === Database::RELATION_SIDE_PARENT
                    ? $this->getSQLTable('_' . $collection->getSequence() . '_' . $relatedCollection->getSequence() . '_perms')
                    : $this->getSQLTable('_' . $relatedCollection->getSequence() . '_' . $collection->getSequence() . '_perms');

                $sql = "DROP TABLE {$junction}; DROP TABLE {$perms}";
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        if (empty($sql)) {
            return true;
        }

        $sql = $this->trigger(Database::EVENT_ATTRIBUTE_DELETE, $sql);

        return $this->execute($this->getPDO()
            ->prepare($sql));
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
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = []): bool
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
                if (isset($indexAttributeTypes[$attr]) && $indexAttributeTypes[$attr] === Database::VAR_STRING) {
                    $attributes[$i] = "\"{$attr}\" COLLATE utf8_ci {$order}";
                } else {
                    $attributes[$i] = "\"{$attr}\" {$order}";
                }
            } else {
                $attributes[$i] = "\"{$attr}\" {$order}";
            }
        }

        $sqlType = match ($type) {
            Database::INDEX_KEY,
            Database::INDEX_FULLTEXT => 'INDEX',
            Database::INDEX_UNIQUE => 'UNIQUE INDEX',
            Database::INDEX_SPATIAL => 'INDEX',
            default => throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_FULLTEXT . ', ' . Database::INDEX_SPATIAL),
        };

        $key = "\"{$this->getNamespace()}_{$this->tenant}_{$collection}_{$id}\"";
        $attributes = \implode(', ', $attributes);

        // Spatial indexes can't include _tenant because GIST indexes require all columns to have compatible operator classes
        if ($this->sharedTables && $type !== Database::INDEX_FULLTEXT && $type !== Database::INDEX_SPATIAL) {
            // Add tenant as first index column for best performance
            $attributes = "_tenant, {$attributes}";
        }

        $sql = "CREATE {$sqlType} {$key} ON {$this->getSQLTable($collection)}";

        // Add USING GIST for spatial indexes
        if ($type === Database::INDEX_SPATIAL) {
            $sql .= " USING GIST";
        }

        $sql .= " ({$attributes});";

        $sql = $this->trigger(Database::EVENT_INDEX_CREATE, $sql);

        try {
            return $this->getPDO()->prepare($sql)->execute();
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

        return $this->execute($this->getPDO()
            ->prepare($sql));
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

        return $this->execute($this->getPDO()
            ->prepare($sql));
    }

    /**
     * Create Document
     *
     * @param Document $collection
     * @param Document $document
     *
     * @return Document
     */
    public function createDocument(Document $collection, Document $document): Document
    {
        $collection = $collection->getId();
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
        if (!empty($document->getSequence())) {
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
		";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_CREATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

        if (!empty($document->getSequence())) {
            $stmt->bindValue(':_id', $document->getSequence(), PDO::PARAM_STR);
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
            $this->execute($stmt);
            $lastInsertedId = $this->getPDO()->lastInsertId();
            // Sequence can be manually set as well
            $document['$sequence'] ??= $lastInsertedId;

            if (isset($stmtPermissions)) {
                $this->execute($stmtPermissions);
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
    }

    /**
     * Update Document
     *
     *
     * @param Document $collection
     * @param string $id
     * @param Document $document
     * @param bool $skipPermissions
     * @return Document
     * @throws DatabaseException
     * @throws DuplicateException
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        $collection = $collection->getId();
        $attributes = $document->getAttributes();
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = json_encode($document->getPermissions());

        $name = $this->filter($collection);
        $columns = '';

        if (!$skipPermissions) {
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

            $this->execute($permissionsStmt);
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
			WHERE _id=:_sequence
			{$this->getTenantQuery($collection)}
		";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_UPDATE, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        $stmt->bindValue(':_sequence', $document->getSequence());
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
            $this->execute($stmt);
            if (isset($stmtRemovePermissions)) {
                $this->execute($stmtRemovePermissions);
            }
            if (isset($stmtAddPermissions)) {
                $this->execute($stmtAddPermissions);
            }
        } catch (PDOException $e) {
            throw $this->processException($e);
        }

        return $document;
    }

    /**
     * @param string $tableName
     * @param string $columns
     * @param array<string> $batchKeys
     * @param array<string> $attributes
     * @param array<mixed> $bindValues
     * @param string $attribute
     * @return mixed
     */
    protected function getUpsertStatement(
        string $tableName,
        string $columns,
        array $batchKeys,
        array $attributes,
        array $bindValues,
        string $attribute = '',
    ): mixed {
        $getUpdateClause = function (string $attribute, bool $increment = false): string {
            $attribute = $this->quote($this->filter($attribute));
            if ($increment) {
                $new = "target.{$attribute} + EXCLUDED.{$attribute}";
            } else {
                $new = "EXCLUDED.{$attribute}";
            }

            if ($this->sharedTables) {
                return "{$attribute} = CASE WHEN target._tenant = EXCLUDED._tenant THEN {$new} ELSE target.{$attribute} END";
            }

            return "{$attribute} = {$new}";
        };
        if (!empty($attribute)) {
            // Increment specific column by its new value in place
            $updateColumns = [
                $getUpdateClause($attribute, increment: true),
                $getUpdateClause('_updatedAt'),
            ];
        } else {
            // Update all columns
            $updateColumns = [];
            foreach (array_keys($attributes) as $attr) {
                /**
                 * @var string $attr
                 */
                $updateColumns[] = $getUpdateClause($this->filter($attr));
            }
        }

        $conflictKeys = $this->sharedTables ? '("_uid", _tenant)' : '("_uid")';

        $stmt = $this->getPDO()->prepare(
            "
            INSERT INTO {$this->getSQLTable($tableName)} AS target {$columns}
            VALUES " . implode(', ', $batchKeys) . "
            ON CONFLICT {$conflictKeys} DO UPDATE
                SET " . implode(', ', $updateColumns)
        );

        foreach ($bindValues as $key => $binding) {
            $stmt->bindValue($key, $binding, $this->getPDOType($binding));
        }
        return $stmt;
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

        $this->execute($stmt) || throw new DatabaseException('Failed to update attribute');
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
            if (!$this->execute($stmt)) {
                throw new DatabaseException('Failed to delete document');
            }

            $deleted = $stmt->rowCount();

            if (!$this->execute($stmtPermissions)) {
                throw new DatabaseException('Failed to delete permissions');
            }
        } catch (\Throwable $th) {
            throw new DatabaseException($th->getMessage());
        }

        return $deleted;
    }

    /**
     * Find Documents
     *
     * @param Document $collection
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
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        $spatialAttributes = $this->getSpatialAttributes($collection);
        $attributes = $collection->getAttribute('attributes', []);
        $collection = $collection->getId();
        $name = $this->filter($collection);
        $roles = Authorization::getRoles();
        $where = [];
        $orders = [];
        $alias = Query::DEFAULT_ALIAS;
        $binds = [];

        $queries = array_map(fn ($query) => clone $query, $queries);

        $cursorWhere = [];

        foreach ($orderAttributes as $i => $originalAttribute) {
            $attribute = $this->getInternalKeyForAttribute($originalAttribute);
            $attribute = $this->filter($attribute);

            $orderType = $this->filter($orderTypes[$i] ?? Database::ORDER_ASC);
            $direction = $orderType;

            if ($cursorDirection === Database::CURSOR_BEFORE) {
                $direction = ($direction === Database::ORDER_ASC)
                    ? Database::ORDER_DESC
                    : Database::ORDER_ASC;
            }

            $orders[] = "{$this->quote($attribute)} {$direction}";

            // Build pagination WHERE clause only if we have a cursor
            if (!empty($cursor)) {
                // Special case: only 1 attribute and it's a unique primary key
                if (count($orderAttributes) === 1 && $i === 0 && $originalAttribute === '$sequence') {
                    $operator = ($direction === Database::ORDER_DESC)
                        ? Query::TYPE_LESSER
                        : Query::TYPE_GREATER;

                    $bindName = ":cursor_pk";
                    $binds[$bindName] = $cursor[$originalAttribute];

                    $cursorWhere[] = "{$this->quote($alias)}.{$this->quote($attribute)} {$this->getSQLOperator($operator)} {$bindName}";
                    break;
                }

                $conditions = [];

                // Add equality conditions for previous attributes
                for ($j = 0; $j < $i; $j++) {
                    $prevOriginal = $orderAttributes[$j];
                    $prevAttr = $this->filter($this->getInternalKeyForAttribute($prevOriginal));

                    $bindName = ":cursor_{$j}";
                    $binds[$bindName] = $cursor[$prevOriginal];

                    $conditions[] = "{$this->quote($alias)}.{$this->quote($prevAttr)} = {$bindName}";
                }

                // Add comparison for current attribute
                $operator = ($direction === Database::ORDER_DESC)
                    ? Query::TYPE_LESSER
                    : Query::TYPE_GREATER;

                $bindName = ":cursor_{$i}";
                $binds[$bindName] = $cursor[$originalAttribute];

                $conditions[] = "{$this->quote($alias)}.{$this->quote($attribute)} {$this->getSQLOperator($operator)} {$bindName}";

                $cursorWhere[] = '(' . implode(' AND ', $conditions) . ')';
            }
        }

        if (!empty($cursorWhere)) {
            $where[] = '(' . implode(' OR ', $cursorWhere) . ')';
        }

        $conditions = $this->getSQLConditions($queries, $binds, attributes:$attributes);
        if (!empty($conditions)) {
            $where[] = $conditions;
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles, $alias, $forPermission);
        }

        if ($this->sharedTables) {
            $binds[':_tenant'] = $this->tenant;
            $where[] = "{$this->getTenantQuery($collection, $alias, condition: '')}";
        }

        $sqlWhere = !empty($where) ? 'WHERE ' . implode(' AND ', $where) : '';
        $sqlOrder = 'ORDER BY ' . implode(', ', $orders);

        $sqlLimit = '';
        if (! \is_null($limit)) {
            $binds[':limit'] = $limit;
            $sqlLimit = 'LIMIT :limit';
        }

        if (! \is_null($offset)) {
            $binds[':offset'] = $offset;
            $sqlLimit .= ' OFFSET :offset';
        }

        $selections = $this->getAttributeSelections($queries);

        $sql = "
            SELECT {$this->getAttributeProjection($selections, $alias, $spatialAttributes)}
            FROM {$this->getSQLTable($name)} AS {$this->quote($alias)}
            {$sqlWhere}
            {$sqlOrder}
            {$sqlLimit};
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_FIND, $sql);

        try {
            $stmt = $this->getPDO()->prepare($sql);

            foreach ($binds as $key => $value) {
                if (gettype($value) === 'double') {
                    $stmt->bindValue($key, $this->getFloatPrecision($value), PDO::PARAM_STR);
                } else {
                    $stmt->bindValue($key, $value, $this->getPDOType($value));
                }
            }
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
                $results[$index]['$sequence'] = $document['_id'];
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
        $binds = [];
        $where = [];
        $alias = Query::DEFAULT_ALIAS;

        $limit = '';
        if (! \is_null($max)) {
            $binds[':limit'] = $max;
            $limit = 'LIMIT :limit';
        }

        $queries = array_map(fn ($query) => clone $query, $queries);

        $conditions = $this->getSQLConditions($queries, $binds);
        if (!empty($conditions)) {
            $where[] = $conditions;
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles, $alias);
        }

        if ($this->sharedTables) {
            $binds[':_tenant'] = $this->tenant;
            $where[] = "{$this->getTenantQuery($collection, $alias, condition: '')}";
        }

        $sqlWhere = !empty($where)
            ? 'WHERE ' . \implode(' AND ', $where)
            : '';

        $sql = "
			SELECT COUNT(1) as sum FROM (
				SELECT 1
				FROM {$this->getSQLTable($name)} AS {$this->quote($alias)}
				{$sqlWhere}
				{$limit}
			) table_count
        ";


        $sql = $this->trigger(Database::EVENT_DOCUMENT_COUNT, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($binds as $key => $value) {
            $stmt->bindValue($key, $value, $this->getPDOType($value));
        }

        $this->execute($stmt);

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
        $alias = Query::DEFAULT_ALIAS;
        $binds = [];

        $limit = '';
        if (! \is_null($max)) {
            $binds[':limit'] = $max;
            $limit = 'LIMIT :limit';
        }

        $queries = array_map(fn ($query) => clone $query, $queries);

        $conditions = $this->getSQLConditions($queries, $binds);
        if (!empty($conditions)) {
            $where[] = $conditions;
        }

        if (Authorization::$status) {
            $where[] = $this->getSQLPermissionsCondition($name, $roles, $alias);
        }

        if ($this->sharedTables) {
            $binds[':_tenant'] = $this->tenant;
            $where[] = "{$this->getTenantQuery($collection, $alias, condition: '')}";
        }

        $sqlWhere = !empty($where)
            ? 'WHERE ' . \implode(' AND ', $where)
            : '';

        $sql = "
			SELECT SUM({$this->quote($attribute)}) as sum FROM (
				SELECT {$this->quote($attribute)}
				FROM {$this->getSQLTable($name)} AS {$this->quote($alias)}
				{$sqlWhere}
				{$limit}
			) table_count
        ";

        $sql = $this->trigger(Database::EVENT_DOCUMENT_SUM, $sql);

        $stmt = $this->getPDO()->prepare($sql);

        foreach ($binds as $key => $value) {
            $stmt->bindValue($key, $value, $this->getPDOType($value));
        }

        $this->execute($stmt);

        $result = $stmt->fetchAll();
        $stmt->closeCursor();
        if (!empty($result)) {
            $result = $result[0];
        }

        return $result['sum'] ?? 0;
    }

    /**
     * @return string
     */
    public function getConnectionId(): string
    {
        $stmt = $this->getPDO()->query("SELECT pg_backend_pid();");
        return $stmt->fetchColumn();
    }

    /**
     * Handle spatial queries
     *
     * @param Query $query
     * @param array<string, mixed> $binds
     * @param string $attribute
     * @param string $alias
     * @param string $placeholder
     * @return string
     */
    protected function handleSpatialQueries(Query $query, array &$binds, string $attribute, string $alias, string $placeholder): string
    {
        switch ($query->getMethod()) {
            case Query::TYPE_CROSSES:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "ST_Crosses({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_NOT_CROSSES:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "NOT ST_Crosses({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_DISTANCE:
                $distanceParams = $query->getValues()[0];
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($distanceParams[0]);
                $binds[":{$placeholder}_1"] = $distanceParams[1];
                return "ST_DWithin({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0), :{$placeholder}_1)";

            case Query::TYPE_NOT_DISTANCE:
                $distanceParams = $query->getValues()[0];
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($distanceParams[0]);
                $binds[":{$placeholder}_1"] = $distanceParams[1];
                return "NOT ST_DWithin({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0), :{$placeholder}_1)";

            case Query::TYPE_DISTANCE_GREATER_THAN:
                $distanceParams = $query->getValues()[0];
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($distanceParams[0]);
                $binds[":{$placeholder}_1"] = $distanceParams[1];
                return "ST_Distance({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0)) > :{$placeholder}_1";

            case Query::TYPE_DISTANCE_LESS_THAN:
                $distanceParams = $query->getValues()[0];
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($distanceParams[0]);
                $binds[":{$placeholder}_1"] = $distanceParams[1];
                return "ST_Distance({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0)) < :{$placeholder}_1";

            case Query::TYPE_EQUAL:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "ST_Equals({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_NOT_EQUAL:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "NOT ST_Equals({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_INTERSECTS:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "ST_Intersects({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_NOT_INTERSECTS:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "NOT ST_Intersects({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_OVERLAPS:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "ST_Overlaps({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_NOT_OVERLAPS:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "NOT ST_Overlaps({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_TOUCHES:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "ST_Touches({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_NOT_TOUCHES:
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return "NOT ST_Touches({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            case Query::TYPE_CONTAINS:
            case Query::TYPE_NOT_CONTAINS:
                // using st_cover instead of contains to match the boundary matching behaviour of the mariadb st_contains
                // postgis st_contains excludes matching the boundary
                $isNot = $query->getMethod() === Query::TYPE_NOT_CONTAINS;
                $binds[":{$placeholder}_0"] = $this->convertArrayToWKT($query->getValues()[0]);
                return $isNot
                    ? "NOT ST_Covers({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))"
                    : "ST_Covers({$alias}.{$attribute}, ST_GeomFromText(:{$placeholder}_0))";

            default:
                throw new DatabaseException('Unknown spatial query method: ' . $query->getMethod());
        }
    }

    /**
     * Get SQL Condition
     *
     * @param Query $query
     * @param array<string, mixed> $binds
     * @param array<mixed> $attributes
     * @return string
     * @throws Exception
     */
    protected function getSQLCondition(Query $query, array &$binds, array $attributes = []): string
    {
        $query->setAttribute($this->getInternalKeyForAttribute($query->getAttribute()));

        $attribute = $this->filter($query->getAttribute());
        $attribute = $this->quote($attribute);
        $alias = $this->quote(Query::DEFAULT_ALIAS);
        $placeholder = ID::unique();

        $attributeType = $this->getAttributeType($query->getAttribute(), $attributes);
        $operator = null;

        if (in_array($attributeType, Database::SPATIAL_TYPES)) {
            return $this->handleSpatialQueries($query, $binds, $attribute, $alias, $placeholder);
        }

        switch ($query->getMethod()) {
            case Query::TYPE_OR:
            case Query::TYPE_AND:
                $conditions = [];
                /* @var $q Query */
                foreach ($query->getValue() as $q) {
                    $conditions[] = $this->getSQLCondition($q, $binds, $attributes);
                }

                $method = strtoupper($query->getMethod());
                return empty($conditions) ? '' : ' ' . $method . ' (' . implode(' AND ', $conditions) . ')';

            case Query::TYPE_SEARCH:
                $binds[":{$placeholder}_0"] = $this->getFulltextValue($query->getValue());
                return "to_tsvector(regexp_replace({$attribute}, '[^\w]+',' ','g')) @@ websearch_to_tsquery(:{$placeholder}_0)";

            case Query::TYPE_NOT_SEARCH:
                $binds[":{$placeholder}_0"] = $this->getFulltextValue($query->getValue());
                return "NOT (to_tsvector(regexp_replace({$attribute}, '[^\w]+',' ','g')) @@ websearch_to_tsquery(:{$placeholder}_0))";

            case Query::TYPE_BETWEEN:
                $binds[":{$placeholder}_0"] = $query->getValues()[0];
                $binds[":{$placeholder}_1"] = $query->getValues()[1];
                return "{$alias}.{$attribute} BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Query::TYPE_NOT_BETWEEN:
                $binds[":{$placeholder}_0"] = $query->getValues()[0];
                $binds[":{$placeholder}_1"] = $query->getValues()[1];
                return "{$alias}.{$attribute} NOT BETWEEN :{$placeholder}_0 AND :{$placeholder}_1";

            case Query::TYPE_IS_NULL:
            case Query::TYPE_IS_NOT_NULL:
                return "{$alias}.{$attribute} {$this->getSQLOperator($query->getMethod())}";

            case Query::TYPE_CONTAINS:
            case Query::TYPE_NOT_CONTAINS:
                if ($query->onArray()) {
                    $operator = '@>';
                } else {
                    $operator = null;
                }

                // no break
            default:
                $conditions = [];
                $operator = $operator ?? $this->getSQLOperator($query->getMethod());
                $isNotQuery = in_array($query->getMethod(), [
                    Query::TYPE_NOT_STARTS_WITH,
                    Query::TYPE_NOT_ENDS_WITH,
                    Query::TYPE_NOT_CONTAINS
                ]);

                foreach ($query->getValues() as $key => $value) {
                    $value = match ($query->getMethod()) {
                        Query::TYPE_STARTS_WITH => $this->escapeWildcards($value) . '%',
                        Query::TYPE_NOT_STARTS_WITH => $this->escapeWildcards($value) . '%',
                        Query::TYPE_ENDS_WITH => '%' . $this->escapeWildcards($value),
                        Query::TYPE_NOT_ENDS_WITH => '%' . $this->escapeWildcards($value),
                        Query::TYPE_CONTAINS => ($query->onArray()) ? \json_encode($value) : '%' . $this->escapeWildcards($value) . '%',
                        Query::TYPE_NOT_CONTAINS => ($query->onArray()) ? \json_encode($value) : '%' . $this->escapeWildcards($value) . '%',
                        default => $value
                    };

                    $binds[":{$placeholder}_{$key}"] = $value;

                    if ($isNotQuery && $query->onArray()) {
                        // For array NOT queries, wrap the entire condition in NOT()
                        $conditions[] = "NOT ({$alias}.{$attribute} {$operator} :{$placeholder}_{$key})";
                    } elseif ($isNotQuery && !$query->onArray()) {
                        $conditions[] = "{$alias}.{$attribute} NOT {$operator} :{$placeholder}_{$key}";
                    } else {
                        $conditions[] = "{$alias}.{$attribute} {$operator} :{$placeholder}_{$key}";
                    }
                }

                $separator = $isNotQuery ? ' AND ' : ' OR ';
                return empty($conditions) ? '' : '(' . implode($separator, $conditions) . ')';
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
     * @param bool $required
     * @return string
     * @throws DatabaseException
     */
    protected function getSQLType(string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string
    {
        if ($array === true) {
            return 'JSONB';
        }

        switch ($type) {
            case Database::VAR_ID:
                return 'BIGINT';

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


            case Database::VAR_POINT:
                return 'GEOMETRY(POINT)';

            case Database::VAR_LINESTRING:
                return 'GEOMETRY(LINESTRING)';

            case Database::VAR_POLYGON:
                return 'GEOMETRY(POLYGON)';

            default:
                throw new DatabaseException('Unknown Type: ' . $type . '. Must be one of ' . Database::VAR_STRING . ', ' . Database::VAR_INTEGER .  ', ' . Database::VAR_FLOAT . ', ' . Database::VAR_BOOLEAN . ', ' . Database::VAR_DATETIME . ', ' . Database::VAR_RELATIONSHIP . ', ' . Database::VAR_POINT . ', ' . Database::VAR_LINESTRING . ', ' . Database::VAR_POLYGON);
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
     * Size of POINT spatial type
     *
     * @return int
    */
    protected function getMaxPointSize(): int
    {
        // https://stackoverflow.com/questions/30455025/size-of-data-type-geographypoint-4326-in-postgis
        return 32;
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
        return true;
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

        // Unknown column
        if ($e->getCode() === "42703" && isset($e->errorInfo[1]) && $e->errorInfo[1] === 7) {
            return new NotFoundException('Attribute not found', $e->getCode(), $e);
        }

        return $e;
    }

    /**
     * @param string $string
     * @return string
     */
    protected function quote(string $string): string
    {
        return "\"{$string}\"";
    }

    /**
     * Is spatial attributes supported?
     *
     * @return bool
    */
    public function getSupportForSpatialAttributes(): bool
    {
        return true;
    }

    /**
     * Does the adapter support null values in spatial indexes?
     *
     * @return bool
    */
    public function getSupportForSpatialIndexNull(): bool
    {
        return true;
    }

    /**
     * Does the adapter includes boundary during spatial contains?
     *
     * @return bool
    */
    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return true;
    }

    /**
     * Does the adapter support order attribute in spatial indexes?
     *
     * @return bool
    */
    public function getSupportForSpatialIndexOrder(): bool
    {
        return false;
    }
}
