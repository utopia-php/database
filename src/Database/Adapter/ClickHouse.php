<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use PDOException;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\ID;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class ClickHouse extends MariaDB
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

        return $this->getPDO()
            ->prepare("CREATE DATABASE IF NOT EXISTS `{$name}` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;")
            ->execute();
    }

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

        if (!\is_null($collection)) {
            $collection = $this->filter($collection);

            $select = 'TABLE_NAME';
            $from = 'INFORMATION_SCHEMA.TABLES';
            $where = 'TABLE_SCHEMA = :schema AND TABLE_NAME = :table';
            $match = "{$this->getNamespace()}_{$collection}";
        } else {
            $select = 'schema_name';
            $from = 'information_schema.schemata';
            $where = 'schema_name = :schema';
            $match = $database;
        }

        $stmt = $this->getPDO()->prepare("SELECT {$select} FROM {$from} WHERE {$where}");

        $stmt->bindValue(':schema', $database, PDO::PARAM_STR);

        if (!\is_null($collection)) {
            $stmt->bindValue(':table', "{$this->getNamespace()}_{$collection}", PDO::PARAM_STR);
        }

        $stmt->execute();

        $document = $stmt->fetch();

        return (($document[$select] ?? '') === $match);
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
        $database = $this->getDefaultDatabase();
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

        foreach ($indexes as $key => $index) {
            $indexId = $this->filter($index->getId());
            $indexType = $index->getAttribute('type');

            $indexAttributes = $index->getAttribute('attributes');
            foreach ($indexAttributes as $nested => $attribute) {
                $indexLength = $index->getAttribute('lengths')[$key] ?? '';
                $indexLength = (empty($indexLength)) ? '' : '(' . (int)$indexLength . ')';
                $indexOrder = $index->getAttribute('orders')[$key] ?? '';
                $indexAttribute = $this->filter($attribute);

                if ($indexType === Database::INDEX_FULLTEXT) {
                    $indexOrder = '';
                }

                $indexAttributes[$nested] = "`{$indexAttribute}`{$indexLength} {$indexOrder}";
            }

            $indexes[$key] = "{$indexType} `{$indexId}` (" . \implode(", ", $indexAttributes) . " ),";
        }

        try {
            $stmt = $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS `{$database}`.`{$namespace}_{$id}` (
                        `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                        `_uid` CHAR(255) NOT NULL,
                        `_createdAt` datetime(3) DEFAULT NULL,
                        `_updatedAt` datetime(3) DEFAULT NULL,
                        `_permissions` MEDIUMTEXT DEFAULT NULL,
                        " . \implode(' ', $attributes) . "
                        PRIMARY KEY (`_id`),
                        " . \implode(' ', $indexes) . "
                        UNIQUE KEY `_uid` (`_uid`),
                        KEY `_created_at` (`_createdAt`),
                        KEY `_updated_at` (`_updatedAt`)
                    )
                    ENGINE = Memory");
            
            var_dump($stmt->queryString);
            $stmt->execute();

            $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS `{$database}`.`{$namespace}_{$id}_perms` (
                        `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                        `_type` VARCHAR(12) NOT NULL,
                        `_permission` VARCHAR(255) NOT NULL,
                        `_document` VARCHAR(255) NOT NULL,
                        PRIMARY KEY (`_id`),
                        UNIQUE INDEX `_index1` (`_document`,`_type`,`_permission`),
                        INDEX `_permission` (`_permission`)
                    )
                    ENGINE = Memory")
                ->execute();
        } catch (\Exception $th) {
            $this->getPDO()
                ->prepare("DROP TABLE IF EXISTS `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$id}`, `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$id}_perms`;")
                ->execute();
            throw $th;
        }

        // Update $this->getIndexCount when adding another default index
        return true;
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
            FROM `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}` 
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

        var_dump($document);

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
        var_dump("Creatring Docuemnt");
        var_dump($document);
        var_dump($collection);

        $attributes = $document->getAttributes();
        $attributes['_createdAt'] = $document->getCreatedAt();
        $attributes['_updatedAt'] = $document->getUpdatedAt();
        $attributes['_permissions'] = json_encode($document->getPermissions());

        $name = $this->filter($collection);
        $columns = '';

        $this->getPDO()->beginTransaction();

        /**
         * Insert Attributes
         */
        $bindIndex = 0;
        foreach ($attributes as $attribute => $value) { // Parse statement
            $column = $this->filter($attribute);
            $bindKey = 'key_' . $bindIndex;
            $columns .= "`{$column}`" . '=:' . $bindKey . ',';
            $bindIndex++;
        }

        var_dump($columns);

        $stmt = $this->getPDO()
            ->prepare("INSERT INTO `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}`
                SET {$columns} _uid = :_uid");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

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
            $queryPermissions = "INSERT INTO `{$this->getDefaultDatabase()}`.`{$this->getNamespace()}_{$name}_perms` (_type, _permission, _document) VALUES " . implode(', ', $permissions);
            $stmtPermissions = $this->getPDO()->prepare($queryPermissions);
        }

        try {
            $temp = $stmt->execute();
            var_dump($temp);

            $statment = $this->getPDO()->prepare("select last_insert_id() as id");
            $statment->execute();
            $last = $statment->fetch();
            var_dump($last);
            $document['$internalId'] = $last['id'];

            if (isset($stmtPermissions)) {
                $stmtPermissions->execute();
            }
        } catch (PDOException $e) {
            $this->getPDO()->rollBack();
            switch ($e->getCode()) {
                case 1062:
                case 23000:
                    throw new Duplicate('Duplicated document: ' . $e->getMessage());

                default:
                    throw $e;
            }
        }

        if (!$this->getPDO()->commit()) {
            throw new Exception('Failed to commit transaction');
        }

        return $document;
    }
}