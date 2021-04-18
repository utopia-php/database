<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;

class MariaDB extends Adapter
{
    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * @var bool
     */
    protected $transaction = false;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param PDO $pdo
     */
    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }
    
    /**
     * Create Database
     * 
     * @return bool
     */
    public function create(): bool
    {
        $name = $this->getNamespace();

        return $this->getPDO()
            ->prepare("CREATE DATABASE {$name} /*!40100 DEFAULT CHARACTER SET utf8mb4 */;")
            ->execute();
    }

    /**
     * List Databases
     * 
     * @return array
     */
    public function list(): array
    {
        $list = [];
        return $list;
    }

    /**
     * Delete Database
     * 
     * @return bool
     */
    public function delete(): bool
    {
        $name = $this->getNamespace();

        return $this->getPDO()
            ->prepare("DROP DATABASE {$name};")
            ->execute();
    }

    /**
     * Create Collection
     * 
     * @param string $id
     * @return bool
     */
    public function createCollection(string $id): bool
    {
        $id = $this->filter($id);

        $this->getPDO()
            ->prepare("CREATE TABLE IF NOT EXISTS {$this->getNamespace()}.{$id}_permissions (
                `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `_uid` CHAR(13) NOT NULL,
                `_action` CHAR(128) NOT NULL,
                `_role` CHAR(128) NOT NULL,
                PRIMARY KEY (`_id`),
                INDEX `_index1` (`_uid`),
                INDEX `_index2` (`_action` ASC, `_role` ASC)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
            ->execute();

        return $this->getPDO()
            ->prepare("CREATE TABLE IF NOT EXISTS {$this->getNamespace()}.{$id} (
                `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `_uid` CHAR(13) NOT NULL,
                PRIMARY KEY (`_id`),
                UNIQUE KEY `_index1` (`_uid`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
            ->execute();
    }

    /**
     * List Collections
     * 
     * @return array
     */
    public function listCollections(): array
    {
        $list = [];

        return $list;
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
        
        $this->getPDO()
            ->prepare("DROP TABLE {$this->getNamespace()}.{$id}_permissions;")
            ->execute();

        return $this->getPDO()
            ->prepare("DROP TABLE {$this->getNamespace()}.{$id};")
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
        $type = $this->getSQLType($type, $size, $signed);

        if($array) {
            return $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS {$this->getNamespace()}.{$name}_arrays_{$id} (
                    `_id` INT(11) unsigned NOT NULL AUTO_INCREMENT,
                    `_uid` CHAR(13) NOT NULL,
                    `_order` INT(11) unsigned NOT NULL,
                    `{$id}` {$type},
                    PRIMARY KEY (`_id`),
                    INDEX `_index1` (`_uid`),
                    INDEX `_index2` (`_uid` ASC, `_order` ASC)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                ->execute();
        }

        return $this->getPDO()
            ->prepare("ALTER TABLE {$this->getNamespace()}.{$name}
                ADD COLUMN `{$id}` {$type};")
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

        if($array) {
            return $this->getPDO()
                ->prepare("DROP TABLE {$this->getNamespace()}.{$name}_arrays_{$id};")
                ->execute();
        }

        return $this->getPDO()
            ->prepare("ALTER TABLE {$this->getNamespace()}.{$name}
                DROP COLUMN `{$id}`;")
            ->execute();
    }

    /**
     * Create Index
     * 
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * 
     * @return bool
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        foreach($attributes as $key => &$attribute) {
            $length = $lengths[$key] ?? '';
            $length = (empty($length)) ? '' : '('.(int)$length.')';
            $order = $orders[$key] ?? 'ASC';
            $attribute = $this->filter($attribute);

            $attribute = "`{$attribute}`{$length} {$order}";
        }

        return $this->getPDO()
            ->prepare("CREATE INDEX `{$id}` ON {$this->getNamespace()}.{$name} (".implode(', ', $attributes).");")
            ->execute();
    }

    /**
     * Delete Index
     * 
     * @param string $collection
     * @param string $id
     * 
     * @return bool
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $name = $this->filter($collection);
        $id = $this->filter($id);

        return $this->getPDO()
            ->prepare("ALTER TABLE {$this->getNamespace()}.{$name}
                DROP INDEX `{$id}`;")
            ->execute();
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return array
     */
    public function getDocument(string $collection, string $id): Document
    {
        $name = $this->filter($collection);

        $stmt = $this->getPDO()->prepare("SELECT * FROM {$this->getNamespace()}.{$name}
            WHERE _uid = :_uid
            LIMIT 1;
        ");

        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);
        $stmt->execute();

        $document = $stmt->fetch();

        $document['$id'] = $document['_uid'];
        $document['$permissions'] = [Database::PERMISSION_READ => [], Database::PERMISSION_WRITE => []];
        unset($document['_id']);
        unset($document['_uid']);

        $stmt = $this->getPDO()->prepare("SELECT * FROM {$this->getNamespace()}.{$name}_permissions
            WHERE _uid = :_uid
            LIMIT 100;
        ");

        $stmt->bindValue(':_uid', $id, PDO::PARAM_STR);
        $stmt->execute();

        $permissions = $stmt->fetchAll();

        foreach ($permissions as $permission) {
            $action = $permission['_action'] ?? '';
            $role = $permission['_role'] ?? '';
            $document['$permissions'][$action][] = $role;
        }

        return new Document($document);
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
        $name = $this->filter($collection);
        $columns = '';

        /**
         * Insert Permissions
         */
        $stmt = $this->getPDO()
            ->prepare("INSERT INTO {$this->getNamespace()}.{$name}_permissions
                SET _uid = :_uid, _action = :_action, _role = :_role");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

        foreach ($document->getPermissions() as $action => $roles) {
            foreach ($roles as $key => $role) {
                $stmt->bindValue(':_action', $action, PDO::PARAM_STR);
                $stmt->bindValue(':_role', $role, PDO::PARAM_STR);

                if(!$stmt->execute()) {
                    throw new Exception('Failed to save permission');
                }
            }
        }

        $arrays = [];
        $attributes = $document->getAttributes();

        /**
         * Insert Attributes
         */
        foreach ($attributes as $attribute => $value) { // Parse statement
            if(is_array($value)) { // arrays should be saved on dedicated table
                $arrays[$attribute] = $value;
                unset($attributes[$attribute]);
                continue;
            }

            $column = $this->filter($attribute);
            $columns .= "`{$column}`" . '=:' . $column . ',';
        }

        $stmt = $this->getPDO()
            ->prepare("INSERT INTO {$this->getNamespace()}.{$name}
                SET {$columns} _uid = :_uid");

        $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);

        foreach ($attributes as $attribute => $value) {
            $attribute = $this->filter($attribute);
            $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
        }
        
        $stmt->execute();

        /**
         * Insert Arrays
         */
        foreach ($arrays as $attribute => $array) {
            $attribute = $this->filter($attribute);

            $stmt = $this->getPDO()
                ->prepare("INSERT INTO {$this->getNamespace()}.{$name}_arrays_{$attribute}
                    SET _uid = :_uid, _order = :_order, {$attribute} = :{$attribute}");

            foreach ($array as $order => $value) {
                $stmt->bindValue(':_uid', $document->getId(), PDO::PARAM_STR);
                $stmt->bindValue(':_order', $order, PDO::PARAM_INT);
                $stmt->bindValue(':' . $attribute, $value, $this->getPDOType($value));
                $stmt->execute();
            }
        }
        
        return $document;
    }

    /**
     * Get max STRING limit
     * 
     * @return int
     */
    public function getStringLimit(): int
    {
        return 4294967295;
    }

    /**
     * Get max INT limit
     * 
     * @return int
     */
    public function getIntLimit(): int
    {
        return 4294967295;
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
     * Get SQL Type
     * 
     * @param string $type
     * @param int $size
     * 
     * @return string
     */
    protected function getSQLType(string $type, int $size, bool $signed = true): string
    {
        switch ($type) {
            case Database::VAR_STRING:
                if($size > 16777215) {
                    return 'LONGTEXT';
                }
                
                if($size > 65535) {
                    return 'MEDIUMTEXT';
                }

                if($size > 16383) {
                    return 'TEXT';
                }
                
                return "VARCHAR({$size})";
            break;

            case Database::VAR_INTEGER:  // We don't support zerofill: https://stackoverflow.com/a/5634147/2299554
                $signed = ($signed) ? '' : ' UNSIGNED';
                return 'INT'.$signed;
            break;

            case Database::VAR_FLOAT:
                $signed = ($signed) ? '' : ' UNSIGNED';
                return 'FLOAT'.$signed;
            break;

            case Database::VAR_BOOLEAN:
                return 'TINYINT(1)';
            break;

            case Database::VAR_DOCUMENT:
                return 'CHAR(13)';
            break;
            
            default:
                throw new Exception('Unknown Type');
            break;
        }
    }

    /**
     * Get PDO Type
     * 
     * @param string $type
     * @param int $size
     * 
     * @return string
     */
    protected function getPDOType($value): string
    {
        switch (gettype($value)) {
            case 'string':
                return PDO::PARAM_STR;
            break;

            case 'boolean':
                return PDO::PARAM_BOOL;
            break;

            case 'integer':
                return PDO::PARAM_INT;
            break;

            case 'float':
            case 'double':
                return PDO::PARAM_STR;
            break;
            
            default:
                throw new Exception('Unknown PDO Type for ' . gettype($value));
            break;
        }
    }

    /**
     * @return PDO
     *
     * @throws Exception
     */
    protected function getPDO()
    {
        return $this->pdo;
    }
}