<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use Utopia\Database\Adapter;

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
     * @param string $name
     * @return bool
     */
    public function createCollection(string $name): bool
    {
        $name = $this->filter($name).'_documents';

        return $this->getPDO()
            ->prepare("CREATE TABLE IF NOT EXISTS {$this->getNamespace()}.{$name} (
                `_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `_uid` varchar(45) NOT NULL,
                -- 'custom2' text() DEFAULT NULL,
                PRIMARY KEY (`_id`),
                UNIQUE KEY `_index1` (`_uid`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
            ->execute();
    }

    /**
     * Delete Collection
     * 
     * @param string $name
     * @return bool
     */
    public function deleteCollection(string $name): bool
    {
        $name = $this->filter($name).'_documents';

        return $this->getPDO()
            ->prepare("DROP TABLE {$this->getNamespace()}.{$name};")
            ->execute();
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