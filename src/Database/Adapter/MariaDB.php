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
     * @param string $name
     * @return bool
     */
    public function create(string $name): bool
    {
        return $this->getPDO()
            ->prepare('CREATE DATABASE `'.$this->getNamespace().'_'.$name.'` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;')
            ->execute();
    }

    /**
     * Delete Database
     * 
     * @param string $name
     * @return bool
     */
    public function delete(string $name): bool
    {
        return $this->getPDO()
            ->prepare('DROP DATABASE `'.$this->getNamespace().'_'.$name.'`;')
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