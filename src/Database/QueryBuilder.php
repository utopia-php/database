<?php

namespace Utopia\Database;

use Exception;
use PDO;
use PDOStatement;

class QueryBuilder
{
    const TYPE_DATABASE = 'DATABASE';
    const TYPE_TABLE = 'TABLE';

    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * @var PDOStatement
     */
    protected $statement;

    /**
     * @var string
     */
    protected $queryTemplate;

    /**
     * @var array
     */
    protected $params;

    /**
     * @var int
     */
    protected $limit;

    /**
     * @param PDO $pdo
     */
    public function __construct(PDO $pdo)
    {
        $this->reset();
        $this->pdo = $pdo;
    }

    /**
     * @return PDOStatement
     */
    public function getStatement(): PDOStatement
    {
        return $this->statement;
    }

    /**
     * @return string
     */
    public function getTemplate(): string
    {
        return $this->queryTemplate;
    }

    /**
     * @return array
     */
    public function getParams(): array
    {
        return $this->params;
    }

    /**
     * @return string
     */
    public function getLimit(): string
    {
        return $this->limit;
    }

    /**
     * @param string $name
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function createDatabase(string $name): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }
        // $this->queryTemplate = 'CREATE DATABASE `:name` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;';
        // $this->params['name'] = $name;
        $this->queryTemplate = "CREATE DATABASE {$name} /*!40100 DEFAULT CHARACTER SET utf8mb4 */;";

        return $this;
    }

    /**
     * @param string $name
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function createTable(string $name): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }
        $this->queryTemplate = 'CREATE TABLE IF NOT EXISTS :name;';
        $this->params[':name'] = $name;

        return $this;
    }


    /**
     * @param string $type one of DATABASE, TABLE
     * @param string $name
     *
     * @throws Exception
     * @return bool
     */
    public function drop(string $type, string $name): bool
    {
        //TODO@kodumbeats with PHP8.1, use enums
        if ($type !== self::TYPE_DATABASE && $type !== self::TYPE_TABLE) {
            throw new Exception('Invalid type');
        }

        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        // $this->queryTemplate = "{$this->statement} {$type} :name;";
        // $this->params['name'] = $name;
        $this->queryTemplate = "{$this->statement} {$type} {$name};";

        return $this->execute();
    }

    /**
     * @param string $table
     * @param string $key
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function from(string $table, string $thing = '*'): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        if ($thing === '*') {
            $this->queryTemplate = "SELECT * FROM {$table};";
        } else {
            $this->queryTemplate = "SELECT :thing FROM {$table};";
            $this->params['thing'] = $thing;
        }

        return $this;
    }

    /**
     * @param string $key
     * @param string $condition
     * @param string $value
     *
     * @return QueryBuilder
     */
    public function where($key, $condition, $value): self
    {
        // strip trailing semicolon if present
        if (\mb_substr($this->getTemplate(), -1) === ';') {
            $this->queryTemplate = \mb_substr($this->getTemplate(), 0, -1);
        }

        $count = \count($this->getParams());
        $this->queryTemplate .= " WHERE :key{$count} {$condition} :value{$count};";
        $this->params["key{$count}"] = $key;
        $this->params["value{$count}"] = $value;

        return $this;
    }

    /**
     * @param int $limit
     *
     * @return QueryBuilder
     */
    public function limit(int $limit): self
    {
        $this->limit = $limit;

        return $this;
    }
    /**
     * @return QueryBuilder
     */
    public function one(): self
    {
        $this->limit = 1;

        return $this;
    }

    /**
     * @throws Exception
     * @return PDOStatement
     */
    public function execute(): bool
    {
        var_dump($this->getTemplate(), $this->getParams());
        $this->statement = $this->pdo->prepare($this->getTemplate());

        try {
            return $this->getStatement()->execute($this->getParams());
        } catch (\Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    public function reset()
    {
        $this->queryTemplate = '';
        $this->params = [];
        $this->limit = 25;
    }
}
