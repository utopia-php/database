<?php

namespace Utopia\Database;

use Exception;
use PDO;

class QueryBuilder
{
    const TYPE_CREATE = 'CREATE';
    const TYPE_DROP = 'DROP';
    const TYPE_INSERT = 'INSERT';
    const TYPE_SELECT = 'SELECT';

    const TYPE_DATABASE = 'DATABASE';
    const TYPE_TABLE = 'TABLE';

    /**
     * @var string $statement one of SELECT, INSERT, CREATE, ALTER, DROP
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

    public function __construct()
    {
        $this->reset();
    }

    /**
     * @return string
     */
    public function getStatement(): string
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
        $this->statement = $this::TYPE_CREATE;
        $this->queryTemplate = 'CREATE DATABASE :name /*!40100 DEFAULT CHARACTER SET utf8mb4 */;';
        $this->params['name'] = $name;

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
        $this->statement = $this::TYPE_CREATE;
        $this->queryTemplate = 'CREATE TABLE IF NOT EXISTS :name;';
        $this->params['name'] = $name;

        return $this;
    }


    /**
     * @param string $type one of DATABASE, TABLE
     * @param string $name
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function drop(string $type, string $name): self
    {
        //TODO@kodumbeats with PHP8.1, use enums
        if ($type !== self::TYPE_DATABASE && $type !== self::TYPE_TABLE) {
            throw new Exception('Invalid type');
        }

        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        $this->statement = $this::TYPE_DROP;
        $this->queryTemplate = "{$this->statement} {$type} :name;";
        $this->params['name'] = $name;

        return $this;
    }

    /**
     * @param string $table
     * @param string $key
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function from(string $table, string $key = '*'): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        $this->statement = self::TYPE_SELECT;
        $this->queryTemplate = 'SELECT :key FROM :table;';
        $this->params['key'] = $key;
        $this->params['table'] = $table;

        return $this;
    }

    /**
     * @param string $key
     * @param string $condition
     * @param string $value
     */
    public function where($key, $condition, $value)
    {
        // strip trailing semicolon if present
        if (\mb_substr($this->getTemplate(), -1) === ';') {
            $this->queryTemplate = \mb_substr($this->getTemplate(), 0, -1);
        }
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
     */
    public function execute()
    {
        try {
            // TODO@kodumbeats prepare PDO statement from template and params and execute
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
