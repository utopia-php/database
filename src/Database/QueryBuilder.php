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
     * TODO@kodumbeats make PDO required
     * @param PDO $pdo
     */
    public function __construct(PDO $pdo = null)
    {
        $this->reset();
        $this->pdo = $pdo;
    }

    public function getPDO(): PDO
    {
        return $this->pdo;
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
        $this->queryTemplate = "CREATE DATABASE `{$name}` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;";

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
        $this->queryTemplate = "CREATE TABLE IF NOT EXISTS `{$name}`;";

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

        $this->queryTemplate = "DROP {$type} {$name};";

        return $this;
    }

    /**
     * @param string $table
     * @param string[] $keys
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function from(string $table, array $keys = ['*']): self
    {
        foreach ($keys as &$key) {
            if ($key !== '*') {
                $key = '`'.$this->filter($key).'`';
            }
        }

        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        $keys = \implode(", ", $keys);
        $this->queryTemplate = "SELECT {$keys} FROM {$table};";

        return $this;
    }

    /**
     * @param string $table
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function deleteFrom(string $table): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        $this->queryTemplate = "DELETE FROM {$table};";

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

        $key = $this->filter($key);
        $this->queryTemplate .= " WHERE {$key} {$condition} :value{$count};";
        $this->params[":value{$count}"] = $value;

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
        if (!\is_null($this->limit)) {
            // strip trailing semicolon if present
            if (\mb_substr($this->getTemplate(), -1) === ';') {
                $this->queryTemplate = \mb_substr($this->getTemplate(), 0, -1);
            }
            $this->queryTemplate .= " LIMIT {$this->limit};";
        }

        $this->getPDO()->beginTransaction();

        $this->statement = $this->getPDO()->prepare($this->getTemplate());

        try {
            $this->getStatement()->execute($this->getParams());
            return $this->getPDO()->commit();
        } catch (\Throwable $th) {
            $this->getPDO()->rollBack();
            throw new Exception($th->getMessage());
        }
    }

    public function reset()
    {
        $this->queryTemplate = '';
        $this->params = [];
        $this->limit = null;
    }

    /**
     * Filter Keys
     * 
     * @throws Exception
     * @return string
     */
    public function filter(string $value): string
    {
        $value = preg_replace("/[^A-Za-z0-9]_/", '', $value);

        if(\is_null($value)) {
            throw new Exception('Failed to filter key');
        }

        return $value;
    }

}
