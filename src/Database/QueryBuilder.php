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
        $name = $this->filter($name);

        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }
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
        $name = $this->filter($name);

        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }
        $this->queryTemplate = "CREATE TABLE IF NOT EXISTS {$name};";

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
        $type = $this->filter($type);
        $name = $this->filter($name);

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
        $table = $this->filter($table);
        foreach ($keys as &$key) {
            $key = $this->filter($key);
        }

        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        $keys = \implode(", ", $keys);
        $this->queryTemplate = "SELECT {$keys} FROM {$table};";

        return $this;
    }

    /**
     * @param string $key
     * @param string $condition
     * @param string $value
     * @param bool $quotedKey
     *
     * @return QueryBuilder
     */
    public function where($key, $condition, $value, $quotedKey = false): self
    {
        // strip trailing semicolon if present
        if (\mb_substr($this->getTemplate(), -1) === ';') {
            $this->queryTemplate = \mb_substr($this->getTemplate(), 0, -1);
        }

        $count = \count($this->getParams());

        // TODO@kodumbeats find a better way to solve this for keys 
        // that cannot be quoted like SCHEMA_NAME
        if ($quotedKey) {
            $key = $this->filter($key);
            $this->queryTemplate .= " WHERE {$key} {$condition} :value{$count};";
            $this->params[":value{$count}"] = $value;
        } else {
            $this->queryTemplate .= " WHERE :key{$count} {$condition} :value{$count};";
            $this->params[":key{$count}"] = $key;
            $this->params[":value{$count}"] = $value;
        }

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
