<?php

namespace Utopia\Database;

use Exception;
use PDO;
use PDOStatement;
use PDOException;
use Throwable;
use Utopia\Database\Exception\Duplicate;

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

    protected ?string $method = null; // SELECT, DELETE

    protected ?string $select = null;
    protected ?string $from = null;
    protected array $orders = [];
    protected array $conditions = [];
    protected ?int $limit = null;
    protected ?int $offset = null;
    protected bool $count = false;
    protected ?string $sum = null;

    private bool $debug = false;

    public function setDebug(): self
    {
        $this->debug = true;

        return $this;
    }

    /**
     * @var string
     */
    protected $queryTemplate;

    /**
     * @var array
     */
    protected $params;

    /**
     * TODO@kodumbeats make PDO required
     * @param PDO $pdo
     */
    public function __construct(PDO $pdo = null)
    {
        $this->pdo = $pdo;
    }

    // public function __call($name, $args) {
    //     $name = \strtolower($name);
    //
    //     // lets do where magic
    //     // $foo[0] is empty, $foo[1] is action
    //     if (\str_contains($name, 'where') && !empty($args)) {
    //         $clause = \explode('where', $name)[1];
    //         if (\in_array($clause, Query::$operators)) {
    //
    //         }
    //     }

    //     var_dump($name, $args);
    // }

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
     * @param string $key
     * @param $value
     */
    protected function setParam($key, $value): void
    {
        if (\array_key_exists($key, $this->getParams())) {
            throw new Exception("Cannot set two of the same named param: `{$key}");
        }
        $this->params[$key] = $value;
    }

    /**
     * @param int $mode PDO fetch mode, defaults to PDO::FETCH_ASSOC
     *
     * @return array
     */
    public function fetch(int $mode = PDO::FETCH_ASSOC)
    {
        return $this->getStatement()->fetch($mode);
    }

    /**
     * @param int $mode PDO fetch mode, defaults to PDO::FETCH_ASSOC
     *
     * @return array
     */
    public function fetchAll(int $mode = PDO::FETCH_ASSOC)
    {
        return $this->getStatement()->fetchAll($mode);
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
     *
     * @return QueryBuilder
     */
    public function from(string $table): self
    {
        $this->from = $table;

        return $this;
    }

    /**
     * @var array $keys
     *
     * @return QueryBuilder
     */
    public function select(array $keys): self
    {
        $this->method = 'SELECT';
        $this->select = \implode(', ', $keys);

        return $this;
    }

    /**
     * @return QueryBuilder
     */
    public function deleteOne(): self
    {
        $this->method = 'DELETE';

        return $this->one();
    }


    /**
     * @param array $keyvalues
     * @param mixed $by
     *
     * @return QueryBuilder
     */
    public function order(array $keyvalues /* ,$by = null */): self
    {
        foreach ($keyvalues as $key => $value) {
            $this->orders[$key] = $value;
        }

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

    /** * @param string $key
     * @param string $type
     *
     * @return QueryBuilder
     */
    public function addColumn($key, $type): self
    {
        return $this->append("ADD COLUMN `{$key}` {$type}");
    }

    /**
     * @param string $key
     *
     * @return QueryBuilder
     */
    public function dropColumn($key): self
    {
        return $this->append("DROP COLUMN `{$key}`");
    }

    /**
     * @param string $key
     * @param string $type
     *
     * @return QueryBuilder
     */
    public function createIndex($key, $type): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        switch ($type) {
            case Database::INDEX_KEY:
            case Database::INDEX_ARRAY:
                $type = 'INDEX';
                break;
            case Database::INDEX_UNIQUE:
                $type = 'UNIQUE INDEX';
                break;
            case Database::INDEX_FULLTEXT:
                $type = 'FULLTEXT INDEX';
                break;
            default:
                throw new Exception('Unknown Index Type:' . $type);
                break;
        }

        $this->queryTemplate = "CREATE {$type} {$key};";

        return $this;
    }

    /**
     * @param string $table
     *
     * @return QueryBuilder
     */
    public function on(string $table): self
    {
        return $this->append("ON {$table}");
    }

    /**
     * @param string $key
     *
     * @return QueryBuilder
     */
    public function dropIndex($key): self
    {
        return $this->append("DROP INDEX `{$key}`");
    }

    /**
     * @param string $table
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function insertInto(string $table): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        $this->queryTemplate = "INSERT INTO {$table};";

        return $this;
    }

    /**
     * @param string $table
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function alterTable(string $table): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        $this->queryTemplate = "ALTER TABLE {$table};";

        return $this;
    }

    /**
     * @param string $table
     *
     * @throws Exception
     * @return QueryBuilder
     */
    public function update(string $table): self
    {
        if ($this->statement) {
            throw new Exception('Multiple statements detected - not supported yet');
        }

        $this->queryTemplate = "UPDATE {$table};";

        return $this;
    }

    /**
     * @param array $values assoc array of columns to update with their values
     *
     * @return QueryBuilder
     */
    public function set(array $values): self
    {
        $set = [];
        foreach ($values as $key => $value) {
            $key = $this->filter($key);
            $set[] = "`{$key}` = :{$key}";

            $this->setParam(":{$key}", $value);
        }

        return $this->append('SET ' . \implode(', ', $set));
    }

    /**
     * @param string $conditions
     *
     * @return QueryBuilder
     */
    public function where(...$conditions): self
    {
        if (!empty($this->conditions) && $this->conditions[count($this->conditions) - 1] !== '(') {
            $this->conditions[] = 'AND';
        }

        \array_push(
            $this->conditions,
            '(',
            \implode(' OR ', $conditions),
            ')',
        );

        return $this;
    }

    /**
     * @param string $conditions
     *
     * @return QueryBuilder
     */
    public function or(...$conditions): self
    {
        \array_push(
            $this->conditions,
            'OR',
            '(',
            \implode(' AND ', $conditions),
            ')',
        );

        return $this;
    }

    /**
     * @return QueryBuilder
     */
    public function count(): self
    {
        $this->count = true;

        return $this;
    }

    /**
     * @return QueryBuilder
     */
    public function sum($attribute): self
    {
        $this->sum = $attribute;

        return $this;
    }

    /**
     * @return QueryBuilder
     */
    public function open(): self
    {
        \array_push(
            $this->conditions,
            'AND',
            '(',
        );
        return $this;
    }

    /**
     * @return QueryBuilder
     */
    public function close(): self
    {
        $this->conditions[] = ')';
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
        return $this->limit(1);
    }

    /**
     * @param int $offset
     *
     * @return QueryBuilder
     */
    public function offset(int $offset): self
    {
        $this->offset = $offset;

        return $this;
    }

    private function buildQuery(): string
    {
        /** @var string[] */
        $template[] = $this->method;

        if ($this->method === 'SELECT') {
            $template[] = $this->select;
        }

        if (!\is_null($this->from)) {
            \array_push(
                $template,
                'FROM',
                $this->from,
            );
        }

        if ((!empty($this->conditions))/* && $template[\count($template) - 1] !== 'WHERE'*/) {
            $template[] = 'WHERE';
        }

        if (!empty($this->conditions)) {
            $template[] = \implode(' ', $this->conditions);
        }

        if (!empty($this->orders)) {
            /** @var string[] */
            $orderings = [];

            foreach ($this->orders as $key => $val) {
                $orderings[] = implode(" ", [$key, $val]);
            }

            $template[] = 'ORDER BY ' . implode(', ', $orderings);
        }

        if (!\is_null($this->limit)) {
            $template[] = "LIMIT {$this->limit}";
        }

        if (!\is_null($this->offset)) {
            $template[] = "OFFSET {$this->offset}";
        }

        if ($this->count) {
            \array_unshift(
                $template,
                'SELECT',
                'COUNT(1)',
                'as',
                'sum',
                'FROM',
                '(',
            );

            \array_push(
                $template,
                ')',
                'table_count',
            );
        }

        if (!\is_null($this->sum)) {
            \array_unshift(
                $template,
                'SELECT',
                "SUM({$this->sum})",
                'as',
                'sum',
                'FROM',
                '(',
            );

            \array_push(
                $template,
                ')',
                'table_count',
            );
        }

        return implode(' ', $template) . ';';
    }

    /**
     * @throws Exception
     * @return PDOStatement
     */
    public function execute(): bool
    {
        if ($this->debug) {
            echo "\n\n";
            print_r($this->conditions);
            print_r($this->buildQuery());
            echo "\n\n";
        }
        $this->getPDO()->beginTransaction();

        $this->statement = $this->getPDO()->prepare($this->buildQuery());

        // foreach ($this->getParams() as $key => $value) {
        //     $this->getStatement()->bindValue($key, $value, $this->getPDOType($value));
        // }

        try {
            $this->getStatement()->execute();
            if (!$this->getPDO()->commit()) {
                throw new Exception('Failed to commit transaction');
            }
            return true;
        } catch (PDOException $e) {
            switch ($e->getCode()) {
                case 1062:
                case 23000:
                    $this->getPDO()->rollBack();
                    throw new Duplicate('Duplicated document: ' . $e->getMessage());
                    break;
                default:
                    throw new Exception($e->getMessage());
                    break;
            }
        } catch (Throwable $th) {
            $this->getPDO()->rollBack();
            throw new Exception($th->getMessage());
        }
    }

    public function reset()
    {
        $this->select = null;
        $this->from = null;
        $this->orders = [];
        $this->conditions = [];
        $this->limit = null;
        $this->offset = null;

        $this->queryTemplate = '';
        $this->params = [];
    }

    /**
     * Filter Keys
     * 
     * @throws Exception
     * @return string
     */
    public function filter(string $value): string
    {
        $value = preg_replace('/[^A-Za-z0-9\_\.\-]/', '', $value);

        if (\is_null($value)) {
            throw new Exception('Failed to filter key');
        }

        return $value;
    }

    /**
     * Get PDO Type
     *
     * @param mixed $value
     *
     * @return int
     */
    protected function getPDOType($value): int
    {
        switch (gettype($value)) {
            case 'string':
                return PDO::PARAM_STR;
                break;

            case 'boolean':
                return PDO::PARAM_INT;
                break;

                //case 'float': // (for historical reasons "double" is returned in case of a float, and not simply "float")
            case 'double':
                return PDO::PARAM_STR;
                break;

            case 'integer':
                return PDO::PARAM_INT;
                break;

            case 'NULL':
                return PDO::PARAM_NULL;
                break;

            default:
                throw new Exception('Unknown PDO Type for ' . gettype($value));
                break;
        }
    }

    /**
     * Get SQL Operator
     *
     * @param string $operator
     *
     * @return string
     */
    protected function getSQLOperator(string $operator): string
    {
        switch ($operator) {
            case Query::TYPE_EQUAL:
                return '=';
                break;

            case Query::TYPE_NOTEQUAL:
                return '!=';
                break;

            case Query::TYPE_LESSER:
                return '<';
                break;

            case Query::TYPE_LESSEREQUAL:
                return '<=';
                break;

            case Query::TYPE_GREATER:
                return '>';
                break;

            case Query::TYPE_GREATEREQUAL:
                return '>=';
                break;

            default:
                throw new Exception('Unknown Operator:' . $operator);
                break;
        }
    }
}
