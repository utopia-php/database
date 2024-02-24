<?php

namespace Utopia\Database\Extend;

use Exception;
use PDO;

class PDOStatement extends \PDOStatement {
    /**
     * @var array[string]mixed $params
     */
    private array $params = [];

    public function bindParam(
        string|int $param,
        mixed &$var,
        int $type = PDO::PARAM_STR,
        int $maxLength = 0,
        mixed $driverOptions = null
    ): bool
    {
        $this->params[$param] = [
            'method' => 'param',
            'var' => $var,
            'type' => $type,
            'maxLength' => $maxLength,
            'driverOptions' => $driverOptions,
        ];
        
        return parent::bindParam($param, $var, $type, $maxLength, $driverOptions);
    }

    public function bindValue(string|int $param, mixed $value, int $type = PDO::PARAM_STR): bool
    {
        $this->params[$param] = [
            'method' => 'value',
            'value' => $value,
            'type' => $type
        ];

        return parent::bindValue($param, $value, $type);
    }

    /**
     * @return array[string]mixed
     */
    public function getParams(): array
    {
        return $this->params;
    }

    public function getQuery(): string
    {
        return $this->queryString;
    }
}