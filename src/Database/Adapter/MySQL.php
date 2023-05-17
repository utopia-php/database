<?php

namespace Utopia\Database\Adapter;

use PDOException;
use Utopia\Database\Exception\Timeout;

class MySQL extends MariaDB
{
    protected int $maxVarcharLength = 16381;

    /**
     * Returns Max Execution Time
     * @param string $sql
     * @param int $milliseconds
     * @return string
     */
    protected function setTimeOut(string $sql, int $milliseconds): string
    {
        return preg_replace('/SELECT/', "SELECT /*+ max_execution_time({$milliseconds}) */", $sql, 1);
    }

    /**
     * @param PDOException $e
     * @throws Timeout
     */
    protected function processException(PDOException $e): void
    {
        if ($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024) {
            throw new Timeout($e->getMessage());
        }

        // PDOProxy which who switches errorInfo
        if ($e->getCode() === 3024 && isset($e->errorInfo[0]) && $e->errorInfo[0] === "HY000") {
            throw new Timeout($e->getMessage());
        }

        throw $e;
    }
}
