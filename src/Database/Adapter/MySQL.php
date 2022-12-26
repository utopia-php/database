<?php

namespace Utopia\Database\Adapter;

use Exception;
use PDO;
use PDOException;
use Utopia\Database\Database;
use Utopia\Database\Exception\Timeout;

class MySQL extends MariaDB
{
    /**
     * Get SQL Index
     * 
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array $attributes
     * 
     * @return string
     */
    protected function getSQLIndex(string $collection, string $id, string $type, array $attributes): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
                $type = 'INDEX';
                break;

            case Database::INDEX_ARRAY:
                $type = 'INDEX';

                foreach ($attributes as $key => $value) {
                    $attributes[$key] = '(CAST(' . $value . ' AS char(255) ARRAY))';
                }
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

        return 'CREATE '.$type.' `'.$id.'` ON `'.$this->getDefaultDatabase().'`.`'.$this->getNamespace().'_'.$collection.'` ( '.implode(', ', $attributes).' );';
    }

    /**
     * Returns Max Execution Time
     * @param string $sql
     * @param float $seconds
     * @return string
     */
    protected function setTimeOut(string $sql, float $seconds): string
    {
        $syntax = '/*+ max_execution_time(' . ($seconds * 1000) . ') */';
        return sprintf($sql, '', $syntax);
    }

    /**
     * Set Max Execution Time Query
     * @param PDO $pdo
     * @param int $milliseconds
     */
    protected function setTimeoutSession(PDO $pdo, int $milliseconds)
    {
        var_dump('SET SESSION max_execution_time = ' . $milliseconds);
        $pdo->prepare('SET SESSION max_execution_time = ' . $milliseconds);
    }

    /**
     * Resets Max Execution Time Query
     * @param PDO $pdo
     */
    protected function resetTimeoutSession(PDO $pdo)
    {
        var_dump('SET SESSION max_execution_time = default');
        $pdo->prepare('SET SESSION max_execution_time = default')->execute();
    }

    /**
     * @throws Timeout
     */
    protected function checkTimeoutException(PDOException $e, PDO $pdo): void
    {
        if($e->getCode() === 'HY000' && isset($e->errorInfo[1]) && $e->errorInfo[1] === 3024){
            $this->resetTimeoutSession($pdo);  // todo: Does this make sense?
            Throw new Timeout($e->getMessage());
        }
    }

}
