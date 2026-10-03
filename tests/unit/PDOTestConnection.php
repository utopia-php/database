<?php

namespace Tests\Unit;

use Utopia\Database\PDO;

class PDOTestConnection extends PDO
{
    public function useConnection(\PDO $connection): void
    {
        $this->pdo = $connection;
    }

    public function connection(): \PDO
    {
        return $this->pdo;
    }
}
