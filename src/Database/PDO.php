<?php

namespace Utopia\Database;

class PDO
{
    protected \PDO $pdo;

    public function __construct(
        protected string $dsn,
        protected ?string $username,
        protected ?string $password,
        protected array $config = []
    ) {
        $this->pdo = new \PDO(
            $this->dsn,
            $this->username,
            $this->password,
            $this->config
        );
    }

    public function __call(string $method, array $args): mixed
    {
        return $this->pdo->{$method}(...$args);
    }

    public function reconnect(): void
    {
        $this->pdo = new \PDO(
            $this->dsn,
            $this->username,
            $this->password,
            $this->config
        );
    }
}
