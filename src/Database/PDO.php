<?php

namespace Utopia\Database;

use Utopia\DSN\DSN;

/**
 * A PDO wrapper that forwards method calls to the internal PDO instance.
 *
 * @mixin \PDO
 */
class PDO
{
    protected \PDO $pdo;

    /**
     * @param string $dsn
     * @param ?string $username
     * @param ?string $password
     * @param array<mixed> $config
     */
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

    /**
     * @param string $method
     * @param array<mixed> $args
     * @return mixed
     * @throws \Throwable
     */
    public function __call(string $method, array $args): mixed
    {
        return $this->pdo->{$method}(...$args);
    }

    /**
     * Create a new connection to the database
     *
     * @return void
     */
    public function reconnect(): void
    {
        $this->pdo = new \PDO(
            $this->dsn,
            $this->username,
            $this->password,
            $this->config
        );
    }

    public function getScheme(): string
    {
        $dsn = new DSN($this->dsn);

        return $dsn->getScheme();
    }

    public function getHostname(): string
    {
        $dsn = new DSN($this->dsn);

        return $dsn->getHost();
    }
}
