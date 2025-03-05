<?php

namespace Utopia\Database;

use Swoole\Database\DetectsLostConnections;
use Utopia\CLI\Console;

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
        try {
            return $this->pdo->{$method}(...$args);
        } catch (\Throwable $e) {
            /** @phpstan-ignore-next-line can't find static method */
            if (DetectsLostConnections::causedByLostConnection($e)) {
                Console::warning('[Database] Lost connection detected. Reconnecting...');
                $this->reconnect();
                return $this->pdo->{$method}(...$args);
            }

            throw $e;
        }
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
}
