<?php

namespace Utopia\Database;

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
        $parts = $this->parseDsn($this->dsn);

        return $parts['scheme'] ?? throw new \Exception('No scheme found in DSN');
    }

    public function getHostname(): string
    {
        $parts = $this->parseDsn($this->dsn);

        return $parts['host'] ?? throw new \Exception('No host found in DSN');
    }

    /**
     * @param string $dsn
     * @return array<mixed>
     */
    public function parseDsn(string $dsn): array
    {
        $result = [];

        [$driver, $params] = explode(':', $dsn, 2);
        $result['driver'] = $driver;

        foreach (explode(';', $params) as $pair) {
            if (str_contains($pair, '=')) {
                [$key, $value] = explode('=', $pair, 2);
                $result[$key] = $value;
            }
        }

        return $result;
    }
}
