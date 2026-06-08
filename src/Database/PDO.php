<?php

namespace Utopia\Database;

use InvalidArgumentException;
use Utopia\Console;

/**
 * A PDO wrapper that forwards method calls to the internal PDO instance.
 *
 * @mixin \PDO
 */
class PDO
{
    protected \PDO $pdo;

    /**
     * @var array<string>
     */
    protected static array $transientErrors = [
        'Access denied',
        'Max connect timeout reached',
        'Connection refused',
        'Too many connections',
    ];

    /**
     * @param string $dsn
     * @param ?string $username
     * @param ?string $password
     * @param array<mixed> $config
     * @param int $retries
     */
    public function __construct(
        protected string $dsn,
        protected ?string $username,
        protected ?string $password,
        protected array $config = [],
        protected int $retries = 3
    ) {
        $this->pdo = $this->createPDO();
    }

    /**
     * Create a new PDO instance
     *
     * @return \PDO
     */
    protected function createPDO(): \PDO
    {
        return new \PDO(
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
            if (Connection::hasError($e)) {
                Console::warning('[Database] ' . $e->getMessage());
                Console::warning('[Database] Lost connection detected. Reconnecting...');

                $inTransaction = $this->pdo->inTransaction();

                // Attempt to reconnect
                $this->reconnect();

                // If we weren't in a transaction, also retry the query
                // In a transaction we can't retry as the state is attached to the previous connection
                if (!$inTransaction) {
                    return $this->pdo->{$method}(...$args);
                }
            }

            throw $e;
        }
    }

    /**
     * Create a new connection to the database with retry logic for transient errors
     *
     * @return void
     * @throws \PDOException
     */
    public function reconnect(): void
    {
        $lastException = null;

        for ($attempt = 1; $attempt <= $this->retries; $attempt++) {
            try {
                $this->pdo = $this->createPDO();
                return;
            } catch (\PDOException $e) {
                $lastException = $e;

                if (!static::isTransientError($e) || $attempt === $this->retries) {
                    throw $e;
                }

                Console::warning('[Database] ' . $e->getMessage());
                Console::warning("[Database] Transient connection error, retrying ({$attempt}/{$this->retries})...");

                \usleep($attempt * 100000); // 100ms, 200ms, 300ms...
            }
        }
    }

    /**
     * Check if an exception is a transient connection error that can be retried
     *
     * @param \PDOException $e
     * @return bool
     */
    protected static function isTransientError(\PDOException $e): bool
    {
        $message = $e->getMessage();
        foreach (static::$transientErrors as $needle) {
            if (\mb_strpos($message, $needle) !== false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the hostname from the DSN.
     *
     * @return string
     * @throws \Exception
     */
    public function getHostname(): string
    {
        $parts = $this->parseDsn($this->dsn);

        /**
         * @var string $host
         */
        $host = $parts['host'] ?? throw new \Exception('No host found in DSN');

        return $host;
    }

    /**
     * Parse a PDO-style DSN string.
     *
     * @return array<string, string|int|float|bool|null>
     * @throws InvalidArgumentException If the DSN is malformed.
     */
    private function parseDsn(string $dsn): array
    {
        if ($dsn === '' || !\str_contains($dsn, ':')) {
            throw new InvalidArgumentException('Malformed DSN: missing driver separator.');
        }

        [$driver, $parameterString] = \explode(':', $dsn, 2);

        $parsed = ['driver' => \trim($driver)];

        // Handle “path only” DSNs like sqlite:/path/to.db
        if (\in_array($driver, ['sqlite'], true) && $parameterString !== '') {
            $parsed['path'] = \ltrim($parameterString, '/');
            return $parsed;
        }

        $parameterSegments = \array_filter(\explode(';', $parameterString));

        foreach ($parameterSegments as $segment) {
            [$name, $rawValue] = \array_pad(\explode('=', $segment, 2), 2, null);

            $name  = \trim($name);
            $value = $rawValue !== null ? \trim($rawValue) : null;

            // Casting for scalars
            if ($value === 'true' || $value === 'false') {
                $value = $value === 'true';
            } elseif (\is_numeric($value)) {
                $value += 0;
            }

            $parsed[$name] = $value;
        }

        return $parsed;
    }
}
