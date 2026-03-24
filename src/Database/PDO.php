<?php

namespace Utopia\Database;

use Exception;
use InvalidArgumentException;
use PDO as PhpPDO;
use PDOStatement;
use Throwable;
use Utopia\Console;

/**
 * A PDO wrapper that forwards method calls to the internal PDO instance.
 *
 * @mixin PhpPDO
 *
 * @method PDOStatement prepare(string $query, array<int, mixed> $options = [])
 * @method int|false exec(string $statement)
 * @method bool beginTransaction()
 * @method bool commit()
 * @method bool rollBack()
 * @method bool inTransaction()
 * @method string|false quote(string $string, int $type = PhpPDO::PARAM_STR)
 * @method bool setAttribute(int $attribute, mixed $value)
 * @method mixed getAttribute(int $attribute)
 * @method string|false lastInsertId(?string $name = null)
 */
class PDO
{
    protected PhpPDO $pdo;

    /**
     * Create a new PDO wrapper instance.
     *
     * @param string $dsn The Data Source Name
     * @param string|null $username The database username
     * @param string|null $password The database password
     * @param  array<mixed>  $config PDO driver options
     */
    public function __construct(
        protected string $dsn,
        protected ?string $username,
        protected ?string $password,
        protected array $config = []
    ) {
        $this->pdo = new PhpPDO(
            $this->dsn,
            $this->username,
            $this->password,
            $this->config
        );
    }

    /**
     * @param  array<mixed>  $args
     *
     * @throws Throwable
     */
    public function __call(string $method, array $args): mixed
    {
        try {
            return $this->pdo->{$method}(...$args);
        } catch (Throwable $e) {
            if (Connection::hasError($e)) {
                Console::warning('[Database] '.$e->getMessage());
                Console::warning('[Database] Lost connection detected. Reconnecting...');

                $inTransaction = $this->pdo->inTransaction();

                // Attempt to reconnect
                $this->reconnect();

                // If we weren't in a transaction, also retry the query
                // In a transaction we can't retry as the state is attached to the previous connection
                if (! $inTransaction) {
                    return $this->pdo->{$method}(...$args);
                }
            }

            throw $e;
        }
    }

    /**
     * Create a new connection to the database
     */
    public function reconnect(): void
    {
        $this->pdo = new PhpPDO(
            $this->dsn,
            $this->username,
            $this->password,
            $this->config
        );
    }

    /**
     * Get the hostname from the DSN.
     *
     * @throws Exception
     */
    public function getHostname(): string
    {
        $parts = $this->parseDsn($this->dsn);

        /**
         * @var string $host
         */
        $host = $parts['host'] ?? throw new Exception('No host found in DSN');

        return $host;
    }

    /**
     * Parse a PDO-style DSN string.
     *
     * @return array<string, string|int|float|bool|null>
     *
     * @throws InvalidArgumentException If the DSN is malformed.
     */
    private function parseDsn(string $dsn): array
    {
        if ($dsn === '' || ! \str_contains($dsn, ':')) {
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

            $name = \trim((string) $name);
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
