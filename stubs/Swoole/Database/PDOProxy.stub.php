<?php

namespace Swoole\Database;

use PDO;

/**
 * @mixin PDO
 */
class PDOProxy
{
    /** @param callable(): PDO $constructor */
    public function __construct(callable $constructor)
    {
    }

    /** @param array<mixed> $arguments */
    public function __call(string $name, array $arguments): mixed
    {
    }

    public function __getObject(): PDO
    {
    }

    public function beginTransaction(): bool
    {
    }

    public function commit(): bool
    {
    }

    public function exec(string $statement): int|false
    {
    }

    public function getAttribute(int $attribute): mixed
    {
    }

    public function getRound(): int
    {
    }

    public function inTransaction(): bool
    {
    }

    public function lastInsertId(?string $name = null): string|false
    {
    }

    /** @param array<int, mixed> $options */
    public function prepare(string $query, array $options = []): PDOStatementProxy|false
    {
    }

    public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatementProxy|false
    {
    }

    public function quote(string $string, int $type = PDO::PARAM_STR): string|false
    {
    }

    public function reconnect(): void
    {
    }

    public function reset(): void
    {
    }

    public function rollBack(): bool
    {
    }

    public function setAttribute(int $attribute, mixed $value): bool
    {
    }
}
