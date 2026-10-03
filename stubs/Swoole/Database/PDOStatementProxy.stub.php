<?php

namespace Swoole\Database;

use PDO;
use PDOStatement;

/**
 * @mixin PDOStatement
 */
class PDOStatementProxy
{
    public string $queryString;

    public function __construct(PDOStatement $statement, PDOProxy $parent)
    {
    }

    /** @param array<mixed> $arguments */
    public function __call(string $name, array $arguments): mixed
    {
    }

    public function bindColumn(
        int|string $column,
        mixed &$variable,
        int $type = PDO::PARAM_STR,
        int $maxLength = 0,
        mixed $driverOptions = null,
    ): bool {
    }

    public function bindParam(
        int|string $param,
        mixed &$variable,
        int $type = PDO::PARAM_STR,
        int $maxLength = 0,
        mixed $driverOptions = null,
    ): bool {
    }

    public function bindValue(int|string $param, mixed $value, int $type = PDO::PARAM_STR): bool
    {
    }

    public function closeCursor(): bool
    {
    }

    /** @param array<mixed>|null $params */
    public function execute(?array $params = null): bool
    {
    }

    public function fetch(
        int $mode = PDO::FETCH_DEFAULT,
        int $cursorOrientation = PDO::FETCH_ORI_NEXT,
        int $cursorOffset = 0,
    ): mixed {
    }

    /** @return array<mixed> */
    public function fetchAll(int $mode = PDO::FETCH_DEFAULT, mixed ...$args): array
    {
    }

    public function fetchColumn(int $column = 0): mixed
    {
    }

    public function getAttribute(int $attribute): mixed
    {
    }

    public function rowCount(): int
    {
    }

    public function setAttribute(int $attribute, mixed $value): bool
    {
    }

    public function setFetchMode(int $mode, mixed ...$args): bool
    {
    }
}
