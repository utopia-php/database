<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

/**
 * Defines raw query and mutation operations for a database adapter.
 */
interface RawQuery
{
    /**
     * Execute a raw query and return results as Documents.
     *
     * @param string $query The raw query string.
     * @param array<mixed> $bindings Parameter bindings for prepared statements.
     * @return array<Document> The query results as Document objects.
     */
    public function rawQuery(string $query, array $bindings = []): array;

    /**
     * Execute a raw mutation and return the number of affected rows.
     *
     * @param string $query The raw mutation string.
     * @param array<mixed> $bindings Parameter bindings for prepared statements.
     * @return int The number of affected rows.
     */
    public function rawMutation(string $query, array $bindings = []): int;
}
