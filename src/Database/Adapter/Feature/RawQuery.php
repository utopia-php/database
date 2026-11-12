<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

interface RawQuery
{
    /**
     * @param array<mixed> $bindings Parameter bindings for prepared statements.
     * @return array<Document> The query results as Document objects.
     */
    public function rawQuery(string $query, array $bindings = []): array;

    /**
     * @param array<mixed> $bindings Parameter bindings for prepared statements.
     */
    public function rawMutation(string $query, array $bindings = []): int;
}
