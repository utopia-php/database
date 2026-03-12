<?php

namespace Utopia\Database\Hook;

use Closure;
use Utopia\Query\Builder\BuildResult;

readonly class WriteContext
{
    /**
     * @param Closure(string, string=): \Utopia\Query\Builder\SQL $newBuilder Create a query builder for a table (with read-side hooks like TenantFilter already applied)
     * @param Closure(BuildResult, string=): mixed $executeResult Prepare a BuildResult with optional event trigger, returns PDO statement
     * @param Closure(mixed): bool $execute Execute a prepared statement
     * @param Closure(array<string, mixed>, array<string, mixed>): array<string, mixed> $decorateRow Apply all write hooks' decorateRow to a row
     * @param Closure(): \Utopia\Query\Builder\SQL $createBuilder Create a raw builder (no hooks, no table)
     * @param Closure(string): string $getTableRaw Get the raw SQL table name with namespace prefix
     */
    public function __construct(
        public Closure $newBuilder,
        public Closure $executeResult,
        public Closure $execute,
        public Closure $decorateRow,
        public Closure $createBuilder,
        public Closure $getTableRaw,
    ) {
    }
}
