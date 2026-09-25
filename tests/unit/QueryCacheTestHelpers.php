<?php

namespace Tests\Unit;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

trait QueryCacheTestHelpers
{
    private function purgeCachedQueries(Database $database, string $collection, ?string $namespace = null): bool
    {
        $method = new \ReflectionMethod(Database::class, 'purgeCachedQueries');

        return $method->invoke($database, $collection, $namespace);
    }

    private function getQueryCacheKey(Database $database, string $collectionId, ?string $namespace = null): string
    {
        $method = new \ReflectionMethod(Database::class, 'getQueryCacheKey');

        return $method->invoke($database, $collectionId, $namespace);
    }

    /**
     * @param array<Query> $queries
     */
    private function getQueryCacheField(
        Database $database,
        ?Document $collection = null,
        array $queries = [],
        string $field = 'documents',
        string $forPermission = Database::PERMISSION_READ,
    ): ?string {
        $method = new \ReflectionMethod(Database::class, 'getQueryCacheField');

        return $method->invoke($database, $collection, $queries, $field, $forPermission);
    }
}
