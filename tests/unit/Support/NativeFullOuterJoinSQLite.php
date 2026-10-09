<?php

namespace Tests\Unit\Support;

use Override;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Builder\ScopesCollections;
use Utopia\Database\Builder\Scoping;
use Utopia\Query\Builder\Feature\FullOuterJoins;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Builder\SQLite as SQLiteBuilder;
use Utopia\Query\Builder\Trait\FullOuterJoins as FullOuterJoinsTrait;

/**
 * SQLite that runs FULL OUTER JOIN natively (SQLite 3.39+) instead of emulating it with a
 * LEFT JOIN UNION ALL RIGHT JOIN, so the host exercises the single-statement path PostgreSQL takes.
 */
final class NativeFullOuterJoinSQLite extends SQLite
{
    #[Override]
    protected function dialectBuilder(): SQLBuilder&Scoping
    {
        return new class () extends SQLiteBuilder implements FullOuterJoins, Scoping {
            use FullOuterJoinsTrait;
            use ScopesCollections;
        };
    }
}
