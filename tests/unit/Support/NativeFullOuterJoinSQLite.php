<?php

namespace Tests\Unit\Support;

use Override;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Builder\Scoping;
use Utopia\Query\Builder\SQL as SQLBuilder;

/**
 * SQLite that runs FULL OUTER JOIN natively (SQLite 3.39+) instead of emulating it with a
 * LEFT JOIN UNION ALL RIGHT JOIN, so the host exercises the single-statement path PostgreSQL takes.
 */
final class NativeFullOuterJoinSQLite extends SQLite
{
    #[Override]
    protected function dialectBuilder(): SQLBuilder&Scoping
    {
        return new FullOuterJoinSQLiteBuilder();
    }
}
