<?php

namespace Tests\Unit\Support;

use Override;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Builder\Scoping;
use Utopia\Query\Builder\SQL as SQLBuilder;

/**
 * SQLite running FULL OUTER JOIN natively (3.39+), the single statement PostgreSQL runs, as the
 * reference an emulated join chain on MariaDB, MySQL and SQLite has to reproduce row for row.
 */
final class NativeJoinChainSQLite extends SQLite
{
    #[Override]
    protected function dialectBuilder(): SQLBuilder&Scoping
    {
        return new FullOuterJoinSQLiteBuilder();
    }
}
