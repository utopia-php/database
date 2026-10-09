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
 * SQLite running FULL OUTER JOIN natively (3.39+), the single statement PostgreSQL runs, as the
 * reference an emulated join chain on MariaDB, MySQL and SQLite has to reproduce row for row.
 */
final class NativeJoinChainSQLite extends SQLite
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
