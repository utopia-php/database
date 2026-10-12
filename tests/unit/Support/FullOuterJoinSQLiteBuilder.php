<?php

namespace Tests\Unit\Support;

use Utopia\Database\Builder\ScopesCollections;
use Utopia\Database\Builder\Scoping;
use Utopia\Query\Builder\Feature\FullOuterJoins;
use Utopia\Query\Builder\SQLite;
use Utopia\Query\Builder\Trait\FullOuterJoins as FullOuterJoinsTrait;

/**
 * The SQLite builder with FULL OUTER JOIN (SQLite 3.39+), scoped as the adapter's builder is.
 */
final class FullOuterJoinSQLiteBuilder extends SQLite implements FullOuterJoins, Scoping
{
    use FullOuterJoinsTrait;
    use ScopesCollections;
}
