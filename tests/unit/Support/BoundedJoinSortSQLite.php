<?php

namespace Tests\Unit\Support;

use Override;
use Utopia\Database\Adapter\SQLite;

/**
 * SQLite that picks the main rows a left-joined page can reach before it joins them, as MariaDB and MySQL do, so the
 * host exercises that statement.
 */
final class BoundedJoinSortSQLite extends SQLite
{
    #[Override]
    protected function boundsJoinedSort(): bool
    {
        return true;
    }
}
