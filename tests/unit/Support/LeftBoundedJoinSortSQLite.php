<?php

namespace Tests\Unit\Support;

use Override;
use Utopia\Database\Adapter\SQLite;

/**
 * SQLite that bounds a joined page only when every main row the read matches gives it a row, as MariaDB does: left
 * joins without conditions on joined attributes, with or without a search.
 */
final class LeftBoundedJoinSortSQLite extends SQLite
{
    #[Override]
    protected function boundsJoinedSort(): bool
    {
        return true;
    }
}
