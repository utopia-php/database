<?php

namespace Tests\Unit\Support;

use Override;
use Utopia\Database\Adapter\SQLite;

/**
 * SQLite that also reads inner-joined and joined-filtered pages from the first main rows they can reach and reads
 * again without them when those do not fill the page, as MySQL does, so the host exercises both statements.
 */
final class MatchedJoinSortSQLite extends SQLite
{
    #[Override]
    protected function boundsJoinedSort(): bool
    {
        return true;
    }

    #[Override]
    protected function boundsMatchedJoins(): bool
    {
        return true;
    }
}
