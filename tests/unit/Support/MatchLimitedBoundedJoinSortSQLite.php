<?php

namespace Tests\Unit\Support;

use Override;
use Utopia\Database\Adapter\SQLite;

/**
 * SQLite that bounds a joined page only when it needs a match from at most $matchedJoins joins, as MariaDB (none) and
 * MySQL (one) do.
 */
final class MatchLimitedBoundedJoinSortSQLite extends SQLite
{
    public function __construct(object $pdo, private readonly int $matchedJoins)
    {
        parent::__construct($pdo);
    }

    #[Override]
    protected function boundsJoinedSort(): bool
    {
        return true;
    }

    #[Override]
    protected function pageMatchedJoins(): int
    {
        return $this->matchedJoins;
    }
}
