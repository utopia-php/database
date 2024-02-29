<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Database;

class MariaDBProxy extends Proxy
{
    /**
     * Returns number of attributes used by default.
     *
     * @return int
     */
    public static function getCountOfDefaultAttributes(): int
    {
        return \count(Database::INTERNAL_ATTRIBUTES);
    }

    /**
     * Returns number of indexes used by default.
     *
     * @return int
     */
    public static function getCountOfDefaultIndexes(): int
    {
        return \count(Database::INTERNAL_INDEXES);
    }

    /**
     * Get maximum width, in bytes, allowed for a SQL row
     * Return 0 when no restrictions apply
     *
     * @return int
     */
    public static function getDocumentSizeLimit(): int
    {
        return 65535;
    }
}
