<?php

namespace Tests\Unit\Documents\Interleaving;

use PDO;
use Utopia\Database\Adapter\SQLite as SQLiteAdapter;

final class SQLite extends SQLiteAdapter
{
    use Interleaves;

    public static function inMemory(): self
    {
        return new self(new PDO('sqlite::memory:'));
    }
}
