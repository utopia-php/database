<?php

namespace Tests\Unit\Adapter;

use PDO;
use Utopia\Database\Adapter\SQLite;

final class HostnameSQLite extends SQLite
{
    public function __construct(string $hostname)
    {
        parent::__construct(new PDO('sqlite::memory:'));
        $this->setHostname($hostname);
    }
}
