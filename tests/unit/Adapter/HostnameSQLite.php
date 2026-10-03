<?php

namespace Tests\Unit\Adapter;

use PDO;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Capability;

final class HostnameSQLite extends SQLite
{
    public function __construct(string $hostname)
    {
        parent::__construct(new PDO('sqlite::memory:'));
        $this->setHostname($hostname);
    }

    #[\Override]
    public function capabilities(): array
    {
        return [...parent::capabilities(), Capability::Hostname];
    }
}
