<?php

namespace Tests\E2E\Adapter\SharedTables;

use Tests\E2E\Adapter\RedisBase;

class RedisTest extends RedisBase
{
    public function setUp(): void
    {
        parent::setUp();

        $database = $this->getDatabase();
        $database->setSharedTables(true);
        $database->setTenant(999);
        $database->setNamespace('');
    }
}
