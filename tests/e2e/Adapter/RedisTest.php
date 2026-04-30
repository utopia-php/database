<?php

namespace Tests\E2E\Adapter;

class RedisTest extends RedisBase
{
    public function setUp(): void
    {
        parent::setUp();

        $this->getDatabase()->setSharedTables(false);
    }
}
