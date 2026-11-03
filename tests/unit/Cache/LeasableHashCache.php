<?php

namespace Tests\Unit\Cache;

final class LeasableHashCache extends RedisLeasableCache
{
    public function failActivations(): void
    {
        $this->failCompletions();
    }
}
