<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter\Memory;
use Utopia\Database\Profiler\QueryProfiler;

final class ProfilerProbeAdapter extends Memory
{
    public ?QueryProfiler $profiled = null;

    public function ping(): bool
    {
        $this->profiled = $this->getProfiler();

        return true;
    }
}
