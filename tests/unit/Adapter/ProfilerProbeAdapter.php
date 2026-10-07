<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Profiler\QueryProfiler;

final class ProfilerProbeAdapter extends Memory implements Feature\Connection
{
    public ?QueryProfiler $profiled = null;

    #[\Override]
    public function ping(): bool
    {
        $this->profiled = $this->getProfiler();

        return true;
    }

    #[\Override]
    public function reconnect(): void
    {
    }

    public function id(): string
    {
        return 'probe';
    }

    public function hostname(): string
    {
        return '';
    }
}
