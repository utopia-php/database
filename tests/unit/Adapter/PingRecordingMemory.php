<?php

namespace Tests\Unit\Adapter;

use Closure;
use Swoole\Coroutine;
use Utopia\Database\Adapter\Memory;

final class PingRecordingMemory extends Memory
{
    /**
     * @var list<array{coroutine: int, before: int|string|null, after: int|string|null}>
     */
    public array $pings = [];

    private ?Closure $pause = null;

    public function pauseNextPing(Closure $pause): void
    {
        $this->pause = $pause;
    }

    public function ping(): bool
    {
        $before = $this->getTenant();
        $pause = $this->pause;
        $this->pause = null;
        if ($pause !== null) {
            $pause();
        }

        $this->pings[] = ['coroutine' => Coroutine::getCid(), 'before' => $before, 'after' => $this->getTenant()];

        return true;
    }
}
