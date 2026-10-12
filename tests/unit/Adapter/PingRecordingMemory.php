<?php

namespace Tests\Unit\Adapter;

use Closure;
use Swoole\Coroutine;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;

final class PingRecordingMemory extends Memory implements Feature\Connection
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

    #[\Override]
    public function ping(): bool
    {
        $before = $this->getTenant();
        $pause = $this->pause;
        $this->pause = null;
        if ($pause !== null) {
            $pause();
        }

        /** @var int $coroutine */
        $coroutine = Coroutine::getCid();
        $this->pings[] = ['coroutine' => $coroutine, 'before' => $before, 'after' => $this->getTenant()];

        return true;
    }

    #[\Override]
    public function reconnect(): void
    {
    }

    #[\Override]
    public function id(): string
    {
        return 'ping-recording';
    }

    #[\Override]
    public function hostname(): string
    {
        return '';
    }
}
