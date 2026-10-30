<?php

namespace Tests\Unit\Cache;

use Closure;
use Utopia\Cache\Exception\Unavailable;

/** Interleaves another writer or a cache loss before the atomic comparison. */
final class OwnershipCache extends RedisLeasableCache
{
    private ?Closure $activation = null;
    private bool $flushDuringActivation = false;
    private bool $failDuringActivation = false;

    #[\Override]
    public function compareAndSwap(string $key, array|string $data, string $hash, string $version): string|false
    {
        $before = $this->load($key, PHP_INT_MAX);
        $finishing = \str_ends_with($key, '#state') && \is_array($before) && \is_array($data) && \is_array($data['owners'] ?? null) && \is_array($before['owners'] ?? null)
            && \count($data['owners']) < \count($before['owners']);
        if ($finishing) {
            $callback = $this->activation;
            $this->activation = null;
            $callback?->__invoke();
            if ($this->flushDuringActivation) {
                $this->flushDuringActivation = false;
                $this->flush();
            }
            if ($this->failDuringActivation) {
                throw new Unavailable('Completion unavailable');
            }
        }

        return parent::compareAndSwap($key, $data, $hash, $version);
    }

    public function pauseNextActivation(Closure $activation): void
    {
        $this->activation = $activation;
    }

    public function flushDuringActivation(): void
    {
        $this->flushDuringActivation = true;
    }

    public function failDuringActivation(): void
    {
        $this->failDuringActivation = true;
    }
}
