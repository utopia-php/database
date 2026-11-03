<?php

namespace Tests\Unit\Cache;

/** Evicts a registration immediately before its owner's completion CAS. */
final class RacedReleaseCache extends RedisLeasableCache
{
    private bool $racing = false;

    public function releaseBeforeNextOwnerRelease(): void
    {
        $this->racing = true;
    }

    #[\Override]
    public function compareAndSwap(string $key, array|string $data, string $hash, string $version): string|false
    {
        $before = $this->load($key, PHP_INT_MAX);
        if ($this->racing && \str_ends_with($key, '#state') && \is_array($before) && \is_array($data) && \is_array($data['owners'] ?? null) && \is_array($before['owners'] ?? null)
            && \count($data['owners']) < \count($before['owners'])) {
            $this->racing = false;
            $this->evict($key);
        }

        return parent::compareAndSwap($key, $data, $hash, $version);
    }
}
