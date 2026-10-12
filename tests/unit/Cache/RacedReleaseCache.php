<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Feature\Leasable;

/**
 * A Redis-like cache where another worker releases a write's owner registration between the write's read of it and
 * its own release, so that release finds nothing left to remove.
 */
final class RacedReleaseCache implements CacheAdapter, Leasable
{
    private RedisLeasableCache $cache;

    private bool $racing = false;

    public function __construct()
    {
        $this->cache = new RedisLeasableCache();
    }

    public function releaseBeforeNextOwnerRelease(): void
    {
        $this->racing = true;
    }

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        return $this->cache->load($key, $ttl, $hash);
    }

    #[\Override]
    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        return $this->cache->save($key, $data, $hash);
    }

    #[\Override]
    public function touch(string $key, string $hash = ''): bool
    {
        return $this->cache->touch($key, $hash);
    }

    /** @return array<string> */
    #[\Override]
    public function list(string $key): array
    {
        return $this->cache->list($key);
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        if ($this->racing && $hash !== '' && \str_ends_with($key, '#owners')) {
            $this->racing = false;
            $this->cache->purge($key, $hash);
        }

        return $this->cache->purge($key, $hash);
    }

    #[\Override]
    public function flush(): bool
    {
        return $this->cache->flush();
    }

    #[\Override]
    public function ping(): bool
    {
        return true;
    }

    #[\Override]
    public function getSize(): int
    {
        return $this->cache->getSize();
    }

    #[\Override]
    public function getName(?string $key = null): string
    {
        return 'raced-release';
    }

    #[\Override]
    public function getGeneration(string $key): string
    {
        return $this->cache->getGeneration($key);
    }

    #[\Override]
    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        return $this->cache->saveWithLease($key, $data, $hash, $generation);
    }
}
