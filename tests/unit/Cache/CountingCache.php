<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Feature\Leasable;

/**
 * Counts the calls that each cost one round trip to a networked cache.
 */
final class CountingCache implements CacheAdapter, Leasable
{
    private int $operations = 0;

    public function __construct(
        private readonly CacheAdapter&Leasable $cache,
    ) {
    }

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $this->operations++;

        return $this->cache->load($key, $ttl, $hash);
    }

    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        $this->operations++;

        return $this->cache->save($key, $data, $hash);
    }

    public function touch(string $key, string $hash = ''): bool
    {
        $this->operations++;

        return $this->cache->touch($key, $hash);
    }

    /** @return array<string> */
    public function list(string $key): array
    {
        $this->operations++;

        return $this->cache->list($key);
    }

    public function purge(string $key, string $hash = ''): bool
    {
        $this->operations++;

        return $this->cache->purge($key, $hash);
    }

    public function flush(): bool
    {
        $this->operations++;

        return $this->cache->flush();
    }

    public function ping(): bool
    {
        $this->operations++;

        return $this->cache->ping();
    }

    public function getSize(): int
    {
        $this->operations++;

        return $this->cache->getSize();
    }

    public function getName(?string $key = null): string
    {
        return $this->cache->getName($key);
    }

    public function getGeneration(string $key): string
    {
        $this->operations++;

        return $this->cache->getGeneration($key);
    }

    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        $this->operations++;

        return $this->cache->saveWithLease($key, $data, $hash, $generation);
    }

    public function getOperations(): int
    {
        return $this->operations;
    }

    public function resetOperations(): void
    {
        $this->operations = 0;
    }
}
