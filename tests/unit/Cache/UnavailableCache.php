<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Exception\Unavailable;
use Utopia\Cache\Feature\Leasable;

/**
 * A Redis-like cache behind utopia-php/cache's circuit breaker. While the circuit is open every call takes its
 * fallback: reads miss and saves report false. A purge the breaker cannot confirm throws instead of reporting
 * nothing to purge. With only writes failing, reads are still served.
 */
final class UnavailableCache implements CacheAdapter, Leasable
{
    private RedisLeasableCache $cache;

    private bool $open = false;

    private bool $failingWrites = false;

    private int $failingPurges = 0;

    private int $failingSaves = 0;

    public function __construct()
    {
        $this->cache = new RedisLeasableCache();
    }

    public function open(): void
    {
        $this->open = true;
    }

    public function failWrites(): void
    {
        $this->failingWrites = true;
    }

    /**
     * A blip rather than an outage: the next purges cannot be confirmed, later ones can.
     */
    public function failNextPurges(int $count): void
    {
        $this->failingPurges = $count;
    }

    /**
     * A blip on writes: the next saves report false, later ones are stored.
     */
    public function failNextSaves(int $count): void
    {
        $this->failingSaves = $count;
    }

    public function recover(): void
    {
        $this->open = false;
        $this->failingWrites = false;
    }

    private function writable(): bool
    {
        return ! $this->open && ! $this->failingWrites;
    }

    private function confirmPurge(): void
    {
        if ($this->failingPurges > 0) {
            $this->failingPurges--;

            throw new Unavailable('Cache purge could not be confirmed');
        }

        if (! $this->writable()) {
            throw new Unavailable('Cache purge could not be confirmed');
        }
    }

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        return $this->open ? false : $this->cache->load($key, $ttl, $hash);
    }

    #[\Override]
    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        if ($this->failingSaves > 0) {
            $this->failingSaves--;

            return false;
        }

        return $this->writable() ? $this->cache->save($key, $data, $hash) : false;
    }

    #[\Override]
    public function touch(string $key, string $hash = ''): bool
    {
        return $this->writable() && $this->cache->touch($key, $hash);
    }

    /** @return array<string> */
    #[\Override]
    public function list(string $key): array
    {
        return $this->open ? [] : $this->cache->list($key);
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        $this->confirmPurge();

        return $this->cache->purge($key, $hash);
    }

    #[\Override]
    public function flush(): bool
    {
        return $this->writable() && $this->cache->flush();
    }

    #[\Override]
    public function ping(): bool
    {
        return ! $this->open;
    }

    #[\Override]
    public function getSize(): int
    {
        return $this->open ? 0 : $this->cache->getSize();
    }

    #[\Override]
    public function getName(?string $key = null): string
    {
        return 'unavailable';
    }

    #[\Override]
    public function getGeneration(string $key): string
    {
        return $this->open ? '0' : $this->cache->getGeneration($key);
    }

    #[\Override]
    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        return $this->writable() ? $this->cache->saveWithLease($key, $data, $hash, $generation) : false;
    }
}
