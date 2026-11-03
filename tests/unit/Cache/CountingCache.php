<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Feature\Versioned;

/**
 * Counts the calls that each cost one round trip to a networked cache.
 */
final class CountingCache implements CacheAdapter, Versioned
{
    #[\Override]
    public function supportsVersioning(): bool
    {
        return true;
    }
    #[\Override]
    public function getVersion(string $key): string
    {
        return $this->cache->getVersion($key);
    }
    #[\Override]
    public function loadVersioned(string $key): array
    {
        return ['version' => $this->getVersion($key), 'value' => $this->load($key, PHP_INT_MAX)];
    }
    #[\Override]
    public function compareAndSwap(string $key, array|string $data, string $hash, string $version): string|false
    {
        if ($this->getVersion($key) !== $version) {
            return false;
        }
        if ($this->save($key, $data, $hash) === false) {
            throw new \Utopia\Cache\Exception\Unavailable('Cache write failed');
        }
        return $this->cache->getVersion($key);
    }

    private int $operations = 0;

    /** @var array<string, int> */
    private array $loads = [];

    public function __construct(
        private readonly CacheAdapter&Versioned $cache,
    ) {
    }

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $this->operations++;
        $this->loads[$key] = ($this->loads[$key] ?? 0) + 1;

        return $this->cache->load($key, $ttl, $hash);
    }

    #[\Override]
    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        $this->operations++;

        return $this->cache->save($key, $data, $hash);
    }

    #[\Override]
    public function touch(string $key, string $hash = ''): bool
    {
        $this->operations++;

        return $this->cache->touch($key, $hash);
    }

    /** @return array<string> */
    #[\Override]
    public function list(string $key): array
    {
        $this->operations++;

        return $this->cache->list($key);
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        $this->operations++;

        return $this->cache->purge($key, $hash);
    }

    #[\Override]
    public function flush(): bool
    {
        $this->operations++;

        return $this->cache->flush();
    }

    #[\Override]
    public function ping(): bool
    {
        $this->operations++;

        return $this->cache->ping();
    }

    #[\Override]
    public function getSize(): int
    {
        $this->operations++;

        return $this->cache->getSize();
    }

    #[\Override]
    public function getName(?string $key = null): string
    {
        return $this->cache->getName($key);
    }

    public function getOperations(): int
    {
        return $this->operations;
    }

    /**
     * @return array<string, int>
     */
    public function getLoads(): array
    {
        return $this->loads;
    }

    public function resetOperations(): void
    {
        $this->operations = 0;
        $this->loads = [];
    }
}
