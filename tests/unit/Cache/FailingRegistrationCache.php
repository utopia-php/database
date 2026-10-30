<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Feature\Versioned;

/**
 * A Redis-like cache that refuses a number of owner registrations while every other read and write succeeds, the way
 * a save that misses its read deadline returns false from an otherwise healthy Redis.
 */
final class FailingRegistrationCache implements CacheAdapter, Versioned
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

    private RedisLeasableCache $cache;

    private int $failures = 0;

    private int $registrations = 0;

    public function __construct()
    {
        $this->cache = new RedisLeasableCache();
    }

    public function failRegistrations(int $count): void
    {
        $this->failures = $count;
    }

    public function getRegistrations(): int
    {
        return $this->registrations;
    }

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        return $this->cache->load($key, $ttl, $hash);
    }

    #[\Override]
    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        if (\str_ends_with($key, '#state') && \is_array($data) && ! empty($data['owners'])) {
            $this->registrations++;
            if ($this->failures > 0) {
                $this->failures--;

                return false;
            }
        }

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
        return 'failing-registration';
    }
}
