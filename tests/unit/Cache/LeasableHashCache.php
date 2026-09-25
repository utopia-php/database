<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Feature\Leasable;

final class LeasableHashCache implements CacheAdapter, Leasable
{
    /** @var array<string, array<string, array{time: int, data: array<int|string, mixed>|string}>> */
    private array $store = [];

    /** @var array<string, int> */
    private array $generations = [];

    /** @var array<string, int> */
    private array $purges = [];

    /** @var array<string, int> */
    private array $writes = [];

    private bool $failActivations = false;

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $hash = $hash === '' ? $key : $hash;
        $saved = $this->store[$key][$hash] ?? null;

        return $saved !== null && $saved['time'] + $ttl > \time() ? $saved['data'] : false;
    }

    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if ($key === '') {
            return false;
        }

        if (
            $this->failActivations
            && \str_ends_with($key, '#epoch')
            && \is_string($data)
            && \str_starts_with($data, 'active:')
        ) {
            return false;
        }

        $hash = $hash === '' ? $key : $hash;
        $this->writes[$key] = ($this->writes[$key] ?? 0) + 1;
        $this->store[$key][$hash] = ['time' => \time(), 'data' => $data];

        return $data;
    }

    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
    }

    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        if ($this->getGeneration($key) !== $generation) {
            return false;
        }

        return $this->save($key, $data, $hash);
    }

    public function touch(string $key, string $hash = ''): bool
    {
        $hash = $hash === '' ? $key : $hash;
        if (! isset($this->store[$key][$hash])) {
            return false;
        }

        $this->store[$key][$hash]['time'] = \time();

        return true;
    }

    /** @return array<string> */
    public function list(string $key): array
    {
        return \array_keys($this->store[$key] ?? []);
    }

    public function purge(string $key, string $hash = ''): bool
    {
        $this->purges[$key] = ($this->purges[$key] ?? 0) + 1;
        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;

        if ($hash === '') {
            unset($this->store[$key]);
        } else {
            unset($this->store[$key][$hash]);
        }

        return true;
    }

    public function flush(): bool
    {
        $this->store = [];
        $this->generations = [];

        return true;
    }

    public function ping(): bool
    {
        return true;
    }

    public function getSize(): int
    {
        return \count($this->store);
    }

    public function getName(?string $key = null): string
    {
        return 'leasable-hash';
    }

    public function resetPurges(): void
    {
        $this->purges = [];
        $this->writes = [];
    }

    public function getPurges(string $key): int
    {
        return $this->purges[$key] ?? 0;
    }

    public function getWrites(string $key): int
    {
        return $this->writes[$key] ?? 0;
    }

    public function failActivations(): void
    {
        $this->failActivations = true;
    }
}
