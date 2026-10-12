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

    private bool $failActivations = false;

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $hash = $hash === '' ? $key : $hash;
        $saved = $this->store[$key][$hash] ?? null;

        return $saved !== null && $saved['time'] + $ttl > \time() ? $saved['data'] : false;
    }

    #[\Override]
    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
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
        $this->store[$key][$hash] = ['time' => \time(), 'data' => $data];

        return $data;
    }

    #[\Override]
    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
    }

    #[\Override]
    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        if ($this->getGeneration($key) !== $generation) {
            return false;
        }

        return $this->save($key, $data, $hash);
    }

    #[\Override]
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
    #[\Override]
    public function list(string $key): array
    {
        return \array_keys($this->store[$key] ?? []);
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;

        if ($hash === '') {
            unset($this->store[$key]);
        } else {
            unset($this->store[$key][$hash]);
        }

        return true;
    }

    #[\Override]
    public function flush(): bool
    {
        $this->store = [];
        $this->generations = [];

        return true;
    }

    #[\Override]
    public function ping(): bool
    {
        return true;
    }

    #[\Override]
    public function getSize(): int
    {
        return \count($this->store);
    }

    #[\Override]
    public function getName(?string $key = null): string
    {
        return 'leasable-hash';
    }

    public function failActivations(): void
    {
        $this->failActivations = true;
    }
}
