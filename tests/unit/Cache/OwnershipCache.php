<?php

namespace Tests\Unit\Cache;

use Closure;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Feature\Leasable;

final class OwnershipCache implements CacheAdapter, Leasable
{
    /** @var array<string, array<string, array{time: int, data: array<int|string, mixed>|string}>> */
    private array $store = [];

    /** @var array<string, int> */
    private array $generations = [];

    private ?Closure $activation = null;

    private bool $flushDuringActivation = false;

    private bool $failDuringActivation = false;

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $saved = $this->store[$key][$this->field($key, $hash)] ?? null;

        return $saved !== null && $saved['time'] + $ttl > \time() ? $saved['data'] : false;
    }

    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if ($key === '') {
            return false;
        }

        if (
            $this->activation !== null
            && \str_ends_with($key, '#epoch')
            && \is_string($data)
            && \str_starts_with($data, 'active:')
        ) {
            $activation = $this->activation;
            $this->activation = null;
            $activation();
        }

        $this->store[$key][$this->field($key, $hash)] = ['time' => \time(), 'data' => $data];

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
        $field = $this->field($key, $hash);
        if (! isset($this->store[$key][$field])) {
            return false;
        }

        $this->store[$key][$field]['time'] = \time();

        return true;
    }

    /** @return array<string> */
    public function list(string $key): array
    {
        return [];
    }

    public function purge(string $key, string $hash = ''): bool
    {
        if ($this->flushDuringActivation && \str_ends_with($key, '#finished')) {
            $this->flushDuringActivation = false;

            return $this->flush();
        }
        if ($this->failDuringActivation && \str_ends_with($key, '#finished')) {
            return false;
        }

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
        return 'ownership';
    }

    public function has(string $key, string $hash = ''): bool
    {
        return isset($this->store[$key][$this->field($key, $hash)]);
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

    private function field(string $key, string $hash): string
    {
        return $hash === '' ? $key : $hash;
    }
}
