<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Feature\Versioned;

/** Hash-backed versioned cache with explicit eviction and transport fault injection. */
class RedisLeasableCache implements CacheAdapter, Versioned
{
    /** @var array<string, string> */
    private array $versions = [];

    #[\Override]
    public function supportsVersioning(): bool
    {
        return true;
    }
    #[\Override]
    public function getVersion(string $key): string
    {
        return $this->versions[$key] ??= \bin2hex(\random_bytes(16));
    }
    #[\Override]
    public function loadVersioned(string $key): array
    {
        return ['version' => $this->getVersion($key), 'value' => $this->load($key, PHP_INT_MAX)];
    }
    #[\Override]
    public function compareAndSwap(string $key, array|string $data, string $hash, string $version): string|false
    {
        $before = $this->load($key, PHP_INT_MAX);
        if ($this->failingCompletions && \str_ends_with($key, '#state') && \is_array($before) && \is_array($data) && \is_array($data['owners'] ?? null) && \is_array($before['owners'] ?? null) && \count($data['owners']) < \count($before['owners'])) {
            throw new \Utopia\Cache\Exception\Unavailable('Completion unavailable');
        }
        if ($this->getVersion($key) !== $version || ($this->tombstones[$key] ?? 0.0) > \microtime(true)) {
            return false;
        }
        if ($this->save($key, $data, $hash) === false) {
            throw new \Utopia\Cache\Exception\Unavailable('Cache write failed');
        }
        return $this->versions[$key] = \bin2hex(\random_bytes(16));
    }

    /** @var array<string, array<string, array{time: int, data: array<int|string, mixed>|string}>> */
    private array $fields = [];

    private bool $failingFieldPurges = false;

    private bool $failingCompletions = false;

    private bool $corruptingFieldWrites = false;

    private int $leaseGraceWindow = 0;

    /** @var array<string, float> */
    private array $tombstones = [];

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $saved = $this->fields[$key][$this->field($key, $hash)] ?? null;

        return $saved !== null && $saved['time'] + $ttl > \time() ? $saved['data'] : false;
    }

    #[\Override]
    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        if ($key === '' || empty($data)) {
            return false;
        }

        if ($this->corruptingFieldWrites && ($hash !== '' || \str_ends_with($key, '#state'))) {
            $data = 'corrupted';
        }

        $this->versions[$key] = \bin2hex(\random_bytes(16));
        $this->fields[$key][$this->field($key, $hash)] = ['time' => \time(), 'data' => $data];

        return $data;
    }

    #[\Override]
    public function touch(string $key, string $hash = ''): bool
    {
        $field = $this->field($key, $hash);
        if (! isset($this->fields[$key][$field])) {
            return false;
        }

        $this->fields[$key][$field]['time'] = \time();

        return true;
    }

    /** @return array<string> */
    #[\Override]
    public function list(string $key): array
    {
        return \array_map(\strval(...), \array_keys($this->fields[$key] ?? []));
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        if ($hash !== '' && $this->failingFieldPurges) {
            return false;
        }

        unset($this->versions[$key]);
        if ($this->leaseGraceWindow > 0) {
            $this->tombstones[$key] = \microtime(true) + $this->leaseGraceWindow / 1000;
        }

        if ($hash === '') {
            $removed = \count($this->fields[$key] ?? []);
            unset($this->fields[$key]);

            return $removed > 0;
        }

        $removed = isset($this->fields[$key][$hash]);
        unset($this->fields[$key][$hash]);
        if (($this->fields[$key] ?? null) === []) {
            unset($this->fields[$key]);
        }

        return $removed;
    }

    #[\Override]
    public function flush(): bool
    {
        $this->fields = [];
        $this->versions = [];
        $this->tombstones = [];

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
        return \count($this->keys());
    }

    #[\Override]
    public function getName(?string $key = null): string
    {
        return 'redis-leasable';
    }

    /**
     * Every key the cache holds, including keys that only hold a generation.
     *
     * @return array<string>
     */
    public function keys(): array
    {
        $keys = \array_map(\strval(...), \array_keys($this->fields + $this->versions));
        \sort($keys);

        return $keys;
    }

    /**
     * Refuse every leased save of a key for the given milliseconds after each purge of it, as the adapters'
     * purge tombstone does (LUA_SAVE_WITH_LEASE).
     */
    public function setLeaseGraceWindow(int $milliseconds): void
    {
        $this->leaseGraceWindow = \max(0, $milliseconds);
    }

    public function expireTombstones(): void
    {
        $this->tombstones = [];
    }

    /**
     * Fail every purge of a single field, leaving the field in place.
     */
    public function failCompletions(): void
    {
        $this->failingCompletions = true;
    }

    public function failFieldPurges(): void
    {
        $this->failingFieldPurges = true;
    }

    /**
     * Store a different value than the one given on every write to a single field.
     */
    public function corruptFieldWrites(bool $corrupting = true): void
    {
        $this->corruptingFieldWrites = $corrupting;
    }

    /**
     * Drop a key with its generation, as Redis does when it evicts the key under memory pressure.
     */
    public function evict(string $key): void
    {
        unset($this->fields[$key], $this->versions[$key], $this->tombstones[$key]);
    }

    /**
     * Every value the cache holds across its keys, leaving out generations.
     */
    public function countValues(): int
    {
        return \array_sum(\array_map(\count(...), $this->fields));
    }

    private function field(string $key, string $hash): string
    {
        return $hash === '' ? $key : $hash;
    }
}
