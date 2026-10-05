<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Feature\Leasable;

/**
 * Mirrors how utopia-php/cache's Redis adapters store keys: every key is a hash whose generation is a reserved
 * field, and purging a key or one of its fields advances that generation instead of deleting the key
 * (LUA_PURGE_BUMP and LUA_PURGE_FIELD). A key, once written, stays behind for good, holding at least its
 * generation, because the adapters never set an expiry.
 */
final class RedisLeasableCache implements CacheAdapter, Leasable
{
    /** @var array<string, array<string, array{time: int, data: array<int|string, mixed>|string}>> */
    private array $fields = [];

    /** @var array<string, int> */
    private array $generations = [];

    private bool $failingFieldPurges = false;

    private bool $corruptingFieldWrites = false;

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $saved = $this->fields[$key][$this->field($key, $hash)] ?? null;

        return $saved !== null && $saved['time'] + $ttl > \time() ? $saved['data'] : false;
    }

    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        if ($key === '' || empty($data)) {
            return false;
        }

        if ($hash !== '' && $this->corruptingFieldWrites) {
            $data = 'corrupted';
        }

        $this->fields[$key][$this->field($key, $hash)] = ['time' => \time(), 'data' => $data];

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
        if (! isset($this->fields[$key][$field])) {
            return false;
        }

        $this->fields[$key][$field]['time'] = \time();

        return true;
    }

    /** @return array<string> */
    public function list(string $key): array
    {
        return \array_map(\strval(...), \array_keys($this->fields[$key] ?? []));
    }

    public function purge(string $key, string $hash = ''): bool
    {
        if ($hash !== '' && $this->failingFieldPurges) {
            return false;
        }

        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;

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

    public function flush(): bool
    {
        $this->fields = [];
        $this->generations = [];

        return true;
    }

    public function ping(): bool
    {
        return true;
    }

    public function getSize(): int
    {
        return \count($this->keys());
    }

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
        $keys = \array_map(\strval(...), \array_keys($this->fields + $this->generations));
        \sort($keys);

        return $keys;
    }

    /**
     * Fail every purge of a single field, leaving the field in place.
     */
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
        unset($this->fields[$key], $this->generations[$key]);
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
