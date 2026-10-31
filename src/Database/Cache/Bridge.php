<?php

namespace Utopia\Database\Cache;

use InvalidArgumentException;
use Throwable;
use Utopia\Cache\Cache;
use Utopia\Cache\Exception\Unavailable;

/**
 * Temporary expand/contract bridge. Roll legacy -> dual -> v2 in separate passes.
 * All cache users must finish each pass before the next starts.
 */
final class Bridge extends Cache
{
    private readonly Cache $legacy;

    public function __construct(private readonly Cache $cache, private readonly string $mode)
    {
        if (! \in_array($mode, ['legacy', 'dual', 'v2'], true)) {
            throw new InvalidArgumentException('Cache migration mode must be legacy, dual or v2');
        }
        parent::__construct($cache->adapter);
        $this->setCaseSensitivity($cache->caseSensitive);
        $this->legacy = $cache->legacy();
    }

    public static function wrap(Cache $cache, string $mode = ''): Cache
    {
        return $mode === '' ? $cache : new self($cache, $mode);
    }

    #[\Override]
    public function supportsVersioning(): bool
    {
        return $this->cache->supportsVersioning();
    }

    private function reader(): Cache
    {
        return $this->mode === 'legacy' ? $this->legacy : $this->cache;
    }

    private function key(string $key): string
    {
        return $this->mode === 'legacy' ? $key : 'v2:'.$key;
    }

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        return $this->reader()->load($this->key($key), $ttl, $hash);
    }

    #[\Override]
    public function save(string $key, mixed $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        return $this->reader()->save($this->key($key), $data, $hash, $ttl);
    }

    #[\Override]
    public function touch(string $key, string $hash = ''): bool
    {
        return $this->reader()->touch($this->key($key), $hash);
    }

    /** @return string[] */
    #[\Override]
    public function list(string $key): array
    {
        return $this->reader()->list($this->key($key));
    }

    /** @param string[] $fields
     * @return array<string, mixed>
     */
    #[\Override]
    public function loadMany(string $key, int $ttl, array $fields = []): array
    {
        return $this->reader()->loadMany($this->key($key), $ttl, $fields);
    }

    /** @param array<string, mixed> $data
     * @return array<string, mixed>|false
     */
    #[\Override]
    public function saveMany(string $key, array $data, int $ttl = 0): array|false
    {
        return $this->reader()->saveMany($this->key($key), $data, $ttl);
    }

    /** @return array{version: string, value: mixed} */
    #[\Override]
    public function loadVersioned(string $key): array
    {
        return $this->cache->loadVersioned('v2:'.$key);
    }

    /** @param array<int|string, mixed>|string $data */
    #[\Override]
    public function compareAndSwap(string $key, array|string $data, string $version): string|false
    {
        return $this->cache->compareAndSwap('v2:'.$key, $data, $version);
    }

    #[\Override]
    public function getLease(string $key): string
    {
        return $this->reader()->getLease($this->key($key));
    }

    #[\Override]
    public function saveWithLease(string $key, array|string $data, string $hash, string $version): bool|string|array
    {
        return $this->reader()->saveWithLease($this->key($key), $data, $hash, $version);
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        $this->recordPurge($key, $hash);
        $result = false;
        $this->both(
            function () use ($key, $hash, &$result): void {
                $result = $this->legacy->purge($key, $hash) || $result;
            },
            function () use ($key, $hash, &$result): void {
                $result = $this->cache->purge('v2:'.$key, $hash) || $result;
            },
        );

        return $result;
    }

    /** Rotate caller-owned withCache regions in both keyspaces, including when one is cold. */
    public function rotate(string $key, int $ttl = 0): void
    {
        $this->both(
            fn () => $this->rotateIn($this->legacy, $key, $ttl),
            fn () => $this->rotateIn($this->cache, 'v2:'.$key, $ttl),
        );
    }

    private function rotateIn(Cache $cache, string $key, int $ttl): void
    {
        $cache->purge($key);
        if ($cache->save($key, \bin2hex(\random_bytes(16)), ttl: $ttl) === false) {
            throw new Unavailable('Failed to rotate cache region');
        }
    }

    public function epoch(string $key, int $timeout, ?int $ttl = null): Epoch
    {
        if ($this->mode !== 'legacy') {
            return (new Mutation($this->cache))->read('v2:'.$key, $timeout);
        }
        $legacy = new Legacy($this->legacy);

        return $ttl === null ? $legacy->document($key, $timeout) : new Epoch($legacy->query($key, $ttl, $timeout));
    }

    public function begin(string $key, string $token, int $timeout): void
    {
        // Establish both barriers before SQL. Do not undo one after an uncertain failure.
        if ($this->mode !== 'v2') {
            (new Legacy($this->legacy))->begin($key, $token);
        }
        (new Mutation($this->cache))->begin('v2:'.$key, $token, $timeout);
    }

    public function finish(string $key, string $token, int $timeout): void
    {
        $this->both(
            fn () => (new Legacy($this->legacy))->finish($key, $token, $timeout),
            fn () => (new Mutation($this->cache))->finish('v2:'.$key, $token, $timeout),
        );
    }

    /** Attempt both completions even when one fails; never disguise failure as a cache miss. */
    private function both(callable $legacy, callable $versioned): void
    {
        $failure = null;
        if ($this->mode !== 'v2') {
            try {
                $legacy();
            } catch (Throwable $error) {
                $failure = $error;
            }
        }
        try {
            $versioned();
        } catch (Throwable $error) {
            $failure ??= $error;
        }
        if ($failure !== null) {
            throw $failure;
        }
    }

    #[\Override]
    public function flush(): bool
    {
        return $this->cache->flush();
    }
}
