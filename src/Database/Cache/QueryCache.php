<?php

namespace Utopia\Database\Cache;

use RuntimeException;
use Utopia\Cache\Cache;
use Utopia\Database\Document;

class QueryCache
{
    private const string ACTIVE_PREFIX = 'active:';

    private const string INITIAL_EPOCH = self::ACTIVE_PREFIX.'initial';

    private const string BLOCKED_PREFIX = 'blocked:';

    private const string SEPARATOR = '@';

    private const string NEVER_STARTED = '0';

    private const int PERMANENT = \PHP_INT_MAX;

    private const int VERSION = 1;

    /** @var array<string, Region> */
    private array $regions = [];

    public function __construct(
        private readonly Cache $cache,
        private readonly string $cacheName = 'default',
    ) {
    }

    public function setRegion(string $collection, Region $region): void
    {
        $this->regions[$collection] = $region;
    }

    public function getRegion(string $collection): Region
    {
        return $this->regions[$collection] ?? new Region();
    }

    public function getCollectionKey(Scope $scope, string $collection): string
    {
        $scopeHash = \md5(\serialize([
            'hostname' => $scope->hostname,
            'database' => $scope->database,
            'namespace' => $scope->namespace,
            'tenant' => $scope->tenant,
            'collection' => $collection,
        ]));

        return "{$this->cacheName}:qcache:{$collection}:{$scopeHash}";
    }

    /**
     * Resolve a query's entry in the collection's current epoch; null while the
     * collection's region is disabled or a write to it is in progress.
     *
     * @param  array<mixed>  $queries
     *
     * @phpstan-impure
     */
    public function getEntry(Scope $scope, string $collection, array $queries, string $context = ''): ?Entry
    {
        if (! $this->getRegion($collection)->enabled) {
            return null;
        }

        $key = $this->getCollectionKey($scope, $collection);
        $epoch = $this->getEpoch($key, $collection);
        if ($epoch === null) {
            return null;
        }

        $hash = \md5(\serialize([
            'queries' => $queries,
            'context' => $context,
        ]));

        return new Entry($key.'#'.$epoch.':'.$hash, $collection);
    }

    /**
     * @return array<Document>|null
     *
     * @phpstan-impure
     */
    public function get(Entry $entry): ?array
    {
        /** @var mixed $data */
        $data = $this->cache->load($entry->key, $this->getRegion($entry->collection)->ttl);

        if ($data === false || $data === null) {
            return null;
        }

        if (
            ! \is_array($data)
            || ($data['version'] ?? null) !== self::VERSION
            || ! \is_array($data['documents'] ?? null)
        ) {
            $this->purgeLoadedKey($entry->key);

            return null;
        }

        $documents = [];
        foreach ($data['documents'] as $item) {
            if ($item instanceof Document) {
                $documents[] = $item;
                continue;
            }

            if (! \is_array($item)) {
                $this->purgeLoadedKey($entry->key);

                return null;
            }

            $typed = [];
            foreach ($item as $key => $value) {
                if (\is_string($key)) {
                    $typed[$key] = $value;
                }
            }
            $documents[] = new Document($typed);
        }

        return $documents;
    }

    public function getGeneration(Entry $entry): string
    {
        return $this->cache->getGeneration($entry->key);
    }

    /**
     * @param  array<mixed>  $results
     */
    public function set(Entry $entry, array $results, string $generation): bool
    {
        $data = [];
        foreach ($results as $result) {
            if (! $result instanceof Document) {
                return false;
            }

            $data[] = $result->getArrayCopy();
        }

        return $this->cache->saveWithLease($entry->key, [
            'version' => self::VERSION,
            'documents' => $data,
        ], '', $generation) !== false;
    }

    public function invalidateCollection(Scope $scope, string $collection): void
    {
        $key = $this->getCollectionKey($scope, $collection);
        $token = \bin2hex(\random_bytes(16));
        $this->blockCollection($key, $token);
        $this->activateCollection($key, $token);
    }

    /**
     * Publish a shared tombstone before a mutation starts.
     */
    public function blockCollection(string $key, string $token): void
    {
        if ($this->cache->save($this->getOwnerKey($key, $token), $token) === false) {
            throw new RuntimeException("Failed to register query cache owner for '{$key}'");
        }

        if ($this->cache->save($this->getEpochKey($key), self::BLOCKED_PREFIX.$token.self::SEPARATOR.\time()) === false) {
            throw new RuntimeException("Failed to block query cache epoch for '{$key}'");
        }

        $this->cache->purge($this->getStartedKey($key));
    }

    /**
     * Replace this mutation's shared tombstone with a fresh usable epoch once no
     * other mutation of the collection is in progress.
     */
    public function activateCollection(string $key, string $token): void
    {
        $ownerKey = $this->getOwnerKey($key, $token);
        $owner = $this->cache->load($ownerKey, self::PERMANENT);
        if ($owner !== false && $owner !== null && $owner !== $token) {
            throw new RuntimeException("Invalid query cache owner for '{$key}'");
        }
        $owned = $owner === $token;
        if ($owned && ! $this->cache->purge($ownerKey)) {
            $owner = $this->cache->load($ownerKey, self::PERMANENT);
            if ($owner !== false && $owner !== null) {
                throw new RuntimeException("Failed to release query cache owner for '{$key}'");
            }
            $owned = false;
        }

        $epochKey = $this->getEpochKey($key);
        $startedKey = $this->getStartedKey($key);
        $finishedKey = $this->getFinishedKey($key);
        $started = $this->cache->getGeneration($startedKey);
        $finished = $this->cache->getGeneration($finishedKey);
        $current = $this->cache->load($epochKey, self::PERMANENT);
        $ours = $this->isTombstoneOf($current, $token);

        if ($started === $finished) {
            if (! $owned && ($ours || ! $this->isTombstone($current))) {
                $this->publish($key, $finished);
            }

            return;
        }

        if (! $owned && ! $ours) {
            return;
        }

        $this->cache->purge($finishedKey);
        $nextStarted = $this->cache->getGeneration($startedKey);
        $nextFinished = $this->cache->getGeneration($finishedKey);

        if ($nextStarted === $nextFinished) {
            $this->publish($key, $nextFinished);

            return;
        }

        if ($nextFinished === $finished && $this->isTombstoneOf($this->cache->load($epochKey, self::PERMANENT), $token)) {
            throw new RuntimeException("Failed to finish query cache invalidation for '{$key}'");
        }
    }

    public function flush(): void
    {
        if (! $this->cache->flush()) {
            throw new RuntimeException('Failed to flush query cache');
        }
    }

    /**
     * Epochs never expire in the cache, so one cannot vanish under a transaction that
     * outlives the region TTL. An active epoch carries the finished generation it was
     * published at: while the started generation still equals it, no mutation has
     * begun since, so a reader needs one generation read. A tombstone carries its
     * write time and lapses with the region, but only once no mutation is in flight.
     */
    private function getEpoch(string $key, string $collection): ?string
    {
        $value = $this->cache->load($this->getEpochKey($key), self::PERMANENT);

        if ($value === false || $value === null) {
            return $this->cache->getGeneration($this->getStartedKey($key)) === self::NEVER_STARTED
                ? self::INITIAL_EPOCH
                : null;
        }

        if (! \is_string($value) || $value === '') {
            throw new RuntimeException("Invalid query cache epoch for '{$key}'");
        }

        $separator = \strrpos($value, self::SEPARATOR);
        if ($separator === false) {
            return null;
        }
        $marker = \substr($value, 0, $separator);
        $stamp = \substr($value, $separator + 1);

        if (\str_starts_with($value, self::BLOCKED_PREFIX)) {
            if ((int) $stamp + $this->getRegion($collection)->ttl > \time() || ! $this->isQuiescent($key)) {
                return null;
            }

            return self::INITIAL_EPOCH;
        }

        if (! \str_starts_with($value, self::ACTIVE_PREFIX)) {
            return null;
        }

        $started = $this->cache->getGeneration($this->getStartedKey($key));
        if ($started !== $stamp && $started !== $this->cache->getGeneration($this->getFinishedKey($key))) {
            return null;
        }

        return $marker;
    }

    private function isQuiescent(string $key): bool
    {
        return $this->cache->getGeneration($this->getStartedKey($key)) === $this->cache->getGeneration($this->getFinishedKey($key));
    }

    private function isTombstone(mixed $value): bool
    {
        return \is_string($value) && \str_starts_with($value, self::BLOCKED_PREFIX);
    }

    private function isTombstoneOf(mixed $value, string $token): bool
    {
        return \is_string($value) && \str_starts_with($value, self::BLOCKED_PREFIX.$token.self::SEPARATOR);
    }

    private function publish(string $key, string $finished): void
    {
        $epoch = self::ACTIVE_PREFIX.\bin2hex(\random_bytes(16)).self::SEPARATOR.$finished;

        if ($this->cache->save($this->getEpochKey($key), $epoch) === false) {
            throw new RuntimeException("Failed to activate query cache for '{$key}'");
        }
    }

    private function getEpochKey(string $key): string
    {
        return $key.'#epoch';
    }

    private function getFinishedKey(string $key): string
    {
        return $key.'#finished';
    }

    private function getOwnerKey(string $key, string $token): string
    {
        return $key.'#owner:'.$token;
    }

    private function getStartedKey(string $key): string
    {
        return $key.'#started';
    }

    private function purgeLoadedKey(string $key): void
    {
        if (! $this->cache->purge($key)) {
            throw new RuntimeException("Failed to purge invalid query cache entry '{$key}'");
        }
    }
}
