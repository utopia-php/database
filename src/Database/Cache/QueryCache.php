<?php

namespace Utopia\Database\Cache;

use RuntimeException;
use Utopia\Cache\Cache;
use Utopia\Database\Document;

class QueryCache
{
    private const string ACTIVE_PREFIX = 'active:';

    private const string BLOCKED_GENERATION = 'blocked';

    private const string BLOCKED_PREFIX = 'blocked:';

    private const int VERSION = 1;

    /** @var array<string, CacheRegion> */
    private array $regions = [];

    private Cache $cache;

    private string $cacheName;

    public function __construct(Cache $cache, string $cacheName = 'default')
    {
        $this->cache = $cache;
        $this->cacheName = $cacheName;
    }

    public function setRegion(string $collection, CacheRegion $region): void
    {
        $this->regions[$collection] = $region;
    }

    public function getRegion(string $collection): CacheRegion
    {
        return $this->regions[$collection] ?? new CacheRegion();
    }

    /**
     * @param  array<mixed>  $queries
     */
    public function buildQueryKey(
        string $collection,
        array $queries,
        string $namespace,
        int|string|null $tenant,
        string $context = '',
    ): string {
        $tenant = match (true) {
            $tenant === null => ['type' => 'null'],
            \is_int($tenant) => ['type' => 'integer', 'value' => $tenant],
            default => ['type' => 'string', 'value' => $tenant],
        };

        $queriesHash = \md5(\serialize([
            'namespace' => $namespace,
            'tenant' => $tenant,
            'queries' => $queries,
            'context' => $context,
        ]));

        return "{$this->cacheName}:qcache:{$collection}#{$queriesHash}";
    }

    /**
     * @return array<Document>|null
     */
    public function get(string $key): ?array
    {
        [$cacheKey, $hash] = $this->splitKey($key);
        $collection = $this->getCollectionFromKey($cacheKey);
        $epoch = $this->getEpoch($cacheKey, $collection);
        if ($epoch === null) {
            return null;
        }
        $physicalKey = $this->getPhysicalKey($cacheKey, $hash, $epoch);

        /** @var mixed $data */
        $data = $this->cache->load($physicalKey, $this->getRegion($collection)->ttl);

        if ($data === false || $data === null) {
            return null;
        }

        if (
            ! \is_array($data)
            || ($data['version'] ?? null) !== self::VERSION
            || ! \is_array($data['documents'] ?? null)
        ) {
            $this->purgeLoadedKey($physicalKey);

            return null;
        }

        $documents = [];
        foreach ($data['documents'] as $item) {
            if ($item instanceof Document) {
                $documents[] = $item;
                continue;
            }

            if (! \is_array($item)) {
                $this->purgeLoadedKey($physicalKey);

                return null;
            }

            $documents[] = new Document($item);
        }

        return $documents;
    }

    public function getGeneration(string $key): string
    {
        [$cacheKey, $hash] = $this->splitKey($key);
        $collection = $this->getCollectionFromKey($cacheKey);
        $epoch = $this->getEpoch($cacheKey, $collection, true);
        if ($epoch === null) {
            return self::BLOCKED_GENERATION;
        }
        $physicalKey = $this->getPhysicalKey($cacheKey, $hash, $epoch);

        return \base64_encode(\json_encode([
            'epoch' => $epoch,
            'lease' => $this->cache->getGeneration($physicalKey),
        ], JSON_THROW_ON_ERROR));
    }

    /**
     * @param  array<mixed>  $results
     */
    public function set(string $key, array $results, string $generation = '0'): bool
    {
        if ($generation === self::BLOCKED_GENERATION) {
            return false;
        }

        $data = [];
        foreach ($results as $result) {
            if (! $result instanceof Document) {
                return false;
            }

            $data[] = $result->getArrayCopy();
        }

        [$cacheKey, $hash] = $this->splitKey($key);
        $collection = $this->getCollectionFromKey($cacheKey);
        if (! $this->isEnabled($collection)) {
            return false;
        }
        $decoded = $this->decodeGeneration($generation, $cacheKey, $hash, $collection);
        if ($decoded === null) {
            return false;
        }
        [$epoch, $lease] = $decoded;
        $physicalKey = $this->getPhysicalKey($cacheKey, $hash, $epoch);

        return $this->cache->saveWithLease($physicalKey, [
            'version' => self::VERSION,
            'documents' => $data,
        ], '', $lease) !== false;
    }

    public function invalidateCollection(string $collection): void
    {
        $token = \bin2hex(\random_bytes(16));
        $this->blockCollection($collection, $token);
        $this->activateCollection($collection, $token);
    }

    /**
     * Publish a shared tombstone before a mutation starts.
     */
    public function blockCollection(string $collection, string $token): void
    {
        $cacheKey = $this->getCollectionKey($collection);
        $epochKey = $this->getEpochKey($cacheKey);
        $existing = $this->cache->load($epochKey, $this->getRegion($collection)->ttl);

        $purged = $existing === false || $existing === null || $this->cache->purge($epochKey);

        if ($this->cache->save($epochKey, self::BLOCKED_PREFIX.$token) === false) {
            throw new RuntimeException("Failed to block query cache for collection '{$collection}'");
        }

        if (! $purged) {
            throw new RuntimeException("Failed to purge query cache epoch for collection '{$collection}'");
        }
    }

    /**
     * Replace this mutation's shared tombstone with a fresh usable epoch.
     */
    public function activateCollection(string $collection, string $token): void
    {
        $epochKey = $this->getEpochKey($this->getCollectionKey($collection));
        $blocked = self::BLOCKED_PREFIX.$token;
        $current = $this->cache->load($epochKey, $this->getRegion($collection)->ttl);

        if ($current !== $blocked) {
            if (\is_string($current) && \str_starts_with($current, self::BLOCKED_PREFIX)) {
                return;
            }

            $this->blockCollection($collection, $token);
        }

        $epoch = self::ACTIVE_PREFIX.\bin2hex(\random_bytes(16));
        if ($this->cache->save($epochKey, $epoch) === false) {
            throw new RuntimeException("Failed to activate query cache for collection '{$collection}'");
        }
    }

    public function isEnabled(string $collection): bool
    {
        $region = $this->getRegion($collection);

        if (! $region->enabled) {
            return false;
        }

        $epoch = $this->cache->load(
            $this->getEpochKey($this->getCollectionKey($collection)),
            $region->ttl,
        );

        return ! \is_string($epoch) || ! \str_starts_with($epoch, self::BLOCKED_PREFIX);
    }

    public function flush(): void
    {
        if (! $this->cache->flush()) {
            throw new RuntimeException('Failed to flush query cache');
        }
    }

    private function getCollectionKey(string $collection): string
    {
        return "{$this->cacheName}:qcache:{$collection}";
    }

    private function getCollectionFromKey(string $key): string
    {
        $prefix = "{$this->cacheName}:qcache:";

        return \str_starts_with($key, $prefix) ? \substr($key, \strlen($prefix)) : '';
    }

    /** @return array{string, string} */
    private function splitKey(string $key): array
    {
        $parts = \explode('#', $key, 2);

        return [$parts[0], $parts[1] ?? ''];
    }

    private function getEpoch(string $cacheKey, string $collection, bool $initialize = false): ?string
    {
        $epoch = $this->cache->load(
            $this->getEpochKey($cacheKey),
            $this->getRegion($collection)->ttl,
        );

        if ($epoch === false || $epoch === null) {
            if (! $initialize) {
                return null;
            }

            $epoch = self::ACTIVE_PREFIX.\bin2hex(\random_bytes(16));
            if ($this->cache->save($this->getEpochKey($cacheKey), $epoch) === false) {
                throw new RuntimeException("Failed to initialize query cache epoch for collection '{$collection}'");
            }

            return $epoch;
        }

        if (! \is_string($epoch) || $epoch === '') {
            throw new RuntimeException("Invalid query cache epoch for collection '{$collection}'");
        }

        if (\str_starts_with($epoch, self::BLOCKED_PREFIX)) {
            return null;
        }

        return $epoch;
    }

    private function getEpochKey(string $cacheKey): string
    {
        return $cacheKey.'#epoch';
    }

    private function getPhysicalKey(string $cacheKey, string $hash, string $epoch): string
    {
        return $cacheKey.'#'.$epoch.':'.$hash;
    }

    private function purgeLoadedKey(string $physicalKey): void
    {
        if (! $this->cache->purge($physicalKey)) {
            throw new RuntimeException("Failed to purge invalid query cache entry '{$physicalKey}'");
        }
    }

    /** @return array{string, string}|null */
    private function decodeGeneration(
        string $generation,
        string $cacheKey,
        string $hash,
        string $collection,
    ): ?array {
        if ($generation === '0') {
            $epoch = $this->getEpoch($cacheKey, $collection, true);
            if ($epoch === null) {
                return null;
            }
            $physicalKey = $this->getPhysicalKey($cacheKey, $hash, $epoch);

            return [$epoch, $this->cache->getGeneration($physicalKey)];
        }

        $encoded = \base64_decode($generation, true);
        $decoded = $encoded === false ? null : \json_decode($encoded, true);
        $epoch = \is_array($decoded) ? ($decoded['epoch'] ?? null) : null;
        $lease = \is_array($decoded) ? ($decoded['lease'] ?? null) : null;

        if (! \is_string($epoch) || $epoch === '' || ! \is_string($lease)) {
            throw new RuntimeException('Invalid query cache generation');
        }

        return [$epoch, $lease];
    }
}
