<?php

namespace Utopia\Database\Cache;

use Utopia\Cache\Cache;
use Utopia\Database\Document;

class QueryCache
{
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
     * @param  array<\Utopia\Database\Query>  $queries
     */
    public function buildQueryKey(
        string $collection,
        array $queries,
        string $namespace,
        ?int $tenant,
        string $context = '',
    ): string {
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

        /** @var mixed $data */
        $data = $this->cache->load($cacheKey, $this->getRegion($collection)->ttl, $hash);

        if ($data === false || $data === null) {
            return null;
        }

        if (
            ! \is_array($data)
            || ($data['version'] ?? null) !== self::VERSION
            || ! \is_array($data['documents'] ?? null)
        ) {
            $this->cache->purge($cacheKey, $hash);

            return null;
        }

        $documents = [];
        foreach ($data['documents'] as $item) {
            if ($item instanceof Document) {
                $documents[] = $item;
                continue;
            }

            if (! \is_array($item)) {
                $this->cache->purge($cacheKey, $hash);

                return null;
            }

            $documents[] = new Document($item);
        }

        return $documents;
    }

    public function getGeneration(string $key): string
    {
        [$cacheKey] = $this->splitKey($key);

        return $this->cache->getGeneration($cacheKey);
    }

    /**
     * @param  array<Document>  $results
     */
    public function set(string $key, array $results, string $generation = '0'): bool
    {
        foreach ($results as $result) {
            if (! $result instanceof Document) {
                return false;
            }
        }

        [$cacheKey, $hash] = $this->splitKey($key);
        $data = \array_map(fn (Document $doc) => $doc->getArrayCopy(), $results);

        return $this->cache->saveWithLease($cacheKey, [
            'version' => self::VERSION,
            'documents' => $data,
        ], $hash, $generation) !== false;
    }

    public function invalidateCollection(string $collection): void
    {
        $this->cache->purge($this->getCollectionKey($collection));
    }

    public function isEnabled(string $collection): bool
    {
        $region = $this->getRegion($collection);

        return $region->enabled;
    }

    public function flush(): void
    {
        $this->cache->flush();
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
}
