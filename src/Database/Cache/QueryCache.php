<?php

namespace Utopia\Database\Cache;

use Utopia\Cache\Cache;
use Utopia\Database\Document;

class QueryCache
{
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
    public function buildQueryKey(string $collection, array $queries, string $namespace, ?int $tenant): string
    {
        $queriesHash = \md5(\serialize($queries));

        return "{$this->cacheName}:qcache:{$namespace}:{$tenant}:{$collection}:{$queriesHash}";
    }

    /**
     * @return array<Document>|null
     */
    public function get(string $key): ?array
    {
        /** @var mixed $data */
        $data = $this->cache->load($key, 0, 0);

        if ($data === false || $data === null || ! \is_array($data)) {
            return null;
        }

        return \array_map(function (mixed $item): Document {
            if ($item instanceof Document) {
                return $item;
            }
            if (\is_array($item)) {
                return new Document($item);
            }

            return new Document();
        }, $data);
    }

    /**
     * @param  array<Document>  $results
     */
    public function set(string $key, array $results): void
    {
        $data = \array_map(fn (Document $doc) => $doc->getArrayCopy(), $results);
        $this->cache->save($key, $data);
    }

    public function invalidateCollection(string $collection): void
    {
        $this->cache->purge("{$this->cacheName}:qcache:*:{$collection}:*");
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
}
