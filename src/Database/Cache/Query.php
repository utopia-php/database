<?php

namespace Utopia\Database\Cache;

use InvalidArgumentException;
use RuntimeException;
use Utopia\Cache\Cache;
use Utopia\Cache\Exception\Unavailable;
use Utopia\Database\Document;

/**
 * Caches find() results per collection. It holds no database: each call takes the cache name and the writer timeout
 * of the database it runs for from the {@see Scope} it is handed (or, for the key-based calls, its writer timeout
 * argument), so one query cache can be shared by databases named and configured differently.
 */
class Query
{
    private const int VERSION = 2;

    private const int SLOTS = 1024;

    private const string BLOCK_FIELD = 'block';

    /** @var array<string, Region> */
    private array $regions = [];

    /**
     * @param  int  $slots  Results a collection scope keeps at most; queries sharing a slot evict each other
     */
    public function __construct(
        private readonly Cache $cache,
        private readonly int $slots = self::SLOTS,
    ) {
        if ($slots < 1) {
            throw new InvalidArgumentException('A query cache needs at least one slot');
        }
    }

    public function setRegion(string $collection, Region $region): static
    {
        $this->regions[$collection] = $region;

        return $this;
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

        return "{$scope->name}:qcache:{$collection}:{$scopeHash}";
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
        if (! $this->cache->supportsVersioning() || ! $this->getRegion($collection)->enabled) {
            return null;
        }

        $key = $this->getCollectionKey($scope, $collection);
        $epoch = (new Mutation($this->cache))->read($key, $scope->writerTimeout)->value;
        if ($epoch === null) {
            return null;
        }

        $field = \md5(\serialize([
            'queries' => $queries,
            'context' => $context,
        ]));

        $slot = (string) ((int) \hexdec(\substr($field, 0, 8)) % $this->slots);

        return new Entry($key, $collection, $field, $epoch, $slot);
    }

    /**
     * @return array<Document>|null
     *
     * @phpstan-impure
     */
    public function get(Entry $entry): ?array
    {
        /** @var mixed $data */
        $data = $this->cache->load($entry->key, $this->getRegion($entry->collection)->ttl, $entry->slot);

        if ($data === false || $data === null) {
            return null;
        }

        if (
            ! \is_array($data)
            || ($data['version'] ?? null) !== self::VERSION
            || ! \is_array($data['documents'] ?? null)
        ) {
            $this->purgeLoadedEntry($entry);

            return null;
        }

        if (($data['epoch'] ?? null) !== $entry->epoch || ($data['field'] ?? null) !== $entry->field) {
            return null;
        }

        $documents = [];
        foreach ($data['documents'] as $item) {
            if ($item instanceof Document) {
                $documents[] = $item;
                continue;
            }

            if (! \is_array($item)) {
                $this->purgeLoadedEntry($entry);

                return null;
            }

            $typed = [];
            foreach ($item as $key => $value) {
                if (\is_string($key)) {
                    $typed[$key] = $value;
                }
            }
            $documents[] = Document::fromStorage($typed);
        }

        return $documents;
    }

    public function getLease(Entry $entry): string
    {
        return $this->cache->getLease($entry->key);
    }

    /**
     * @param  array<mixed>  $results
     */
    public function set(Entry $entry, array $results, string $version): bool
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
            'epoch' => $entry->epoch,
            'field' => $entry->field,
            'documents' => $data,
        ], $entry->slot, $version) !== false;
    }

    public function invalidateCollection(Scope $scope, string $collection): void
    {
        $key = $this->getCollectionKey($scope, $collection);
        $token = $this->createToken();
        $this->blockCollection($key, $token);
        $this->activateCollection($key, $token, $scope->writerTimeout);
    }

    /**
     * A write's token, which records when it was created so a later activation can tell an abandoned write.
     */
    public function createToken(): string
    {
        return \bin2hex(\random_bytes(16));
    }

    public function blockCollection(string $key, string $token): void
    {
        if ($this->cache->supportsVersioning()) {
            (new Mutation($this->cache))->begin($key, $token, Scope::WRITER_TIMEOUT);
            $this->cache->purge($key, self::BLOCK_FIELD);
        }
    }

    public function activateCollection(string $key, string $token, int $writerTimeout): void
    {
        if ($this->cache->supportsVersioning()) {
            (new Mutation($this->cache))->finish($key, $token, $writerTimeout);
        }
    }

    public function flush(): void
    {
        if (! $this->cache->flush()) {
            throw new Unavailable('Failed to flush query cache');
        }
    }

    private function purgeLoadedEntry(Entry $entry): void
    {
        if (! $this->cache->purge($entry->key, $entry->slot)) {
            throw new RuntimeException("Failed to purge invalid query cache entry '{$entry->key}'");
        }
    }
}
