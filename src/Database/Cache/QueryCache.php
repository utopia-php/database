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

    private const string LAPSED_PREFIX = 'lapsed:';

    private const string TOKEN_SEPARATOR = '.';

    private const string SEPARATOR = '@';

    private const string NEVER_STARTED = '0';

    private const int PERMANENT = \PHP_INT_MAX;

    private const int VERSION = 2;

    private const int WRITER_TIMEOUT = 3600;

    /** @var array<string, Region> */
    private array $regions = [];

    /**
     * @param  int  $writerTimeout  Seconds after which a write that has not activated is treated as abandoned
     */
    public function __construct(
        private readonly Cache $cache,
        private readonly string $cacheName = 'default',
        private readonly int $writerTimeout = self::WRITER_TIMEOUT,
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
     * Every result of a collection scope is a field of one hash, keyed by the query, whose value records
     * the query and the epoch it was filled under: a cache that keeps no fields holds one result per scope.
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

        $field = \md5(\serialize([
            'queries' => $queries,
            'context' => $context,
        ]));

        return new Entry($key, $collection, $field, $epoch);
    }

    /**
     * @return array<Document>|null
     *
     * @phpstan-impure
     */
    public function get(Entry $entry): ?array
    {
        /** @var mixed $data */
        $data = $this->cache->load($entry->key, $this->getRegion($entry->collection)->ttl, $entry->field);

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
            'epoch' => $entry->epoch,
            'field' => $entry->field,
            'documents' => $data,
        ], $entry->field, $generation) !== false;
    }

    public function invalidateCollection(Scope $scope, string $collection): void
    {
        $key = $this->getCollectionKey($scope, $collection);
        $token = $this->createToken();
        $this->blockCollection($key, $token);
        $this->activateCollection($key, $token);
    }

    /**
     * A write's token, which records when it was created so a later activation can tell an abandoned write.
     */
    public function createToken(): string
    {
        return \time().self::TOKEN_SEPARATOR.\bin2hex(\random_bytes(16));
    }

    /**
     * Publish a shared tombstone before a mutation starts, and drop the results the previous epoch filled.
     */
    public function blockCollection(string $key, string $token): void
    {
        if (! (new Owners($this->cache))->register($key, $token)) {
            throw new RuntimeException("Failed to register query cache owner for '{$key}'");
        }

        if ($this->cache->save($this->getEpochKey($key), self::BLOCKED_PREFIX.$token.self::SEPARATOR.\time()) === false) {
            throw new RuntimeException("Failed to block query cache epoch for '{$key}'");
        }

        $this->cache->purge($this->getStartedKey($key));
        $this->cache->purge($key);
    }

    /**
     * Replace this mutation's shared tombstone with a fresh usable epoch once no
     * other mutation of the collection is in progress.
     */
    public function activateCollection(string $key, string $token): void
    {
        $registration = (new Owners($this->cache))->find($key, $token);
        $owner = $this->cache->load($registration->key, self::PERMANENT, $registration->field);
        if ($owner !== false && $owner !== null && $owner !== $token) {
            throw new RuntimeException("Invalid query cache owner for '{$key}'");
        }
        $owned = $owner === $token;
        if ($owned && ! $this->cache->purge($registration->key, $registration->field)) {
            $owner = $this->cache->load($registration->key, self::PERMANENT, $registration->field);
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
            if ($this->isActive($current)) {
                $this->publish($key, $started);
            }

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

        if ($registration->field !== '' && $this->releaseAbandonedOwners($registration->key)) {
            $this->publish($key, $nextStarted);
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
     * outlives the region TTL. An active epoch carries the started generation it was
     * published at: while the started generation still equals it, no mutation has
     * begun since, so a reader needs one generation read.
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
            return null;
        }

        $separator = \strrpos($value, self::SEPARATOR);
        if ($separator === false) {
            return null;
        }
        $marker = \substr($value, 0, $separator);
        $stamp = \substr($value, $separator + 1);

        if (\str_starts_with($value, self::BLOCKED_PREFIX)) {
            return $this->getLapsedEpoch($key, $collection, $value, (int) $stamp);
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

    /**
     * A tombstone lapses after the region TTL once no write is counted in flight, and after the writer
     * timeout while one is, since its writer may have died before activating. The lapsed epoch belongs
     * to this tombstone and the finished generation, so nothing filled before the block, or before a
     * later activation, is served under it.
     */
    private function getLapsedEpoch(string $key, string $collection, string $tombstone, int $stamp): ?string
    {
        $now = \time();
        $ttl = $this->getRegion($collection)->ttl;
        if ($stamp + \min($ttl, $this->writerTimeout) > $now) {
            return null;
        }

        $started = $this->cache->getGeneration($this->getStartedKey($key));
        $finished = $this->cache->getGeneration($this->getFinishedKey($key));
        if ($stamp + ($started === $finished ? $ttl : $this->writerTimeout) > $now) {
            return null;
        }

        return self::LAPSED_PREFIX.\substr($tombstone, \strlen(self::BLOCKED_PREFIX)).self::SEPARATOR.$finished;
    }

    /**
     * Release every other writer still registered when all of them are older than the writer timeout.
     * A token without a creation time counts as live.
     */
    private function releaseAbandonedOwners(string $owners): bool
    {
        $now = \time();
        $abandoned = [];
        foreach ($this->cache->list($owners) as $token) {
            $created = $this->getTokenTime($token);
            if ($created === null || $created + $this->writerTimeout > $now) {
                return false;
            }

            $abandoned[] = $token;
        }

        foreach ($abandoned as $token) {
            $this->cache->purge($owners, $token);
        }

        return true;
    }

    private function getTokenTime(string $token): ?int
    {
        $separator = \strpos($token, self::TOKEN_SEPARATOR);
        $time = $separator === false ? '' : \substr($token, 0, $separator);

        return \ctype_digit($time) ? (int) $time : null;
    }

    private function isActive(mixed $value): bool
    {
        return \is_string($value) && \str_starts_with($value, self::ACTIVE_PREFIX);
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

    private function getStartedKey(string $key): string
    {
        return $key.'#started';
    }

    private function purgeLoadedEntry(Entry $entry): void
    {
        if (! $this->cache->purge($entry->key, $entry->field)) {
            throw new RuntimeException("Failed to purge invalid query cache entry '{$entry->key}'");
        }
    }
}
