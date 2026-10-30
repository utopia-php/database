<?php

namespace Utopia\Database\Cache;

use RuntimeException;
use Utopia\Cache\Cache;
use Utopia\Cache\Exception\Unavailable;

/** Temporary cache-5 wire coordinator, shared by the two bridge read modes. */
final readonly class Legacy
{
    private const string ACTIVE_PREFIX = 'active:';
    private const string INITIAL_EPOCH = 'active:initial';
    private const string BLOCKED_PREFIX = 'blocked:';
    private const string LAPSED_PREFIX = 'lapsed:';
    private const string TOKEN_SEPARATOR = '.';
    private const string SEPARATOR = '@';
    private const string NEVER_STARTED = '0';
    private const int PERMANENT = \PHP_INT_MAX;

    public function __construct(private Cache $cache)
    {
    }

    public function document(string $key, int $timeout): Epoch
    {
        $now = \time();
        $value = $this->cache->load($key.'#epoch', self::PERMANENT);
        if (\is_string($value) && $value !== '') {
            $separator = \strrpos($value, '@');
            $marker = $separator === false ? $value : \substr($value, 0, $separator);
            $stamp = $separator === false ? '' : \substr($value, $separator + 1);
            if (\str_starts_with($value, 'blocked:')) {
                $blocked = \ctype_digit($stamp) ? (int) $stamp : 0;

                return $blocked + $timeout > $now
                    ? new Epoch(blockedAt: $blocked)
                    : new Epoch('lapsed:'.\substr($value, 8).'@'.$this->cache->getLease($key.'#finished'));
            }
            $started = $this->cache->getLease($key.'#started');
            if (($separator !== false && $started === $stamp) || $started === $this->cache->getLease($key.'#finished')) {
                return new Epoch($marker);
            }
        }
        $started = $this->cache->getLease($key.'#started');
        if ($started !== $this->cache->getLease($key.'#finished')) {
            if ($this->cache->save($key.'#epoch', 'blocked:'.$now.'.'.\bin2hex(\random_bytes(16)).'@'.$now) === false) {
                throw new Unavailable('Failed to restore legacy tombstone');
            }

            return new Epoch(blockedAt: $now);
        }
        $this->publish($key, $started);
        $value = $this->cache->load($key.'#epoch', self::PERMANENT);
        if (! \is_string($value)) {
            throw new Unavailable('Failed to restore legacy epoch');
        }

        return new Epoch(\substr($value, 0, (int) \strrpos($value, '@')));
    }

    public function begin(string $key, string $token): void
    {
        if ($this->cache->save($key.'#owners', $token, $token) === false) {
            throw new Unavailable("Failed to register query cache owner for '{$key}'");
        }

        if ($this->cache->save($this->getEpochKey($key), self::BLOCKED_PREFIX.$token.self::SEPARATOR.\time()) === false) {
            throw new Unavailable("Failed to block query cache epoch for '{$key}'");
        }

        $this->cache->purge($this->getStartedKey($key));
    }

    /**
     * Replace this mutation's shared tombstone with a fresh usable epoch once no
     * other mutation of the collection is in progress.
     *
     * @param  int  $writerTimeout  Seconds after which another writer's registration is treated as abandoned
     */
    public function finish(string $key, string $token, int $writerTimeout): void
    {
        $owners = $key.'#owners';
        $owner = $this->cache->load($owners, self::PERMANENT, $token);
        if ($owner !== false && $owner !== null && $owner !== $token) {
            throw new RuntimeException("Invalid query cache owner for '{$key}'");
        }
        $owned = $owner === $token;
        if ($owned && ! $this->cache->purge($owners, $token)) {
            $owner = $this->cache->load($owners, self::PERMANENT, $token);
            if ($owner !== false && $owner !== null) {
                throw new Unavailable("Failed to release query cache owner for '{$key}'");
            }
            $owned = false;
        }

        $epochKey = $this->getEpochKey($key);
        $startedKey = $this->getStartedKey($key);
        $finishedKey = $this->getFinishedKey($key);
        $started = $this->cache->getLease($startedKey);
        $finished = $this->cache->getLease($finishedKey);
        $current = $this->cache->load($epochKey, self::PERMANENT);
        $ours = $this->isTombstoneOf($current, $token);

        if ($started === $finished) {
            if ($ours || ! $this->isTombstone($current)) {
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
        $nextStarted = $this->cache->getLease($startedKey);
        $nextFinished = $this->cache->getLease($finishedKey);

        if ($nextStarted === $nextFinished) {
            $this->publish($key, $nextFinished);

            return;
        }

        if ($nextFinished === $finished && $this->isTombstoneOf($this->cache->load($epochKey, self::PERMANENT), $token)) {
            throw new Unavailable("Failed to finish query cache invalidation for '{$key}'");
        }

        if ($this->releaseAbandonedOwners($owners, $writerTimeout)) {
            $this->publish($key, $nextStarted);
        }
    }

    public function query(string $key, int $ttl, int $writerTimeout): ?string
    {
        $value = $this->cache->load($this->getEpochKey($key), self::PERMANENT);

        if ($value === false || $value === null) {
            return $this->cache->getLease($key) === self::NEVER_STARTED
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
            return $this->getLapsedEpoch($key, $ttl, $value, (int) $stamp, $writerTimeout);
        }

        if (! \str_starts_with($value, self::ACTIVE_PREFIX)) {
            return null;
        }

        $started = $this->cache->getLease($this->getStartedKey($key));
        if ($started !== $stamp && $started !== $this->cache->getLease($this->getFinishedKey($key))) {
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
    private function getLapsedEpoch(string $key, int $ttl, string $tombstone, int $stamp, int $writerTimeout): ?string
    {
        $now = \time();
        if ($stamp + \min($ttl, $writerTimeout) > $now) {
            return null;
        }

        $started = $this->cache->getLease($this->getStartedKey($key));
        $finished = $this->cache->getLease($this->getFinishedKey($key));
        if ($stamp + ($started === $finished ? $ttl : $writerTimeout) > $now) {
            return null;
        }

        return self::LAPSED_PREFIX.\substr($tombstone, \strlen(self::BLOCKED_PREFIX)).self::SEPARATOR.$finished;
    }

    /**
     * Release every other writer still registered when all of them are older than the writer timeout.
     * A token without a creation time counts as live.
     */
    private function releaseAbandonedOwners(string $owners, int $writerTimeout): bool
    {
        $now = \time();
        $abandoned = [];
        foreach ($this->cache->list($owners) as $token) {
            $created = $this->getTokenTime($token);
            if ($created === null || $created + $writerTimeout > $now) {
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
            throw new Unavailable("Failed to activate query cache for '{$key}'");
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

}
