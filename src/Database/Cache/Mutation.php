<?php

namespace Utopia\Database\Cache;

use Utopia\Cache\Cache;
use Utopia\Cache\Exception\Unavailable;

/**
 * One evictable record holds every writer; its CAS version is the epoch. A cold record
 * gets a fresh identity; even a writer whose registration was evicted rotates
 * that identity after commit, retiring anything filled while it was in flight.
 */
final readonly class Mutation
{
    public function __construct(private Cache $cache)
    {
    }

    public function read(string $key, int $timeout, ?int $ttl = null): Epoch
    {
        return $this->cache instanceof Bridge
            ? $this->cache->epoch($key, $timeout, $ttl)
            : $this->change($key, $timeout);
    }

    public function begin(string $key, string $token, int $timeout): void
    {
        if ($this->cache instanceof Bridge) {
            $this->cache->begin($key, $token, $timeout);
        } else {
            $this->change($key, $timeout, $token, true);
        }
    }

    public function finish(string $key, string $token, int $timeout): void
    {
        if ($this->cache instanceof Bridge) {
            $this->cache->finish($key, $token, $timeout);
        } else {
            $this->change($key, $timeout, $token);
        }
    }

    private function change(string $key, int $timeout, ?string $token = null, bool $begin = false): Epoch
    {
        $key .= '#state';
        for ($attempt = 0; $attempt < 16; $attempt++) {
            $snapshot = $this->cache->loadVersioned($key);
            $state = $snapshot['value'];
            $changed = $state === false;
            if ($state === false) {
                $state = ['owners' => []];
            }
            if (! \is_array($state) || ! \is_array($state['owners'] ?? null)) {
                throw new Unavailable("Invalid cache mutation record '{$key}'");
            }
            $epoch = $snapshot['version'];
            $owners = [];
            $now = \time();
            foreach ($state['owners'] as $owner => $created) {
                if (! \is_string($owner) || ! \is_int($created)) {
                    throw new Unavailable("Invalid cache writer in '{$key}'");
                }
                if ($begin || $created + $timeout > $now) {
                    $owners[$owner] = $created;
                } elseif ($token !== null) {
                    $changed = true;
                }
            }
            if ($token !== null) {
                if ($begin) {
                    $owners[$token] = $now;
                } else {
                    unset($owners[$token]);
                }
                $changed = true;
            }
            if ($changed) {
                $epoch = $this->cache->compareAndSwap($key, ['owners' => $owners], $snapshot['version']);
                if ($epoch === false) {
                    continue;
                }
            }

            return $owners === []
                ? new Epoch('active:'.$epoch)
                : new Epoch(blockedAt: \min($owners));
        }

        throw new Unavailable("Cache mutation record kept changing '{$key}'");
    }
}
