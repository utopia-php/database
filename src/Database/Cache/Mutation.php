<?php

namespace Utopia\Database\Cache;

use Utopia\Cache\Cache;
use Utopia\Cache\Exception\Unavailable;

/**
 * One evictable record holds both the epoch and every live writer. A cold record
 * gets a fresh identity; even a writer whose registration was evicted rotates
 * that identity after commit, retiring anything filled while it was in flight.
 */
final readonly class Mutation
{
    public function __construct(private Cache $cache)
    {
    }

    public static function supported(Cache $cache): bool
    {
        return \interface_exists(\Utopia\Cache\Feature\Versioned::class) && $cache->supportsVersioning();
    }

    public static function lease(Cache $cache, string $key): string
    {
        return \interface_exists(\Utopia\Cache\Feature\Versioned::class) ? $cache->getLease($key) : $cache->getGeneration($key);
    }

    public function read(string $key, int $timeout): Epoch
    {
        return $this->change($key, $timeout);
    }

    public function begin(string $key, string $token, int $timeout): void
    {
        $this->change($key, $timeout, $token, true);
    }

    public function finish(string $key, string $token, int $timeout): void
    {
        $this->change($key, $timeout, $token);
    }

    private function change(string $key, int $timeout, ?string $token = null, bool $begin = false): Epoch
    {
        $key .= '#state';
        for ($attempt = 0; $attempt < 16; $attempt++) {
            $snapshot = $this->cache->loadVersioned($key);
            $state = $snapshot['value'];
            $changed = $state === false;
            if ($state === false) {
                $state = ['epoch' => \bin2hex(\random_bytes(16)), 'owners' => []];
            }
            if (! \is_array($state) || ! \is_string($state['epoch'] ?? null) || ! \is_array($state['owners'] ?? null)) {
                throw new Unavailable("Invalid cache mutation record '{$key}'");
            }
            $epoch = $state['epoch'];
            $owners = [];
            $now = \time();
            foreach ($state['owners'] as $owner => $created) {
                if (! \is_string($owner) || ! \is_int($created)) {
                    throw new Unavailable("Invalid cache writer in '{$key}'");
                }
                if ($begin || $created + $timeout > $now) {
                    $owners[$owner] = $created;
                } else {
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
                $epoch = \bin2hex(\random_bytes(16));
                $state = ['epoch' => $epoch, 'owners' => $owners];
                if (! $this->cache->compareAndSwap($key, $state, $snapshot['version'])) {
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
