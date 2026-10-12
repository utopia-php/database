<?php

namespace Utopia\Database\Cache;

use Utopia\Cache\Cache;

/**
 * Where an invalidation registers the token that owns a collection's cache while its write is in flight.
 *
 * A Redis purge keeps the purged key, holding its generation, with no expiry, so a key per token would outlive
 * every write. Tokens are fields of one hash per collection instead. An adapter that keeps no fields lists none,
 * and gives each token a key of its own, which its purge deletes. Once a cache lists a field it is not listed again.
 */
final readonly class Owners
{
    public function __construct(private Cache $cache)
    {
    }

    public function register(string $key, string $token): bool
    {
        $owners = $this->getOwnersKey($key);
        if ($this->cache->save($owners, $token, $token) === false) {
            return false;
        }

        if ($this->isField($owners, $token)) {
            return true;
        }

        return $this->cache->save($this->getOwnerKey($key, $token), $token) !== false;
    }

    public function find(string $key, string $token): Registration
    {
        $owners = $this->getOwnersKey($key);

        return $this->isField($owners, $token)
            ? new Registration($owners, $token)
            : new Registration($this->getOwnerKey($key, $token));
    }

    private function isField(string $owners, string $token): bool
    {
        if (Fields::kept($this->cache)) {
            return true;
        }

        if (! \in_array($token, $this->cache->list($owners), true)) {
            return false;
        }

        Fields::remember($this->cache);

        return true;
    }

    private function getOwnersKey(string $key): string
    {
        return $key.'#owners';
    }

    private function getOwnerKey(string $key, string $token): string
    {
        return $key.'#owner:'.$token;
    }
}
