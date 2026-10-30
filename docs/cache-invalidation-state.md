# Invalidation state and evicting caches

Document caching keeps a collection epoch, started/finished generations and writer registrations. These are coordination state: independently evicting one key can make the generations unequal even after every writer has finished. A replacement blocked epoch then disables document caching until the writer timeout; further eviction can restart that wait. No key TTL does not mean no eviction.

Keep this state in a shared, non-evicting store, separately from disposable document/schema/query values:

```php
$database = new Database($adapter, $dataCache);
$database->setInvalidationCache($stateCache);
$queryCache = new Utopia\Database\Cache\Query($dataCache, invalidationCache: $stateCache);
```

Configure the same state store on every reader and writer before using the handles. For Redis use `noeviction`; for Dragonfly leave `cache_mode` disabled. Size and monitor this store for the collection inventory; do not add expiry to its accounting keys. Do not wrap it in a circuit breaker that substitutes `0`, empty lists or cache misses for failed reads. A failed accounting operation must remain distinguishable from missing state. Document/schema values and their fill generations remain in the data cache.

Without an explicit state cache the previous single-store behavior remains for compatibility. `setCache()` changes the data cache; it does not replace an explicitly configured state cache. Flushing data/query results does not clear state or erase in-flight writer barriers.

## Existing installations

Changing stores is a coordinated cutover, not a rolling environment-variable update. Pause all database writers (API, workers, tasks and migrations), drain in-flight transactions, stop old processes, and configure every process with the same state store. Clear the disposable data cache before starting the new processes and resuming writes, so no cached schema carries an epoch from the previous store. Retain rollback access to the old store. To roll back, repeat the drain/stop/data-cache-clear process with the previous configuration.

Never flush an invalidation store with active writers. Availability and failover of this store are part of the cache consistency protocol.
