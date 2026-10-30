# Eviction-safe invalidation

Redis and Redis multiplexing use one evictable record per collection for the epoch and live writer registrations. Atomic versioned snapshots replace the independent started/finished counter ledger. Eviction drops the whole record; its replacement gets a fresh random identity. A writer finishing after its registration was evicted still rotates the current epoch, retiring pre-commit fills. A newer writer's registration keeps its own barrier in place.

Fill leases also use random identities stored with the cached value. A key evicted and recreated cannot reset to generation zero and accidentally accept an old reader's fill. Snapshot and compare-and-swap operations propagate cache transport failures instead of converting them into missing state.

The existing cache is sufficient: no separate state cache, non-evicting store or additional Dragonfly is needed. Adapters and older cache versions without atomic versioned operations retain the previous protocol for compatibility. Their counter state is not made eviction-safe by this change.

## Upgrade

Deploy matching cache/database library versions together. Drain and stop legacy API/worker/task/migration writers, clear disposable cache data, deploy the new protocol everywhere and resume. Do not mix old and new protocols while writers are running. Rollback requires the same drain/clear procedure.

The writer timeout still bounds abandoned registrations. A writer completing after that timeout retires any intervening fills. If the cache rejects writes because it is unavailable or out of memory, coordination fails closed; it does not guess an empty owners list. Successful post-commit invalidation is still required. Durable replay of lost invalidations is a separate concern.
