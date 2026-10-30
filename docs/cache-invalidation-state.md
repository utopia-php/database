# Eviction-safe invalidation (breaking change)

The collection's CAS version is its epoch. Its single evictable record contains only writer registrations. Atomic snapshots and CAS replace the started/finished counters, owner sidecars and separately maintained epoch. Eviction creates a fresh identity. Every completion changes the version, including a writer whose registration disappeared, retiring pre-commit fills while preserving newer live writers.

The same opaque versions fence document, definition and query fills. CAS returns the exact identity it committed, or false for a version conflict. Errors and open circuit breakers throw; they are never treated as eviction. Expiry checks use the caller's writer timeout; a reader with a shorter timeout does not delete another caller's live registration. Later writer completion still retires fills made after its timeout.

This requires cache 6.x; the old generation API and compatibility protocol are removed. Redis, Redis multiplexing and Memory support versioning. Database document/query/withCache caching is bypassed on unsupported adapters; ordinary database operations continue. The old cache-5 compatibility CI tier is removed because it is no longer a supported dependency.

A direct upgrade to 9.0 requires draining legacy writers and clearing disposable cache data. For a rolling migration without a regional stop, use the explicit bridge below instead. No additional Dragonfly or coordination DSN is needed.

A successful post-commit invalidation remains necessary. Durable replay of completions lost during a cache outage is separate work. A cache timeout/OOM must never be interpreted as an empty writer record.

## Rolling migration (database 9.1 / cache 6.1)

Pass the same `Cache\Bridge` instance to Database and its query cache. `Bridge::wrap($cache, $mode)` accepts an empty mode for the ordinary v2 path, or one of three migration phases:

| Phase | Reads | Mutation registration/completion and purges |
| --- | --- | --- |
| `legacy` | Existing cache-5 keyspace and wire format | Both protocols |
| `dual` | Isolated `v2:` keyspace | Both protocols |
| `v2` | Same `v2:` keyspace | V2 only |

Roll every cache user through `legacy` before enabling `dual`. Old APIs, enterprise/storage pools, realtime/MQTT, workers, schedules and one-off scripts must have terminated gracefully, including their in-flight SQL transactions. Readiness alone does not prove that barrier. Never enable a v2 reader alongside an unmodified legacy-only writer sharing its cache domain. The wrapper does not discover fleet membership; the deployment operator must verify each barrier.

During `dual`, either bridge read mode can coexist and rollback to `legacy` remains safe. Once every reader uses v2 and the rollback window closes, roll `v2`. After the first v2-only writer, rolling back to legacy readers is unsafe because their entries are no longer maintained; rollback to `dual` with v2 reads remains possible. Preserve the `v2:` namespace during eventual bridge removal.

No additional Dragonfly, whole-cache flush or regional shutdown is needed. Use API surge capacity, `maxUnavailable: 0`, readiness and graceful draining; hand workers over using their normal acknowledgement/retry semantics. Warm the new namespace gradually and budget memory for both keyspaces. Watch SQL load, cache evictions, errors, latency and queue age. Cross-region semantic purge delivery must stay available and be tested in both directions.

Compatibility is deliberately limited to the Redis wire protocol (including multiplexing, pools, sharding and circuit breakers). Legacy fills retain numeric leases only inside the explicit facade; v2 remains opaque CAS with no fallback. No opaque version fields are written into legacy owner hashes. Required failures throw, and both post-write completions are attempted even if one fails. Definitions, negative documents, queries and caller-owned `withCache` regions are invalidated in both keyspaces. Do not mix separate raw and bridge caches in one Database/query-cache pair.

The compatibility test runtime installs pinned stable database 8.3.15/cache 5.2.0 under `compat/` and runs in a separate process against the same Redis and SQLite database. Locally, run `composer install --working-dir=compat --no-dev` before the unit suite; the CI image installs it automatically.
