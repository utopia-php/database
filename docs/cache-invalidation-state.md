# Eviction-safe invalidation (breaking change)

The collection's CAS version is its epoch. Its single evictable record contains only writer registrations. Atomic snapshots and CAS replace the started/finished counters, owner sidecars and separately maintained epoch. Eviction creates a fresh identity. Every completion changes the version, including a writer whose registration disappeared, retiring pre-commit fills while preserving newer live writers.

The same opaque versions fence document, definition and query fills. CAS returns the exact identity it committed, or false for a version conflict. Errors and open circuit breakers throw; they are never treated as eviction. Expiry checks use the caller's writer timeout; a reader with a shorter timeout does not delete another caller's live registration. Later writer completion still retires fills made after its timeout.

This requires the opaque-version API in cache 6.1.3 or 7.x; the old generation API and compatibility protocol are removed. Redis, Redis multiplexing and Memory support versioning. Database document/query/withCache caching is bypassed on unsupported adapters; ordinary database operations continue. The old cache-5 compatibility CI tier is removed because it is no longer a supported dependency.

A successful post-commit invalidation remains necessary. Durable replay of completions lost during a cache outage is separate work. A cache timeout/OOM must never be interpreted as an empty writer record.

## Completing the rolling migration

The temporary database 9.1 bridge, legacy writer bookkeeping and released cache-5 compatibility runtime are removed. Finish that migration before upgrading: every reader must already use v2, and all legacy-reading Jobs and draining processes must have exited.

Keep the existing key namespace when replacing the bridge. A cache view under `v2:` must share the same backing cache, hash fields and CAS identities with the released bridge in v2 mode; use that same view for Database and its query cache. Do not clear the cache or switch back to unprefixed keys. This is a rolling code cleanup, with no regional stop or extra Dragonfly.

Leave migration flags in configuration until the complete old bridge fleet has been replaced; removing them earlier could return an old process to legacy reads. After contraction, rollback only to code that continues v2 reads. Observe cache hits, memory, evictions, SQL load, latency, queue age and cross-region purge catchup during the rollout.
