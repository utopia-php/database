# Eviction-safe invalidation (breaking change)

The collection's CAS version is its epoch. Its single evictable record contains only writer registrations. Atomic snapshots and CAS replace the started/finished counters, owner sidecars and separately maintained epoch. Eviction creates a fresh identity. Every completion changes the version, including a writer whose registration disappeared, retiring pre-commit fills while preserving newer live writers.

The same opaque versions fence document, definition and query fills. CAS returns the exact identity it committed, or false for a version conflict. Errors and open circuit breakers throw; they are never treated as eviction. Expiry checks use the caller's writer timeout; a reader with a shorter timeout does not delete another caller's live registration. Later writer completion still retires fills made after its timeout.

This requires the new cache major release (6.x); the old generation API and compatibility protocol are removed. Redis, Redis multiplexing and Memory support versioning. Database document/query/withCache caching is bypassed on unsupported adapters; ordinary database operations continue. The old cache-5 compatibility CI tier is removed because it is no longer a supported dependency. The temporary development dependency must be replaced with the stable major release before production.

No additional Dragonfly or coordination DSN is needed. Drain and stop legacy API/worker/task/migration writers, clear disposable cache data, deploy matching library versions everywhere and resume. Do not mix protocols. Rollback requires the same drain/clear procedure.

A successful post-commit invalidation remains necessary. Durable replay of completions lost during a cache outage is separate work. A cache timeout/OOM must never be interpreted as an empty writer record.
