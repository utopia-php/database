## Problem

`getDocument()` uses a look-aside cache that is invalidated by **purging**. The purge is **not atomic with the committing transaction**, so under concurrency a reader that missed the cache can read the pre-commit row and re-`save` it into the cache **after** the writer's purge — leaving a stale entry that subsequent reads serve until TTL.

The existing `forUpdate` bypass (added in `ForUpdateCacheTest`) only protects the **locking read inside `updateDocument`**. A normal `getDocument()` still serves the stale snapshot — `testForUpdateReadBypassesStaleCache` even asserts this. That remaining read-path window is the root cause of **flaky project-config E2E tests**: `ProjectsConsoleClientTest::testGetProject` issues ~12 rapid read-modify-write PATCHes against a single project document and intermittently reads back old `auths` / `services` values.

## Evidence

Runtime trace under Swoole (one coroutine per request, `upd` = `$updatedAt`):

```
[20] SAVE  upd=51.030   coro 20 caches the row it read at request-init
[20] WRITE upd=51.085   coro 20 commits a NEWER version
[20] PURGE              in-transaction purge
[20] PURGE              post-commit purge
[21] HIT   upd=51.030   coro 21 served STALE 51.030 from cache
```

In a real cloud E2E environment, `testGetProject` failed ~19 of 20 runs at varying assertions before the fix, and **20/20 after**. The failures were field-agnostic (`authPasswordDictionary`, `authPersonalDataCheck`, `authSessionsLimit` reverting to its default `0`, service flags, list sizes) — consistent with whichever write lost the cache race on a given run.

## Fix

- On every committed `updateDocument` / `deleteDocument`, record the committed `$updatedAt` in a **sibling cache key** (`:__ver`) that `purgeCachedDocument()` does **not** delete, so it outlives the purge of the document body.
- On read, discard any cached snapshot whose `$updatedAt` predates that marker and reload from the adapter.
- The guard is **skipped** when no marker exists or the timestamp can't be parsed, so behaviour is unchanged outside the race. `cacheVersionStamp()` normalises `string` / `DateTimeInterface` / Mongo `UTCDateTime` values to a comparable microsecond timestamp.

## Tests

- New deterministic regression test `ForUpdateCacheTest::testReadRejectsStaleCacheSnapshotReCachedAfterUpdate` — **fails without the fix** (`'stale'`), passes with it. It reproduces the race at the cache layer (re-`save` of a pre-commit snapshot after a committed update) without needing real concurrency.
- Full unit suite: **367 tests, 2160 assertions — OK**. Pint PSR-12 ✅, PHPStan level 7 ✅.

## Notes / open questions

- The `:__ver` marker relies on `$updatedAt` advancing per write; with `preserveDates` enabled or sub-microsecond updates two versions could tie (comparison is strict `<`, so a tie keeps today's behaviour). Could switch to a monotonic counter if preferred.
- Marker reads use `self::TTL`, so the marker expires in lockstep with the document cache entry it guards.