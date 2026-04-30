# Redis Adapter Contract (Wave 1, locked)

This document is the load-bearing contract for the Redis adapter. Wave-2
architects MUST NOT modify this file. If a contract gap is found, write
`CONTRACT_GAP.md` in your worktree root and escalate to the consolidator.

## Storage key schema

```
{ns}                      = getNamespace()
{db}                      = current setDatabase() value
{col}                     = collection ID

Key                                          | Type   | Holds
---------------------------------------------+--------+----------------------------------
{ns}:{db}:dbs                                | SET    | database names
{ns}:{db}:cols                               | SET    | collection IDs in this db
{ns}:{db}:meta:{col}                         | HASH   | fields: schema, attrs, indexes, docCount, sizeBytes
{ns}:{db}:doc:{col}:{id}                     | STRING | JSON-encoded Document
{ns}:{db}:idx:{col}                          | SET    | doc IDs in collection (for SCAN/list)
{ns}:{db}:perm:{col}:r/w/u/d:{role}          | SET    | doc IDs by action+role
{ns}:{db}:perm:doc:{col}:{id}                | HASH   | role -> csv("read,update,delete")
{ns}:{db}:tenants:{col}:{tenant}             | SET    | doc IDs filtered by tenant (shared mode)
{ns}:{db}:journal:{txid}                     | LIST   | WAL entries for rollback (T56 owns)
```

All keys begin with the static prefix `utopia:` (the `Redis::KEY_PREFIX`
constant) joined by the `Redis::SEP` separator (`:`). The `{ns}:{db}` portion
is produced by the locked `ns()` helper.

## DSN format

```
redis://[user:pass@]host:port[/db]
```

* No query parameters are recognised.
* The path segment is treated as the namespace, defaulting to `"utopia"` when
  omitted.

## Constants

| Constant | Visibility | Type | Value |
|----------|------------|------|-------|
| `KEY_PREFIX` | `public` | `string` | `'utopia'` |
| `SEP` | `public` | `string` | `':'` |
| `TX_MAX_RETRIES` | `private` | `int` | `3` |
| `TX_BACKOFF_MS` | `private` | `array` | `[10, 50, 250]` |

## Helpers

### T1-owned (real bodies, do not change)

| Signature | Purpose |
|-----------|---------|
| `private function key(string ...$parts): string` | Joins parts with `SEP`. Does NOT prepend `KEY_PREFIX` — call sites compose the prefix by passing `$this->ns()` as the first argument. |
| `private function ns(): string` | Returns `"{KEY_PREFIX}:{namespace}:{database}"`. Every adapter-produced key starts with this prefix. |
| `private function encode(Document $document): string` | `json_encode` with `JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE`. |
| `private function decode(string $payload): Document` | Wraps `json_decode` (`JSON_THROW_ON_ERROR`) in a `Document`. |
| `protected function tx(callable $fn): mixed` | Network-error retry loop. Does NOT provide isolation. `getSupportForTransactionRetries()` returns `false` to keep the trait's OCC tests off. Real `WATCH`/`MULTI`/`EXEC` is a follow-up. |

### Cross-architect (locked signatures, stub-throwing in Wave 1)

| Signature | Owner |
|-----------|-------|
| `private function writePermissions(string $collection, string $id, Document $document): void` | T50 |
| `private function clearPermissions(string $collection, string $id): void` | T50 |
| `private function applyPermissionFilter(string $collection, array $ids, string $action): array` | T50 |
| `protected function journal(string $op, array $payload): void` | T56 |
| `protected function rollbackJournal(): void` | T56 |
| `protected function commitJournal(): void` | T56 |
| `private function rawDeleteDoc(string $collection, string $id): void` | T56 |
| `private function rawRestoreDoc(string $collection, string $id, string $payload): void` | T56 |
| `protected function evaluateQueries(string $collection, array $queries, ?int $limit, ?int $offset, array $orderAttributes, array $orderTypes, array $cursor, string $cursorDirection): array` | T40 |

## Method-group ownership map

| Region | Owner | Methods |
|--------|-------|---------|
| schema + collection + attribute | **T20** | `create`, `exists`, `list`, `delete`, `createCollection`, `deleteCollection`, `analyzeCollection`, `getSizeOfCollection`, `getSizeOfCollectionOnDisk`, `createAttribute`, `createAttributes`, `updateAttribute`, `deleteAttribute`, `renameAttribute`, `getSchemaAttributes`, `getCountOfAttributes` |
| document CRUD + bulk + increase | **T30** | `getDocument`, `createDocument`, `createDocuments`, `updateDocument`, `updateDocuments`, `upsertDocuments`, `getSequences`, `deleteDocument`, `deleteDocuments`, `increaseDocumentAttribute` |
| indexes + queries + counts | **T40** | `createIndex`, `deleteIndex`, `renameIndex`, `find`, `sum`, `count`, `getSchemaIndexes`, `getCountOfIndexes`, plus the `evaluateQueries` helper body |
| permissions + relationships | **T50** | `createRelationship`, `updateRelationship`, `deleteRelationship`, plus the `writePermissions`, `clearPermissions`, `applyPermissionFilter` helper bodies |
| transactions + journal | **T56** | `startTransaction`, `commitTransaction`, `rollbackTransaction`, plus the `tx`, `journal`, `rollbackJournal`, `commitJournal`, `rawDeleteDoc`, `rawRestoreDoc` bodies |

Each region is delimited by `// === @architect:Tnn owns: ... ===` /
`// === @architect:Tnn end ===` markers in `Redis.php`. Wave-2 architects
edit ONLY the bodies inside their region; the markers and the five-blank-line
buffer between adjacent regions are locked.

## `getSupportFor*` parity table

| Method | Memory | Redis | Rationale |
|--------|--------|-------|-----------|
| `getSupportForSchemas` | `true` | `true` | Multiple databases supported via key namespace. |
| `getSupportForAttributes` | `$supportForAttributes` (default `true`) | `true` | Always true — no toggle in Wave 1. |
| `getSupportForSchemaAttributes` | `false` | `false` | Match. |
| `getSupportForSchemaIndexes` | `false` | `false` | Match. |
| `getSupportForIndex` | `true` | `true` | Match. |
| `getSupportForIndexArray` | `false` | `false` | Match. |
| `getSupportForCastIndexArray` | `false` | `false` | Already-decided unsupported. |
| `getSupportForUniqueIndex` | `true` | `true` | Implementable via SETNX on signature key. |
| `getSupportForFulltextIndex` | `true` | `false` | Already-decided unsupported (no inverted index in Redis core). |
| `getSupportForFulltextWildcardIndex` | `false` | `false` | Already-decided unsupported. |
| `getSupportForCasting` | `true` | `true` | JSON encode/decode round-trips through string types. |
| `getSupportForQueryContains` | `true` | `true` | Implemented in T40 via array scan. |
| `getSupportForTimeouts` | `false` | `false` | Already-decided unsupported. |
| `getSupportForRelationships` | `true` | `false` | Already-decided unsupported in Wave 1. |
| `getSupportForUpdateLock` | `false` | `false` | Already-decided unsupported (optimistic only). |
| `getSupportForBatchOperations` | `true` | `true` | Match — pipelined in Wave 2. |
| `getSupportForAttributeResizing` | `true` | `true` | Schemaless, no-op. |
| `getSupportForGetConnectionId` | `false` | `false` | Already-decided unsupported. |
| `getSupportForUpserts` | `false` | `false` | Match. |
| `getSupportForUpsertOnUniqueIndex` | `false` | `false` | Match. |
| `getSupportForVectors` | `false` | `false` | Already-decided unsupported. |
| `getSupportForCacheSkipOnFailure` | `false` | `false` | Match. |
| `getSupportForReconnection` | `false` | `false` | Match — connection lifecycle owned by caller. |
| `getSupportForHostname` | `false` | `false` | Already-decided unsupported. |
| `getSupportForBatchCreateAttributes` | `true` | `true` | Match. |
| `getSupportForSpatialAttributes` | `false` | `false` | Already-decided unsupported. |
| `getSupportForObject` | `true` | `true` | JSON encoding handles nested objects natively. |
| `getSupportForObjectIndexes` | `true` | `false` | Already-decided unsupported in Wave 1. |
| `getSupportForSpatialIndexNull` | `false` | `false` | Already-decided unsupported. |
| `getSupportForOperators` | `true` | `true` | Match. |
| `getSupportForOptionalSpatialAttributeWithExistingRows` | `false` | `false` | Already-decided unsupported. |
| `getSupportForSpatialIndexOrder` | `false` | `false` | Already-decided unsupported. |
| `getSupportForSpatialAxisOrder` | `false` | `false` | Already-decided unsupported. |
| `getSupportForBoundaryInclusiveContains` | `false` | `false` | Already-decided unsupported. |
| `getSupportForDistanceBetweenMultiDimensionGeometryInMeters` | `false` | `false` | Already-decided unsupported. |
| `getSupportForMultipleFulltextIndexes` | `false` | `false` | Already-decided unsupported. |
| `getSupportForIdenticalIndexes` | `false` | `false` | Already-decided unsupported. |
| `getSupportForOrderRandom` | `true` | `true` | Implementable via shuffle on result list. |
| `getSupportForInternalCasting` | `false` | `false` | Match. |
| `getSupportForUTCCasting` | `false` | `false` | Match. |
| `getSupportForIntegerBooleans` | `false` | `false` | Match — JSON booleans are native. |
| `getSupportForAlterLocks` | `false` | `false` | Match. |
| `getSupportNonUtfCharacters` | `false` | `false` | Match. |
| `getSupportForTrigramIndex` | `false` | `false` | Match. |
| `getSupportForPCRERegex` | `true` | `true` | Match — Wave 2 evaluates via PHP `preg_match`. |
| `getSupportForPOSIXRegex` | `false` | `false` | Match. |
| `getSupportForTransactionRetries` | `false` | `false` | `tx()` is currently a network-error retry loop, NOT optimistic concurrency control. Real `WATCH`/`MULTI`/`EXEC` is deferred to a follow-up PR. |
| `getSupportForNestedTransactions` | `true` | `true` | Match — modelled via journal stack. |

Total abstract methods on `Adapter`: **119** (counted via
`grep -c '^    abstract' src/Database/Adapter.php`).

## Rollback contract

`rollbackJournal()` MUST use raw `\Redis` client commands only; never call
public adapter methods. Public methods append to the journal, which would
re-enter rollback and recurse infinitely. T56 enforces this by routing all
inverse operations through `rawDeleteDoc()` and `rawRestoreDoc()`.

## Per-test cleanup strategy

* `setUp` generates a unique namespace via `'utopia_test_' . uniqid()` and
  passes it to `setNamespace()`.
* `tearDown` performs `SCAN MATCH "{ns}:*"` and `DEL` in batches of 500.
* Tests must NEVER call `FLUSHDB` or `FLUSHALL` — the test runner shares the
  same Redis instance across workers.

## Wave-2 etiquette

* Do not modify Contract.md.
* Do not modify locked imports, constants, helper signatures, region markers,
  or the five-blank-line buffer between regions.
* If a contract gap is found, write `CONTRACT_GAP.md` in your worktree root
  and escalate to the consolidator instead of editing this file.
