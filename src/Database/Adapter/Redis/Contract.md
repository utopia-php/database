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
| `protected function tx(callable $fn): mixed` | Single-shot wrapper for journal-tracked Redis operations. Does NOT retry — Redis transient errors propagate as `TransactionException`. Retrying would replay journal side-effects (duplicate entries, double-`INCR` on sequence keys). `getSupportForTransactionRetries()` returns `false` so the trait's OCC tests stay off. Real `WATCH`/`MULTI`/`EXEC` is a follow-up. |
| `private function surfaceRelationshipAttributes(string $collection, Document $document): Document` | Reads `meta.attrs`, materialises any registered relationship attribute as `null` when the document does not carry it. METADATA is exempt (relationship attrs there live nested inside the row's `attributes` array). Mirrors `Memory::documentToRow`'s null-surface pass. |
| `private function surfaceRelationshipAttributesUsing(array $relationshipKeys, Document $document): Document` | Loop-friendly companion that takes a pre-computed positional list of relationship keys (from `extractRelationshipKeys`) so callers iterating large result sets don't re-read `meta.attrs` per document. |
| `private function extractRelationshipKeys(array $attributes): array` | Filters a decoded `meta.attrs` records list down to the relationship attribute keys. Returns a positional `array<int, string>`. |
| `private function renameDocumentField(string $collection, string $oldKey, string $newKey): void` | Iterates `idx:{col}` via `sMembers`, GETs each `doc:{col}:{id}`, renames `oldKey` → `newKey` in the decoded payload, SETs back. Wrapped in `tx()` for `\RedisException` surfacing only — no journal entries (schema op). |
| `private function dropDocumentField(string $collection, string $field): void` | Same shape as `renameDocumentField` but `unset`s `field`. Wrapped in `tx()`, non-journalled. |
| `private function resolveJunctionCollection(string $collection, string $relatedCollection, string $side): ?string` | Resolves the M2M junction name from the parent/child METADATA sequence pair (`_{parent}_{child}` for parent side, reversed for child). Returns `null` when either METADATA row is missing or carries no `$sequence` — callers treat as no-op. |
| `private function loadMetadataDocument(string $collection): ?Document` | Reads a METADATA row directly from its `doc:_metadata:{col}` key, bypassing the public `getDocument` path so schema helpers can call it without first constructing a `Document` collection wrapper. |

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
| `getSupportForRelationships` | `true` | `false` [^rel] | Wave 1 ships the helpers + null-surfacing contract; flipped to `true` in T4 once T2 (schema ops) and T3 (read-path call sites) land. |
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
| `getSupportForTransactionRetries` | `false` | `false` | `tx()` is currently a single-shot wrapper that surfaces transient errors as `TransactionException` — NOT optimistic concurrency control. Real `WATCH`/`MULTI`/`EXEC` is deferred to a follow-up PR. |
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

## Relationships

Relationship support is split across waves: T1 (this PR) lands the helper
contract; T2 implements `createRelationship` / `updateRelationship` /
`deleteRelationship`; T3 wires read-path null surfacing into every decoded
document path; T4 flips the capability bit and adds a cross-process smoke
test. The rules below are the locked contract — Wave-2 architects MUST NOT
deviate.

1. **Storage policy.** Relationship attributes ride on the existing
   `meta:{col}` HASH's `attrs` JSON field. The adapter writes a MINIMAL
   attribute record per registered relationship — `{$id, key, type:
   relationship, size: 0, signed: true, array: false, required: false}` —
   matching what `Memory::registerRelationshipField` stores. The full options
   map (`relatedCollection`, `relationType`, `twoWay`, `twoWayKey`, `side`,
   `onDelete`) is owned by the orchestrator and persisted into the METADATA
   collection's document via standard `updateDocument` CRUD; the adapter
   never sees that map.

2. **Non-journalled schema ops.** `createRelationship`,
   `updateRelationship`, and `deleteRelationship` are NOT journalled — they
   follow the same convention as `createAttribute`, `deleteAttribute`, and
   `renameAttribute`. Schema mutations are not transactional; their `tx()`
   wrapper exists solely to surface `\RedisException` as
   `TransactionException`. NO new cases are added to `rollbackJournal()`'s
   switch; the locked T56 region is unchanged by relationship work.

3. **Read-path null surfacing.** Every read path that decodes a stored
   document MUST call `surfaceRelationshipAttributes` (or the `Using`
   companion when iterating in a loop with a pre-computed key list) so
   registered relationship columns materialise as `null` even on documents
   written before the relationship was registered — mirroring MariaDB's
   `DEFAULT NULL` column behaviour. Read paths in scope (T3): `getDocument`
   (post-decode, pre-projection), `loadCollectionDocuments` (bulk find), and
   in-transaction decodes inside `updateDocument`, `updateDocuments`, and
   `upsertDocuments`. The METADATA collection is exempt — its relationship
   attrs are nested inside the row's `attributes` payload, not top-level
   keys, so surfacing nulls there would clobber the nested array.

4. **Junction-name resolution.** M2M renames use
   `resolveJunctionCollection`, which reads the METADATA rows for both
   sides, extracts each `$sequence`, and returns
   `_{parentSequence}_{childSequence}` for `RELATION_SIDE_PARENT` (reversed
   for child). Returns `null` when either METADATA row is missing or
   carries no sequence — callers (i.e. T2's `updateRelationship` M2M
   branch) treat this as a no-op and skip the rename. This mirrors
   `Database::getJunctionCollection` exactly.

5. **Adapter non-goals.** The Redis adapter NEVER:
   * creates the M2M junction collection — the wrapper / orchestrator
     drives that through standard `createCollection` with explicit
     attributes;
   * propagates parent permissions to children — relationship-aware
     permission cascade is the orchestrator's job;
   * decomposes nested relationship `Query` filters — the orchestrator's
     `convertRelationshipQueries` flattens those before they reach the
     adapter, so adapter-level query evaluation only ever sees plain
     attribute filters.

6. **Helper inventory** (relationship-specific; full T1 inventory is in
   the Helpers table above):

   | Signature | Owner |
   |-----------|-------|
   | `private function surfaceRelationshipAttributes(string $collection, Document $document): Document` | T1 |
   | `private function surfaceRelationshipAttributesUsing(array $relationshipKeys, Document $document): Document` | T1 |
   | `private function extractRelationshipKeys(array $attributes): array` | T1 |
   | `private function renameDocumentField(string $collection, string $oldKey, string $newKey): void` | T1 |
   | `private function dropDocumentField(string $collection, string $field): void` | T1 |
   | `private function resolveJunctionCollection(string $collection, string $relatedCollection, string $side): ?string` | T1 |
   | `private function loadMetadataDocument(string $collection): ?Document` | T1 |

7. **Capability bit.** `getSupportForRelationships()` returns `false` until
   T2 + T3 ship; T4 flips it to `true` and adds the cross-process smoke
   test. See the parity-table footnote below.

[^rel]: Returns `false` while T2 (schema ops) and T3 (read-path null
surfacing) are still pending. T4 flips it to `true` once both ship and the
cross-process smoke test passes.

## Known limitations

* Multi-attribute cursor pagination has known off-by-one issues in corner
  cases (`testFindOrderByMultipleAttributeAfter`,
  `testFindOrderByMultipleAttributeBefore`). Single-attribute cursor
  pagination works correctly. Tracked for follow-up.
