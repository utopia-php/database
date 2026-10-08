# HANDOVER: query-lib train, database API redesign, 2026-10-08 (from the cloud session)

Copy this file to `~/Local/.query-train/fix-plan-2026-09-23/handoff/`. Then append the "STATUS lines" section below to
`STATUS.md`. Start from this file, then read `subtasks/api-redesign-db-handoff.md` (STOP 2026-10-08) for the
older context. Everything listed here is pushed to `utopia-php/database`.

## Branch heads (origin)

| branch | head | what |
|---|---|---|
| `qlt-fix/api-w7` | `4c67e22f5` | **Checkpoint C candidate.** `ws-api-redesign` (`ee1936f2c`) + W7-1 + W7-2 docs + BENCH-B fixes. CI dispatched (below). |
| `ws-api-redesign` = `qlt-fix/api-redesign` | `ee1936f2c` | Unchanged (waves 1-6, CI green). Fast-forward it to the checkpoint-C head once CI and the W7 review are green. |
| `ws-api-redesign-w7-1` | `096a61fb1` | W7-1 finished (merged into `qlt-fix/api-w7`). |
| `ws-api-redesign-w7-2` | `af6959fa7` | W7-2 docs (merged into `qlt-fix/api-w7`, then updated there). |
| `qlt-fix/api-bench-b` | `fef489d49` | BENCH-B fix-forward (merged into `qlt-fix/api-w7`). |
| `wip/ws-api-redesign-w7-1-snapshot` | `58def99c1` | Superseded by the conductor's `e233b9dbd`. Safe to delete. |
| `feat-query-lib` | `8b9cdfba1` | Untouched. Fast-forward only after checkpoint C + BENCH-C. |

Consumers are not re-pinned (still on `8b9cdfba1`), as instructed.

## CI on `4c67e22f5` (workflow_dispatch, ref `qlt-fix/api-w7`)

- Tests `37732703099`, Linter `37732705419`, CodeQL `37732707659`. They were in progress at hand-off. Check with
  `gh run view <id> --repo utopia-php/database`. If all three are green, that is checkpoint C, provided the W7
  review (below) is also clean.
- The `claude.yml` push-run failures on these branches are that workflow reacting to pushes and are unrelated.
- Locally on `4c67e22f5` (PHP 8.5.11, Swoole 6.2.2): PHPStan 0 errors on both configs, Pint clean, unit suite 9890
  tests with 1 failure. That failure is `SwooleAbsentTest` and is an environment artifact: the subprocess runs
  `php -n`, and in the nix build mbstring is a shared extension, so `-n` drops it. The CI image compiles mbstring
  in. No e2e was run locally except the bench agent's filtered runs (below). Docker was unavailable.

## What was done this session

### W7-1 finished: `5da7563fe`, `096a61fb1`, `8b55e93e2`
- `e233b9dbd` (the WIP sweep) had 8 PHPStan errors and 5 Pint failures. The PHPStan errors were dead
  `assertNotNull()` calls: `filterJoin()` became non-null. All fixed.
- `isValid(mixed $value)` on `Permissions`, `Roles`, `Structure`, `PartialStructure` and `Authorization`. Named-arg
  callers with the old names now break; this is documented in UPGRADE.
- Name-restating docblock lines removed from `Adapter`, `Database`, `Trait/*` and `Validator/**` (111 lines).
- **Decision (conductor-open item):** `Storage::PERMS_SUFFIX`/`PERM_DOCUMENT`/`PERM_TYPE`/`PERM_PERMISSION` are now
  `PERMISSIONS_TABLE_SUFFIX`/`PERMISSIONS_DOCUMENT`/`PERMISSIONS_TYPE`/`PERMISSIONS_PERMISSION`. Reason: the
  full-word rule. `Storage` is new in 8.0 and appwrite has no uses. The private `Hook\Permissions::PERM_TYPES` is
  now `PERMISSION_TYPES`.
- **Decision:** `Database::snapshot()` is now `@internal`, like `withSnapshot()`. Only Mirror and the relationship
  hook use it, and a public snapshot had no public way to be applied. `Role::user()`/`any()` return `self`, a
  leftover of the namespace rewrite.

### W7-2 docs updated: `7889ad9b5` (agent), plus the docs part of `8b55e93e2`
- Every class/member/fence in README, UPGRADE, CHANGELOG, SPEC, AGENTS and `docs/` was checked against HEAD.
  Only UPGRADE and CHANGELOG needed edits.
- Added:
  - `withSnapshot`/`snapshot` are `@internal`;
  - the permission filter subclasses must be `readonly`;
  - the four protected SQL methods now return `Adapter\SQL\Expression`, and `compileAdapterFilter()` `$joins` is
    `list<JoinAlias>`;
  - `Cache\Region` and `Profiler\Log` are `final readonly`;
  - the `Storage::PERMISSIONS_*` rows;
  - the `isValid()` `value:` note;
  - `Validator\Queries` is now `Validator\Queries\Base` in CHANGELOG.
- All 33 W7-1 moves plus the 3 Mongo hook replacements were already in UPGRADE.

### BENCH-B fix-forward: `qlt-fix/api-bench-b`, merged
Statement counts come from local nix MySQL 8.0.44 and PostgreSQL 16.15 (general log / `log_statement=all`).
Local timings vary by about ±15%, so treat them as indicative only.

1. **`transaction.update_then_read`: fixed** (`30d567f7e`)
   - Cause: a definition read inside a transaction never fills the shared cache (deliberate). A batch write purges
     the cached definition, so a collection that only transactions read kept going back to SQL:
     `SELECT * FROM _metadata WHERE _uid=...` right after BEGIN.
   - Fix: after the outermost commit and a successful invalidation, the definitions read by SQL are re-read through
     the normal filling path (`Trait/Transactions.php` `cacheTransactionDefinitions()`). Nothing is filled after a
     rollback or a failed invalidation.
   - Statements per call:
     - MySQL: 7 (7.4.0), 8 (before), 7 (after).
     - PostgreSQL pooled: 10 (7.4.0, with 3 SET LOCAL), 9 (before), 8 (after).
   - CPU ratio is now about 1.04-1.19.
   - Tests: `TransactionDefinitionReadTest` (3).
2. **`collection.get.miss` (PostgreSQL plain): partly fixed** (`98d6aefd4`)
   - Fix: the metadata definition and each cached definition now build their Attribute/Index models once, so
     clones share them. PHP time per miss dropped from about 130 µs to 97 µs.
   - Remaining: a miss makes 5 cache round trips against 4 in 7.4.0, because the definition entry carries the epoch
     (`loadDocumentCacheState()`, from `3a41562c2`).
   - Local ratio 1.03-1.27, which is noisy.
   - Tests: `DefinitionModelCacheTest` (2).
3. **`decrease`/`increase`: partly fixed** (`9b0363ebc`, `fef489d49`)
   - Fix 1: BigInt has native-int fast paths. 7.4.0 used native arithmetic; 8.0 made about 7 string-math calls per
     update.
   - Fix 2: `Pool::supports(DefinedAttributes)` is now cached for pools without a schemaless mode (SQL engines).
     Before, every counter update and every validated read checked out a connection.
   - No extra statement or cache round trip against 7.4.0. The remaining cost is about 1.15-1.5 locally, spread
     over many small costs: about 40 definition clones per op, per-coroutine state lookups, Pool
     `syncBorrowed`/`withTenant`, builder, and epoch bookkeeping.
   - Tests: `BigIntTest` (equivalence), `PoolCapabilityTest`.
4. **`relationship.oneToMany.depth3.get`:** statement counts are identical (MySQL 3/3, PG 9/9). Likely noise;
   re-measure with 9 rounds in BENCH-C.

The probe scripts are in the cloud scratchpad only and are lost with the container. The method is described above.

## User decisions 2026-10-08 (this session)
- **Bench gate:** "Keep cutting first". Before checkpoint C, keep reducing per-call overhead on `collection.get.miss`
  and the counters, then let BENCH-C judge.
- **Cache-protocol changes approved** (each needs fail-first tests):
  1. **Refill after batch write:** a batch write re-fills the definition cache after activating the epoch instead
     of purging it. This saves one `_metadata` SQL read per `createDocuments`, and the cold cache after it.
  2. **Drop the in-transaction purge:** `purgeWrittenDocuments()` already purges after commit. This saves one Lua
     round trip per write.
  3. Not approved: reading the epoch in the same round trip / lazily on the miss path.
- **CI:** push and dispatch from the session. REST `gh api` works through the proxy; GraphQL is blocked, so
  `gh pr list` fails but `gh run list`/dispatch work.
- **BENCH-C:** run with your local `doctl`. A cloud session cannot reach your Mac, so BENCH-C has to run locally.

## Next steps, in order
1. **W7 review** of `ee1936f2c..4c67e22f5`.
   - Covers the sweep `e233b9dbd` (large, verified only by PHPStan/unit), the transaction cache refill, the shared
     definition models, the BigInt fast paths and the Pool caching.
   - A review agent was running at hand-off. If its result is not in this file, re-run the review. Fix findings on
     a branch from `qlt-fix/api-w7`.
2. **Implement the two approved cache changes** (refill after batch write; drop the in-txn purge) on a branch from
   `qlt-fix/api-w7`, with fail-first tests.
   - Re-count statements/round trips against 7.4.0 (`1c99c2179d`) for `collection.get.miss`, `decrease`/`increase`,
     the `createDocuments` path and `transaction.update_then_read`.
   - Keep cutting per-call overhead on the counter path: definition clones per op, Pool sync per pinned call.
3. **Merge**, then run local checks and CI (dispatch tests/linter/codeql on the branch). Green + review clean =
   **checkpoint C**. Fast-forward `ws-api-redesign` = `qlt-fix/api-redesign` to it.
4. **BENCH-C (local doctl):**
   - Full run: MySQL, PG, Mongo `find25` guard, MariaDB, joins; 9 rounds for depth3.
   - JIT off on both sides; destroy the droplet after. The harness port from BENCH-B (`probes/bench-api-redesign-b/`
     with `harness/src/Api/Eight.php`) was not in the bundle; it should still be on the Mac.
5. **Fast-forward `feat-query-lib`** to the checkpoint-C head.
6. **Consumers:**
   - migration ∥ appwrite, then cloud, per `plans/plan-api-redesign.md`;
   - narrow the appwrite packages audit/usage to `utopia-php/query ^0.7`;
   - consumers must use the new names: `Storage::PERMISSIONS_*`, `isValid(value:)`, and `snapshot()` is no longer
     public API.
7. **Per-PR merge go-ahead, then releases.**

## Open items outside the redesign (unchanged)
- Mongo shared JSON-export JIT crash (~2.5% of probe jobs): the hunt is paused; undecided.
- Maintainer approvals on database#823 and migration#222.

## Cloud-environment notes (if another cloud session resumes)
- No Docker, no doctl, and the system PHP is 8.3.
- PHP 8.5.11 + Swoole 6.2.2 were built via nix:
  - pkg `php85.buildEnv`, extensions mongodb, redis, pdo_*, mbstring, intl, bcmath, and swoole;
  - swoole overridden to `swoole-src` v6.2.2 via `builtins.fetchGit`, with `allowBroken`, pkg-config + openssl/curl/
    brotli/zlib/zstd, and `--enable-http2`.
- The proxy blocks `api.github.com` zipballs and `codeload`. Composer therefore can't fetch dists. The workaround:
  shallow-`git fetch` each locked commit, `git archive` it to a local zip, point a throwaway copy of `composer.lock`
  at the zips, install, then restore the lock.
- Never `--prefer-source`: phpstan's history alone is 20 GB and filled the disk.
- The handoff bundle has to be uploaded into the session; the cloud can't read `~/Local`.

## STATUS lines (append to STATUS.md)

```
- 10-08 (cloud instance) resumed from the handoff bundle. Env: PHP 8.5.11 + swoole 6.2.2 via nix (scratchpad php85-env), deps via local dist zips (proxy blocks api.github.com/codeload; git works); no Docker, no doctl, gh token invalid (no CI dispatch from here).
- 10-08 W7-1 verified + finished, pushed ws-api-redesign-w7-1 @ 096a61fb1: e233b9dbd had PHPStan 8 (dead assertNotNull after filterJoin() went non-null) + Pint 5 → fixed; isValid(mixed $value) on Permissions/Roles/Structure/PartialStructure/Authorization; name-restating docblock lines removed (Adapter, Database, Trait/*, Validator/**; 111 lines); DECISION (conductor-open item): Storage::PERMS_SUFFIX/PERM_DOCUMENT/PERM_TYPE/PERM_PERMISSION → PERMISSIONS_TABLE_SUFFIX/PERMISSIONS_DOCUMENT/PERMISSIONS_TYPE/PERMISSIONS_PERMISSION (full-word rule; Storage is 8.0-new, appwrite has no uses). PHPStan 0 both configs, Pint clean, unit 9881 tests: 1 fail = SwooleAbsentTest env artifact (nix mbstring is shared, `php -n` drops it; CI image has it static).
- 10-08 BENCH-B fix-forward running as a background agent on qlt-fix/api-bench-b (base ee1936f2c), statement counts vs 7.4.0 on local nix MySQL/PG (timing still needs the droplet).
- 10-08 qlt-fix/api-w7 @ 8b55e93e2: W7-1+W7-2 merged; docs agent 7889ad9b5 (UPGRADE/CHANGELOG match HEAD: withSnapshot @internal, readonly filter subclasses, Expression/JoinAlias protected returns, Cache\Region/Profiler\Log readonly, Storage PERMISSIONS_* rows, isValid value: note); DECISION: Database::snapshot() also @internal (only Mirror + relationship hook use it); Role::user()/any() return self.
- 10-08 BENCH-B fix-forward (qlt-fix/api-bench-b @ fef489d49): txn update_then_read FIXED (extra _metadata SELECT after BEGIN: txn-only readers never refilled the cache after a batch purge → refill after outermost commit; MySQL 8→7 = 7.4.0, PG 9→8; CPU ratio ~1.05-1.19); collection.get.miss PARTIAL (shared definition models built once; miss = 5 cache round trips vs 4 in 7.4.0 by epoch design; local ratio 1.03-1.27 noisy); decrease/increase PARTIAL (BigInt native fast path, Pool DefinedAttributes cached for non-schemaless; no extra statement/round trip, residual CPU spread over clones/state/pool sync, ~1.15-1.5 local); depth3: identical statement counts. Agent questions open: (1) refill vs purge on batch write, (2) drop in-txn purge, (3) miss path epoch read lazily/same round trip. Merged into qlt-fix/api-w7 @ 4c67e22f5: PHPStan 0/0, Pint clean, unit 9890 (1 env-only fail). W7 review next, then ff ws-api-redesign + CI = checkpoint C.
- 10-08 CI dispatched on qlt-fix/api-w7 @ 4c67e22f5: Tests 37732703099, Linter 37732705419, CodeQL 37732707659. User decisions: keep cutting before checkpoint C; approved cache changes = refill definition cache after batch write + drop in-txn purge (not: epoch in one trip); CI via push+REST dispatch from session; BENCH-C via user's local doctl. Handoff: handoff/HANDOVER-2026-10-08-cloud.md (also on origin branch handoff/query-train-2026-10-08).
```
