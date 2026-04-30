# Contract gaps observed by T50

These are notes for the consolidator. T50 has NOT modified `Contract.md` —
the gaps are documented here and the implementation made the most contract-
faithful choice available.

## 1. Action-letter mapping is incomplete

`Contract.md` lists the permission set keys as
`{ns}:{db}:perm:{col}:r/w/u/d:{role}` but `Database::PERMISSIONS` covers four
distinct actions (`create`, `read`, `update`, `delete`) plus the composite
`write` alias. The four-letter shorthand `r/w/u/d` cannot represent both
`create` and `write` simultaneously.

The T50 task brief explicitly resolves this with a five-letter mapping:

| Action | Letter |
|--------|--------|
| `read`   | `r` |
| `create` | `c` |
| `update` | `u` |
| `delete` | `d` |
| `write`  | `w` |

T50 followed the task-brief mapping. Storage keys therefore include `c` (for
create) in addition to the `r/w/u/d` documented in the contract. The keys
themselves are entirely owned by `writePermissions` / `clearPermissions` /
`applyPermissionFilter`, so no other architect's region observes the change.

Recommended consolidator action: amend `Contract.md` line 21 to
`{ns}:{db}:perm:{col}:r/w/u/d/c:{role}` (or simply note that the action set
is `Database::PERMISSIONS` rather than enumerating letters).

## 2. Per-doc HASH csv format

Contract.md describes `{ns}:{db}:perm:doc:{col}:{id}` as
`HASH | role -> csv("read,update,delete")`. The example is illustrative; T50
stores the **single-letter csv** (`r,u,d`) so the inverse path
(`clearPermissions`) can SREM the right per-action set keys without re-
parsing English action names. This is fully internal to T50 — no other
architect reads or writes these hashes.

## 3. Tenant scoping pattern not defined for perm keys

Contract.md defines `{ns}:{db}:tenants:{col}:{tenant}` for doc-id-by-tenant
filtering in shared-tables mode, but does not specify whether the perm keys
themselves should be tenant-scoped. T20/T30 are still empty at the time of
T50 implementation, so there is no upstream convention to mirror.

T50 chose: when `getSharedTables()` is true, the perm keys are scoped by
inserting `t:{tenant}` after the `perm:` literal:

```
{ns}:{db}:perm:t:{tenant}:{col}:{letter}:{role}
{ns}:{db}:perm:t:{tenant}:doc:{col}:{id}
```

Both write/clear/filter use the same `permKey` / `permDocKey` helpers, so
the behaviour is internally consistent. No other architect touches these
keys.

Recommended consolidator action: confirm the tenant-scoping pattern aligns
with whatever T20/T30 settle on for doc keys, and bring all four keyspaces
(doc, idx, perm, tenants) into a single convention.

## 4. `@phpstan-ignore` pragmas retained on the three helpers

The T50 task brief states "No `@phpstan-ignore` pragmas." T1 originally
declared `writePermissions`, `clearPermissions`, and `applyPermissionFilter`
with `@phpstan-ignore-next-line method.unused` annotations because their
call sites live inside T20 (`createDocument`/`deleteDocument` paths) and
T40 (`find`) which are still stub-throwing.

After filling the helper bodies, removing those pragmas raises three
`method.unused` errors against the locked 28-error baseline. Reaching the
helpers from any current code path requires editing T20/T30/T40 regions,
which the brief explicitly forbids.

Resolution: the pragmas are kept verbatim. Once T20/T30/T40 land their
real bodies (which will call these helpers), the consolidator should drop
the three `@phpstan-ignore-next-line method.unused` lines as a follow-up
chore — the same pattern applied in commit `cfb1a91a chore: drop stale
phpstan-ignore pragmas after T1 lands`.

The newly introduced private helpers `actionLetter`, `permKey`, and
`permDocKey` carry no pragmas — they are reachable through the three
helpers above and PHPStan resolves them transitively.
