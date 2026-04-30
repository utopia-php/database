# T30 Contract Gap

## Mismatch between task spec and Contract.md

The task assignment lists T30 as owning these methods:
`getDocument, getDocuments, createDocument, createDocuments, updateDocument,
updateDocuments, upsertDocuments, increaseDocumentAttribute, deleteDocument,
deleteDocuments, count, sum`.

`Contract.md` (locked, source of truth) lists T30 as owning:
`getDocument, createDocument, createDocuments, updateDocument,
updateDocuments, upsertDocuments, getSequences, deleteDocument,
deleteDocuments, increaseDocumentAttribute`.

Differences:
- `getDocuments` does not exist on `Adapter` (no abstract method by that name).
- `count` and `sum` are owned by **T40** in Contract.md and live in the T40
  region of `Redis.php`.
- `getSequences` is owned by T30 in Contract.md but missing from the task spec.

## Resolution

Followed Contract.md (the locked truth) and the actual `// === @architect:T30
... ===` markers in `Redis.php`. Implemented exactly the 10 methods that
appear in the T30 region. `count` and `sum` remain T40 stubs and were not
touched. No `getDocuments` method exists anywhere in the abstract surface,
so nothing to implement.

If T40 needs `count`/`sum` to be reassigned to T30 the consolidator should
amend Contract.md and the region markers; this worktree did not edit any
locked surface.
