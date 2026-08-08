# Current-state reader discriminating oracle

This is a test/report-only oracle. It does not modify production code, storage,
selectors, or candidate refs. It is intended to run against the first
immutable successor of 1f742.

## Fixture

Use one repository with:

- one authenticated global state root;
- one branch-local state root;
- raw ordered keys:
  app.row/a, app.row/b, app.row/c, app.row/d, and app.row/missing;
- global rows: a=global-a, b=global-b, c=NULL;
- branch rows: a=local-a, b=TOMBSTONE, d=NULL;
- two historical commits whose state roots differ in a, b, and d;
- one row with a distinct file_id but the same EntityPk;
- one valid CommitCatalog/ChangeCatalog source+ordinal for every historical row.

The fixture must be built through existing authenticated ForkTree model helpers
or an owner test seam. It must not insert legacy tracked-state spaces.

## Deterministic runtime cases

The case IDs and required observations are:

| ID | Operation | Expected result |
| --- | --- | --- |
| R1 | Acquire reader/view, mutate current selectors in a separate ordinary writer, then point-read a/b/c | Exactly one retained read; original global+branch overlay is returned, not the post-publication selector. |
| R2 | Point-read a with tombstones excluded | one row, source Branch, cell Value(local-a). |
| R3 | Point-read b with tombstones excluded | absence; no global-b leak. |
| R4 | Point-read b with tombstones included | one row, source Branch, cell Tombstone. |
| R5 | Point-read c | one row, source Global, cell Null. |
| R6 | Point-read d | one row, source Branch, cell Null. |
| R7 | Ordered range with no bounds, limit 3, tombstones excluded | keys a, c, d in raw-key order after overlay and filtering. |
| R8 | Ordered range with no bounds, tombstones included | keys a, b, c, d in raw-key order; b is one local tombstone. |
| R9 | Bounded range [b, d) with continuation/page size 1 | strict ordered pages, no duplicate key, upper bound excluded, local b shadows global b. |
| R10 | Historical diff old-root -> new-root | exact added/removed/modified classification and deterministic identity tuple (schema,file,EntityPk,ChangeId,CommitId). |
| R11 | Same EntityPk under file_id=None and file_id=file-a | two distinct diff identities; no coalescing. |
| R12 | Historical row with forged ChangeId or CommitId | fail closed before materialization; no fallback to current row. |
| R13 | Missing or remapped CommitCatalog source object | fail closed; no output. |
| R14 | Missing/malformed/wrong-kind global or local root | fail closed; selector/epoch/receipt/progress digest unchanged. |
| R15 | Truncated leaf, duplicate key, out-of-order key, invalid summary | fail closed; no repair write or retry. |
| R16 | Global tombstone injection | fail closed; never returned as a global visible row. |
| R17 | Distinct second read paired with first view roots/cursor | reject the pairing; no cross-view row or continuation result. |
| R18 | Read error after first page, then caller resumes | error poisons the reader/continuation; no manual drop or stub is needed to prevent reuse. |
| R19 | Reader-only full sequence | write count, selector bytes, epoch, receipt and GC-progress digests unchanged. |

## Instrumentation requirements

The test adapter must expose only counters and barriers; it must not alter
production behavior:

- begin_read count;
- read handle identity for every get/scan;
- point/range request keys and bounds;
- physical object/tree reads;
- writes, deletes, selector mutations, and epoch rotations;
- pre/post digest of global selector, branch selector, repository root,
  branch snapshot, receipt/progress selectors.

R1 must pause after the single underlying read is acquired and before the view
is returned, permit a normal separate publication, then return the pinned
view. The expected result is the pre-publication root pair. Any second
publisher begin_read or any post-publication row is a blocker.

R17 must use two independently acquired reads with the same selector values.
A cache or cursor bound to one read must not be accepted by the other.

R18 must inject a real page/tree read error, not a fake return value. A
continuation after that error must fail closed without requiring a caller
drop/stub hook.

## Authority assertions

For every successful row:

1. selector and root objects were decoded from the retained CoherentView;
2. state tree kind, node digest, ordered key, and value codec were validated;
3. branch-local precedence was applied only after both roots were authenticated;
4. historical identity came from authenticated CommitCatalog/ChangeCatalog
   source+ordinal and not from a row-shaped cache or old changelog space.

For every failed case:

1. the error is typed/corruption or invalid-cursor as appropriate;
2. no alternate reader, old owner, or current selector is consulted;
3. no output row is returned;
4. no write/selector/epoch/receipt/progress mutation occurs.

## Acceptance

The successor is accepted for this reader slice only when R1-R19 pass on both
Memory and the available persistent adapters, with the exact output/order and
counter digest recorded. A compile-only or source-only pass is not a runtime
approval. Any missing historical diff identity, global/branch overlay error,
cross-view reuse, hidden fallback, or write mutation is an immediate blocker.
