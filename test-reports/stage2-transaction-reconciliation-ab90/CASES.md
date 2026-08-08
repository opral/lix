# Deterministic transaction reconciliation cases

Every case uses one operation-owned retained view and records opening/final
selectors and roots, undo target, redo cursor, result/order digest, plan count,
prepare count, backend commit count, write count, and epoch/receipt delta. The
same sequence runs on Memory, RocksDB, and SlateDB; an error after a partial
write fails the oracle.

| ID | Required sequence and assertion |
|---|---|
| R01 | Explicit authenticated empty bootstrap succeeds; absent selected root/row is typed corruption, not empty. One view/read. |
| R02 | Global plus branch overlay contains replacement, NULL, tombstone, and untracked rows. Preserve typed order/precedence and policy-controlled tombstones before LIMIT. |
| R03 | Same-owner stale writer revalidates exact owner/key identity and succeeds through one plan/prepare/commit. |
| R04 | Unrelated-owner stale writer composes disjoint changes without erasure or legacy full-state fallback. |
| R05 | Unsafe mixed conflict/substituted owner rejects before plan: zero writes, selector/epoch changes, or receipt. |
| R06 | Three ordered first-parent changes, undo twice, redo twice; state bytes and chronology/cursors match at every boundary. |
| R07 | Undo, divergent publication, then redo; redo is rejected/discarded per public semantics and divergent head remains sole head. |
| R08 | Reconcile after concurrent publication with staged rows and runtime state; opening view remains the read owner, no second `begin_read`. |
| R09 | Savepoint failure rolls back only the failed statement; valid prefix, RETURNING/result indexes, and runtime sequence state match control. |
| R10 | Explicit transaction rollback after multiple statements leaves no write, selector/epoch/receipt mutation, and original state after reopen. |
| R11 | Repeat a durable idempotency key returns the exact prior result, performs no second publication, and rejects mismatched payloads. |
| R12 | Checkpoint after an undoable change; undo/redo across boundary and release; roots remain until final release. |
| R13 | Cold reopen after R06/R07/R12 preserves history, cursor, overlay, NULL/tombstone distinction, and result identity. |
| R14 | Corrupt/missing selected root, branch control, catalog member, chronology, or undo/redo record; fail closed before append/LIMIT/plan/commit. |
| R15 | Duplicate/reorder/substitute owner/member/catalog entries; reject first malformed authority, never fall back to `TrackedStateContext` or cache. |
| P0 | Historical duplicate/order/member prerequisite is independently accepted before R14/R15 can become green; local transaction checks may not self-certify it. |

The baseline therefore contains 16 preserved cases: R01-R15 plus P0.

## Successor discriminators

| ID | Required sequence and assertion |
|---|---|
| D01 | Open through every supported transaction/reconciliation helper. All helpers borrow one operation-owned retained view/read; exactly one underlying acquisition, no refresh/extraction, and identical captured selector/root/epoch bytes. |
| D02 | Capture `owner_epoch` and `view_id`, mutate the backend after capture, and attempt publication. Plan and commit authenticate both values; stale capture fails closed with zero writes and no selector/epoch/receipt change. |
| D03 | Same-owner reconciliation is enforced by the publication owner itself. Unrelated-owner disjoint composition succeeds; unsafe mixed conflict fails before writes even when the caller bypasses the normal convenience helper. |
| D04 | Historical opening captures immutable global/branch roots and chronology. External mutation after capture cannot change the read; tombstone inclusion/exclusion is consistent across snapshot, undo/redo, and reopen. |
| D05 | Transition includes the desired local state explicitly. Missing desired state, missing source/target root, or a zero/default digest fails closed; no mutable-reader reread or fallback is permitted. |
| D06 | Root identity is content-authenticated, not prefix/length authenticated. A same-prefix different-content root or row transplant fails before plan/publication; the valid content-authenticated root still succeeds. |
| D07 | Full-workspace source scan classifies every legacy reader/cache/fallback/compatibility symbol, then applies negative rules only to the exact migrated function bodies. Deferred checkpoint/GC owners do not create false positives; a migrated function cannot hide a forbidden call behind a wrapper. |
| D08 | Alternate opening, reconciliation, rollback, and idempotency paths all preserve one view/read and one plan/prepare/backend commit. Any partial write, retry, second read, or second authority is a hard failure. |

## Historical prerequisite

R14/R15 are blocked until the historical direct-reader duplicate invariant is
independently accepted. The prior semantic package is anchored at
`413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`, ref
`origin/codex/forktree-stage2-sql-entity-semantic-oracle-413`, head
`6c7e3c4d67256b5e7e91b763081c7831e1f22cc7`. ab90 is deliberately not an
accepted replacement. A future run must pass an accepted immutable
prerequisite SHA to the verifier; otherwise the migration remains RED.
