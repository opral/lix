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

## Historical prerequisite

R14/R15 are blocked until the historical direct-reader duplicate invariant is
independently accepted. The prior semantic package is anchored at
`413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`, ref
`origin/codex/forktree-stage2-sql-entity-semantic-oracle-413`, head
`6c7e3c4d67256b5e7e91b763081c7831e1f22cc7`. ab90 is deliberately not an
accepted replacement. A future run must pass an accepted immutable
prerequisite SHA to the verifier; otherwise the migration remains RED.
