# Deterministic acceptance cases

The following cases are the independent gate for the narrow semantic replay
bridge. They are source/runtime contract cases; runtime is intentionally not
run on the compiler-red fd2 anchor.

| ID | Case | Required result |
|---|---|---|
| R1 | One transaction opens replay/history/undo view | Exactly one operation-owned coherent read; no helper `begin_read` |
| R2 | Topology-only ancestry | Zero Change/member reads; authenticated commit identity/parents/generation only |
| R3 | Requested semantic commit | Lazy member read through the same view; exact catalog/object/domain checks |
| R4 | Missing CommitCatalog/ChangeCatalog/commit/member | Error before append, limit, plan, or write; never empty/default |
| R5 | Malformed/wrong-kind/object substitution | Error before output; no partial result or fallback |
| R6 | Duplicate, reordered, skipped, or bad source ordinal/back-edge | Error; source commit and member order remain authenticated |
| R7 | Generation/parent cycle and first-parent mismatch | Error; chronology remains commit graph authority |
| R8 | Apply/revert tracked transition | Exact selected changes and final state; one plan/prepare/commit |
| R9 | Undo then redo | Exact chronology, tombstone/NULL distinction, and final state after reopen |
| R10 | Same-owner stale transaction | CAS/precondition failure, zero backend writes/epoch/selector mutation |
| R11 | Unrelated-owner reconciliation | Allowed composition in the one existing transaction plan/commit |
| R12 | Rollback and savepoint | Staged replay state and read identity are restored; no publication occurs |
| R13 | No-op | Zero writes, no selector/epoch/receipt mutation |
| R14 | Repeated idempotent replay | Stable result; no duplicate member or second authority |
| R15 | Cold reopen/recovery | Persisted authenticated graph/root reopens; arbitrary nonempty or missing state fails |
| R16 | Unsupported deferred cohort | GC/init/replacement/current-serving/reachability/multi-branch rejects before plan |
| R17 | Source-only structural gate | Exact caller/read arguments pass; forbidden wrapper/space/raw/cache/fallback symbols fail |

The future adapter order is Memory, RocksDB, SlateDB after the source gate is
green. No adapter command is valid for this fd2 package.
