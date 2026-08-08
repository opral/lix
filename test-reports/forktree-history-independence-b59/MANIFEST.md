# ForkTree history-independence decision oracle

This is a test/report-only package anchored to exact production object
`b59e1f11a51153e0a787a81f0f25bf104d150aaf` (tree
`700fd04d21bc40c05425c9fc9e10d65c9e1eda24`). It contains no production
source, no generated database, and no benchmark result. Every adapter command
in `RUN_COMMANDS.md` is intentionally **UNRUN**.

Package purpose:

* define deterministic pairs of histories that finish with the same logical
  rows, file identities, and blob identities;
* distinguish logical equality from physical-root equality (physical roots
  need not match unless a future canonicalization change proves that they
  should);
* measure the future candidate's object, read, synchronization, publication,
  allocation, disk, and final-reference-GC effects without allowing a
  history-dependent identity to be mistaken for a semantic result;
* preserve fail-closed corruption and atomic-publication requirements.

The pure model in `history_independence_model.rs` is an executable semantic
and accounting oracle, not a ForkTree codec or a substitute authority. Its
row-key-independent content ObjectIds, pair equality assertions,
authenticated-fingerprint publication, GC, and ceiling values must not be
used to claim adapter runtime results.

## Frozen identity

| Item | Value |
|---|---|
| production base | `b59e1f11a51153e0a787a81f0f25bf104d150aaf` |
| production tree | `700fd04d21bc40c05425c9fc9e10d65c9e1eda24` |
| package scope | `test-reports/forktree-history-independence-b59/*` only |
| adapter runtime status | **UNRUN** |
| standalone pure-model status | **PASS — 6/6** |
| adapters | Memory, RocksDB, SlateDB, in that order |
| production edits | none |

The package is independent of the historical benchmark/prototype refs. It
does not assert that their physical roots or numbers are applicable to b59.
