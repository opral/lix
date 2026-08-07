# ForkTree Stage-2 post-cursor residue policy

Status: test/source-only draft. It does not approve the mutable cursor checkpoint or start Stage 2.

Review-only cursor checkpoint:

- base `4763408467d265b288a124e24b1d47be423f5d17`, tree `a2a261220fb08f88ac44ca7776b2bc7ba7d6441c`;
- head `770d73c17afd4d3a569b31820696fe28b65e25d3`, tree `aa2de4a32d2d0bf33375e476d8c34c9dfd993eaf`;
- full-index binary diff SHA-256 `d28aec542dccb919d1eb94d268c4e3e2e3f0358409982af8d3370f142629d190`;
- mutable ref `origin/codex/review/storage-streaming-cursor-mutable`.

The immutable final head supersedes this checkpoint. Final review compares only checkpoint..final, then runs this package against the exact final object and post-cursor main.

## Sole surviving cursor contract

The storage plane may expose only:

- `BeginScanOptions { projection, order }`;
- `ScanOrder::{Ascending, Descending}`;
- `ScanCursor<'read>::next_page(limit_rows)`;
- owned `ScanChunk` pages;
- doc-hidden adapter implementation trait `StorageScanSource`;
- `StorageRead::begin_scan(space, KeyRange, BeginScanOptions)` and the identical `StorageAdapterRead` forwarding boundary.

`Descending` remains an explicit capability request. Memory, RocksDB, and SlateDB return `Unsupported(ReverseScan)` and production has no descending caller.

Forbidden residue includes `StorageRead::scan`, `ScanOptions`, `StorageScanOptions`, `ScanPlan`, `ScanPlanCursor`, `first_page`, `page`, `resume_after`, resume statistics, Slate hidden continuation/cache fallback, and loops that reconstruct an adapter iterator for each page. `packages/lix/src/storage_adapter/scan.rs` is deleted. Cancellation, backend error, malformed ordering/bounds/cardinality, or validation failure poisons the ephemeral cursor terminally.

Crash/reopen persists only an owner-authenticated last key. A new coherent read uses `KeyRange.lower = Bound::Excluded(authenticated_last_key)` and opens one new cursor. No iterator, adapter token, cache identity, or `has_more` value is durable authority.

## Compile probes

- `cursor_begin_scan_compile.rs` must compile and directly exercise `begin_scan` plus `next_page` over `KeyRange`.
- `cursor_restart_key_compile.rs` must compile and express exclusive restart from a caller-supplied authenticated key.
- `cursor_resume_field_rejection.rs` must fail because `BeginScanOptions` has no continuation field.
- `old_scan_compile_rejection.rs` must fail because the one-shot scan API and `ScanOptions` are absent.
- `space_forge_rejection.rs` must fail against the integrated Stage1/Stage2 owner because equivalent raw-space construction and generic object mutation remain sealed.

## Landed #1258 map retained

`verify_landed_1258_map.sh` remains byte-for-byte in the package. It must continue to cover all 21 changed production paths and all 39 physical CAS/retention symbols. Only `stage_repository_gc`, `stage_repository_gc_with_preconditions`, and `load_plugin_registry_at_commit` may retain semantic facade names, and only after their bodies delegate to the sealed ForkTree owner.

## Branch/HOT deterministic-sequence invariant map

| Current physical proof | New sole owner | Required semantic oracle | Deletion condition |
|---|---|---|---|
| `BranchHeadControlContext` selects branch generation | coherent global+branch selector view | stale/missing generation fails closed across reopen | branch control module/space absent |
| `HOT_COLLECTION_CONTROL_SPACE.live_count` | authenticated untracked collection root | valid empty/present/tombstone/tracked/untracked cases preserve public values | mixed HOT control space absent |
| `ordered_identity_digest` plus canonical HOT row-key order | canonical ordered identities inside that untracked root | same-count substitution, duplicate/noncanonical identity and missing member fail closed | digest/HOT helper names absent |
| `validate_exact_collection_closure` | owner-local selected-closure validation | deterministic-sequence next UUID survives hot/cold reopen; corrupt member rejects | legacy closure reader absent |
| `canonicalize_hot_scan_rows` | ForkTree/untracked-owner ordered range | file-backed key order and duplicate physical encoding cannot alter logical order | HOT scan materializer absent |

This is a semantic transfer, not permission to retain the old branch/HOT plane as a compatibility reader, cache authority, or side mutation index. The full Stage-2 scanner therefore rejects those helper names together with the legacy spaces/modules.
