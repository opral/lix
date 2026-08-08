# ForkTree residue verifier package — d6b

This directory is test/report-only. It does not alter Lix production modules,
public APIs, storage formats, or runtime behavior.

## Immutable binding

- exact approved base: `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768`
- base tree: `641654079f60fcd1c9ff9ccbbd06d3edcabe4096`
- parent: `1f742a382c755399b8a49ab536c4f6dc55fffdd8`
- parent tree: `860a047b98eaa38368a3d889497628e244c2e0ec`
- source delta: `packages/lix/src/sql2/providers/change.rs` only, `+85/-7`
- semantic delta: fail closed when CommitCatalog records are missing, short,
  or returned for the wrong commit ID; no residue policy change.

The embedded verifier is the previously frozen corrected deterministic oracle,
copied byte-for-byte from its immutable source. Its policy is exactly:

```text
42 forbidden durable spaces
23 forbidden modules
151 forbidden legacy owner/reader/writer/codec symbols
17 required ForkTree owner symbols
19 retained public semantic facade rules
```

The complete machine-readable arrays are in `verifier/main.rs`:
`LEGACY_SPACES`, `LEGACY_OWNER_TOKENS`, `DELETE_MODULES`,
`REQUIRED_OWNER_TOKENS`, and `SEMANTIC_RULES`. The semantic TSV is bound by
`PUBLIC_SEMANTIC_ALLOWLIST.tsv`.

## Recalibration result

The same verifier was run against the exact parent `1f742` and d6b. All hard
residue classes and the cursor baseline are byte-identical. The only budget
delta is the allowed provider correction:

```text
CommitCatalog 83 -> 86 occurrences
ChangeId      449 -> 450 occurrences
CommitId      805 -> 808 occurrences
```

Those three increases are caused by the new fail-closed `change.rs` helper and
its tests. No forbidden space, legacy owner, module, cursor residue, required
owner, or semantic-facade classification changed.

The four baseline cursor findings remain classified, not blindly deleted:

1. `SlateDBScanOptions` is a dependency-qualified SlateDB alias, not the old
   Lix scan API.
2. Plugin arena `.page(` is not storage pagination.
3. `snapshot_scan_cursor` is a backend snapshot test name.
4. Conformance `resume_after` is a local variable used to form an exclusive
   `KeyRange`; it is not a persisted/caller-owned storage cursor. A
   `resume_after` field in scan options/plans/cursors remains forbidden.

SQLite adapter/package selectors and `FileStorage`/`FileLix` owners remain
zero. Normal CLI routing remains RocksDB (`lix_storage_rocksdb` 2,
`RocksDB::open` 1, `Lix<RocksDB>` 1). Legitimate filesystem cleanup of legacy
`db.sqlite*` files is not adapter residue.

## Future immutable-successor command

```text
rustc --edition 2024 -D warnings \
  packages/lix/tests/forktree_stage2_residue_d6b/verifier/main.rs \
  -o /tmp/forktree_stage2_residue_d6b
/tmp/forktree_stage2_residue_d6b self-test
/tmp/forktree_stage2_residue_d6b audit /path/to/candidate
/tmp/forktree_stage2_residue_d6b cursor-audit /path/to/candidate
/tmp/forktree_stage2_residue_d6b semantic-audit /path/to/candidate
/tmp/forktree_stage2_residue_d6b budget /path/to/candidate
/tmp/forktree_stage2_residue_d6b definitions /path/to/candidate
```

`audit`, `cursor-audit`, and `semantic-audit` must exit zero on a first
runnable candidate. The candidate must have no listed space/module/symbol,
must expose the 17 typed owner symbols and the approved streaming cursor, and
must preserve the 19 public semantic facades without raw storage ownership.
