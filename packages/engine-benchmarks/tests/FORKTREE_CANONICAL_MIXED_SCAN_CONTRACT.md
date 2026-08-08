# Canonical mixed tracked/untracked scan discriminator

Status: immutable test/report-only package. It is anchored to the rejected
candidate review at `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` and carries no
production edit. The prior review and its terminal blocker are bound by
`/root/repos/lix-evidence/forktree-public-sql-413-review/FINAL_REVIEW.md`
(SHA-256 `b8248ee1ad85d3d27395626f331272268e9910c66041e61663a60deeb12860aa`).

## Acceptance contract

For ordinary SQL, `LiveStateFilter.untracked == None` means one complete
logical current-state overlay. The single operation-owned coherent view must
read tracked and untracked candidates, apply global/branch precedence and
tracked/untracked replacement or tombstone precedence by typed identity, then
return one ordered batch. Snapshot and primary-key results are terminal
projections of that batch; `LIMIT` is applied after identity resolution and
ordering.

`Some(false)` is tracked-only and `Some(true)` is untracked-only only where the
existing `LiveStateReadDomain` contract exposes those explicit modes. They must
remain narrow modes and must not be used as an ordinary-SQL fallback.

The same coherent view must reject malformed or duplicate/conflicting typed
identity authority. A projection cannot acquire another `StorageRead`, view,
raw ForkTree range, cache, or compatibility fallback. One scan operation owns
one `CoherentView`/`StorageRead`; each terminal projection calls
`LiveStateReader::scan_batch` exactly once.

## Frozen provenance and source gate

Rejected candidate:

- head `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`
- tree `820fe560da3bbd2b00b788b0b1759c409048cd6e`
- parent `11442c1e0023e20307a7231d88cd557bc704fd13`
- parent-to-head full-index diff SHA-256
  `e9be5053f44fa9e009aaa665b69d328f6ee0ac718b18e773fb79a2eb6d7af8d4`
- stable patch ID `02310ae525c028488e654d3cb26eb7d1f85974cb`

Run the discriminator from the repository root:

```sh
node scripts/forktree_canonical_mixed_scan_contract_verify.mjs --root "$PWD"
```

The exact 413 calibration is RED. It finds one `CoherentView` acquisition and
one scan per terminal projection, but no combined `None` path, no canonical
tracked/untracked overlay resolver, no explicit tracked-only path on the
ForkTree view, and no duplicate/conflict check in the combined path. Its log
SHA-256 is recorded with the frozen package.

The earlier scoped direct-reader gate remains useful as a separate check:

```sh
node scripts/forktree_mixed_tracked_untracked_residue_verify.mjs --root "$PWD"
```

That gate is GREEN on 413 because the rejected direct `scan_entity_rows` seam
is gone. It does not prove that the canonical scan itself contains the mixed
overlay, which is why this discriminator is required.

## Pure model gate

The std-only model includes six tests: rejected direct-route calibration,
combined overlay replacement, explicit tracked/untracked modes, typed PK
ordering and `LIMIT`, NULL/tombstone projection, one-scan terminal
projections, and duplicate/malformed-domain fail-closed behavior.

```sh
rustc --edition=2021 --test -D warnings \
  packages/engine-benchmarks/tests/forktree_mixed_tracked_untracked_oracle.rs \
  -o /tmp/forktree-canonical-mixed-model
/tmp/forktree-canonical-mixed-model --nocapture --test-threads=1
```

The pure model is GREEN: 6/6. This is an executable logical contract, not
evidence that 413's production reader satisfies it.

## Deferred adapter commands

The candidate remains on the inherited compiler-red frontier, so no Cargo,
Memory, RocksDB, or SlateDB runtime result is claimed here. Once a successor
compiles, rerun the same model and source gates, then execute the adapter
runner's exact backend selectors in isolation:

```sh
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo test -p lix_benchmarks --test forktree_mixed_tracked_untracked_oracle \
  --features storage-benches,slatedb -- --nocapture --test-threads=1

# The adapter runner must expose these three exact semantic cases before
# runtime acceptance is possible:
cargo test -p lix_benchmarks --test forktree_mixed_tracked_untracked_oracle \
  canonical_memory_mixed_overlay -- --exact --nocapture --test-threads=1
cargo test -p lix_benchmarks --test forktree_mixed_tracked_untracked_oracle \
  canonical_rocksdb_mixed_overlay -- --exact --nocapture --test-threads=1
cargo test -p lix_benchmarks --test forktree_mixed_tracked_untracked_oracle \
  canonical_slatedb_mixed_overlay -- --exact --nocapture --test-threads=1
```

Each backend case must include overlapping and non-overlapping tracked and
untracked identities, global/branch overlay, explicit NULL, tombstone, typed
PK filtering, ordering, projection, post-resolution LIMIT, duplicate and
corruption rejection, cold reopen, and a counter proving one coherent
view/read with no fallback acquisition.

## Exact calibration artifacts

The final test-only source/script hashes before freezing are:

- `scripts/forktree_canonical_mixed_scan_contract_verify.mjs`:
  `9b679619b027a2147ac89b4cd0393cd3005a6dbccc652db59723412ea8410bf0`
- `packages/engine-benchmarks/tests/forktree_mixed_tracked_untracked_oracle.rs`:
  `00c60f10476b3ccd3531ac77cfb6ea10eec69a4f3204c86f6189c35ac1f8f509`

Exact 413 source-gate logs:

- canonical mixed-scan contract: **RED**, SHA-256
  `57a0f960085c53c2b6c77d884d521df3b1346cecc7801310c51b2646653b81a7`
- scoped direct-reader residue gate: **GREEN**, SHA-256
  `ad0c3a60373ce067f699d7dc417391d2347d5ab37557ddf5644d43bb3e662e1b`

The canonical RED is specifically three missing obligations: no complete
`untracked=None` overlay selector/resolver, no tracked/untracked winner
precedence in that path, and no duplicate/conflict fail-closed check. The
one-view count, projection scan count, explicit domain contract, ordering,
typed filter, and provider default request checks pass.

Pure model artifacts:

- warnings-denied test binary SHA-256
  `06be2b24a177b59a240d07b9f448705488e470ddc641022f101b32374bbe912e`
- model log SHA-256
  `435a0a89266598e8a1bfad23fba2b37c0e5b4a02070ccf09a374d60ba77a60e7`
- result: **6/6 GREEN**
- `cargo fmt --all -- --check`: GREEN
- `git diff --check`: GREEN

The package is intentionally not an approval of 413: the pure model proves
the required behavior, while the canonical source gate proves that 413 does
not yet implement it. Adapter runtime is deliberately deferred until a
successor is compiler-green.
