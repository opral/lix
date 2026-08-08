# ForkTree W0 StorageSpace/type-boundary oracle

Status: test/report-only acceptance package. It makes no production, SQL,
Stage-2, adapter, or PR change. Its purpose is to freeze the compiler/source
contract that must be satisfied before the W0 production hard cut is attempted.

## Immutable anchor

The oracle is based on Cut B `e92ea2e505ee3d96abbb529dbaedb23d4908ff42`, whose
parent is corrected d6 `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768`. The d6 tree is
`641654079f60fcd1c9ff9ccbbd06d3edcabe4096`; Cut B tree is
`0d0797c024706beb1510cb2f0f88f8414a9a0c96`. The accepted W2 predecessor is
`0a1955269c0d1fd5d23bac24f0a35f4e9a51d687`; this package does not alter it.

The package freezes the following retained boundary, not an implementation:

* `StorageSpace` is a sealed engine-declared descriptor containing a private
  `SpaceId`, name, and `ValueSemantics`. Only Lix engine declarations may call
  `engine_declared`; adapters cannot forge IDs or construct mutable/immutable
  spaces.
* The ForkTree object domain is the only physical object authority:
  `ObjectId`, `ObjectDomain`, `CoherentView`, and the engine descriptors
  `OBJECT_SPACE` (immutable objects), `SELECTOR_SPACE` (mutable selector), and
  `UNTRACKED_ROW_SPACE` (mutable untracked rows). RocksDB and SlateDB receive
  opaque descriptors through the existing Storage APIs; they do not own a
  registry, raw space, alternate index, or second authority.
* Existing public `StorageRead` streaming semantics remain: one coherent view,
  ordered point/range reads, cursor continuation bound to that view, reopen and
  corruption errors, and no writes during reads. The model oracle exercises the
  same contract for Memory, RocksDB, and SlateDB labels; durable adapter runs
  are not claimed until the anchored production crate is compile-green.

## Hard-cut deletion map

The production successor must remove, rather than alias or deprecate:

| Area | Exact residue to delete | Replacement/authority |
|---|---|---|
| `packages/lix/src/storage/types.rs` | `StorageSpace::mutable`, `StorageSpace::immutable`, `StorageSpace::new`, raw `SpaceId(...)`/`StorageSpaceId(...)` construction and public exports | sealed `engine_declared` descriptors |
| columnar owners | `columnar_row_group.rs`; `live_state/entity_columnar.rs`, `entity_columnar_cache.rs`, `entity_decoded_column_cache.rs`; `sql2/entity_batch.rs`, `sql2/entity_columnar_layout.rs`; imports such as `EntityColumnar*`, `ColumnarBaseCoordinate`, row-group spaces | ForkTree `ObjectDomain`/`ObjectId` state objects |
| tracked physical owners | `tracked_state/codec.rs`, `storage.rs`, `tree.rs`; `TrackedStateStoreReader`, scan/filter/column readers | ForkTree state objects and one `CoherentView` |
| changelog physical owners | `COMMIT_SPACE`, `CHANGE_SPACE`, `COMMIT_CHANGE_ID_SPACE`, legacy change-record loaders and physical scans | ForkTree commit/change catalog objects; commit graph remains chronology authority |
| binary-CAS legacy owners | `binary_cas/kv.rs`, old manifest/chunk/presence modules and their raw owner symbols | W2 `BlobRef`/`ObjectId` domain only |

No scalar SQL implementation is in scope. Parser, binder, DML, transaction,
RETURNING, and public cursor semantics are inputs to this boundary, not new
owners here.

## Source/error contract

The residue verifier scans all production source under `packages/lix/src` and
rejects the legacy files/symbols above. It also requires the retained boundary
tokens. The negative probes in
`forktree_w0_compile_probes/` make the intended compiler failures explicit:
raw space forging, columnar-owner imports, tracked/changelog imports, and the
removed native filesystem export names must be unnameable. The positive probe
uses only the opaque object-domain/read-view boundary.

Missing/malformed descriptors, wrong value semantics, forged/raw spaces,
missing selected roots, malformed object bytes, invalid range/cursor state,
wrong view, stale cursor, and deleted columnar-owner access must fail closed.
There is no compatibility registry, fallback reader, dual writer, or raw-space
authority. The pure model also checks NULL/tombstone-style absence as an
ordinary value outcome rather than a physical-space escape hatch.

## Exact controls and order

Run from an isolated checkout of the exact candidate, with one target directory
per candidate and `CARGO_BUILD_JOBS=2`:

```sh
node scripts/forktree_w0_storage_boundary_residue_verify.mjs --root "$PWD"
cargo fmt --all -- --check
git diff --check
cargo clippy -p lix_benchmarks --test forktree_w0_storage_boundary_oracle -- -D warnings
cargo test -p lix_benchmarks --test forktree_w0_storage_boundary_oracle --no-run
cargo test -p lix_benchmarks --test forktree_w0_storage_boundary_oracle -- --nocapture --test-threads=1
W0_BACKEND=memory  cargo test -p lix_benchmarks --test forktree_w0_storage_boundary_oracle -- --nocapture --test-threads=1
W0_BACKEND=rocksdb cargo test -p lix_benchmarks --test forktree_w0_storage_boundary_oracle -- --nocapture --test-threads=1
W0_BACKEND=slatedb cargo test -p lix_benchmarks --test forktree_w0_storage_boundary_oracle -- --nocapture --test-threads=1
```

The model test is storage-independent and must pass for all three labels. A
candidate's real adapter controls must add flush/drop/reopen for RocksDB and
SlateDB, repeat point/range/cursor reads, assert exact ordering and bytes,
assert zero writes during reads, and run malformed descriptor/root/object
controls. The first production compiler/no-run gate precedes all durable
runtime cells; every cell is capped at 20 minutes. Do not broaden the matrix
from Memory until the static and compile gates are green.

## Expected compiler reduction

This package does not invent a numeric reduction. Baseline d6/Cut B is
deliberately calibrated red: it still contains legacy raw constructors and
columnar/tracked/changelog owners. The verifier records the exact path/line
residue count and hash. A W0 candidate must rerun `cargo check -p lix --lib
--all-features`, warnings-denied Clippy, and the verifier; the acceptance claim
is removal of those diagnostics/owners, not a source-only count substituted for
compiler evidence. Any remaining old constructor/type/import diagnostic is a
blocker. Record compiler wall time and peak RSS only as supporting evidence.

The model is intentionally limited to public semantics: descriptor identity and
value semantics, opaque object-domain reads, point/range streaming and cursor
binding, reopen, missing/corrupt roots, and no write amplification. It is not a
performance claim and does not authorize a scalar SQL path.

## Exact Cut B calibration

The static verifier was run against the exact Cut B worktree with:

```text
node scripts/forktree_w0_storage_boundary_residue_verify.mjs --root "$PWD"
```

It found no missing retained-boundary token and 565 forbidden residues. The
complete 571-line output is captured as
`/tmp/w0-e92-residue.log`, SHA-256
`57ea9499e868c2611e892774e04175388a8aa6b8eba5ba99b41bfad10dc58e9c`.
This red result is the expected pre-cut comparator: the source still contains
raw constructors, columnar owners, tracked/changelog readers, and old binary
CAS owners. It is not evidence against the W0 oracle itself.

The pure model was compiled with warnings denied and ran 5/5 tests green. The
test executable SHA-256 is
`7d6d5f8cc646e21e3521b150950514ff1d1759f98f5d3d90c2cf2d9524b133e7`.

The exact package no-run gate was then attempted once in an isolated target
with `CARGO_BUILD_JOBS=2`. It terminated at 8:44.60 with exit 101 before
linking the oracle: `lix` emitted 190 errors and 7 warnings, including
unresolved legacy tracked/changelog/columnar imports and private/raw `SpaceId`
field/constructor accesses. `/root/repos/lix-w0-storage-boundary-oracle/evidence/cut-b-package-no-run.log`
is 94,600 bytes, SHA-256
`4539f7b4672df47c08f939a18d86732364749b214f2a271ff6eb3e41308e5232`;
`/usr/bin/time -v` recorded 1,620,648 KiB maximum RSS, 943.61 seconds user
CPU, 40.97 seconds system CPU, and 4,385 seconds wall time. This is an
inherited Cut B compile blocker and prevents any honest Memory/RocksDB/SlateDB
runtime claim. No adapter runtime cell was started, and no production source
was changed.
