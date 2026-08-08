# W3 e1af structural and GC contract oracle

Status: **TEST/REPORT-ONLY CORRECTION FROZEN**. This package directly embeds
the complete W3 map and diagnostics and adds executable source-structure and
pure GC model gates. It makes no production change and has no compiler,
adapter, or production-runtime result.

## Immutable anchor

- source head: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- source tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- parent tree: `4477c83b246bddac09cd972564bd4ccd67f90f7b`
- source full-index diff: `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c`
- source patch ID: `31cc575644bf17e65c59d558a03acffc848c2e20`

The package embeds the complete 14-cluster map in
`W3_B484_READINESS_MAP.md` and the complete diagnostic rows in
`DIAGNOSTICS.tsv`; they are not external dependencies. Their embedded hashes
are `d9a6653f5f5f62e476d7dac10a7bcb5377d0642d9365cbd330c13e778841e471` and
`1d6cb84157c64eed06d5e4a3cc6925b645fd2ddab2c28a901a774aaf55d49126`.

## Structural one-operation contract

`w3_structural_gate.py` checks an operation shape containing exactly:

```text
one caller-owned begin_read
  -> one CoherentView / one PreparedPublication
  -> one prepare_write_set
  -> one prepared_commit.commit
```

It runs three negative fixtures and requires each to be rejected: a second
read acquisition, a second publication object, and a second commit. It also
checks the embedded 14-row diagnostics, exact e1af identity, required owner
tokens, and the expected e1af legacy-residue calibration. The current e1af
source is intentionally RED; the gate's successful exit means the RED control
and negative fixtures behaved as expected, not that production is runnable.

## Pure 65-entry GC model

`w3_65_gc_model.py` is independent Python model code. It proves:

- a 64-entry prefix followed by a one-entry suffix drains 65 entries;
- a blocked head emits exactly one debt, does not advance or spin, and does
  not reclaim the suffix prematurely;
- releasing the safe-point debt advances once, drains, and clears the debt;
- a further call is an idempotent drained no-op; and
- the one epoch/progress fence changes only on an advancing GC publication.

The model is not a production runtime and was run only as a standalone pure
model gate.

## Preserved owner and boundary contract

W3 still targets the two authenticated planes `OBJECT_SPACE` and
`SELECTOR_SPACE`, one coherent retained read, exact epoch/progress/owner CAS,
H/S/C chronology-serving separation, reader pins, open-upload roots, shared
and final references, corruption/cold-reopen fail-closed behavior, and the
exact W4 root handoff. W5 owns bounded persisted queue/mark/continuation,
safe-point waiting, sweep, and final-reference reclamation.

H4's cross-boundary audit is explicit: legacy binary-CAS and media-upload
authorities remain RED on e1af, and `advance_gc`, `abort_corrupt_gc`, and
internal `commit_progress` are deferred W5 owner operations. Their presence
does not satisfy the hard cut and no second CAS/upload authority is accepted.

## Dormant first-runnable commands

Every cell is fresh and capped at 1200 seconds; stop at the first blocker.
These commands were not run here:

```sh
# static source/deletion gate, then formatting
python3 test-report/forktree-w3-e1af-structural-oracle/w3_structural_gate.py <candidate-root>
cargo fmt --all -- --check
git diff --check <exact-base>..<exact-head>

# pure/Memory contract target
CARGO_TARGET_DIR=<memory-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_benchmarks --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,rocksdb,slatedb -- \
  --nocapture --test-threads=1

# identical semantic target on RocksDB
FORKTREE_W5_R7_BACKEND=rocksdb CARGO_TARGET_DIR=<rocks-target> \
  CARGO_BUILD_JOBS=2 timeout 1200 cargo test -p lix_benchmarks \
  --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,rocksdb,slatedb -- \
  --nocapture --test-threads=1

# identical semantic target on SlateDB
FORKTREE_W5_R7_BACKEND=slatedb CARGO_TARGET_DIR=<slate-target> \
  CARGO_BUILD_JOBS=2 timeout 1200 cargo test -p lix_benchmarks \
  --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,rocksdb,slatedb -- \
  --nocapture --test-threads=1
```
