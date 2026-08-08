# W5/R7 GC and reachability oracle — e1af structural correction

Status: **TEST/REPORT-ONLY EXPECTED-RED CORRECTION**. This is a direct
successor to the immutable `5850b4b9a40540dd027ef95a3b1139f262efd76d` package.
No production source, Cargo, adapter, runtime, benchmark, PR, or merge action
is included.

## Exact source anchor

- source commit: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- source tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- parent tree: `4477c83b246bddac09cd972564bd4ccd67f90f7b`
- source parent-to-head full-index diff SHA-256:
  `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c`
- source parent-to-head patch ID: `31cc575644bf17e65c59d558a03acffc848c2e20`

The e1af source delta is exactly the two b484 historical/file working-diff
provider paths. This rebind does not modify them.

## Preserved W5/R7 oracle identity

The existing oracle remains the authoritative test/report package, unchanged:

- ref/head: `origin/codex/forktree-w5-r7-gc-reachability-oracle` /
  `6487170dfa11b24411dbbd73e3c003439072df09`
- tree: `94eefb7de3260a8c8a3217805a5372cb8670157c`
- parent/base: `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768`
- parent-to-head full-index diff SHA-256:
  `b12d49fbb8f991459ca9a9e6513f26f392ce642c9b25e95efc1be44ecb166345`
- stable patch ID: `3b8ef7eeec6cb3b6edbc5f5b1d5226f79615a247`
- external report SHA-256: `fd47899844bafc72fb47c254f77c74b91d4d40f43d0bb2a54d043823892b6a35`
- external manifest SHA-256: `ea5a278b81d23136e276b29e350752b8c25ce656ba375864362fc2ab0d60ee4c`

The five original artifact hashes are recorded in
`ORIGINAL_ARTIFACT_HASHES.tsv`; the oracle bytes are not rewritten here.

## e1af expected-RED calibration

The original deterministic residue verifier was run without building or
executing production code against a clean e1af worktree. It returned:

```text
RED 168 forbidden production residues
```

Raw output: `SOURCE_RED.log`, SHA-256
`da2df9406124f627f28f53bb37dc7d3216dc2396ffadeccf68199ac95c56f846`.

This RED is required. H4's cross-boundary audit confirms that e1af still has
legacy CAS/upload authorities in the binary-CAS and media-upload paths, while
W5's `advance_gc`, `abort_corrupt_gc`, and internal `commit_progress` remain
deferred owner operations. The rebind must not misclassify those deferred
seams as completed merely because ForkTree reachability symbols exist.

The expected-RED boundary includes old CAS manifest/chunk/presence or upload
receipt writers/readers, old GC/recovery/tree-sweep spaces and codecs, raw
storage-space construction, and any independent root or progress writer. The
sole accepted destination is the two-plane ForkTree owner:

- `OBJECT_SPACE` for immutable authenticated objects and root edges;
- `SELECTOR_SPACE` for typed selectors and the epoch/progress fence.

## Semantic contract preserved

The pure W5/R7 model and future adapter gate must preserve:

- one coherent retained read for selector, root, queue, mark, upload, and
  object traversal;
- exact epoch/progress/owner CAS, with publication-first and GC-first stale
  work rejected and no partial publication;
- 64 prefix entries plus the suffix (65 total), one blocked debt token, no
  retry spin, `advanced=false/drained=false` while blocked, and release only
  after the safe point advances;
- checkpoint chronology `[S first-parent H, serving C]`, with no permanent
  checkpoint-to-H edge and no later-diverged-main bridge;
- reader pins, open uploads, shared objects, final-reference release,
  malformed/missing/cyclic/wrong-kind fail-closed validation, and cold reopen;
- W4's exact selector/root handoff; W5 owns bounded persisted queue/mark/
  continuation, safe-point waiting, sweep, and final reclamation.

No second root authority, compatibility reader, fallback, cache, raw
`StorageSpace` forge, or independent writer is accepted.

## Strengthened model and candidate-parametric source gate

`w5_r7_e1af_readiness_model.rs` is the executable model for the selector,
authenticated object graph, transitive H/S/C roots, owner-bound fence,
authenticated queue identity/order, owner+view-scoped pins, root owners,
atomic publication/GC state, and cold-reopen controls. Corruption is bound to
object and queue fingerprints; failed graph, queue, fence, or root validation
must leave the complete staged state unchanged. It is independent of Lix
production and must compile with warnings denied before any adapter gate.

```sh
timeout 1200 rustc --edition=2021 --test -D warnings \
  test-report/forktree-w5-r7-e1af-rebind/w5_r7_e1af_readiness_model.rs \
  -o /tmp/w5-r7-e1af-readiness-model
timeout 1200 /tmp/w5-r7-e1af-readiness-model --nocapture --test-threads=1
```

The verifier is cwd-independent and takes a candidate target and exact e1af
anchor. It now performs a candidate-parametric structural check over the full
allowed production closure, rejects changed paths outside that closure, and
extracts operation bodies to require:

- typed `OBJECT_SPACE` and `SELECTOR_SPACE` declarations plus the ForkTree
  `CoherentView` and `PreparedPublication` owners;
- exactly one retained coherent-read construction per publication/GC operation,
  with selector, queue, mark, upload, and object use through that read;
- exactly one `into_storage_plan`, one transaction `prepare_write_set`, one
  transaction `.commit`, and no direct `PreparedPublication::commit`;
- exact positional owner/epoch/progress/selector CAS arguments; and
- rejection of fallback/cache/alternate/second-reader aliases and second
  authority declarations.

The structural fixtures include one genuine GREEN operation and three
discriminating RED cases for a second read, a second writer, and a fallback
reader alias:

```sh
timeout 1200 bash test-report/forktree-w5-r7-e1af-rebind/verify_e1af_rebind.sh \
  <repo-root> <candidate-or-e1af> e1af471b9ab0f598dafa7c2ddec7867667c81740

timeout 1200 bash test-report/forktree-w5-r7-e1af-rebind/run_structural_fixtures.sh
```

The baseline `SOURCE_RED.log` remains bound to the exact e1af residue count;
it is not used to mask candidate results. The old controls remain exact: e1af
prints RED 168 and the unchanged 5850 candidate prints RED 384.

## Dormant first-runnable order

Every cell is independently capped at 1200 seconds and stops on the first
blocker. These commands are dormant until a compile-green immutable candidate:

```sh
# source/residue, format, and diff gates first
node scripts/forktree_w5_r7_residue_verify.mjs --root <exact-candidate-root>
cargo fmt --all -- --check
git diff --check <exact-base>..<exact-head>

# Memory, then RocksDB, then SlateDB with the same semantic target
FORKTREE_W5_R7_BACKEND=memory \
  timeout 1200 cargo test -p lix_benchmarks \
  --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,rocksdb,slatedb \
  forktree_stage2_gc_publication_acceptance -- --exact --nocapture
FORKTREE_W5_R7_BACKEND=rocksdb \
  timeout 1200 cargo test -p lix_benchmarks \
  --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,rocksdb,slatedb \
  forktree_stage2_gc_publication_acceptance -- --exact --nocapture
FORKTREE_W5_R7_BACKEND=slatedb \
  timeout 1200 cargo test -p lix_benchmarks \
  --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,rocksdb,slatedb \
  forktree_stage2_gc_publication_acceptance -- --exact --nocapture
```

No production Cargo, adapter, or benchmark command is claimed here. The
standalone model and source gate are the only readiness controls executed for
this correction.
