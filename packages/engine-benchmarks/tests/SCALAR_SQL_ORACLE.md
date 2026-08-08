# Stage2 scalar SQL/entity oracle

This is a test/report-only artifact anchored to approved d6b. It does not add a
production reader, storage authority, persisted format, cache, or adapter
implementation.

## Exact provenance

- base: d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
- base tree: 641654079f60fcd1c9ff9ccbbd06d3edcabe4096
- direct parent: 1f742a382c755399b8a49ab536c4f6dc55fffdd8
- parent tree: 860a047b98eaa38368a3d889497628e244c2e0ec
- parent..base full-index binary diff SHA-256: be940f41bac602d9cf5374952f01240f7c5ec2204b513362f3a48cc751ef7a55
- d6b production delta: packages/lix/src/sql2/providers/change.rs only
- d6b production delta purpose: fail closed on missing commit records

The final test-only successor must publish its own exact head/tree, parent,
base, full-index diff, stable patch ID, changed-file list, and clean-worktree
status. No production path may be changed by this package.

## Source gate

Run first, without compiling or widening runtime:

    timeout 1200 bash scripts/stage2_scalar_sql_source_gate.sh

The gate must prove all of the following on the later candidate:

1. No packages/lix/src columnar row-group file, current-layout entity-columnar
   owner, decoded-column cache, or entity-columnar layout remains.
2. No production reference to columnar_row_group, RowGroupManifest,
   RowGroupSetId, RowGroupScalar, EncodedRowGroupSet, row-group loaders/stagers,
   ROW_GROUP symbols, plan_entity_columnar_scan, or EntityColumnarOverlayRow.
3. The scalar SQL provider contains only the authenticated CoherentView path
   and does not retain a columnar branch.
4. The old tracked-head/current-state envelope/tree/storage/codec/scoped-range
   or branch-head-control owners are absent from the affected production path.
5. The canonical reader symbols remain present in ForkTree: CoherentView,
   open_coherent_view or open_coherent_view_on_read, state_point, and
   state_range.

d6b is deliberately expected to fail this gate. The captured d6b failure is
source evidence, not a correctness or performance result.

Calibration on exact d6b:

- command: timeout 1200 bash scripts/stage2_scalar_sql_source_gate.sh
- exit: 1 (expected RED)
- log: /root/repos/evidence/stage2-scalar-sql-d6b/source-gate-d6b.log
- log SHA-256: e0fe2ab8308e98879f0916670469bd25daa12991d2318bc3b780410732e22b42
- observed: four legacy entity-columnar owner paths, legacy symbols in
  live_state, deleted branch/current-layout owners, and the SQL provider's
  entity-columnar branch; ForkTree coherent/point/range symbols were present.

## Canonical semantic contract

The later candidate must expose one authenticated CoherentView for each logical
operation and thread that same read through branch selector, catalog/topology,
authenticated root/internal/leaf, visible state row, and value/BlobRef. A helper
that opens a second StorageRead or reselects the branch is a blocker.

The public oracle covers:

- point reads and bounded ranges with strict ordering and bounds;
- entity projection, residual filter, ordering, and limit through scalar SQL;
- SQL NULL and tombstone distinction;
- INSERT/UPDATE/DELETE RETURNING postimages, including zero-row delete;
- UPSERT DO UPDATE and DO NOTHING plus atomic multi-statement writes;
- divergent branch snapshots and pinned-session identity after switch/reopen;
- exact result digest before flush, after flush/drop, and after cold reopen;
- selector, catalog/topology, internal/leaf/value, and BlobRef/manifest mutation;
- corruption must fail closed, not become an empty result, cache miss, rebuild,
  or alternate-reader success;
- a pure read must perform zero writes.

The small deterministic contract fixture in
forktree_stage2_scalar_sql_acceptance.rs has SHA-256
c9a948fd503d674738d12ad03d88e3506957bb299894f202392fb68ce8eadcde. It is a
logical-result checksum, not an internal storage-object checksum. The later
1K adapter fixture must print an exact canonical-result digest and match across
Memory, RocksDB, and SlateDB after the same seed/mutation sequence.

## Exact focused commands

Every command has a 20-minute cap. Setup/seed time is reported separately from
operation latency. Do not run the adapter matrix when the source gate or
Memory semantic gate fails.

Memory/coherent reader:

    timeout 1200 cargo test -p lix --lib \
      forktree::tests::coherent_state_point_and_range_preserve_overlay_semantics -- --nocapture
    timeout 1200 cargo test -p lix --lib \
      forktree::tests::coherent_open_uses_one_read_and_visited_edges_fail_closed -- --nocapture
    timeout 1200 cargo test -p lix --test integration \
      sql::entity_view -- --nocapture
    timeout 1200 cargo test -p lix --test integration \
      sql::write_returning -- --nocapture
    timeout 1200 cargo test -p lix --test integration \
      sql::delete_returning -- --nocapture
    timeout 1200 cargo test -p lix --test integration \
      sql::lix_branch -- --nocapture
    timeout 1200 cargo test -p lix --test integration \
      sql::untracked_current_state -- --nocapture

RocksDB and SlateDB public semantic controls:

    export CARGO_TARGET_DIR=/tmp/lix-stage2-scalar-sql-target
    timeout 1200 cargo test --release -p lix_benchmarks \
      --features 'storage-benches slatedb' \
      --test forktree_stage2_scalar_sql_acceptance -- --nocapture
    timeout 1200 cargo test --release -p lix_benchmarks \
      --features 'storage-benches slatedb' \
      --test tracked_state_crud_public_result -- --nocapture
    timeout 1200 cargo test --release -p lix_benchmarks \
      --features 'storage-benches slatedb' \
      --test corruption_recovery_qualification -- --nocapture

The focused target must label RocksDB and SlateDB separately and report one
CoherentView acquisition, logical get/scan counts, writes, backend calls and
bytes where available, process CPU, allocations/calls, peak/settled RSS,
settled disk after flush/close, and the canonical digest. It must exercise
point/range/entity SQL, NULL/tombstone, RETURNING, branch snapshots, cold
reopen, and all four corruption substitutions.

The existing tracked_state_crud_public_result target is a secondary public
semantic control. It is not evidence that the old columnar owner survived or
that an OLAP path is accepted.

## Exact rejection expectations

- d6b source gate: RED because legacy production columnar files/symbols remain.
- d6b cargo target: RED at compile before runtime because the approved frontier
  still has inherited live-state/sql2/tracked-state migration diagnostics.
- d6b compile command: CARGO_TARGET_DIR=/tmp/lix-stage2-scalar-sql-d6b-target
  timeout 1200 cargo test --no-run -p lix_benchmarks --features
  'storage-benches slatedb' --test forktree_stage2_scalar_sql_acceptance --
  --nocapture; exit 101, 324 errors, 11 warnings.
- d6b compile log: /root/repos/evidence/stage2-scalar-sql-d6b/
  cargo-test-no-run-d6b.log; SHA-256
  eae4e1b6dc0ab375057850f23275f6a98829af2b06ebf06624f91d0e990fc6a9.
- d6b adapter/runtime cells: NOT RUN; no compile failure may be relabeled as a
  runtime result.
- later candidate source gate: GREEN only with zero forbidden production paths.
- later candidate Memory/RocksDB/SlateDB controls: GREEN only with identical
  logical digests, strict corruption failure, cold-reopen parity, and no read
  writes.
- any missing digest, adapter-specific semantic drift, silent fallback, new
  materialized authority/cache, or old symbol residue: BLOCKER.

## Complexity and ownership

The intended point complexity is O(log_F N plus one visited leaf and authenticated
value/object work); range is O(log_F N plus visited leaves plus output). SQL
projection/filter/order is scalar evaluation over canonical rows. No row-group,
late-materialized, decoder, overlay, or physical co-location optimization is
part of this landing. Such ForkTree-native acceleration is deferred to a
separate post-landing design and owner.
