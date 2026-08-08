# ForkTree historical-correction acceptance package

This is a test/report-only package for immutable production baseline
`cd91b9b90f7f468158b4df154adbed9551eb5d60`. It contains no Cargo target,
production edit, adapter invocation, or compatibility path.

The package makes the narrow successor contract executable at source level:

1. SQL checkpoint, checkpoint creation, filesystem checkpoint working diff,
   and ordinary working diff borrow the exact caller-owned
   `HistoryQuerySource.forktree_reader`/retained ForkTree view identity.
2. Local `ForkTreeReadFacade::new`, `begin_read`, branch-head controls,
   `TrackedHeadContext`, fresh `TrackedStateStoreReader`, and
   `TrackedStateContext::diff_commits` fallback are forbidden in those paths.
3. Historical tombstones remain deletion events. Required value rows,
   descriptors, plugin rows, BlobRefs, and blob payloads fail closed when
   missing, malformed, wrong-kind, duplicated, substituted, or unavailable.
4. A single retained view produces reads only: zero plans, writes, commits, or
   selector/epoch rotations for the read oracle.

The pure model is dependency-free and is not registered with Cargo. The
source gate is calibrated RED against this baseline because the baseline still
contains the legacy paths described above.

## Frozen provenance

```text
baseline/head: cd91b9b90f7f468158b4df154adbed9551eb5d60
tree:          5ad2a0c8399971d6803e096fd228c5a6149e06ee
parent:        47957d30ae7c16c89c3c523feea23e2f98461fed
```

## Static command

From a clean candidate checkout, with no build or runtime:

```text
bash test-reports/forktree-historical-correction-cd91/source_gate.sh .
```

The exact baseline calibration is recorded in `CD91_RED_CALIBRATION.log` and
must remain RED until the direct successor removes every listed residue. A
future candidate must retain `cd91b9b9` in its ancestry and must not override
the pinned baseline.

## Future runtime order

This package does not run it. Once a successor is compile-green, the owner may
apply the pure model first, then execute one warmed Memory case, one RocksDB
case, and one SlateDB case, each with a fresh read view and a 20-minute cap.
No broad benchmark or current-main comparator claim is implied.
