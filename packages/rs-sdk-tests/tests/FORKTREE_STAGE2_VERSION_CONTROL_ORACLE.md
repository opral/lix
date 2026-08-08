# ForkTree Stage2 version-control acceptance oracle

This test-only oracle binds public Lix version-control semantics to the same
closed `AcceptancePhysicalLayout::{Current,ForkTree}` selector as the accepted
Stage2 SQL DML oracle. It adds no production implementation, parser, model
loop, storage compatibility path, benchmark, or PR.

## Immutable comparators

- Current semantic control: `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`
  / tree `9a705d36392e88d8f5f363b2b23d373deec3321d`.
- Accepted ForkTree Stage1 model: `138b55e1de90806c380ad27b2b349f4c66a1387f`
  / tree `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`.
- Accepted Stage1 independent report SHA-256:
  `b96d2420d157ca3e569e165351ceaa6dcf89270a295c3e0409296fcd34e12f82`.
- Approved SQL/physical-owner contract SHA-256:
  `b4a6479b6883556040db140ddef595ddf5531df58cd92d6aeab8435619b6f6a0`.

The Stage1 model supplies authenticated branch snapshots, commit/change
catalogs, selector publication, checkpoints, undo/redo change kinds, branch
tombstones, and bounded GC root packs. The public trace below is the required
end-to-end binding of those accepted model outcomes to current Lix semantics.

## Scope

Each adapter test runs separate fresh Current and ForkTree repositories over
one deterministic 1,000-row typed fixture. It covers:

- branch create, switch, merge, and delete;
- divergent disjoint edits and a published merge commit;
- true same-identity conflict preview and fail-closed merge;
- exact cold-reopen `lix_diff` and typed history;
- undo and redo;
- checkpoint, flush, complete handle drop, reopen, and selector recovery;
- stale same-owner rejection and stale unrelated-owner success;
- production GC retaining one sibling commit through an explicit branch root,
  plus an isolated fresh-repository sibling deletion proving final public
  commit-fact retirement.

The oracle calls the existing `storage-benches`
`collect_repository_gc_for_bench` as a deterministic completion barrier. That
helper invokes the single production repository-GC planner and commit path; it
is not a second collector or model implementation. Stage2 must route that
existing helper through its sole physical owner.

Retention and final release use separate fresh 1K repositories. This is
deliberate: exact `a12` has the separately owned retired-checkpoint-root
composition defect when a second sweep follows a prior sweep. The oracle does
not hide or duplicate that bug; each required reachability contract is proven
with one valid production sweep.

The 1K fixture uses one ordinary checkpoint solely for flush/drop/recovery;
GC uses sibling branch roots and deliberately does not reproduce H3's
three-row checkpoint oracle. No files, binary payloads, uploads, or multimedia
paths overlap H4.

Frozen exact-current artifacts on both RocksDB and SlateDB:

- semantic result digest:
  `98f32ba6e147d8c2f8bb88c691aa92cfc5de149e2d23fb439ee79fec4fdeb791`;
- final logical-state digest:
  `fda3c0c062441132e70e594714fb00bb274dab48838afe268e5349e3e68d0839`.

## Required cfg-only SPI

Exact `a12` is intentionally compile-red only until Stage2 provides:

```rust
use lix::integration::AcceptancePhysicalLayout;

open_lix()
    .with_storage(storage)
    .with_acceptance_physical_layout(AcceptancePhysicalLayout::ForkTree)
    .await
```

The selector is immutable before initialization and may only choose the closed
transaction physical owner. It cannot be a SQL/provider hook, persisted layout
marker, environment switch, fallback, or second authority.

## Exact baseline evidence

The final source was compiled on exact `a12` with:

```text
CARGO_TARGET_DIR=/root/repos/lix/target CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage2_version_control --no-run
```

It fails only because `lix::integration::AcceptancePhysicalLayout` and
`OpenLixBuilder::with_acceptance_physical_layout` do not yet exist. The full
compiler log SHA-256 is
`bbe8e65b5e1f77a1d2dd0da26d92909c3c66c52fb2c6c9a08c1e942c9317eabb`.

Before restoring those intentional red references, a temporary test-local
identity shim routed both variants through the exact current physical owner.
It did not change production source and is absent from the frozen test. Both
complete adapter controls passed:

- RocksDB log:
  `5da304c638cc1317e7ba380e1a3f5cff5bb7dc0c7f7473debf0f9dd45bef995e`;
- SlateDB log:
  `63129e83c9339e51e72b8d163285f280c74abf7d43191da77ec170a8061c6cc1`;
- semantic-smoke executable:
  `f3142bf0f1127d1cbbdab380ce624034a4b2a9882e8929a002a05907a8d10c2a`.

The frozen oracle source SHA-256 before this report was finalized is
`c120b5ca3ab20acb67dd472791e281af03bffb39e280a5b4d28212337e42a6f9`.

## Acceptance commands after Stage2 wiring

```text
RUST_MIN_STACK=8388608 CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage2_version_control forktree_stage2_version_control_rocksdb -- --exact --nocapture --test-threads=1
RUST_MIN_STACK=8388608 CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage2_version_control forktree_stage2_version_control_slatedb -- --exact --nocapture --test-threads=1
```

`RUST_MIN_STACK=8388608` is the repository's canonical CI configuration, not
a layout selector or production runtime change. Both commands must pass
unchanged with exact public artifact equality and frozen semantic/final hashes.
