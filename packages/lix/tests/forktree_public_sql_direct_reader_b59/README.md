# Public SQL direct-reader / columnar acceptance package

Status: test/report-only and source-gated. This package is rebased to the
accepted ForkTree frontier below. It contains no production change, no SQL
provider change, no adapter change, no compatibility reader, and no runtime
claim. The durable commands are intentionally dormant until a compile-green
successor is supplied.

## Immutable binding

The exact accepted target is:

```text
base/head:  b59e1f11a51153e0a787a81f0f25bf104d150aaf
tree:       700fd04d21bc40c05425c9fc9e10d65c9e1eda24
parent:     713455a3557907ce705d06f720fcdc4486bddd4a
```

The semantic oracle was rebased from
`origin/codex/forktree-public-sql-acceptance-oracle` at
`1d3d87ac506f670e0b28bb9c87c1f399c0900911`, tree
`118fe79888f2df061a06067542ce003c205f8625`.

No b59 production file is modified by this package. The only intended
successor changes are files below this directory.

## Current reader inventory on b59

The inventory distinguishes the public SQL reader closure from deferred
physical writers/GC. A future cut must not pretend that deleting a public
reader token also deletes these other owners.

| Surface | Current b59 owner | Role | Smallest-cut disposition |
| --- | --- | --- | --- |
| Public snapshot/PK entry | `sql2/providers/entity.rs:EntitySpec::plan_scan` | Chooses direct snapshot/PK reader, then decodes projection | Retain provider semantics; direct branches delegate only to ForkTree reader |
| Snapshot/PK trait | `sql2/entity_batch.rs:EntitySnapshotReader` and `CurrentEntitySnapshotReader` | Terminal SQL capability | Retain API; make its sole source `LiveStateReader` ForkTree path |
| SQL session wiring | `session/context.rs:entity_snapshot_reader` | Constructs the terminal reader | Retain wiring; no alternate tracked/columnar reader |
| Direct read facade | `live_state/context.rs:scan_forktree_operation`, `load_exact_batch` | Opens a ForkTree view and forwards point/range requests | Retain only as a thin delegation boundary; no `TrackedHead` read in this closure |
| Authenticated reads | `live_state/forktree_reader.rs:scan_view`, `scan_tracked_view`, `scan_combined_view`, `load_exact_batch` | `state_range`/`state_point`, overlay and tombstone resolution | Sole current-state read owner |
| Public overlay | `live_state/visibility.rs:overlay_*`, `resolve_rows` | Staged values plus local/global precedence | Retain; must not be reimplemented by row groups |
| Projection | `sql2/entity_projection.rs:EntityProjectionDecoder` | Projection after canonical identity/overlay ordering | Retain |
| Typed identity | `entity_pk.rs:EntityPk` and `sql2/catalog/entity_surface.rs` | PK arity/kind/schema authority | Retain |
| Legacy context closure | `live_state/context.rs:TrackedHeadContext`, `BranchHeadControl`, `HotStateTransactionCache` | Generation/control and other legacy paths | Exclude from direct SQL closure now; delete in the separate TrackedHead/BranchHead hard cut |
| Columnar layout | `sql2/entity_columnar_layout.rs`, `live_state/entity_columnar.rs` | Row-group encoding/write metadata | Deferred physical owner; never a public SQL read authority |
| Columnar write | `sql2/exec/bound_public_write.rs:encode_*row_groups` | Legacy write/materialization path | Deferred W2/W3/W4 deletion; must not be imported by direct readers |
| GC row groups | `gc.rs` row-group set/manifest load, retention, delete | Physical retention/sweep owner | Deferred GC/physical deletion; not a SQL-reader residue in this package |
| Lifecycle support | `session/execute.rs` row-group assertions/corruption tests | Tests/support for old physical lifecycle | Deferred with physical owner; not a serving path |

The exact b59 reader call shape is `EntitySpec::plan_scan` →
`EntitySnapshotReader` → `LiveStateReader::scan_batch` or
`load_exact_batch` → `ForkTreeReadFacade` → one coherent view →
`state_point`/`state_range`. The non-reader paths above are deliberately
listed so a future source gate can report them rather than silently treating
them as a second SQL authority.

## Smallest future migration cut

1. Keep one `CoherentView`/one retained `StorageRead` for each public request.
   The direct snapshot and direct primary-key readers must authenticate the
   selected global/branch pair and every state object through that same read.
2. Keep `EntityPk`, schema surface validation, visibility overlay, projection,
   and canonical ordering as semantic layers above ForkTree. Order by typed
   identity before projection and `LIMIT`.
3. Remove any direct row-group planner, manifest/ID lookup, columnar overlay,
   decoded column cache, or row-group import from the SQL reader closure. The
   token `plan_direct_entity_columnar_scan` is forbidden even though it is not
   present in b59; the prohibition prevents reintroduction by the successor.
4. Keep row-group encoding, writer, and GC ownership in their explicit
   deferred cuts. They cannot serve a row, resolve reachability, or provide a
   fallback if a ForkTree object is missing.
5. After the public reader closure is compiler-clean, perform the separate
   TrackedHead/BranchHead/physical deletion waves. This package does not claim
   those owners are already deletable on b59.

The cut is rejected if it adds a second state root, a cache used as authority,
a compatibility reader, a row-group fallback, a raw physical lookup, or a
helper that silently opens another read snapshot.

## Required semantics

The pure model in `forktree_public_sql_direct_reader_oracle.rs` is the
rebased semantic contract. It requires:

* typed `EntityPk` arity and component kinds (text, integer, UUID), with
  canonical lowercase UUID parsing for both stored and requested PKs;
* local branch values overriding global values by identity;
* local tombstones hiding global values, with tombstones returned only when
  requested; `NULL` is a value and never a tombstone;
* canonical identity ordering before projection and `LIMIT`, with requested
  projection order preserved;
* exact typed PK filters, schema/branch binding, and no string-lookalike
  coercion;
* malformed/missing/wrong-kind rows, missing schema/branch, invalid
  projection, and conflicting same-owner duplicates failing closed; and
* equality with an independent sorted reference for full, projected, limited,
  tombstone-inclusive, and exact-PK shapes. The reference is a semantic test
  oracle only, not a permitted production authority.

## Source gate contract

`verify_direct_reader_source.sh` is a future-candidate gate. It is
path-aware: it scans the direct SQL reader closure strictly, including the
direct methods in `live_state/context.rs` at function scope while leaving its
separate legacy helpers deferred. It reports deferred writer/GC row-group
symbols separately and rejects any direct fallback or second authority. It
requires the ForkTree boundary symbols and rejects:

* `TrackedHeadContext`, `TrackedStateStoreReader`, and legacy tracked-head or
  branch-control acquisition in the direct SQL methods and their snapshot/PK
  callers;
* `plan_direct_entity_columnar_scan`, `EntityColumnar*` serving owners,
  `RowGroupManifest`, `RowGroupSetId`, `ColumnarRowGroup`, row-group manifest
  lookup, and columnar imports in reader/provider paths;
* independent `begin_read`/snapshot refresh in direct helper paths;
* a compatibility/fallback/rebuild/cache authority; and
* missing `EntitySnapshotReader`, `EntityPk`, `state_point`, `state_range`,
  and `ForkTreeReadFacade` boundary tokens.

On exact b59 the direct-closure result may be green while the report lists
legacy context, columnar writers, and GC physical owners as deferred. That is
not a runtime result and is not a claim that deferred physical owners are
already removable.

## Future execution order

Use the exact commands in `FUTURE_GATE_COMMANDS.md`, with a fresh isolated
target and fresh adapter paths. The order is source gate, formatting/diff,
standalone model, package no-run, Memory, RocksDB, then SlateDB. Each adapter
cell is capped at 20 minutes and stops on the first focused blocker. Durable
green requires identical public rows/errors/order/bytes, zero read-side
writes/commits, flush/drop/reopen, and fail-closed malformed/missing/wrong-
kind objects. No current-main comparator, scaling, or multimedia prerequisite
is part of this minimum SQL reader package.

No source gate, compiler, or adapter result is claimed in this freeze; only
the immutable source inventory and dormant acceptance contract are frozen.
