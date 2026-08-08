# Calibrated e166 control report

## Provenance

| item | value |
|---|---|
| worktree | `/root/repos/lix-direct-reader-oracle-e166` |
| anchor commit | `e1666edd0b4d814a88d985086ecc5a477b5d32e6` |
| anchor tree | `c680bd7e7f7b70cd784676515839af2dcbbc7917` |
| anchor parent | `3def82e48ed74ab3d914867767e3bf06def3ffc2` |
| calibration package commit | `a0faa378ba4cfcc7daa95174abf31fe05d17bec0` |
| calibration package tree | `84f214877b88fb1d706da4b19b0fadb453354ebd` |
| mode | source/report-only; no Cargo build or runtime |
| command | `bash test-reports/direct-sql-reader-e166/verify_direct_reader.sh . control` |
| stdout/stderr | stdout below; stderr empty on exit 0 |
| path normalization | verifier resolves its worktree argument with `cd`/`pwd`; source paths are repository-relative Git paths |

## Result

The control is intentionally RED for the direct-reader migration. Eleven
positive source predicates pass, all five expected competing-owner predicates
are RED, and no verifier predicate fails. Exit status is 0 because `control`
mode accepts exact e166 or its test-only descendant and treats those five REDs
as the calibrated e166 result. The output below records the first frozen
test-only calibration commit; later package-only metadata commits preserve
the same source result. The package changes no `packages/` path.

```text
PASS	anchor	HEAD=a0faa378ba4cfcc7daa95174abf31fe05d17bec0 TREE=84f214877b88fb1d706da4b19b0fadb453354ebd
PASS	coherent_view	forktree_reader uses caller-provided read and open_coherent_view_on_read; context routes through ForkTreeReadFacade
PASS	no_raw_read_getter	direct reader/context/entity adapter has no public raw-read, storage_read, or begin_read helper
PASS	overlay_order	serving merges global_state_root/local_state_root with branch precedence and ordered local_key comparison
PASS	entity_filter_before_limit	scan_view filters entity_pks before output.push and output limit
PASS	null_preserved	StateCell::Null and Tombstone map to None while deleted() distinguishes SQL NULL from delete
PASS	tombstone_policy	tombstones are filtered unless include_tombstones is requested, including exact point reads
PASS	decode_auth_fail_closed	view/range/point/key decode errors use ? and direct reader has no unwrap/expect
PASS	untracked_same_view	explicit untracked requests use scan_untracked_view/scan_untracked_rows on the supplied coherent view
PASS	unsupported_fail_before_view	derived/history are rejected by validate_scan_request and ambiguous branch/row lanes reject before output
RED	no_tracked_head_owner	context/SQL reader still names tracked-head or TrackedState owners
RED	no_columnar_owner	context/SQL live-state reader still owns or reaches durable EntityColumnar/EntityDecoded paths
RED	no_current_state_cache	context/SQL reader still owns or invokes entity snapshot/columnar caches
RED	no_fallback	reader trait/direct SQL sources still expose a fallback or direct fallback owner
RED	no_raw_state_shortcut	SQL entity adapter still calls scan_direct_* or plan_direct_*
PASS	no_write_side_effect	reader-only sources contain no write-set, StorageWrite, commit, or write call
SUMMARY	mode=control	pass=11	red=5	fail=0	head=a0faa378ba4cfcc7daa95174abf31fe05d17bec0	tree=84f214877b88fb1d706da4b19b0fadb453354ebd
```

## Causal source findings

The e166 source has the intended ForkTree path, but it has not completed the
compiler-driven owner deletion:

- `packages/lix/src/live_state/context.rs:15,22,295-323,411-415` retains
  `TrackedHeadContext`, `TrackedStateContext`, and entity point/columnar cache
  fields in the live-state context and reader.
- `packages/lix/src/live_state/context.rs:449-599` exposes
  `scan_direct_entity_snapshots`, `scan_direct_entity_primary_keys`, and
  `plan_direct_entity_columnar_scan`; these query tracked-head/columnar
  owners and cache results instead of resolving ForkTree state rows.
- `packages/lix/src/sql2/entity_batch.rs:156-214` owns/borrows columnar caches
  and dispatches the SQL entity reader to those `scan_direct_*` and
  `plan_direct_*` methods.
- `packages/lix/src/live_state/reader.rs:77-87` still documents and provides
  the generic scan fallback. The direct SQL migration must not use that
  fallback to mask an unsupported ForkTree lane.
- `packages/lix/src/live_state/mod.rs:4-20` still exposes columnar cache
  types. This is part of the forbidden current-state owner surface, not an
  acceptance of a second authority.

The positive ForkTree source seam is present at
`packages/lix/src/live_state/context.rs:719-732`: the operation's supplied
store is passed to `ForkTreeReadFacade`, which obtains the coherent branch
view, and `forktree_reader.rs:196-205` uses
`open_coherent_view_on_read`/`state_point` without a second `begin_read`.
The facade itself is the read owner at `packages/lix/src/forktree/view.rs:234-258`;
its branch method calls `open_coherent_view_on_read(&self.read)` and does not
open or refresh a read.
`forktree_reader.rs:55-99` decodes and filters rows before applying `limit`;
`forktree_reader.rs:288-291` preserves NULL versus deletion. The merge
ordering predicates are in `packages/lix/src/forktree/serving.rs:1263-1344`.

## Required successor behavior

A successor is source-green only when the `candidate` mode exits 0. It must:

1. route each supported direct SQL operation through the operation-owned
   coherent view/read and ForkTree state rows;
2. remove the tracked-head/direct entity/columnar owner and its caches from
   the SQL current-state path, without restoring a compatibility reader;
3. remove the generic/direct fallback from this path and reject unsupported
   derived/history/ambiguous requests before partial output;
4. retain global/branch overlay, ordered keys, entity-before-limit, NULL,
   tombstone, untracked, and decode/auth fail-closed semantics; and
5. keep the reader read-only: no write set, commit, selector, or epoch
   mutation is permitted in the reader source.

The oracle does not certify runtime behavior. A future successor must add its
own runtime/reopen/corruption evidence; this package only freezes the
discriminating source gate and the exact e166 RED control.
