# Live-state reader red oracle for `9f3c703e`

Status: TEST/REPORT-ONLY. This package deliberately contains no production
source, no Cargo wiring, no storage writes, and no runtime qualification.

## Frozen provenance

- Target head: `9f3c703e953440cde1d60b1511467c4337648c8f`
- Target tree: `51a0026c0c3eced6fdaa5e5ed4824111377f086c`
- Target parent: `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768`
- Production scope inspected: `packages/lix/src/live_state/{context,derived,forktree_reader,reader,types}.rs`
- This report's verifier is `verify_source_contract.sh`; its expected exit is nonzero
  on this old head because it proves the red conditions below.

## Causal source proof

1. `packages/lix/src/live_state/context.rs:684-692` sends every public
   `LiveStateStoreReader::scan_batch` request directly to
   `scan_forktree_branch`; no derived/history capability gate exists at this
   boundary.
2. `packages/lix/src/live_state/forktree_reader.rs:35-42` rejects
   `untracked = true`, nonempty constraints, and `rows = None`, but the same
   guard does not reject a registered derived schema, a history schema, or a
   mixed/complex schema list.
3. `forktree_reader.rs:50-57` treats `schema_keys` only as a post-read string
   filter, and `:94` returns `Ok(MaterializedLiveStateBatch::from_rows(output))`.
   With a derived/history key absent from the current ForkTree, the result is
   therefore a successful empty current-state batch rather than a typed
   unsupported/corruption result. A mixed list can likewise silently return
   only current rows or an empty batch.
4. `context.rs:897-918` recognizes derived identities, but lowers each one to
   `self.scan_batch(&request.row_scan_request(row))`; an empty scan is converted
   into an aligned `None` slot. This is the exact-batch form of the same silent
   empty-success bug.
5. `context.rs:941-947` calls `scan_scope`; its implementation at
   `:1526-1594` constructs `BranchHeadControlContext` readers. The exact batch
   then calls `self.tracked_head.reader(&self.store)` at `:1017-1027` and loads
   through `load_projected_live_batch_refs_for_domain`. Thus the candidate still
   acquires the superseded BranchHead-control/TrackedHead path instead of one
   retained authenticated CoherentView/ForkTree read. The relevant imports are
   also visible at `context.rs:7`, `:15`, and `:24-25`.

`derived.rs:514-536` already has the authoritative derived-provider registry
(`is_derived_schema`, `is_derived_only_request`, and the `lix_commit` /
`lix_commit_edge` / branch-ref descriptors). The missing guard is therefore a
reader-boundary decision, not permission to resurrect the provider or a second
durable owner.

## Discriminating model/public test

`reader_red_oracle_model.rs` is a dependency-free model of the public
`scan_batch`/`load_exact_batch` contract. It is intentionally not wired into
Cargo on this compiler-red head. If compiled as a standalone test, its
assertions are expected to fail against the old-9f3 behavior and to pass only
after the correction is implemented.

The model fixture has one current row under `app.row` and no current rows for
`lix_commit`, `lix_commit_edge`, or an untracked lane. It distinguishes
`EmptySuccess` from `TypedError`, so an empty result cannot satisfy a
fail-closed test accidentally.

Required cases:

| ID | Request | Required corrected result | Old-9f3 discriminator |
| --- | --- | --- | --- |
| S1 | ordinary `app.row`, one branch, tracked | current row | valid control; `Ok(row)` |
| S2 | `lix_commit` derived/history schema | typed unsupported or corruption before row materialization | **RED:** `Ok(empty)` is possible |
| S3 | `lix_commit_edge` history schema | typed unsupported or corruption | **RED:** `Ok(empty)` is possible |
| S4 | mixed `app.row` + `lix_commit` schema list | typed unsupported or corruption; no partial current rows | **RED:** post-read filtering can return empty/current subset |
| S5 | `untracked = true` | typed unsupported or corruption | existing explicit guard is a control and must remain |
| S6 | nonempty structured constraint / `rows = None` | typed unsupported or corruption | existing explicit guard is a control and must remain |
| S7 | exact batch containing `lix_commit` | typed unsupported/corruption, never aligned `None` from an empty scan | **RED:** `:905-918` converts empty to `None` |
| S8 | exact current row after a selector change | one coherent current-state owner/read; no legacy reader calls | **RED:** BranchHeadControl + `TrackedHead` path is acquired |
| S9 | malformed/missing/wrong-kind current root or selector | typed corruption/unavailable; no empty success or fallback | required fail-closed control |

The exact error code is intentionally not invented by this report. The
successor must return the repository's typed unsupported/corruption `LixError`
category (the old helper currently uses `CODE_INTERNAL_ERROR` for its generic
unsupported message) and must preserve the existing invalid-parameter and
corruption distinctions. The acceptance assertion is `Err`, not a particular
new public string.

## Old-9f3 red output

The source-only verifier was run against this exact head with no build. Its
captured output is `OLD_9F3_RED_OUTPUT.txt` and its expected process status is
`1`:

```text
RED-01 derived/history schema reaches current-state filtering and may return Ok(empty)
RED-02 mixed/complex schema request has no boundary rejection and may return partial/empty current rows
RED-03 load_exact_batch lowers derived rows through scan_batch and maps empty to None
RED-04 load_exact_batch transitively acquires BranchHeadControlContext through scan_scope
RED-05 load_exact_batch directly acquires TrackedHead through load_projected_live_batch_refs_for_domain
CONTROL-01 explicit untracked/constraint/rows=None guard is present
ORACLE_STATUS=RED
```

The verifier also checks the target head/tree and emits a hard failure if a
future invocation accidentally inspects a different commit.

## Exact correction behavior

The next production successor must, at the generic reader boundary:

- classify derived/history schemas, mixed/complex schema requests, and
  untracked requests before they can produce a current-state batch;
- return a typed unsupported/corruption error, never `Ok(empty)` and never a
  partial current-state result for an unsupported lane;
- retain the explicit untracked/constraint/row-none guards and fail closed on
  malformed, missing, or wrong-kind authenticated state;
- make supported current-state exact batches use one retained CoherentView and
  the authenticated ForkTree owner, with no `TrackedHead`,
  `BranchHeadControl`, legacy tracked-state reader, fallback, cache, or second
  durable authority;
- preserve exact request ordering/cardinality only for supported current rows;
  a missing required owner/root is a typed corruption/unavailable result, not a
  synthesized empty slot.

No format, selector, checkpoint, GC, compatibility, migration, or public API
change is authorized by this package. This is a discriminating oracle for the
reader correction only.

## Future qualification commands (not run here)

After a source successor exists, run the verifier first, then the model/public
test and focused owner tests on Memory, RocksDB, and SlateDB. The minimum
semantic names are:

```text
forktree_reader_rejects_derived_history_untracked_and_complex_scan_lanes
load_exact_batch_uses_one_coherent_view_and_no_legacy_reader
forktree_reader_rejects_missing_or_wrong_kind_current_root
```

The persistent command shape is:

```text
cargo test -p lix --lib live_state::context::tests::<focused_name> -- --exact --nocapture --test-threads=1
cargo test -p lix --lib live_state::forktree_reader::tests::<focused_name> -- --exact --nocapture --test-threads=1
```

Those commands are qualification requirements, not claims about this
compiler-red snapshot. No command in this package opens a legacy reader or
mutates a repository.
