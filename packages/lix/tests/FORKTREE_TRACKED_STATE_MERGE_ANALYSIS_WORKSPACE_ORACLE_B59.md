# ForkTree tracked-state merge-analysis workspace oracle (b59)

This is a TEST/REPORT-ONLY acceptance package. It contains no Lix production
change, does not build or execute production, and is bound to exact base
`b59e1f11a51153e0a787a81f0f25bf104d150aaf` (tree
`700fd04d21bc40c05425c9fc9e10d65c9e1eda24`). The source verifier is expected
to return RED on b59: the purpose of this second corrected package is to make
the remaining H1 blocker explicit and reproducible before a future production
successor is reviewed.

## Future GREEN contract

The production merge path must have no merge-owned `TrackedStateStoreReader`,
`tracked_state.reader`, `tracked_state_reader`, `with_opening_tracked_reader`, renamed callback/factory,
merge cache, payload fallback, compatibility reader, retry path, or alternate
merge authority. This is a workspace-wide requirement over
`packages/lix/src`, not merely a check of `session/merge`. The verifier prints
every occurrence of the old reader symbols and classifies it as either
`ALLOWLISTED_UNRELATED` or `FORBIDDEN_MERGE`.

The explicit unrelated retained cohorts are:

| Production path | Allowed duty |
|---|---|
| `checkpoint.rs` | checkpoint/undo/redo reader helpers |
| `session/checkpoint.rs`, `session/undo_redo.rs` | unrelated checkpoint and undo/redo reader factories |
| `gc.rs` | authenticated GC/replay observation |
| `init.rs` | initialization bootstrap reader |
| `sql2/providers/file_history.rs` | existing file-history provider |
| `sql2/providers/filesystem_working_diff.rs` | existing filesystem working-diff provider |
| `tracked_state/context.rs`, `tracked_state/diff.rs`, `tracked_state/mod.rs` | retained tracked-state service; `mod.rs` only its export is allowlisted |
| `transaction/context/cohort.rs` | unrelated transaction cohort reader |
| `transaction/context.rs` | unrelated transaction helpers only; the exact b59 merge callback/factory span at lines 7390–7413 is forbidden and checked separately |

No other production path is allowlisted. In particular, any old-reader hit in
`session/merge/**` is a merge blocker. The legitimate private
ForkTree-owned topology reader cache is not a merge authority and is not
treated as a global lexical `cache` violation; merge-owned cache/fallback
identifiers are independently forbidden. Two current b59 prose-only
exceptions are reported explicitly: the actor-cache comment at
`session/merge/branch.rs:724` and the user-facing retry hint at line 1891;
they do not authorize an implementation cache or retry.

`merge_payload_fallback_ids` and its sorting helper are stricter than the
general allowlist: the verifier scans the entire `packages/lix/src` tree and
rejects them wherever found, including the retained-looking
`tracked_state/context.rs` call. This prevents a renamed or relocated payload
fallback from surviving outside `session/merge`.

## Executable model obligations

`forktree_tracked_state_merge_analysis_workspace_model_b59.rs` is standalone
and has a `main` with assertions for:

- merge-base, base, source, and target identities plus exact generations;
- `Added`, `Updated`, `Deleted`, and explicit `Unchanged` classification;
- authenticated `NULL` and `Tombstone` rows, including a tombstone as a
  deletion rather than absence;
- convergent equal semantic values with distinct payload IDs producing no
  conflict (semantic equality uses authenticated payload digest and row
  metadata, not payload identity alone);
- plugin registry and file-owner handoff;
- missing/malformed/wrong-kind/identity-substituted CommitCatalog, Root,
  Member, Payload, and FileOwner authority;
- one caller-owned `RetainedStorageRead`, one `MergeOperation` borrow, and
  `assert_one_owner` over every topology/state/plugin event.

Missing objects never become an empty state and no unauthenticated absent row
is interpreted as a deletion. The model's future Memory/RocksDB/SlateDB
commands are intentionally omitted until the production facade is
compiler-green; this package is a source/model gate only.

## Reproduction

From this exact detached worktree:

```sh
bash packages/lix/tests/forktree_tracked_state_merge_analysis_workspace_oracle_b59.sh \
  "$PWD" b59e1f11a51153e0a787a81f0f25bf104d150aaf \
  700fd04d21bc40c05425c9fc9e10d65c9e1eda24
```

The command performs only Git identity checks, static token checks, and a
workspace-wide source scan. It does not invoke Cargo, rustc, a storage
adapter, a runtime, or a benchmark. A future corrected production head must
run the same verifier with its exact head/tree and return `RESULT=GREEN` before
any runtime acceptance is attempted.
