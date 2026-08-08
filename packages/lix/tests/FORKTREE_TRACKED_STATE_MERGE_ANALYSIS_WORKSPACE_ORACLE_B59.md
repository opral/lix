# ForkTree tracked-state merge-analysis workspace oracle (ac8 successor)

This is a TEST/REPORT-ONLY direct successor to exact `ac8a7bb1823954939662ad4a5255df9a4db2417f`. It contains no Lix production change, does not build or execute production, and keeps the production source line anchored to b59. The verifier binds the candidate HEAD and tree, requires exactly these three test/report paths in the base-to-candidate diff, scans the full `packages/lix/src` production workspace, and rejects any production diff.

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
`tracked_state/context.rs` call. It also scans any identifier containing
`merge`/`tracked_state` plus reader/cache/fallback/compat/retry/factory/
wrapper/store in either word order, classifying only the explicitly unrelated
checkpoint/undo/GC/service cohorts as allowlisted, so a renamed merge authority
cannot evade the exact-symbol checks.

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
- a genuinely disjoint merge-success fixture (source adds `c`, target adds
  `d`) with no conflicts;
- explicit missing, malformed, wrong-kind, and identity-substituted Root,
  Member, CommitCatalog, and FileOwner cases, plus malformed/missing/
  wrong-kind/identity-substituted Payload cases;
- one caller-owned `RetainedStorageRead` with an actual numeric
  `(reader_instance, view_id)` identity. Foreign events from a separate
  reader/view are rejected; self-labelled event strings are not trusted.

Missing objects never become an empty state and no unauthenticated absent row
is interpreted as a deletion. The model's future Memory/RocksDB/SlateDB
commands are intentionally omitted until the production facade is
compiler-green; this package is a source/model gate only.

## Reproduction

From this exact detached worktree:

```sh
bash packages/lix/tests/forktree_tracked_state_merge_analysis_workspace_oracle_b59.sh \
  "$PWD" <successor-head> <successor-tree>
```

The command performs only Git identity checks, static token checks, and a
workspace-wide source scan. It does not invoke Cargo, rustc, a storage
adapter, a runtime, or a benchmark. A future corrected production head must
run the same verifier with its exact head/tree and return `RESULT=GREEN` before
any runtime acceptance is attempted.
