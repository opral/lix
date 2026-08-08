# Correction-I e26 reverse-dependency baseline

These counts are frozen from exact e26 production source
`e26d5d1984b8f2d8516842dded37e1232d2ea1a0` / tree
`7417ab5cbca4895c409b396a7633ff8b5a884d71`. They are source-token counts over
`packages/lix/src`, using fixed-string matches. The successor gate rejects any
increase and reports the signed delta. A new owner token or any increase is a
blocker; deletion reductions are allowed.

| family/token | e26 count |
|---|---:|
| `TrackedStateStoreReader` | 17 |
| `TrackedStateContext` | 143 |
| `TrackedStateScanRequest` | 40 |
| `TrackedStateReadColumns` | 15 |
| `TrackedStateWriter` | 6 |
| `TrackedHeadContext` | 34 |
| `TrackedWorkingDiff` | 4 |
| `TrackedWorkingDiffEpoch` | 2 |
| `WorkingDiffIndexCoverage` | 13 |
| `BranchHeadControl` | 86 |
| `BranchHeadControlContext` | 35 |
| `BranchHeadControlCache` | 13 |
| `stage_branch_head_control` | 32 |
| `branch_head_control_precondition` | 6 |
| `TrackedStateStoreReader` in checkpoint.rs | 3 |
| `TrackedStateContext` in working_diff.rs | 2 |
| `checkpoint_history_for_branch` | 1 |
| `checkpoint_history_from_head` | 7 |
| `checkpoint_history_from_checkpoint` | 6 |
| `is_checkpoint_commit` | 4 |
| `latest_checkpoint_for_branch` | 3 |
| `scan_state_rows_at_commit` | 11 |

Route-scoped direct compatibility/fallback/authority counts cover
`checkpoint.rs`, `sql2/providers/working_diff.rs`,
`sql2/providers/checkpoint.rs`, `sql2/providers/filesystem_working_diff.rs`,
`sql2/history_route.rs`, `sql2/context.rs`, and `sql2/mod.rs` production text
(tests excluded):

| token | e26 count |
|---|---:|
| `fallback` | 1 |
| `compat` | 0 |
| `compatibility` | 0 |
| `legacy` | 0 |
| `authority` | 1 |
| `authoritative` | 0 |
| `writer` | 3 |

The allowed authority direction is the existing `HistoryQuerySource` retained
`forktree_reader`; the gate does not cap its count because Correction I must
wire both consumers to that one identity. It does cap all superseded owner,
writer, chronology, fallback, and compatibility tokens above.
