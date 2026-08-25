---
type: minor
---

Replace heterogeneous working diffs with relation-specific commit comparisons and row-based diff commands.

`lix_diff(relation, from_commit_id, to_commit_id)` now exposes paired before/after relation columns, aggregated file changes, and underlying changed-row counts. Revert, apply, and partial checkpoints select `(relation, row_pk)`; commit parents and the repository root are available through `lix_commit.parent_commit_ids` and `lix_root_commit_id()`.
