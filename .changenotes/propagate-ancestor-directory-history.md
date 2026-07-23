---
type: patch
---

Fixed `lix_file_history` and `lix_directory_history` so changes to an ancestor
directory produce revisions for every affected descendant.

Ancestor renames, subtree moves, deletions, and restorations now revise the
composed `path` of nested files and directories without changing their stable
`id`. Each revision is reconstructed from the exact observed commit and its
direct-parent roots, preserving distinct sibling revisions in a commit DAG.

`lixcol_source_changes` now includes every same-commit ancestor descriptor that
shaped the descendant projection. Recursive deletion rows retain both the
descendant's direct tombstone and the tombstones of its deleted ancestors.

Exact `id` queries keep observed and direct-parent reconstruction scoped to the
selected file and its ancestor chain instead of rescanning unrelated filesystem
state.
