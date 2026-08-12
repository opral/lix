---
type: patch
---

Deleting a directory that a file still points at is now rejected instead of silently accepted.

`lix_file_descriptor.directory_id` declares a foreign key to `lix_directory_descriptor.id`, but its
delete restriction could never fire. A file-descriptor row is scoped to its own file, a directory row
is scoped to no file at all, and the delete check only looked for referencing rows in the directory's
own scope — so it never saw a single file. Directory deletes that should have been refused went
through, leaving files pointing at a parent that no longer existed. The restriction now looks in
every file scope for this one pair, in both the tracked and untracked lanes.
