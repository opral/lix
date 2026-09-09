---
type: patch
---

Merge preview and merge now explicitly reject plugin-owned content conflicts when the file descriptor or ancestor path differs between the merge inputs. This unsupported combination could render file contents with the target's old format while selecting the source's new path and format metadata.

The operation returns a merge conflict before modifying the target, with a hint to merge the rename separately. This includes disjoint row edits whose combined file contents require materialization. Ordinary renames without conflicting file contents remain supported.
