---
type: patch
---

Fixed rebuilding repositories after checkpoint garbage collection.

Rebuilding change tracking now validates deleted rows without requiring mutation bodies that garbage collection has already removed, while preserving checks against the repository's recorded state.
