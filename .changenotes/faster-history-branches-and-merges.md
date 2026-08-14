---
type: patch
---

History, branch, and merge operations now scale with the relevant changes instead of the total repository size.

History traversal skips unrelated work, file history prunes irrelevant paths and plugin states, and branch-head moves reuse existing state instead of copying the complete working set.
