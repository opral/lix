---
type: patch
---

Fixed missing values and directory paths in working diffs.

Default-range `lix_diff` queries now return the requested before and after values and metadata instead of silently returning null or failing to reconstruct directory paths.
