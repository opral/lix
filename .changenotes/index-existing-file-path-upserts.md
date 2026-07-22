---
type: patch
---

Improved existing `lix_file` path upserts by reusing the revisioned filesystem
path index instead of scanning and rebuilding the workspace filesystem state.
