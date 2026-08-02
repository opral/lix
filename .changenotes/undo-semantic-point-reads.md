---
type: patch
---

Reduced undo history classification to commit-delta schema bounds and one exact operation-marker read. Inspecting an undo target no longer scans its parent commit's full delta.
