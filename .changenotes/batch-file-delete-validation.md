---
type: patch
---

File-heavy commits now validate file deletions in batches instead of scanning
committed state once per deleted file.
