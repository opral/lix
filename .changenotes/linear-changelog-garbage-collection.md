---
type: patch
---

Improved Lix garbage-collection planning by scanning changelog records in
ordered batches instead of reopening the storage backend once per commit.

This keeps checkpoint cleanup practical for repositories with long automatic
commit histories, especially on remote LSM-backed storage.
