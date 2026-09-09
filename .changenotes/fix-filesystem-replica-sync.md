---
type: patch
---

Fix synchronization of edits made on disk while a filesystem repository is connected to a server.

The filesystem watcher now shares the connected repository's write admission and authenticated account, and wakes its existing sync worker after an edit.
