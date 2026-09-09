---
type: patch
---

Fixed migrations failing when opening repositories connected to sync.

Existing server repositories and local replicas now retain their sync ownership while migrating, allowing previously failed opens to be retried without deleting repository data.
