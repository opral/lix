---
type: patch
---

Removed a storage plane that recorded reclamation hints nothing ever read.

Every commit that superseded or deleted a large untracked JSON payload wrote a durable hint row into a dedicated storage space. No maintenance path ever consumed those rows, so they accumulated for the life of a repository. The hints and the space are gone; repositories now spend nothing on them.
