---
type: patch
---

Automatically upgrade supported older server repositories when opening them.

Clients await server migration inside the normal open call. The SDK reports
upgrade progress through `onProgress`, so applications can display status without
owning migration or retry logic. Concurrent opens
share one upgrade, which retains source storage and verifies preservation before
serving the repository. Failed or unsupported migrations preserve the existing
data and return an error.

Transport-only upgrades no longer reject repositories with compatible storage.
Ordinary admission remains independent of repository size.
