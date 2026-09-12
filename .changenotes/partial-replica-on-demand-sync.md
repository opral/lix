---
type: minor
---

Open partial replicas with on-demand sync instead of downloading a full repository.

JavaScript clients opt in with storage and `server.mode: "partial_replica"`.
Opening transfers bounded metadata; SQL fetches missing native inputs and retains
them locally. Covered reads and prepared writes execute locally, including offline,
while background synchronization updates the working set and uploads commits.
Execute the expected SQL query to prefetch its inputs before an interaction; no
separate preparation API is required.

Server mode defaults to `"remote"` and rejects client storage. Existing synchronized
callers must explicitly opt into `"partial_replica"`; the former `"sync"` spelling
is not supported. Upgrade SDK and server together. Existing full replicas require
explicit conversion that preserves their source and pending work; see the partial
replica migration guide for supported formats and recovery boundaries.
