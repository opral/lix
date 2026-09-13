---
type: patch
---

Keep legacy synced replica initialization and snapshot installation coherent while migration heartbeats commit. Preserve archived local edits as recovery exports without blocking conversion of the clean active replica to partial storage. Reject unsupported restoration before it can create pending work that prevents branch switching.
