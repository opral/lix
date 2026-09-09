---
type: patch
---

Large offline sync queues reuse a metadata-only upload plan across pages and
load only the commit payloads being sent. Ordinary edits remain local and join
the next upload wave; restores and server resets invalidate cached work.
Checkpoint acknowledgment metadata is retired after all branches converge.
