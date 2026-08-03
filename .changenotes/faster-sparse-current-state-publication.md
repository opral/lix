---
type: patch
---

Reduced sparse current-state publication latency and serving-index allocation.

Lix now stores contiguous scoped-range leaves as shared scope runs and encodes
immutable node fields through borrowed views, while retaining authenticated
point reads, structural sharing, and opaque physical-part payloads.
