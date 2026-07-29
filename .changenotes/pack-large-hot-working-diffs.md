---
type: patch
---

Reduced storage amplification from eager working-diff materialization by persisting batches of 64 or more dirty identities in bounded, checksummed segments while preserving the direct small-batch path.
