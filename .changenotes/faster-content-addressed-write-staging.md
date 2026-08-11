---
type: patch
---

Faster writes for transactions that stage large amounts of tracked state.

Coalescing content-addressed puts now hashes only the content key instead of the key together with the whole payload, so staging a commit no longer runs every tracked-state tree chunk through the hasher twice.
