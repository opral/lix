---
type: patch
---

Concurrent browser sync no longer crashes an in-flight write when its transaction read expires.

Lix now returns the transient storage error to its bounded write retry path instead of panicking while opening transaction-scoped history readers.
