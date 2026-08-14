---
type: patch
---

Fixed `LIX_TRANSACTION_CONFLICT` being raised for writers that do not actually conflict.

Saving a file while a resumable media upload was in progress — or running the four parts of one upload's in-flight window — could fail with "transaction snapshot is stale", because every content-addressed publication shared a single compare-and-set row with every garbage-collection sweep. Publications now fence only against sweeps, so independent writers commit independently; a sweep still cannot commit across a concurrent publication, and a publication still cannot commit across a concurrent sweep.
