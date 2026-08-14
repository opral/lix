---
type: patch
---

Garbage collection now reclaims the storage a deleted or superseded branch leaves behind.

Every branch serves its current state from a generation of derived rows. When a branch was deleted, or when a lifecycle publication replaced its generation, those rows were never reclaimed and stayed on disk forever. Repository GC now retires them as part of its ordinary pass, so deleting a branch and collecting frees the space it actually used — measured at roughly 85% less residual storage after deleting a branch in a 10,000-row repository.
