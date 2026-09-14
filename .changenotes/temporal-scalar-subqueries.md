---
type: minor
---

Resolve historical read and diff commit arguments with scalar subqueries.

`lix_as_of` and `lix_diff` can now look up commit IDs within the same statement,
including through parameters and common table expressions. This removes the
separate query previously needed to resolve a branch before reading or comparing
its state. The one-argument working diff continues to compare the active branch's
working baseline with its current head.
