---
type: patch
---

Fixed a 6.3x current-value read penalty on commits of exactly 512 rows.

A commit that filled one whole commit-delta segment inlined that segment into
its commit-state manifest, so every later point read of those rows fetched the
manifest and decoded all 512 rows to return one. A full segment is now written
as its own segment row; smaller single-segment commits still inline.
