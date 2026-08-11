---
type: patch
---

History queries that ask for a shallow depth or a small `LIMIT` no longer read the whole commit graph.

Depth ranges and row limits are now applied while walking history instead of afterwards, so a query such as `WHERE lixcol_depth = 0` or `LIMIT 10` costs the same on a repository with fifty thousand commits as on one with a thousand. Reading a full history is also faster, because each generation of commits is now fetched in one batched read.
