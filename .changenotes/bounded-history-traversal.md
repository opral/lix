---
type: patch
---

Commit-graph history traversal applies depth ranges and row limits while walking instead of afterwards.

A traversal that asks for a shallow depth or a small `LIMIT` no longer reads the whole commit graph, and reading a full history is faster because each generation of commits is fetched in one batched read.

This bounds the traversal itself. It does **not** bound every surface built on top of it: a row-shaping surface such as `lix_file_history()` still walks the reachable graph in full, because it can collapse or discard entries after the walk and so cannot pass a row limit down, and because composing a file's path needs ancestor records that may be older than the requested depth window. On `lix_file_history()` a one-row `WHERE lixcol_depth = 0` query therefore still costs more on a larger repository — measured at 2.3 ms against 20 commits and 56.2 ms against 2000. Bounding that surface is separate work.
