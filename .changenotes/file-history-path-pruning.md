---
type: patch
---

`lix_file_history()` filtered by `path` now costs what the answer costs instead of scanning every file at every commit.

A `WHERE path = '...'` predicate is resolved to the files that could ever render that path and then routed like a `WHERE id = '...'` lookup, so the traversal no longer reconstructs the whole filesystem at each observed commit. Reading one file's history in a repository with five thousand files drops from over a second to a few milliseconds, and the cost stops growing with the number of files in the repository. Results are unchanged, including for paths a file has since been renamed away from.
