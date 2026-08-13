---
type: patch
---

Reading file history no longer reconstructs the plugin registry at every reachable commit.

`lix_file_history()` discovered which plugins existed by rebuilding the plugin registry once per commit in the repository's history, on every query, regardless of how few rows the query asked for. It now reads the registry's own change history once instead: the registry any commit sees was written by a registry change that history already contains, so one traversal yields the same answer as thousands of per-commit reconstructions. Registries are still rebuilt where they are actually compared — commits that changed the registry, and their direct parents.

On a repository with 2000 commits this cuts a file's history read from 59 ms to 34 ms, and a single-revision read from 56 ms to 39 ms. The saving grows with the number of commits in the repository.
