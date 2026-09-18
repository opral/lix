---
type: patch
---

Partial replicas now query the authoritative server for complete `lix_commit` and `lix_change` inventories when their local data cannot prove completeness.

Queries that combine these global history tables with local replica data remain local and retain the existing partial-replica scope error, avoiding results that silently omit pending local edits.
