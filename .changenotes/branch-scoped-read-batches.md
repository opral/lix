---
type: minor
---

Lix now supports `executeReadBatch({ branchId, statements })` for read-only SQL
statements that share one explicit branch and coherent storage snapshot.

The batch returns its branch head and opaque storage revision with ordered query
results, without reading or changing the workspace's active branch selector.
