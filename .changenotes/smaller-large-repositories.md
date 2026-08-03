---
type: patch
---

Improved performance and reduced memory and disk use for large repositories.

History queries, checkpoints, working changes, binary and media storage, remote observations, and large inserts now do less redundant work. Million-row inserts complete more than 20% faster on both RocksDB and SlateDB.
