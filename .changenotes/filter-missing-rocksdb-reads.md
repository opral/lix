---
type: patch
---

RocksDB-backed Lix instances now use whole-key Bloom filters to avoid unnecessary SST reads when a requested key is absent.
