---
type: patch
---

Large bound tracked-entity INSERT batches now stage certified typed columns directly and avoid repeated per-row transaction bookkeeping.

Million-row creates use less peak memory and complete more than 20% faster on both RocksDB and SlateDB while retaining the existing history page granularity.
