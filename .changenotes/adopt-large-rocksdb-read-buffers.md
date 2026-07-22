---
type: patch
---

RocksDB-backed Lix instances now reuse RocksDB's owned buffers for full values of at least 64 KiB instead of copying them into a second allocation.

Smaller values keep the existing copy path, which remains faster at that size.
