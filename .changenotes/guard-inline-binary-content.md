---
type: patch
---

Improved 32–64 KiB binary file reads and repeat writes on RocksDB and SlateDB.

Lix now stores this common size band in one inline manifest and uses a key-only manifest probe to avoid repeated payload rewrites.
