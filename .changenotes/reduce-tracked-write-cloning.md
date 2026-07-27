---
type: patch
---

Large tracked writes now avoid cloning rows, branch IDs, absence-guard
identities, hot-head payloads, and encoded storage keys when the commit path
does not need owned copies.

Against the same RocksDB fixture on 10,000 rows, process-median transaction
latency improved from 107.561 ms to 87.848 ms for inserts (18.3%) and from
116.695 ms to 103.957 ms for updates (10.9%).
