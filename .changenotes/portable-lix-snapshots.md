---
type: minor
---

Added deterministic, stream-first `.lixsnap` export and restore APIs for Rust and JavaScript.

Snapshots capture a complete logical Lix for reproduction, transfer, and recovery, verify integrity with BLAKE3, and restore atomically only into fresh storage.
