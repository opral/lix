---
type: patch
---

Fixed explicit transactions failing on SlateDB-backed servers when reading cached file and directory state.

SlateDB reads, writes, and flushes now also work when called from an executor without a Tokio runtime.
