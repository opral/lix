---
type: minor
---

Added `lix.observe()` for subscribing to SQL query results.

The Rust and JavaScript SDKs can now create observe streams that emit an initial result and re-run after Lix mutations, making it possible to build reactive views without manual polling.
