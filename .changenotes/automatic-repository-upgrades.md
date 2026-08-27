---
type: minor
---

Opening a Lix now upgrades supported older repository formats automatically and reports typed progress to Rust and JavaScript applications.

`open_lix()` is the single repository lifecycle API. The explicit public migration and inspection APIs have been removed, and every opened handle exposes an immutable report describing initialization or migration performed during open.
