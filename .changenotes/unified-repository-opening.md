---
type: minor
---

Open supported older repositories through the normal Rust and JavaScript APIs.

`open_lix()` and `openLix()` now coordinate local and hosted upgrades, retain recoverable source data, and report upgrade progress. Native filesystem applications can upgrade repositories created with SDK 0.16.0 without a separate migration tool. Opening reports include local and authority upgrades; replica states that cannot be reconciled safely return a recovery error.
