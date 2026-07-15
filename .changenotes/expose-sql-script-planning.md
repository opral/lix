---
type: patch
---

Added SQL script planning to the Rust and Workerd SDKs.

Lix now parses single and multi-statement SQL into one atomic statement plan with request-wide parameter ranges.
