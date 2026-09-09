---
type: minor
---

Removed the JavaScript SDK's `@lix-js/sdk/server-protocol` helper export.

Use the SDK's remote connection API to access hosted repositories, or the documented HTTP protocol for custom clients. Remote SDK operations continue to use the shared Rust protocol implementation.
