---
type: minor
---

Lix can now open a workspace as a thin remote client.

Use `openLix({ server: { mode: "remote", url } })` to execute SQL and manage branches through the versioned Lix HTTP protocol without loading a local engine.
