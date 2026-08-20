---
type: minor
---

Added durable local-first repository sync for `openLix({ storage, server: { mode: "sync" } })`.

Reads and writes stay local while Lix synchronizes commits with the server in
the background. Browser apps can use OPFS for durable offline work. Older
history and binary content download only when needed.
