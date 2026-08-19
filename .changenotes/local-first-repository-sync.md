---
type: minor
---

Added durable local-first repository sync for `openLix({ storage, server: { mode: "sync" } })`.

Reads and writes stay on the local repository while complete commits, branch refs, and BLAKE3-addressed binary chunks synchronize in the background. Browser replicas support OPFS persistence, realtime long polling, offline outboxes, lazy history and binary hydration, and authenticated reconnects through the same sync state machine as native replicas.
