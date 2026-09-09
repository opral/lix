---
type: patch
---

Fixed concurrent browser writes interrupting synchronization during reconnect.

Transient local read conflicts now retry without terminating live queries. Repository identity and account mismatches continue to stop synchronization.
