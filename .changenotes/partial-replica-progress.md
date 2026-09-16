---
type: minor
---

Partial replicas continue syncing current data when retained historical data is unavailable on the server.

Historical retention no longer blocks current-state updates, and background progress discovery no longer depends on optional prefetch. Use `sync_health()` in Rust or `syncHealth()` in JavaScript to distinguish stalled synchronization from successful local reads and compare observed and applied cursors.

This release requires sync protocol 17. Upgrade SDK and server together; protocol 16 peers are rejected. Existing repository data remains in format 81.
