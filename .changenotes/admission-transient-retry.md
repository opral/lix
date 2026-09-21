---
type: patch
---

Retry read-only repository admission on transient network and gateway failures with bounded exponential backoff and per-request timeouts. Cancel stalled requests, keep authentication and compatibility failures terminal, and preserve verified offline admission when the network remains unavailable.
