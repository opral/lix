---
type: patch
---

Fix browser HTTP deadline classification across WASM, fetch adapters and worker RPC. Deadline exhaustion carries the standard TimeoutError reason and remains retryable network unavailability; explicit caller cancellation remains cancelled. Header and body failures retain the distinction without exposing abort reason text or changing existing deadlines and resource limits.
