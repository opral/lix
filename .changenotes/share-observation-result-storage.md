---
type: patch
---

Reduced CPU time and memory copying when remote observations fan out large blob results.

Immutable query results now share their backing storage across observation subscribers and retained transport delta bases.
