---
type: patch
---

Worker-host shutdown now cancels every active HTTP fetch across the client boundary before retiring local streams and handles, releasing retained browser fetch controllers and body readers even when no pull is pending.
