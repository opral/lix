---
type: patch
---

Remote session handshakes can now validate storage concurrently instead of serializing behind the session registry.

Session admission, eviction, and shutdown remain bounded and coordinated while slow storage reads no longer block unrelated handshakes.
